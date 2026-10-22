package org.mqtt4j.hato.common.io;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/// A single FIFO whose regular and reserved quotas come from a possibly shared [Budget].
/// Active sends retain their quota until completed or ended, even after close. A consumer takes
/// one send at a time, or a batch of them that it writes together.
public final class SendQueue<T> {

    public enum OfferResult {
        ACCEPTED,
        FULL,
        CLOSED
    }

    public record Send<T>(long ticket, T value, int byteCount, boolean reserved) {
    }

    /// Capacity shared by queues whose active sends can outlive their connection.
    public static final class Budget {

        private final int[] maxCount;
        private final long[] maxBytes;
        private final int[] count = new int[2];
        private final long[] bytes = new long[2];

        public Budget(int regularCount, long regularBytes, int reservedCount, long reservedBytes) {
            if (regularCount < 0 || regularBytes < 0 || reservedCount < 0 || reservedBytes < 0) {
                throw new IllegalArgumentException("Send capacity must not be negative");
            }
            maxCount = new int[] {regularCount, reservedCount};
            maxBytes = new long[] {regularBytes, reservedBytes};
        }

        private synchronized boolean available(
                long byteCount,
                boolean reserved,
                int countHeadroom,
                long byteHeadroom
        ) {
            if (byteCount < 0 || countHeadroom < 0 || byteHeadroom < 0) {
                throw new IllegalArgumentException("Send size and headroom must not be negative");
            }
            int kind = reserved ? 1 : 0;
            return count[kind] < maxCount[kind] - countHeadroom
                    && byteHeadroom <= maxBytes[kind] - bytes[kind]
                    && byteCount <= maxBytes[kind] - bytes[kind] - byteHeadroom;
        }

        private synchronized boolean reserve(
                int byteCount,
                boolean reserved,
                int countHeadroom,
                long byteHeadroom
        ) {
            if (!available(byteCount, reserved, countHeadroom, byteHeadroom)) {
                return false;
            }
            int kind = reserved ? 1 : 0;
            count[kind]++;
            bytes[kind] += byteCount;
            return true;
        }

        private synchronized void release(int byteCount, boolean reserved) {
            int kind = reserved ? 1 : 0;
            count[kind]--;
            bytes[kind] -= byteCount;
        }

        /// Sends holding either quota, including active ones.
        public synchronized int count() {
            return count[0] + count[1];
        }

        /// Bytes of the sends counted by [#count()].
        public synchronized long bytes() {
            return bytes[0] + bytes[1];
        }
    }

    private final ArrayDeque<Send<T>> pending = new ArrayDeque<>();

    private final Budget budget;

    private boolean closed;

    // The sends that the consumer has taken and not completed, in queue order; empty when none.
    private final List<Send<T>> active = new ArrayList<>();

    public SendQueue(int regularCount, long regularBytes, int reservedCount, long reservedBytes) {
        this(new Budget(regularCount, regularBytes, reservedCount, reservedBytes));
    }

    public SendQueue(Budget budget) {
        this.budget = budget;
    }

    /// Checks current capacity without reserving it; [#offer] makes the final admission decision.
    public synchronized OfferResult checkCapacity(long byteCount, boolean reserved) {
        if (closed) {
            return OfferResult.CLOSED;
        }
        return budget.available(byteCount, reserved, 0, 0)
                ? OfferResult.ACCEPTED : OfferResult.FULL;
    }

    public synchronized OfferResult offer(long ticket, T value, int byteCount, boolean reserved) {
        return offer(ticket, value, byteCount, reserved, 0, 0);
    }

    /// Admits only if the selected shared quota retains the requested headroom afterwards.
    public synchronized OfferResult offer(
            long ticket,
            T value,
            int byteCount,
            boolean reserved,
            int countHeadroom,
            long byteHeadroom
    ) {
        if (closed) {
            return OfferResult.CLOSED;
        }
        if (!budget.reserve(byteCount, reserved, countHeadroom, byteHeadroom)) {
            return OfferResult.FULL;
        }
        pending.addLast(new Send<>(ticket, value, byteCount, reserved));
        return OfferResult.ACCEPTED;
    }

    /// Reports whether a pending send can start without taking it or changing its quota.
    public synchronized boolean hasReady() {
        return !closed && active.isEmpty() && !pending.isEmpty();
    }

    /// Starts the next send only when open and no other send is outstanding. Never waits.
    public synchronized Optional<Send<T>> poll() {
        if (!hasReady()) {
            return Optional.empty();
        }
        Send<T> send = pending.removeFirst();
        active.add(send);
        return Optional.of(send);
    }

    /// Starts the waiting sends as one batch, in order: the next one, and those after it while
    /// their bytes stay within `byteLimit` in all, so a send larger than the limit goes alone.
    /// Empty when closed, when nothing waits, or while another send is outstanding. Never waits.
    /// The batch completes as one, through the ticket of its last send.
    public synchronized List<Send<T>> pollBatch(long byteLimit) {
        if (!hasReady()) {
            return List.of();
        }
        long bytes = 0;
        do {
            Send<T> send = pending.removeFirst();
            active.add(send);
            bytes += send.byteCount();
        } while (!pending.isEmpty() && pending.getFirst().byteCount() <= byteLimit - bytes);
        return List.copyOf(active);
    }

    /// Releases the active sends, named by the ticket of the last one. Once closed, an unknown
    /// ticket is ignored instead of rejected.
    public synchronized void completed(long ticket) {
        if (active.isEmpty() || active.getLast().ticket() != ticket) {
            if (!closed) {
                throw new IllegalStateException("Unexpected send completion: " + ticket);
            }
            return;
        }
        active.forEach(this::release);
        active.clear();
    }

    /// Stops admission and discards waiting sends. The active sends stay with their consumer.
    public synchronized void close() {
        closed = true;
        pending.forEach(this::release);
        pending.clear();
    }

    /// Releases the active sends only after their consumer has stopped using the values.
    public synchronized void ended() {
        close();
        active.forEach(this::release);
        active.clear();
    }

    private void release(Send<T> send) {
        budget.release(send.byteCount(), send.reserved());
    }
}
