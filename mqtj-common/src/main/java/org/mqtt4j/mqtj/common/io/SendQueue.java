package org.mqtt4j.mqtj.common.io;

import java.util.ArrayDeque;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/// A single FIFO whose regular and reserved quotas come from a possibly shared [Budget].
/// Active sends retain their quota until completed or ended, even after close.
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
    }

    private final ArrayDeque<Send<T>> pending = new ArrayDeque<>();

    private final Budget budget;

    private boolean closed;

    private @Nullable Send<T> active;

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
        return !closed && active == null && !pending.isEmpty();
    }

    /// Starts the next send only when open and no other send is outstanding. Never waits.
    public synchronized Optional<Send<T>> poll() {
        if (!hasReady()) {
            return Optional.empty();
        }
        Send<T> send = pending.removeFirst();
        active = send;
        return Optional.of(send);
    }

    /// Releases the active send. Once closed, an unknown ticket is ignored instead of rejected.
    public synchronized void completed(long ticket) {
        Send<T> send = active;
        if (send == null || send.ticket() != ticket) {
            if (!closed) {
                throw new IllegalStateException("Unexpected send completion: " + ticket);
            }
            return;
        }
        active = null;
        release(send);
    }

    /// Stops admission and discards waiting sends. The active send stays with its consumer.
    public synchronized void close() {
        closed = true;
        pending.forEach(this::release);
        pending.clear();
    }

    /// Releases the active send only after its consumer has stopped using the value.
    public synchronized void ended() {
        close();
        Send<T> send = active;
        if (send != null) {
            active = null;
            release(send);
        }
    }

    private void release(Send<T> send) {
        budget.release(send.byteCount(), send.reserved());
    }
}
