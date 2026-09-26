package org.mqtt4j.mqtj.common.io;

import java.util.ArrayDeque;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/// A single FIFO with independent regular/reserved quotas. Active writes retain their quota.
public final class SendQueue {

    public enum OfferResult {
        ACCEPTED,
        FULL,
        CLOSED
    }

    public record Send(long ticket, byte[] bytes, boolean reserved) {
    }

    private final ArrayDeque<Send> pending = new ArrayDeque<>();
    private final int[] maxCount;
    private final long[] maxBytes;
    private final int[] count = new int[2];
    private final long[] bytes = new long[2];

    private boolean closed;

    private @Nullable Send active;

    public SendQueue(int regularCount, long regularBytes, int reservedCount, long reservedBytes) {
        maxCount = new int[] {regularCount, reservedCount};
        maxBytes = new long[] {regularBytes, reservedBytes};
    }

    public synchronized OfferResult offer(long ticket, byte[] value, boolean reserved) {
        if (closed) {
            return OfferResult.CLOSED;
        }
        int kind = reserved ? 1 : 0;
        if (count[kind] == maxCount[kind]
                || value.length > maxBytes[kind] - bytes[kind]) {
            return OfferResult.FULL;
        }
        count[kind]++;
        bytes[kind] += value.length;
        pending.addLast(new Send(ticket, value, reserved));
        return OfferResult.ACCEPTED;
    }

    /// Starts the next write only when open and no other write is outstanding. Never waits.
    public synchronized Optional<Send> poll() {
        if (closed || active != null) {
            return Optional.empty();
        }
        active = pending.pollFirst();
        return Optional.ofNullable(active);
    }

    public synchronized void completed(long ticket) {
        Send send = active;
        if (send == null || send.ticket() != ticket) {
            if (!closed) {
                throw new IllegalStateException("Unexpected send completion: " + ticket);
            }
            return;
        }
        active = null;
        release(send);
    }

    /// Stops admission and discards waiting sends. The active write stays with its writer.
    public synchronized void close() {
        closed = true;
        pending.forEach(this::release);
        pending.clear();
    }

    /// Releases the active write only after its writer has stopped using the buffer.
    public synchronized void ended() {
        close();
        Send send = active;
        if (send != null) {
            active = null;
            release(send);
        }
    }

    private void release(Send send) {
        int kind = send.reserved() ? 1 : 0;
        count[kind]--;
        bytes[kind] -= send.bytes().length;
    }
}
