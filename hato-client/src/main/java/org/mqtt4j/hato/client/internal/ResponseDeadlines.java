package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// Actor-owned response waits for one connection. Every stage has the same timeout, so write
/// completion order is also deadline order. Answering never moves the timer: its next firing
/// either finds no waits, expires the head, or schedules the new head's remaining time.
final class ResponseDeadlines {

    enum Kind {
        PUBACK,
        PUBREC,
        PUBCOMP,
        SUBACK,
        UNSUBACK
    }

    // Nanoseconds; converting once also rejects a duration too long for nanosecond arithmetic.
    private final long timeout;

    // Admitted writes by ticket, each a wait of its own until its write completes, and the same
    // writes in admission order, so that an answer can find those of its response without
    // boxing a ticket or an identifier.
    private final LongMap<Wait> writing = new LongMap<>();

    private @Nullable Wait firstWriting;

    private @Nullable Wait lastWriting;

    // The awaited responses by kind and identifier, each the first completed write of its stage,
    // and the same waits in write completion order, which is deadline order.
    private final LongMap<Wait> waiting = new LongMap<>();

    private @Nullable Wait first;

    private @Nullable Wait last;

    private boolean armed;

    ResponseDeadlines(Duration timeout) {
        if (!timeout.isPositive()) {
            throw new IllegalArgumentException("Response timeout must be positive");
        }
        this.timeout = timeout.toNanos();
    }

    /// Register only an admitted write. Repeated writes of a stage share its first deadline.
    void add(long ticket, Kind kind, int packetId) {
        var wait = new Wait(kind, packetId);
        writing.put(ticket, wait);
        wait.before = lastWriting;
        if (lastWriting == null) {
            firstWriting = wait;
        } else {
            lastWriting.after = wait;
        }
        lastWriting = wait;
    }

    /// Returns a delay only when this completion needs to arm the single response timer.
    Optional<Duration> written(long ticket, long now) {
        return written(ticket, now, now);
    }

    Optional<Duration> written(long ticket, long writtenAt, long now) {
        // Every write of the connection comes here, acknowledgements among them, which await no
        // response.
        Wait wait = writing.isEmpty() ? null : writing.remove(ticket);
        if (wait == null) {
            return Optional.empty();
        }
        unlinkWriting(wait);
        long key = wait.key();
        if (wait.answered || waiting.containsKey(key)) {
            return Optional.empty();
        }
        wait.writtenAt = writtenAt;
        waiting.put(key, wait);
        wait.before = last;
        if (last == null) {
            first = wait;
        } else {
            last.after = wait;
        }
        last = wait;
        if (armed) {
            return Optional.empty();
        }
        armed = true;
        return Optional.of(Duration.ofNanos(Math.max(0, timeout - (now - writtenAt))));
    }

    void answered(Kind kind, int packetId) {
        Wait wait = waiting.isEmpty() ? null : waiting.remove(key(kind, packetId));
        if (wait != null) {
            Wait before = wait.before;
            Wait after = wait.after;
            if (before == null) {
                first = after;
            } else {
                before.after = after;
            }
            if (after == null) {
                last = before;
            } else {
                after.before = before;
            }
        }
        // Writes of this response still in flight start no wait when they complete. Pending writes
        // are bounded by the core's exchange and write windows. Index them by response only if
        // those windows grow enough to make this short scan significant.
        for (Wait pending = firstWriting; pending != null; pending = pending.after) {
            if (pending.kind == kind && pending.packetId == packetId) {
                pending.answered = true;
            }
        }
    }

    /// At a timer firing, empty means no response is pending and zero means the head expired.
    /// A positive delay rearms the timer for the current head, without touching later waits.
    Optional<Duration> expired(long now) {
        armed = false;
        Wait head = first;
        if (head == null) {
            return Optional.empty();
        }
        long remaining = timeout - (now - head.writtenAt);
        if (remaining <= 0) {
            return Optional.of(Duration.ZERO);
        }
        armed = true;
        return Optional.of(Duration.ofNanos(remaining));
    }

    /// How long the oldest awaited response has been awaited at `now` on the client's clock,
    /// since the write that it answers completed; empty if no response is awaited.
    Optional<Duration> oldestWait(long now) {
        Wait head = first;
        return head == null
                ? Optional.empty()
                : Optional.of(Duration.ofNanos(now - head.writtenAt));
    }

    Kind headKind() {
        return requireNonNull(first).kind;
    }

    private void unlinkWriting(Wait wait) {
        Wait before = wait.before;
        Wait after = wait.after;
        if (before == null) {
            firstWriting = after;
        } else {
            before.after = after;
        }
        if (after == null) {
            lastWriting = before;
        } else {
            after.before = before;
        }
        wait.before = null;
        wait.after = null;
    }

    private static long key(Kind kind, int packetId) {
        return (long) kind.ordinal() << 16 | packetId;
    }

    /// One admitted write and, once it completes, the wait for its response. It lies in the
    /// writing order until then and in the waiting order after.
    private static final class Wait {

        final Kind kind;
        final int packetId;
        long writtenAt;
        // An answer arrived before the write completed.
        boolean answered;
        @Nullable Wait before;
        @Nullable Wait after;

        Wait(Kind kind, int packetId) {
            this.kind = kind;
            this.packetId = packetId;
        }

        long key() {
            return ResponseDeadlines.key(kind, packetId);
        }
    }
}
