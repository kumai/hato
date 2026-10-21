package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

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

    private final Map<Long, Response> writing = new HashMap<>();

    private final LinkedHashMap<Response, Long> waiting = new LinkedHashMap<>();

    private boolean armed;

    ResponseDeadlines(Duration timeout) {
        if (!timeout.isPositive()) {
            throw new IllegalArgumentException("Response timeout must be positive");
        }
        this.timeout = timeout.toNanos();
    }

    /// Register only an admitted write. Repeated writes of a stage share its first deadline.
    void add(long ticket, Kind kind, int packetId) {
        writing.put(ticket, new Response(kind, packetId));
    }

    /// Returns a delay only when this completion needs to arm the single response timer.
    Optional<Duration> written(long ticket, long now) {
        return written(ticket, now, now);
    }

    Optional<Duration> written(long ticket, long writtenAt, long now) {
        Response response = writing.remove(ticket);
        if (response == null || waiting.putIfAbsent(response, writtenAt) != null || armed) {
            return Optional.empty();
        }
        armed = true;
        return Optional.of(Duration.ofNanos(Math.max(0, timeout - (now - writtenAt))));
    }

    void answered(Kind kind, int packetId) {
        var response = new Response(kind, packetId);
        waiting.remove(response);
        // Pending writes are bounded by the core's exchange and write windows. Index tickets
        // by response only if those windows grow enough to make this short scan significant.
        writing.values().removeIf(response::equals);
    }

    /// At a timer firing, empty means no response is pending and zero means the head expired.
    /// A positive delay rearms the timer for the current head, without touching later waits.
    Optional<Duration> expired(long now) {
        armed = false;
        Map.Entry<Response, Long> head = waiting.firstEntry();
        if (head == null) {
            return Optional.empty();
        }
        long remaining = timeout - (now - head.getValue());
        if (remaining <= 0) {
            return Optional.of(Duration.ZERO);
        }
        armed = true;
        return Optional.of(Duration.ofNanos(remaining));
    }

    /// How long the oldest awaited response has been awaited at `now` on the client's clock,
    /// since the write that it answers completed; empty if no response is awaited.
    Optional<Duration> oldestWait(long now) {
        Map.Entry<Response, Long> head = waiting.firstEntry();
        return head == null
                ? Optional.empty()
                : Optional.of(Duration.ofNanos(now - head.getValue()));
    }

    Kind headKind() {
        return requireNonNull(waiting.firstEntry()).getKey().kind();
    }

    private record Response(Kind kind, int packetId) {
    }
}
