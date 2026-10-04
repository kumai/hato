package org.mqtt4j.mqtj.client.internal;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/// Actor-owned receive order. An identifier stays occupied until its PUBACK is written, so a
/// retransmission with DUP joins the occupying reception, while a reuse without DUP supersedes it.
/// Delivery tokens remain unique for the client's lifetime, including after identifier reuse.
final class AckSequencer {

    private final int capacity;

    private final LinkedHashMap<Long, Entry> pending = new LinkedHashMap<>();

    private final Map<Integer, Entry> identifiers = new HashMap<>();

    private final Map<Long, Entry> writing = new HashMap<>();

    AckSequencer(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("Acknowledgement capacity must be positive");
        }
        this.capacity = capacity;
    }

    /// Superseded receptions keep counting while their PUBACK is still being written.
    boolean add(long token, int packetId) {
        if (pending.size() + writing.size() >= capacity || contains(packetId)) {
            return false;
        }
        var entry = new Entry(token, packetId);
        pending.put(token, entry);
        identifiers.put(packetId, entry);
        return true;
    }

    boolean contains(int packetId) {
        return identifiers.containsKey(packetId);
    }

    /// Frees `packetId` for a new reception because the broker reused it without DUP, which it
    /// may do only after considering the earlier exchange acknowledged. An earlier reception
    /// whose PUBACK is not queued yet leaves receive order without one, and its token is
    /// returned. A queued PUBACK stays in flight; its later write no longer frees the identifier.
    OptionalLong supersede(int packetId) {
        Entry entry = identifiers.remove(packetId);
        if (entry == null || pending.remove(entry.token()) == null) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(entry.token());
    }

    void ready(long token) {
        Entry entry = pending.get(token);
        if (entry != null) {
            entry.ready = true;
        }
    }

    Optional<Entry> nextReady() {
        Map.Entry<Long, Entry> first = pending.firstEntry();
        if (first == null || !first.getValue().ready) {
            return Optional.empty();
        }
        return Optional.of(first.getValue());
    }

    /// Move the ready head only after send admission succeeds. FULL leaves it in place.
    void submitted(long ticket) {
        Entry entry = requireNonNull(pending.pollFirstEntry()).getValue();
        writing.put(ticket, entry);
    }

    int writingCount() {
        return writing.size();
    }

    void written(long ticket) {
        Entry entry = writing.remove(ticket);
        if (entry != null) {
            identifiers.remove(entry.packetId(), entry);
        }
    }

    void clear() {
        pending.clear();
        identifiers.clear();
        writing.clear();
    }

    static final class Entry {

        private final long token;
        private final int packetId;
        private boolean ready;

        Entry(long token, int packetId) {
            this.token = token;
            this.packetId = packetId;
        }

        long token() {
            return token;
        }

        int packetId() {
            return packetId;
        }
    }
}
