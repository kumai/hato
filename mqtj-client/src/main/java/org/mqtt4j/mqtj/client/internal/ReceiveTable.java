package org.mqtt4j.mqtj.client.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/// Actor-owned reception state. Tokens remain unique for the client's lifetime, while packet
/// identifiers can be reused after acknowledgement or superseded by a publication without DUP.
final class ReceiveTable {

    private final int capacity;

    private final Map<Long, Entry> entries = new HashMap<>();

    private final Map<Integer, Entry> identifiers = new HashMap<>();

    ReceiveTable(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("Acknowledgement capacity must be positive");
        }
        this.capacity = capacity;
    }

    /// Superseded receptions keep counting until their pending write is removed.
    Optional<Entry> add(long token, int packetId) {
        if (entries.size() >= capacity || identifiers.containsKey(packetId)) {
            return Optional.empty();
        }
        var entry = new Entry(token, packetId);
        entries.put(token, entry);
        identifiers.put(packetId, entry);
        return Optional.of(entry);
    }

    Optional<Entry> find(int packetId) {
        return Optional.ofNullable(identifiers.get(packetId));
    }

    void ready(long token) {
        ready(token, true);
    }

    void ready(long token, boolean ready) {
        Entry entry = entries.get(token);
        if (entry != null) {
            entry.ready = ready;
        }
    }

    List<Entry> entries() {
        return List.copyOf(entries.values());
    }

    /// Frees the identifier without releasing capacity held by an admitted acknowledgement.
    Optional<Entry> supersede(int packetId) {
        return Optional.ofNullable(identifiers.remove(packetId));
    }

    void remove(Entry entry) {
        entries.remove(entry.token(), entry);
        identifiers.remove(entry.packetId(), entry);
    }

    void clear() {
        entries.clear();
        identifiers.clear();
    }

    static final class Entry {

        private final long token;
        private final int packetId;
        private boolean ready;
        private @Nullable SendReceipt receipt;

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

        boolean ready() {
            return ready;
        }

        @Nullable SendReceipt receipt() {
            return receipt;
        }

        void submitted(SendReceipt receipt) {
            this.receipt = receipt;
        }
    }
}
