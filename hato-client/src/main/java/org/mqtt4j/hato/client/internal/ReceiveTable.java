package org.mqtt4j.hato.client.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Qos;

/// Actor-owned reception state. Tokens remain unique for the client's lifetime, while packet
/// identifiers can be reused after PUBACK or PUBREL. Capacity also counts completed receptions
/// whose PUBCOMP is still waiting to be written, and superseded pending acknowledgements.
final class ReceiveTable {

    enum Stage {
        RECEIVING,
        WAITING_PUBREL,
        COMPLETING
    }

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
    Optional<Entry> add(long token, int packetId, Qos qos) {
        if (entries.size() >= capacity || identifiers.containsKey(packetId)) {
            return Optional.empty();
        }
        var entry = new Entry(token, packetId, qos);
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
        if (entry != null && entry.stage == Stage.RECEIVING) {
            entry.ready = ready;
        }
    }

    List<Entry> entries() {
        return List.copyOf(entries.values());
    }

    /// How many more receptions the table can hold.
    int room() {
        return capacity - entries.size();
    }

    /// Frees the identifier without releasing capacity held by an admitted acknowledgement.
    Optional<Entry> supersede(int packetId) {
        return Optional.ofNullable(identifiers.remove(packetId));
    }

    /// PUBREL returns the identifier; its response still owns the reception's capacity.
    void complete(Entry entry) {
        identifiers.remove(entry.packetId(), entry);
        entry.stage = Stage.COMPLETING;
        entry.ready = true;
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
        private final Qos qos;
        private Stage stage = Stage.RECEIVING;
        private boolean ready;
        private @Nullable SendReceipt receipt;

        Entry(long token, int packetId, Qos qos) {
            this.token = token;
            this.packetId = packetId;
            this.qos = qos;
        }

        long token() {
            return token;
        }

        int packetId() {
            return packetId;
        }

        Qos qos() {
            return qos;
        }

        Stage stage() {
            return stage;
        }

        boolean ready() {
            return ready;
        }

        @Nullable SendReceipt receipt() {
            return receipt;
        }

        void submitted(SendReceipt receipt) {
            observeReceipt();
            this.receipt = receipt;
        }

        /// Transport can accept PUBREC before the actor sees its write completion. Returns the
        /// single read of acceptance, so a caller that also judges it cannot see a later value.
        boolean observeReceipt() {
            boolean accepted = receipt != null && receipt.accepted();
            if (accepted) {
                pubrecAccepted();
            }
            return accepted;
        }

        void pubrecAccepted() {
            if (qos == Qos.EXACTLY_ONCE && stage == Stage.RECEIVING) {
                stage = Stage.WAITING_PUBREL;
                ready = true;
            }
        }
    }
}
