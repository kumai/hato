package org.mqtt4j.hato.client.internal;

import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.client.internal.AckSequencer.Kind;

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

    // By token and by packet identifier, without boxing either on each reception.
    private final LongMap<Entry> entries = new LongMap<>();

    private final LongMap<Entry> identifiers = new LongMap<>();

    ReceiveTable(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("Acknowledgement capacity must be positive");
        }
        this.capacity = capacity;
    }

    /// Superseded receptions keep counting until their pending write is removed. `now` is the
    /// time of the reception on the client's clock.
    Optional<Entry> add(long token, int packetId, Qos qos, long now) {
        if (entries.size() >= capacity || identifiers.containsKey(packetId)) {
            return Optional.empty();
        }
        var entry = new Entry(token, packetId, qos, now);
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
        return entries.values();
    }

    /// How many more receptions the table can hold.
    int room() {
        return capacity - entries.size();
    }

    int capacity() {
        return capacity;
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
        // When the reception arrived, and when it began to await PUBREL, on the client's clock.
        private final long receivedAt;
        private long awaitingReleaseSince;
        private Stage stage = Stage.RECEIVING;
        private boolean ready;
        private @Nullable SendReceipt receipt;
        // AckSequencer's: the acknowledgement that this reception awaits on the connection, and
        // the receptions before and after it in the acknowledgement order.
        @Nullable Kind pending;
        @Nullable Entry before;
        @Nullable Entry after;

        Entry(long token, int packetId, Qos qos, long receivedAt) {
            this.token = token;
            this.packetId = packetId;
            this.qos = qos;
            this.receivedAt = receivedAt;
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

        long receivedAt() {
            return receivedAt;
        }

        long awaitingReleaseSince() {
            return awaitingReleaseSince;
        }

        void submitted(SendReceipt receipt, long now) {
            observeReceipt(now);
            this.receipt = receipt;
        }

        /// Transport can accept PUBREC before the actor sees its write completion. Returns the
        /// single read of acceptance, so a caller that also judges it cannot see a later value.
        boolean observeReceipt(long now) {
            boolean accepted = receipt != null && receipt.accepted();
            if (accepted) {
                pubrecAccepted(now);
            }
            return accepted;
        }

        /// `now` is when the actor learned that PUBREC was written, on the client's clock.
        void pubrecAccepted(long now) {
            if (qos == Qos.EXACTLY_ONCE && stage == Stage.RECEIVING) {
                stage = Stage.WAITING_PUBREL;
                ready = true;
                awaitingReleaseSince = now;
            }
        }
    }
}
