package org.mqtt4j.hato.client.internal;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;

import org.mqtt4j.hato.client.internal.ReceiveTable.Entry;

/// Actor-owned acknowledgement order and pending writes for one connection. PUBACK and PUBREC
/// leave in receive order. PUBCOMP has no order requirement and is always ready, so it also
/// passes a head that is not ready: the broker's inflight window counts its exchange until
/// PUBCOMP arrives.
final class AckSequencer {

    enum Kind {
        PUBACK,
        PUBREC,
        PUBCOMP
    }

    /// A write keeps its original kind even after PUBREL changes the reception's stage.
    record Acknowledgement(Kind kind, Entry entry) {
    }

    // Repeated pending responses share their existing place in the receive order.
    private final LinkedHashSet<Acknowledgement> pending = new LinkedHashSet<>();

    // Pending PUBCOMPs in receive order, at most one per identifier: repeated PUBRELs share it,
    // and a later reception of the identifier needs its PUBREC to leave before its PUBREL,
    // which happens only after every earlier pending acknowledgement.
    private final LinkedHashMap<Integer, Acknowledgement> completions = new LinkedHashMap<>();

    private final Map<Long, Acknowledgement> writing = new HashMap<>();

    void add(Entry entry, Kind kind) {
        var acknowledgement = new Acknowledgement(kind, entry);
        pending.add(acknowledgement);
        if (kind == Kind.PUBCOMP) {
            completions.put(entry.packetId(), acknowledgement);
        }
    }

    /// A repeated PUBREL shares this PUBCOMP instead of owning another response.
    boolean completing(int packetId) {
        return completions.containsKey(packetId);
    }

    boolean contains(Entry entry, Kind kind) {
        // The core bounds outstanding writes to 16; index tokens here if that window grows.
        var acknowledgement = new Acknowledgement(kind, entry);
        return pending.contains(acknowledgement) || writing.containsValue(acknowledgement);
    }

    /// An admitted acknowledgement cannot be withdrawn while its write is pending.
    boolean remove(Entry entry, Kind kind) {
        return pending.remove(new Acknowledgement(kind, entry));
    }

    Optional<Acknowledgement> nextReady() {
        if (!pending.isEmpty() && pending.getFirst().entry().ready()) {
            return Optional.of(pending.getFirst());
        }
        return completions.isEmpty()
                ? Optional.empty()
                : Optional.of(completions.firstEntry().getValue());
    }

    /// Move the next ready acknowledgement only after send admission succeeds. FULL leaves it
    /// in place.
    void submitted(long ticket) {
        Acknowledgement acknowledgement = nextReady().orElseThrow();
        pending.remove(acknowledgement);
        completions.remove(acknowledgement.entry().packetId(), acknowledgement);
        writing.put(ticket, acknowledgement);
    }

    int writingCount() {
        return writing.size();
    }

    Optional<Acknowledgement> written(long ticket) {
        return Optional.ofNullable(writing.remove(ticket));
    }

    void clear() {
        pending.clear();
        completions.clear();
        writing.clear();
    }
}
