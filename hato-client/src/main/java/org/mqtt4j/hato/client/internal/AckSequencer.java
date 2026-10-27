package org.mqtt4j.hato.client.internal;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.ReceiveTable.Entry;

import static java.util.Objects.requireNonNull;

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

    // Admitted writes, the ticket of each at the same index as its acknowledgement. Receptions
    // admits at most 16 at once, so a scan finds a ticket without boxing it or keeping an entry
    // for each write; the arrays grow if that window does. A finished write leaves its slot to
    // the last one, since the order of writes means nothing here.
    private long[] writingTickets = new long[16];

    private @Nullable Acknowledgement[] writingAcknowledgements = new Acknowledgement[16];

    private int writingCount;

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
        var acknowledgement = new Acknowledgement(kind, entry);
        if (pending.contains(acknowledgement)) {
            return true;
        }
        for (int at = 0; at < writingCount; at++) {
            if (acknowledgement.equals(writingAcknowledgements[at])) {
                return true;
            }
        }
        return false;
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
        // Without a pending PUBCOMP, the lookup would only box the identifier.
        if (!completions.isEmpty()) {
            completions.remove(acknowledgement.entry().packetId(), acknowledgement);
        }
        if (writingCount == writingTickets.length) {
            writingTickets = Arrays.copyOf(writingTickets, writingCount * 2);
            writingAcknowledgements = Arrays.copyOf(writingAcknowledgements, writingCount * 2);
        }
        writingTickets[writingCount] = ticket;
        writingAcknowledgements[writingCount] = acknowledgement;
        writingCount++;
    }

    int writingCount() {
        return writingCount;
    }

    Optional<Acknowledgement> written(long ticket) {
        for (int at = 0; at < writingCount; at++) {
            if (writingTickets[at] == ticket) {
                Acknowledgement acknowledgement = requireNonNull(writingAcknowledgements[at]);
                writingCount--;
                writingTickets[at] = writingTickets[writingCount];
                writingAcknowledgements[at] = writingAcknowledgements[writingCount];
                writingAcknowledgements[writingCount] = null;
                return Optional.of(acknowledgement);
            }
        }
        return Optional.empty();
    }
}
