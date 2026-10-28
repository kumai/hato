package org.mqtt4j.hato.client.internal;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.ReceiveTable.Entry;

import static java.util.Objects.requireNonNull;

/// Actor-owned acknowledgement order and pending writes for one connection. PUBACK and PUBREC
/// leave in receive order. PUBCOMP has no order requirement and is always ready, so it also
/// passes a head that is not ready: the broker's inflight window counts its exchange until
/// PUBCOMP arrives. A reception awaits at most one pending acknowledgement, since PUBREL
/// withdraws a queued PUBREC before its PUBCOMP joins, so the order runs through the receptions
/// themselves, and [#clear()] ends it with the connection.
final class AckSequencer {

    enum Kind {
        PUBACK,
        PUBREC,
        PUBCOMP
    }

    /// A write keeps its original kind even after PUBREL changes the reception's stage.
    record Acknowledgement(Kind kind, Entry entry) {
    }

    // The receptions with a pending acknowledgement, in receive order, linked through their
    // entries. A repeated pending response shares its existing place.
    private @Nullable Entry first;

    private @Nullable Entry last;

    // Pending PUBCOMPs in receive order, at most one per identifier: repeated PUBRELs share it,
    // and a later reception of the identifier needs its PUBREC to leave before its PUBREL,
    // which happens only after every earlier pending acknowledgement.
    private final LinkedHashMap<Integer, Entry> completions = new LinkedHashMap<>();

    // Admitted writes, the ticket, kind, and reception of each at the same index. Receptions
    // admits at most 16 at once, so a scan finds a ticket without boxing it or keeping an entry
    // for each write; the arrays grow if that window does. A finished write leaves its slot to
    // the last one, since the order of writes means nothing here.
    private long[] writingTickets = new long[16];

    private Kind[] writingKinds = new Kind[16];

    private @Nullable Entry[] writingEntries = new Entry[16];

    private int writingCount;

    void add(Entry entry, Kind kind) {
        if (entry.pending == kind) {
            return;
        }
        if (entry.pending != null) {
            throw new IllegalStateException(
                    "A reception awaiting " + entry.pending + " cannot also await " + kind
            );
        }
        entry.pending = kind;
        entry.before = last;
        if (last == null) {
            first = entry;
        } else {
            last.after = entry;
        }
        last = entry;
        if (kind == Kind.PUBCOMP) {
            completions.put(entry.packetId(), entry);
        }
    }

    /// A repeated PUBREL shares this PUBCOMP instead of owning another response.
    boolean completing(int packetId) {
        return completions.containsKey(packetId);
    }

    boolean contains(Entry entry, Kind kind) {
        if (entry.pending == kind) {
            return true;
        }
        for (int at = 0; at < writingCount; at++) {
            if (writingEntries[at] == entry && writingKinds[at] == kind) {
                return true;
            }
        }
        return false;
    }

    /// An admitted acknowledgement cannot be withdrawn while its write is pending.
    boolean remove(Entry entry, Kind kind) {
        if (entry.pending != kind) {
            return false;
        }
        unlink(entry);
        return true;
    }

    /// The reception whose acknowledgement may leave next; [#pendingKind(Entry)] tells which.
    Optional<Entry> nextReady() {
        Entry head = first;
        if (head != null && head.ready()) {
            return Optional.of(head);
        }
        return completions.isEmpty()
                ? Optional.empty()
                : Optional.of(completions.firstEntry().getValue());
    }

    /// The kind of the acknowledgement that `entry`, a reception from [#nextReady()], awaits.
    Kind pendingKind(Entry entry) {
        return requireNonNull(entry.pending);
    }

    /// Move the next ready acknowledgement only after send admission succeeds. FULL leaves it
    /// in place.
    void submitted(long ticket) {
        Entry entry = nextReady().orElseThrow();
        Kind kind = pendingKind(entry);
        unlink(entry);
        if (writingCount == writingTickets.length) {
            writingTickets = Arrays.copyOf(writingTickets, writingCount * 2);
            writingKinds = Arrays.copyOf(writingKinds, writingCount * 2);
            writingEntries = Arrays.copyOf(writingEntries, writingCount * 2);
        }
        writingTickets[writingCount] = ticket;
        writingKinds[writingCount] = kind;
        writingEntries[writingCount] = entry;
        writingCount++;
    }

    int writingCount() {
        return writingCount;
    }

    Optional<Acknowledgement> written(long ticket) {
        for (int at = 0; at < writingCount; at++) {
            if (writingTickets[at] == ticket) {
                var acknowledgement = new Acknowledgement(
                        writingKinds[at],
                        requireNonNull(writingEntries[at])
                );
                writingCount--;
                writingTickets[at] = writingTickets[writingCount];
                writingKinds[at] = writingKinds[writingCount];
                writingEntries[at] = writingEntries[writingCount];
                writingEntries[writingCount] = null;
                return Optional.of(acknowledgement);
            }
        }
        return Optional.empty();
    }

    /// Drops the connection's pending acknowledgements and writes, so that each reception can
    /// await one again on the next connection.
    void clear() {
        Entry entry = first;
        while (entry != null) {
            Entry after = entry.after;
            entry.pending = null;
            entry.before = null;
            entry.after = null;
            entry = after;
        }
        first = null;
        last = null;
        completions.clear();
        Arrays.fill(writingEntries, 0, writingCount, null);
        writingCount = 0;
    }

    // Takes `entry` out of the receive order, and out of the pending PUBCOMPs if it is one.
    private void unlink(Entry entry) {
        if (entry.pending == Kind.PUBCOMP) {
            completions.remove(entry.packetId(), entry);
        }
        Entry before = entry.before;
        Entry after = entry.after;
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
        entry.pending = null;
        entry.before = null;
        entry.after = null;
    }
}
