package org.mqtt4j.hato.client.internal;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.mqtt4j.hato.client.internal.ReceiveTable.Entry;

import static java.util.Objects.requireNonNull;

/// Actor-owned acknowledgement order and pending writes for one connection.
final class AckSequencer {

    private final LinkedHashMap<Long, Entry> pending = new LinkedHashMap<>();

    private final Map<Long, Entry> writing = new HashMap<>();

    void add(Entry entry) {
        pending.put(entry.token(), entry);
    }

    boolean contains(Entry entry) {
        // The core bounds outstanding writes to 16; index tokens here if that window grows.
        return pending.get(entry.token()) == entry || writing.containsValue(entry);
    }

    /// An admitted acknowledgement cannot be withdrawn while its write is pending.
    boolean remove(Entry entry) {
        return pending.remove(entry.token(), entry);
    }

    Optional<Entry> nextReady() {
        Map.Entry<Long, Entry> first = pending.firstEntry();
        if (first == null || !first.getValue().ready()) {
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

    Optional<Entry> written(long ticket) {
        return Optional.ofNullable(writing.remove(ticket));
    }

    void clear() {
        pending.clear();
        writing.clear();
    }
}
