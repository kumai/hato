package org.mqtt4j.hato.client.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// A map from `long` keys to values that boxes no key and keeps no entry object for each
/// mapping, for the tables that the client consults for every message, such as its receptions
/// by token and by packet identifier. Values are never null. Not thread-safe.
///
/// The keys sit in one array and the values at the same index in another, placed by open
/// addressing with linear probing. Removal moves later keys of the probe run back into the hole,
/// so no deleted markers build up. Each map scrambles its keys with a random odd multiplier: a
/// peer that chooses the keys, as a broker chooses packet identifiers, cannot line them up into
/// one long run. The arrays double once half full, and an empty map holds none.
final class LongMap<V> {

    private static final int FIRST_CAPACITY = 16;

    private final long multiplier;

    private long[] keys = new long[0];

    // A value at the index of its key, or null where the slot is free.
    private @Nullable Object[] values = new Object[0];

    // The bits of a scrambled key that pick its home slot.
    private int shift;

    private int size;

    LongMap() {
        this(ThreadLocalRandom.current().nextLong() | 1);
    }

    /// `multiplier`, odd, scrambles the keys; a test can choose one that makes keys collide.
    LongMap(long multiplier) {
        if ((multiplier & 1) == 0) {
            throw new IllegalArgumentException("The multiplier must be odd: " + multiplier);
        }
        this.multiplier = multiplier;
    }

    int size() {
        return size;
    }

    boolean isEmpty() {
        return size == 0;
    }

    boolean containsKey(long key) {
        return slot(key) >= 0;
    }

    @Nullable V get(long key) {
        int slot = slot(key);
        return slot < 0 ? null : valueAt(slot);
    }

    /// Maps `key` to `value` and returns the value that it replaced, if any.
    @Nullable V put(long key, V value) {
        if (keys.length == 0) {
            resize(FIRST_CAPACITY);
        }
        int mask = keys.length - 1;
        int at = home(key);
        while (values[at] != null) {
            if (keys[at] == key) {
                V replaced = valueAt(at);
                values[at] = value;
                return replaced;
            }
            at = (at + 1) & mask;
        }
        keys[at] = key;
        values[at] = value;
        size++;
        if (size * 2 > keys.length) {
            resize(keys.length * 2);
        }
        return null;
    }

    /// Removes the mapping of `key` and returns its value, if any.
    @Nullable V remove(long key) {
        int slot = slot(key);
        if (slot < 0) {
            return null;
        }
        V removed = valueAt(slot);
        free(slot);
        return removed;
    }

    /// Removes the mapping of `key` only while it maps to a value equal to `value`, as
    /// [java.util.Map#remove(Object, Object)] does.
    boolean remove(long key, V value) {
        int slot = slot(key);
        if (slot < 0 || !value.equals(values[slot])) {
            return false;
        }
        free(slot);
        return true;
    }

    void clear() {
        Arrays.fill(values, null);
        size = 0;
    }

    /// A copy of the values, in no particular order, which later changes of the map leave as it
    /// is.
    List<V> values() {
        var copy = new ArrayList<V>(size);
        for (int at = 0; at < values.length; at++) {
            if (values[at] != null) {
                copy.add(valueAt(at));
            }
        }
        return copy;
    }

    // The slot that holds `key`, or -1 if none does.
    private int slot(long key) {
        if (size == 0) {
            return -1;
        }
        int mask = keys.length - 1;
        for (int at = home(key); values[at] != null; at = (at + 1) & mask) {
            if (keys[at] == key) {
                return at;
            }
        }
        return -1;
    }

    // Where the probe for `key` starts: the top bits of the scrambled key.
    private int home(long key) {
        return (int) ((key * multiplier) >>> shift);
    }

    // Frees `hole` and moves back into it each later key of the run whose home does not lie
    // after the hole, so that every key stays reachable from its home without a gap.
    private void free(int hole) {
        int mask = keys.length - 1;
        int at = hole;
        while (true) {
            at = (at + 1) & mask;
            if (values[at] == null) {
                break;
            }
            int home = home(keys[at]);
            if (((at - home) & mask) >= ((at - hole) & mask)) {
                keys[hole] = keys[at];
                values[hole] = values[at];
                hole = at;
            }
        }
        values[hole] = null;
        size--;
    }

    private void resize(int capacity) {
        long[] oldKeys = keys;
        @Nullable Object[] oldValues = values;
        keys = new long[capacity];
        values = new Object[capacity];
        shift = Long.SIZE - Integer.numberOfTrailingZeros(capacity);
        int mask = capacity - 1;
        for (int from = 0; from < oldValues.length; from++) {
            if (oldValues[from] != null) {
                int at = home(oldKeys[from]);
                while (values[at] != null) {
                    at = (at + 1) & mask;
                }
                keys[at] = oldKeys[from];
                values[at] = oldValues[from];
            }
        }
    }

    @SuppressWarnings("unchecked") // Only put() stores values, and only values of type V.
    private V valueAt(int slot) {
        return (V) requireNonNull(values[slot]);
    }
}
