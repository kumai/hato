package org.mqtt4j.hato.common.protocol;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.common.value.Topic;

/// Remembers the topic names that a connection's PUBLISH packets carried, so that decoding a
/// name that comes again takes the topic decoded from the same bytes before, instead of
/// decoding and checking the name again. Most streams repeat a few topics. One decoding thread
/// at a time owns an instance; pass it to [PacketCodec#decode(byte[], int, int, TopicCache)].
///
/// A deliberate simplification: once it holds [#LIMIT] names it forgets them all and starts
/// again, so a stream of ever new names, such as one per device, pays a lookup and an insertion
/// per PUBLISH and never hits. An eviction policy that keeps the most used names would serve a
/// stream that mixes a few hot names into many cold ones.
public final class TopicCache {

    // The slots of an open-addressing table that holds at most LIMIT names, half of them, so
    // that a probe finds a name or a free slot within a few steps. A power of two.
    private static final int SLOTS = 64;

    private static final int LIMIT = SLOTS / 2;

    private final @Nullable Entry[] entries = new Entry[SLOTS];

    private int count;

    /// The topic whose name lies, after its two-byte length, at the buffer's position, when this
    /// cache holds a name of the same bytes; the buffer then moves past the name. Otherwise null,
    /// and the buffer stays where it was. Null rather than an empty Optional because this runs
    /// for every received PUBLISH.
    @Nullable Topic find(ByteBuffer input) {
        int position = input.position();
        if (!input.hasArray() || input.remaining() < 2) {
            return null;
        }
        byte[] array = input.array();
        int start = input.arrayOffset() + position;
        int length = ((array[start] & 0xFF) << 8) | (array[start + 1] & 0xFF);
        if (length == 0 || input.remaining() - 2 < length) {
            return null;
        }
        int from = start + 2;
        int to = from + length;
        for (int slot = slot(array, from, to); ; slot = (slot + 1) & (SLOTS - 1)) {
            Entry entry = entries[slot];
            if (entry == null) {
                return null;
            }
            if (Arrays.equals(entry.name(), 0, entry.name().length, array, from, to)) {
                input.position(position + 2 + length);
                return entry.topic();
            }
        }
    }

    /// Remembers that the name whose two-byte length starts at `fieldStart` in `input` decoded
    /// to `topic`, which decoding has checked.
    void remember(ByteBuffer input, int fieldStart, Topic topic) {
        if (!input.hasArray()) {
            return;
        }
        byte[] array = input.array();
        int start = input.arrayOffset() + fieldStart;
        int length = ((array[start] & 0xFF) << 8) | (array[start + 1] & 0xFF);
        int from = start + 2;
        int to = from + length;
        if (count == LIMIT) {
            Arrays.fill(entries, null);
            count = 0;
        }
        int slot = slot(array, from, to);
        while (entries[slot] != null) {
            slot = (slot + 1) & (SLOTS - 1);
        }
        entries[slot] = new Entry(Arrays.copyOfRange(array, from, to), topic);
        count++;
    }

    // The slot where the probe for the name in bytes [from, to) of `array` starts.
    private static int slot(byte[] array, int from, int to) {
        int hash = 0;
        for (int i = from; i < to; i++) {
            hash = 31 * hash + array[i];
        }
        return (hash ^ (hash >>> 16)) & (SLOTS - 1);
    }

    // A remembered name: its UTF-8 bytes and the topic decoded from them.
    private record Entry(byte[] name, Topic topic) {}
}
