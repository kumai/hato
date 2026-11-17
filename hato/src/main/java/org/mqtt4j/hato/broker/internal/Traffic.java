package org.mqtt4j.hato.broker.internal;

import java.util.concurrent.atomic.AtomicLongArray;

import org.mqtt4j.hato.Qos;

// The PUBLISH packets that one loop has read from its connections and written to them, by QoS
// level, for the application to read. Only the loop's thread counts, so a count costs it a
// plain read and an ordered write, with no atomic operation; a reader sees each count whole, as
// it was a moment ago.
final class Traffic {

    private final AtomicLongArray received = new AtomicLongArray(3);

    private final AtomicLongArray delivered = new AtomicLongArray(3);

    // Counts a PUBLISH packet at `qos` that the loop has read. Called on the loop's thread.
    void received(Qos qos) {
        add(received, qos.level(), 1);
    }

    // Counts the PUBLISH packets that the loop has written, `counts[level]` at each QoS level.
    // Called on the loop's thread.
    void delivered(int[] counts) {
        for (int level = 0; level < counts.length; level++) {
            if (counts[level] != 0) {
                add(delivered, level, counts[level]);
            }
        }
    }

    // Adds what the loop has read to `received` and what it has written to `delivered`, by QoS
    // level.
    void addTo(long[] received, long[] delivered) {
        for (int level = 0; level < received.length; level++) {
            received[level] += this.received.getAcquire(level);
            delivered[level] += this.delivered.getAcquire(level);
        }
    }

    private static void add(AtomicLongArray counts, int index, long count) {
        counts.setRelease(index, counts.getPlain(index) + count);
    }
}
