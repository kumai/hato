package org.mqtt4j.hato.comparison;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.mqtt4j.hato.comparison.Mqtt311.PublishFrame;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/// Sends the messages of a [Traffic] on one connection, each when its schedule makes it due, at
/// QoS 1 with at most `window` of them unacknowledged. Whoever reads the connection passes each
/// PUBACK to [#acknowledged()].
///
/// Each pass writes the messages that are due and then flushes them, so that a scheduled message
/// goes out when it is due. Message `n` carries the time that it was due, on the wall clock that
/// subscribers read latency from, so that a message held back by a full window or by a reader that
/// stopped reading counts its wait as latency, as it would wait in a broker's queue; a flood's
/// messages carry the time they were written. Keeping the schedule on another clock and
/// converting it would carry any delay between the first readings of the two into every latency.
final class PacedSender {

    // How long the reader may leave every message of the window unacknowledged.
    private static final Duration ACKNOWLEDGEMENT_TIMEOUT = Duration.ofMinutes(1);

    // Waiting for the next message parks until this long before it is due and spins the rest:
    // parking alone can oversleep, while spinning through long gaps keeps a processor busy.
    private static final long SPIN_MICROS = 1_000;

    private final Traffic traffic;

    private final OutputStream out;

    // Shared with whoever else writes on the connection, such as a PINGRESP.
    private final ReentrantLock writing;

    // Each permit is room for one unacknowledged QoS 1 message.
    private final Semaphore unacknowledged;

    // The most that a scheduled message went out after it was due, in microseconds.
    private volatile long maxLatenessMicros;

    PacedSender(Traffic traffic, OutputStream out, ReentrantLock writing) {
        this.traffic = traffic;
        this.out = out;
        this.writing = writing;
        this.unacknowledged = new Semaphore(traffic.window());
    }

    /// Records that the receiver has acknowledged one QoS 1 message.
    void acknowledged() {
        unacknowledged.release();
    }

    /// The most that a scheduled message went out after it was due so far, in microseconds: 0 for
    /// a flood.
    ///
    /// A message goes out late when the receiver holds the sender back, through a full window or
    /// by not reading, and also when the sender itself does not get to run. A QoS 0 run against a
    /// receiver that keeps reading shows the sender alone, and so how much of the latencies
    /// measured the sender can account for.
    long maxLatenessMicros() {
        return maxLatenessMicros;
    }

    /// Sends every message, and returns once all have been written and, at QoS 1, acknowledged.
    void send() throws IOException, InterruptedException {
        Schedule schedule = traffic.schedule();
        var frames = traffic.topics().stream()
                .map(topic -> new PublishFrame(topic, traffic.qos(), traffic.payloadSize()))
                .toArray(PublishFrame[]::new);
        long startMicros = Payloads.nowMicros();
        int packetId = 0;
        long sequence = 0;
        while (sequence < traffic.count()) {
            long due = traffic.count();
            if (schedule.paced()) {
                long elapsed = Payloads.nowMicros() - startMicros;
                due = Math.min(due, schedule.dueBy(elapsed));
                if (sequence == due) {
                    long wait = schedule.dueMicros(sequence) - elapsed;
                    if (wait > 2 * SPIN_MICROS) {
                        LockSupport.parkNanos((wait - SPIN_MICROS) * 1_000);
                    } else {
                        Thread.onSpinWait();
                    }
                    continue;
                }
                // The first message due in this pass is the one that has waited longest.
                long lateness = elapsed - schedule.dueMicros(sequence);
                if (lateness > maxLatenessMicros) {
                    maxLatenessMicros = lateness;
                }
            }
            for (; sequence < due; sequence++) {
                if (traffic.qos() > 0) {
                    if (!unacknowledged.tryAcquire()) {
                        // The receiver cannot acknowledge what is still in the buffer.
                        flush();
                        await(1);
                    }
                    packetId = packetId % 65_535 + 1;
                }
                long sentMicros = schedule.paced()
                        ? startMicros + schedule.dueMicros(sequence)
                        : Payloads.nowMicros();
                PublishFrame frame = frames[(int) (sequence % frames.length)];
                writing.lock();
                try {
                    frame.write(out, packetId, sequence, sentMicros);
                } finally {
                    writing.unlock();
                }
            }
            flush();
        }
        if (traffic.qos() > 0) {
            // Every permit back means every message acknowledged.
            await(traffic.window());
        }
    }

    private void await(int permits) throws IOException, InterruptedException {
        if (!unacknowledged.tryAcquire(permits, ACKNOWLEDGEMENT_TIMEOUT.toMillis(), MILLISECONDS)) {
            throw new IOException("No acknowledgement for " + ACKNOWLEDGEMENT_TIMEOUT);
        }
    }

    private void flush() throws IOException {
        writing.lock();
        try {
            out.flush();
        } finally {
            writing.unlock();
        }
    }
}
