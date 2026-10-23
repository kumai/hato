package org.mqtt4j.hato.comparison;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.jspecify.annotations.Nullable;

/// Counts the messages that reach a subscriber's handler, takes each one's latency, and marks the
/// stretch that is measured.
///
/// The messages numbered below `warmup` warm up the client and are not measured. The stretch
/// starts with the first message handled from `warmup` on, and ends with the last message sent,
/// numbered `expected - 1`. A client that drops messages may never handle that one, so the run
/// also ends once no message has arrived for a while; the stretch then ends with the last message
/// handled. Counting the warm-up in sent messages rather than handled ones lets a run measure a
/// client that drops most of them.
///
/// The sequence numbers also tell how many messages came off the wire in the stretch, handled or
/// dropped: every client delivers a subscription's messages in order, so the stretch spans the
/// numbers from its first handled message to its last.
///
/// A message's latency runs from the send time in its payload to the start of its handler.
final class Reception {

    // How often the waiting thread checks for the end.
    private static final Duration POLL = Duration.ofMillis(5);

    // The latency slot of a message that the handler has not received.
    private static final long NOT_HANDLED = Long.MIN_VALUE;

    private static final ThreadMXBean threads = ManagementFactory.getThreadMXBean();

    private final long expected;

    private final long warmup;

    // The latency of each message from `warmup` on, in microseconds, at its sequence number less
    // `warmup`. Written by the handler before the volatile fields below, so that a thread that
    // has read those sees it.
    private final long[] latencies;

    private final AtomicLong handled = new AtomicLong();

    private volatile long lastNanos;

    private volatile long lastSequence;

    // The first handled message of the stretch: its sequence number, and the handled count with it.
    private volatile long firstSequence;

    private volatile long firstHandled;

    private final boolean reportThreads;

    // Each platform thread's CPU time when the stretch started, when the run reports threads.
    private volatile @Nullable Map<Long, ThreadUsage.Times> threadsAtStart;

    // Written after the fields above, so that a thread that reads it sees them.
    private volatile @Nullable ProcessUsage start;

    private volatile @Nullable ProcessUsage end;

    /// @param expected the messages that the broker sends
    /// @param warmup the leading messages that are not measured, at least one
    /// @param reportThreads whether to take each thread's CPU time for [#threadReport]; taking it
    ///     holds up the message that starts the stretch
    Reception(long expected, long warmup, boolean reportThreads) {
        if (warmup < 1 || warmup >= expected - 1) {
            throw new IllegalArgumentException(
                    "Warm-up must be at least 1 and below " + (expected - 1) + ": " + warmup
            );
        }
        this.expected = expected;
        this.warmup = warmup;
        this.reportThreads = reportThreads;
        this.latencies = new long[Math.toIntExact(expected - warmup)];
        Arrays.fill(latencies, NOT_HANDLED);
    }

    /// The handler under measurement: it takes the message's latency, counts the message, and
    /// does nothing else with it.
    void handle(long sequence, long sentMicros) {
        if (sequence >= warmup) {
            latencies[(int) (sequence - warmup)] = Payloads.nowMicros() - sentMicros;
        }
        long count = handled.incrementAndGet();
        lastNanos = System.nanoTime();
        lastSequence = sequence;
        if (start == null && sequence >= warmup) {
            firstSequence = sequence;
            firstHandled = count;
            threads.resetPeakThreadCount();
            if (reportThreads) {
                threadsAtStart = ThreadUsage.now();
            }
            start = ProcessUsage.now();
        }
        if (sequence == expected - 1) {
            end = ProcessUsage.now();
        }
    }

    /// Waits until every expected message has been handled, or until none has arrived for
    /// `idle`, and returns what the measured stretch used. When no message after the warm-up
    /// arrived, as when the connection ended early, the stretch is empty and measures nothing.
    ///
    /// @throws IllegalStateException if `limit` passed first
    Result await(Duration idle, Duration limit) throws InterruptedException {
        long deadline = System.nanoTime() + limit.toNanos();
        while (end == null) {
            long now = System.nanoTime();
            if (handled.get() > 0 && now - lastNanos > idle.toNanos()) {
                break;
            }
            if (now > deadline) {
                throw new IllegalStateException(
                        "Handled " + handled.get() + " of " + expected + " messages within " + limit
                );
            }
            Thread.sleep(POLL);
        }
        ProcessUsage first = start;
        if (first == null) {
            var nothing = new ProcessUsage(0, 0, 0, 0, 0);
            return new Result(expected, handled.get(), 0, 0, 0, nothing, 0, Latency.NONE);
        }
        long arrived = lastSequence - firstSequence;
        ProcessUsage last = end;
        ProcessUsage used = (last != null ? last : ProcessUsage.now()).since(first);
        return new Result(
                expected,
                handled.get(),
                handled.get() - firstHandled,
                arrived,
                lastNanos - first.nanos(),
                used,
                threads.getPeakThreadCount(),
                Latency.of(latencies)
        );
    }

    /// The CPU time of the measured stretch of `result`, broken down by thread as
    /// [ThreadUsage#report] does it, or nothing when the run does not report threads or measured
    /// nothing. Call it after [#await].
    List<String> threadReport(Result result) {
        Map<Long, ThreadUsage.Times> first = threadsAtStart;
        if (first == null || result.arrived() == 0) {
            return List.of();
        }
        return ThreadUsage.report(
                first,
                ThreadUsage.now(),
                result.used().cpuNanos(),
                result.arrived()
        );
    }

    /// What one run measured.
    ///
    /// @param expected the messages that the broker sent
    /// @param handled the messages that reached the handler
    /// @param measured the messages handled after the warm-up
    /// @param arrived the messages that came off the wire after the warm-up, handled or dropped
    /// @param elapsedNanos the time from the end of the warm-up to the last message handled
    /// @param used what the process used from the end of the warm-up to the end of the run
    /// @param peakThreads the most platform threads alive at once after the warm-up
    /// @param latency the latencies of the messages handled after the warm-up
    record Result(
            long expected,
            long handled,
            long measured,
            long arrived,
            long elapsedNanos,
            ProcessUsage used,
            int peakThreads,
            Latency latency
    ) {

        /// The result as `key=value` words separated by spaces, which [Arguments] reads back.
        String format() {
            return "expected=" + expected
                    + " handled=" + handled
                    + " measured=" + measured
                    + " arrived=" + arrived
                    + " elapsedNanos=" + elapsedNanos
                    + " cpuNanos=" + used.cpuNanos()
                    + " allocatedBytes=" + used.allocatedBytes()
                    + " gcCount=" + used.gcCount()
                    + " gcMillis=" + used.gcMillis()
                    + " peakThreads=" + peakThreads
                    + " p50Micros=" + latency.p50()
                    + " p99Micros=" + latency.p99()
                    + " p999Micros=" + latency.p999()
                    + " maxMicros=" + latency.max();
        }
    }

    /// Percentiles of the latencies of handled messages, in microseconds.
    record Latency(long p50, long p99, long p999, long max) {

        static final Latency NONE = new Latency(0, 0, 0, 0);

        /// The percentiles of `latencies`, skipping the slots of messages never handled.
        static Latency of(long[] latencies) {
            long[] sorted = Arrays.stream(latencies)
                    .filter(latency -> latency != NOT_HANDLED)
                    .sorted()
                    .toArray();
            if (sorted.length == 0) {
                return NONE;
            }
            return new Latency(
                    below(sorted, 0.5),
                    below(sorted, 0.99),
                    below(sorted, 0.999),
                    sorted[sorted.length - 1]
            );
        }

        /// The value below which the `fraction` of the values in `sorted` lie.
        private static long below(long[] sorted, double fraction) {
            return sorted[Math.min(sorted.length - 1, (int) (sorted.length * fraction))];
        }
    }
}
