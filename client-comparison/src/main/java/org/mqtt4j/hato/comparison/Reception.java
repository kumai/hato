package org.mqtt4j.hato.comparison;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.jspecify.annotations.Nullable;

/// Counts the messages that reach a subscriber's handler, takes each one's latency, and marks the
/// stretch that is measured.
///
/// The messages numbered below `warmup` warm up the client and are not measured. The stretch
/// starts with the first message handled from `warmup` on, and ends with the last message sent,
/// numbered `expected - 1`, once every message has been handled. A client that drops messages may
/// never handle that one, so the run also ends once no message has arrived for a while; the
/// stretch then ends with the last message handled. Counting the warm-up in sent messages rather
/// than handled ones lets a run measure a client that drops most of them.
///
/// The sequence numbers also tell how many messages came off the wire in the stretch, handled or
/// dropped: the stretch spans the numbers from its first handled message to the highest. Several
/// subscriptions may share one reception, and a client that runs each on a thread of its own can
/// handle their messages out of order; the counts then hold to within the messages in flight.
///
/// A message's latency runs from the send time in its payload to the start of its handler. The
/// send times and the handling times together also tell the backlog: how many messages were due
/// but not yet handled, wherever they waited.
final class Reception {

    // How often the waiting thread checks for the end.
    private static final Duration POLL = Duration.ofMillis(5);

    // How long the run waits, after the last message sent, for messages that it overtook.
    private static final Duration STRAGGLERS = Duration.ofSeconds(2);

    // The handling time of a message that the handler has not received.
    static final long NOT_HANDLED = Long.MIN_VALUE;

    private static final ThreadMXBean threads = ManagementFactory.getThreadMXBean();

    private final long expected;

    private final long warmup;

    // The send time and the handling time of each message from `warmup` on, in microseconds since
    // the epoch, at its sequence number less `warmup`. Written by the handler before the volatile
    // fields below, so that a thread that has read those sees them.
    private final long[] sentMicros;

    private final long[] handledMicros;

    private final AtomicLong handled = new AtomicLong();

    private final AtomicLong highestSequence = new AtomicLong(-1);

    // Lets one handler call start the stretch when several run at once.
    private final AtomicBoolean starting = new AtomicBoolean();

    private volatile long lastNanos;

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
        this.sentMicros = new long[Math.toIntExact(expected - warmup)];
        this.handledMicros = new long[sentMicros.length];
        Arrays.fill(handledMicros, NOT_HANDLED);
    }

    /// The handler under measurement: it takes the message's times, counts the message, and does
    /// nothing else with it.
    void handle(long sequence, long sentMicros) {
        if (sequence >= warmup) {
            int slot = (int) (sequence - warmup);
            this.sentMicros[slot] = sentMicros;
            handledMicros[slot] = Payloads.nowMicros();
        }
        long count = handled.incrementAndGet();
        highestSequence.accumulateAndGet(sequence, Math::max);
        lastNanos = System.nanoTime();
        if (sequence >= warmup && start == null && starting.compareAndSet(false, true)) {
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
        while (true) {
            long now = System.nanoTime();
            long quiet = now - lastNanos;
            // The last message sent ends the run once the messages before it have been handled
            // too, which a subscription on a thread of its own can leave behind; those that do
            // not come soon after it never come.
            if (end != null && (handled.get() >= expected || quiet > STRAGGLERS.toNanos())) {
                break;
            }
            if (handled.get() > 0 && quiet > idle.toNanos()) {
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
            return new Result(expected, handled.get(), 0, 0, 0, nothing, 0, Latency.NONE, 0, 0);
        }
        long arrived = highestSequence.get() - firstSequence;
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
                Latency.of(sentMicros, handledMicros),
                peakBacklog(sentMicros, handledMicros),
                longestGap(handledMicros)
        );
    }

    /// The longest time in which the handler received nothing, between two of the messages in
    /// `handled` that it did receive, in microseconds: the time that a lost connection left the
    /// application without messages. A handling time of [#NOT_HANDLED] leaves its message out.
    static long longestGap(long[] handled) {
        long[] times = Arrays.stream(handled)
                .filter(time -> time != NOT_HANDLED)
                .sorted()
                .toArray();
        long longest = 0;
        for (int next = 1; next < times.length; next++) {
            longest = Math.max(longest, times[next] - times[next - 1]);
        }
        return longest;
    }

    /// The most messages that were due but not yet handled at any one moment, among the messages
    /// in `sent` and `handled` that were handled: those waiting in the broker and in the client
    /// together. A message waits from the time it was due until the time it was handled, and a
    /// handling time of [#NOT_HANDLED] leaves its message out.
    static long peakBacklog(long[] sent, long[] handled) {
        long[] due = IntStream.range(0, handled.length)
                .filter(slot -> handled[slot] != NOT_HANDLED)
                .mapToLong(slot -> sent[slot])
                .sorted()
                .toArray();
        long[] done = Arrays.stream(handled)
                .filter(time -> time != NOT_HANDLED)
                .sorted()
                .toArray();
        // The backlog grows only as messages become due, so its peak follows one of them: when
        // the next becomes due, those due so far less those handled by then are waiting.
        long peak = 0;
        int handledByThen = 0;
        for (int dueSoFar = 1; dueSoFar <= due.length; dueSoFar++) {
            long now = due[dueSoFar - 1];
            while (handledByThen < done.length && done[handledByThen] <= now) {
                handledByThen++;
            }
            peak = Math.max(peak, dueSoFar - handledByThen);
        }
        return peak;
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
    /// @param peakBacklog the most of those messages that were due but not yet handled at once,
    ///     as [Reception#peakBacklog] counts them
    /// @param longestGapMicros the longest time without a message for the handler after the
    ///     warm-up, as [Reception#longestGap] measures it
    record Result(
            long expected,
            long handled,
            long measured,
            long arrived,
            long elapsedNanos,
            ProcessUsage used,
            int peakThreads,
            Latency latency,
            long peakBacklog,
            long longestGapMicros
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
                    + " p90Micros=" + latency.p90()
                    + " p99Micros=" + latency.p99()
                    + " p999Micros=" + latency.p999()
                    + " maxMicros=" + latency.max()
                    + " peakBacklog=" + peakBacklog
                    + " longestGapMicros=" + longestGapMicros;
        }
    }

    /// Percentiles of the latencies of handled messages, in microseconds.
    record Latency(long p50, long p90, long p99, long p999, long max) {

        static final Latency NONE = new Latency(0, 0, 0, 0, 0);

        /// The percentiles of the latencies from `sent` to `handled`, leaving out the messages
        /// whose handling time is [#NOT_HANDLED].
        static Latency of(long[] sent, long[] handled) {
            long[] sorted = IntStream.range(0, handled.length)
                    .filter(slot -> handled[slot] != NOT_HANDLED)
                    .mapToLong(slot -> handled[slot] - sent[slot])
                    .sorted()
                    .toArray();
            if (sorted.length == 0) {
                return NONE;
            }
            return new Latency(
                    below(sorted, 0.5),
                    below(sorted, 0.9),
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
