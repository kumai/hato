package org.mqtt4j.hato.comparison;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Prints the results of the scenarios as tables, one row per library, each figure the median of
/// the library's runs.
///
/// CPU time and allocation are per message that came off the wire, handled or dropped: a client
/// that drops messages still reads and decodes them. A run whose connection ended before the
/// warm-up was over measured nothing and counts only towards "dropped" and "lost"; "lost" counts
/// the connections that ended in all runs, and NaN marks a figure that no run measured.
final class Tables {

    // A client keeps up with a rate when it drops nothing, keeps its connection, and handles at
    // least this share of the rate: one that falls behind holds the stub broker back.
    private static final double KEEPING_UP = 0.98;

    private Tables() {
    }

    /// The receive scenario: what a message costs each client.
    static void receive(Map<Library, List<Arguments>> results) {
        System.out.printf(
                "%-7s | %10s | %10s | %11s | %9s | %4s | %7s%n",
                "client",
                "handled/s",
                "CPU µs/msg",
                "alloc B/msg",
                "dropped",
                "lost",
                "threads"
        );
        results.forEach((library, runs) -> System.out.printf(
                "%-7s | %,10.0f | %10.2f | %,11.0f | %,9.0f | %4d | %7.0f%n",
                library.label(),
                measuredMedian(runs, Tables::handledPerSecond),
                measuredMedian(runs, Tables::cpuMicrosPerMessage),
                measuredMedian(runs, result -> (double) result.number("allocatedBytes")
                        / result.number("arrived")),
                median(runs, Tables::dropped),
                lost(runs),
                measuredMedian(runs, result -> result.number("peakThreads"))
        ));
    }

    /// The latency scenario, by rate, and the highest rate that each client kept up with.
    ///
    /// Latency runs from the time a message was due to the start of its handler. Two columns help
    /// to read its tail: "GC ms" is the time that garbage collection took in the measured stretch,
    /// and "stub µs" the most that the stub broker sent a message after it was due, which the
    /// client's holding back explains only at QoS 1 or when the client stopped reading.
    static void latency(Map<Long, Map<Library, List<Arguments>>> byRate) {
        System.out.printf(
                "%9s | %-7s | %10s | %9s | %9s | %9s | %10s | %9s | %4s | %10s | %5s | %10s%n",
                "rate/s",
                "client",
                "handled/s",
                "p50 µs",
                "p99 µs",
                "p99.9 µs",
                "max µs",
                "dropped",
                "lost",
                "CPU µs/msg",
                "GC ms",
                "stub µs"
        );
        // The highest rate that each library kept up with in every run.
        var keptUp = new LinkedHashMap<Library, Long>();
        byRate.forEach((rate, results) -> results.forEach((library, runs) -> {
            System.out.printf(
                    "%,9d | %-7s | %,10.0f | %,9.0f | %,9.0f | %,9.0f | %,10.0f | %,9.0f | %4d"
                            + " | %10.2f | %5.0f | %,10.0f%n",
                    rate,
                    library.label(),
                    measuredMedian(runs, Tables::handledPerSecond),
                    measuredMedian(runs, result -> result.number("p50Micros")),
                    measuredMedian(runs, result -> result.number("p99Micros")),
                    measuredMedian(runs, result -> result.number("p999Micros")),
                    measuredMedian(runs, result -> result.number("maxMicros")),
                    median(runs, Tables::dropped),
                    lost(runs),
                    measuredMedian(runs, Tables::cpuMicrosPerMessage),
                    measuredMedian(runs, result -> result.number("gcMillis")),
                    median(runs, result -> result.number("stubLateMicros"))
            );
            keptUp.putIfAbsent(library, 0L);
            if (runs.stream().allMatch(result -> keepsUp(result, rate))) {
                keptUp.merge(library, rate, Math::max);
            }
        }));
        System.out.printf(
                "Highest rate kept up with in every run (nothing dropped, no connection lost,"
                        + " at least %.0f%% of the rate handled):%n",
                KEEPING_UP * 100
        );
        keptUp.forEach((library, rate) -> System.out.printf(
                "  %-7s %s%n",
                library.label(),
                rate == 0 ? "none of the rates" : String.format("%,d/s", rate)
        ));
    }

    /// The scenarios through RabbitMQ.
    ///
    /// Latency runs from the time a message was due at the publisher, through the broker, to the
    /// start of its handler, for the messages after the warm-up; the message that waited longest
    /// tells how long a backlog took to drain. "backlog" is the most of those messages that were
    /// due but not yet handled at once. A client holds at most the broker's prefetch of them
    /// unacknowledged; the rest waited in the broker.
    static void broker(Map<Library, List<Arguments>> results) {
        System.out.printf(
                "%-7s | %8s | %8s | %8s | %9s | %8s | %9s | %4s | %10s%n",
                "client",
                "p50 ms",
                "p90 ms",
                "p99 ms",
                "max ms",
                "backlog",
                "dropped",
                "lost",
                "CPU µs/msg"
        );
        results.forEach((library, runs) -> System.out.printf(
                "%-7s | %,8.1f | %,8.1f | %,8.1f | %,9.1f | %,8.0f | %,9.0f | %4d | %10.2f%n",
                library.label(),
                measuredMedian(runs, result -> result.number("p50Micros") / 1e3),
                measuredMedian(runs, result -> result.number("p90Micros") / 1e3),
                measuredMedian(runs, result -> result.number("p99Micros") / 1e3),
                measuredMedian(runs, result -> result.number("maxMicros") / 1e3),
                measuredMedian(runs, result -> result.number("peakBacklog")),
                median(runs, Tables::dropped),
                lost(runs),
                measuredMedian(runs, Tables::cpuMicrosPerMessage)
        ));
    }

    /// The reconnect scenario.
    ///
    /// "gap s" is the longest time in which the handler received no message, which a lost
    /// connection sets: the time to notice the loss, to reconnect, and to subscribe again, and any
    /// time that the client then spends before it hands messages on. "lost" counts the messages
    /// that never reached the handler, those that the broker had nowhere to send while no
    /// subscription was in place. "conns" is the connections that each run lost, which should be
    /// the one cut.
    static void reconnect(Map<Library, List<Arguments>> results) {
        System.out.printf(
                "%-13s | %7s | %9s | %8s | %9s | %8s | %5s | %10s%n",
                "client",
                "gap s",
                "lost",
                "p99 ms",
                "max ms",
                "backlog",
                "conns",
                "CPU µs/msg"
        );
        results.forEach((library, runs) -> System.out.printf(
                "%-13s | %7.2f | %,9.0f | %,8.1f | %,9.1f | %,8.0f | %5.0f | %10.2f%n",
                library.label(),
                measuredMedian(runs, result -> result.number("longestGapMicros") / 1e6),
                median(runs, Tables::dropped),
                measuredMedian(runs, result -> result.number("p99Micros") / 1e3),
                measuredMedian(runs, result -> result.number("maxMicros") / 1e3),
                measuredMedian(runs, result -> result.number("peakBacklog")),
                median(runs, result -> result.number("connectionsLost")),
                measuredMedian(runs, Tables::cpuMicrosPerMessage)
        ));
    }

    private static boolean keepsUp(Arguments result, long rate) {
        return dropped(result) == 0
                && result.number("connectionsLost") == 0
                && result.number("measured") > 0
                && handledPerSecond(result) >= KEEPING_UP * rate;
    }

    private static double handledPerSecond(Arguments result) {
        return result.number("measured") * 1e9 / result.number("elapsedNanos");
    }

    private static double cpuMicrosPerMessage(Arguments result) {
        return result.number("cpuNanos") / 1e3 / result.number("arrived");
    }

    private static double dropped(Arguments result) {
        return result.number("expected") - result.number("handled");
    }

    /// The connections that ended in all runs.
    private static long lost(List<Arguments> results) {
        return results.stream().mapToLong(result -> result.number("connectionsLost")).sum();
    }

    /// The median of `metric` over the runs that measured something, or NaN when none did.
    private static double measuredMedian(List<Arguments> results, Metric metric) {
        return median(
                results.stream().filter(result -> result.number("measured") > 0).toList(),
                metric
        );
    }

    private static double median(List<Arguments> results, Metric metric) {
        double[] values = results.stream().mapToDouble(metric::of).sorted().toArray();
        return values.length == 0 ? Double.NaN : values[values.length / 2];
    }

    @FunctionalInterface
    private interface Metric {

        double of(Arguments result);
    }
}
