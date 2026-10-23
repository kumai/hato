package org.mqtt4j.hato.comparison;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.mqtt4j.hato.comparison.StubBroker.Traffic;

import static java.nio.charset.StandardCharsets.UTF_8;

/// Runs the comparison and prints its results.
///
/// For each case, the driver runs every library `runs` times. A run starts a [StubBroker] in this
/// JVM and a [Subject] in a JVM of its own, so that what one client leaves behind, such as
/// compiled code, garbage, and threads, never reaches another, and so that the subject's CPU time
/// is its client's. The order of the libraries rotates from run to run, so that none always runs
/// first. The tables show the median of the runs for each library.
///
/// Arguments, as `key=value` words, all optional. `scenario` picks what to measure:
///
/// - `receive`, the default: the stub broker sends as fast as each client takes the messages,
///   which shows what a message costs. `messages` and `warmup` count the messages.
/// - `latency`: the stub broker sends at fixed rates, which shows how long messages take to reach
///   the handler and which rates each client keeps up with. `seconds` and `warmupSeconds` set the
///   time at each rate, and `qos0Rates` and `qos1Rates` the rates.
///
/// Both take `runs`, `window`, `libraries`, `qos`, and `sizes`, and `verbose=true` to print each
/// run's result as well. Lists are comma-separated. For example,
/// `./gradlew :client-comparison:run --args="scenario=latency qos=1 runs=5"`.
public final class Driver {

    // Every subject JVM gets the same options: a fixed heap, and the access that Netty, which the
    // HiveMQ client uses, needs on Java 25 to run without warnings.
    private static final List<String> SUBJECT_JVM_OPTIONS = List.of(
            "-Xms1g",
            "-Xmx1g",
            "--sun-misc-unsafe-memory-access=allow",
            "--enable-native-access=ALL-UNNAMED"
    );

    private static final String TOPIC = "comparison/receive";

    private static final long RUN_TIMEOUT_MINUTES = 15;

    // The default rates per second for the latency scenario. With a window of 10, QoS 1 tops out
    // at a few tens of thousands of messages per second on loopback.
    private static final String QOS0_RATES = "50000,100000,200000,300000,400000,600000,800000";

    private static final String QOS1_RATES = "5000,20000,30000,40000,50000";

    // A client keeps up with a rate when it drops nothing, keeps its connection, and handles at
    // least this share of the rate: one that falls behind holds the stub broker back.
    private static final double KEEPING_UP = 0.98;

    private Driver() {
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(Arrays.asList(args));
        var settings = new Settings(
                (int) arguments.number("runs", 3),
                (int) arguments.number("window", 10),
                arguments.list("libraries", "paho,hato,hivemq").stream()
                        .map(Library::of)
                        .toList(),
                Boolean.parseBoolean(arguments.text("verbose", "false"))
        );
        System.out.printf(
                "Receiving from a stub broker on loopback; Java %s, %d processors%n",
                Runtime.version(),
                Runtime.getRuntime().availableProcessors()
        );
        String scenario = arguments.text("scenario", "receive");
        switch (scenario) {
            case "receive" -> receive(arguments, settings);
            case "latency" -> latency(arguments, settings);
            default -> throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
    }

    /// What every scenario takes: the runs per library, the QoS 1 window, the libraries, and
    /// whether to print each run's result.
    private record Settings(int runs, int window, List<Library> libraries, boolean verbose) {}

    /// One configuration of the stub broker and the subscriber: what the stub broker sends, and
    /// how many of the leading messages the subscriber does not measure.
    private record Case(Traffic traffic, long warmup) {}

    /// Receiving as fast as each client takes the messages.
    private static void receive(Arguments arguments, Settings settings) throws Exception {
        long messages = arguments.number("messages", 500_000);
        long warmup = arguments.number("warmup", 100_000);
        for (String qos : arguments.list("qos", "0,1")) {
            for (String size : arguments.list("sizes", "16,1024")) {
                var traffic = new Traffic(
                        TOPIC,
                        Integer.parseInt(qos),
                        Integer.parseInt(size),
                        settings.window(),
                        messages,
                        0
                );
                var receive = new Case(traffic, warmup);
                printReceive(receive, settings.runs(), measure(receive, settings));
            }
        }
    }

    /// Receiving at fixed rates, each for `warmupSeconds` and then `seconds`.
    private static void latency(Arguments arguments, Settings settings) throws Exception {
        // A shorter warm-up leaves the clients' CPU time per message still falling.
        long seconds = arguments.number("seconds", 5);
        long warmupSeconds = arguments.number("warmupSeconds", 5);
        for (String qos : arguments.list("qos", "0,1")) {
            int level = Integer.parseInt(qos);
            List<String> rates = level == 0
                    ? arguments.list("qos0Rates", QOS0_RATES)
                    : arguments.list("qos1Rates", QOS1_RATES);
            for (String size : arguments.list("sizes", "16")) {
                int payloadSize = Integer.parseInt(size);
                System.out.printf(
                        "%nQoS %d, %d-byte payloads, window %d, at fixed rates: median of %d runs"
                                + " of %d s, after %d s not measured%n",
                        level,
                        payloadSize,
                        settings.window(),
                        settings.runs(),
                        seconds,
                        warmupSeconds
                );
                var byRate = new LinkedHashMap<Long, Map<Library, List<Arguments>>>();
                for (String text : rates) {
                    long rate = Long.parseLong(text);
                    var traffic = new Traffic(
                            TOPIC,
                            level,
                            payloadSize,
                            settings.window(),
                            rate * (warmupSeconds + seconds),
                            rate
                    );
                    var paced = new Case(traffic, rate * warmupSeconds);
                    byRate.put(rate, measure(paced, settings));
                }
                printLatency(byRate);
            }
        }
    }

    /// Runs every library the set number of times and returns each library's results in run
    /// order, printing each result as the subject reported it when the settings ask for it.
    private static Map<Library, List<Arguments>> measure(
            Case measured,
            Settings settings
    ) throws Exception {
        // The first run takes the libraries in the given order, which the tables keep.
        var results = new LinkedHashMap<Library, List<Arguments>>();
        for (int run = 0; run < settings.runs(); run++) {
            var order = new ArrayList<>(settings.libraries());
            Collections.rotate(order, -run);
            for (Library library : order) {
                Arguments result = runOnce(measured, library);
                if (settings.verbose()) {
                    System.out.println(library.label() + ": " + result.values());
                }
                results.computeIfAbsent(library, _ -> new ArrayList<>()).add(result);
            }
        }
        return results;
    }

    private static Arguments runOnce(Case measured, Library library) throws Exception {
        try (var broker = new StubBroker(measured.traffic())) {
            var serving = new FutureTask<Boolean>(() -> {
                broker.serve();
                return true;
            });
            Thread.ofPlatform().name("stub-broker").daemon().start(serving);
            Process subject = startSubject(measured, library, broker.port());
            String result = readResult(subject, library);
            if (!subject.waitFor(1, TimeUnit.MINUTES)) {
                subject.destroyForcibly();
                throw new IllegalStateException(library.label() + " did not exit");
            }
            Arguments outcome = Arguments.parse(result);
            // A client that ended its connection leaves the stub broker unable to finish, and
            // its result says so. Otherwise the stub broker must have served the run through.
            if (outcome.number("connectionsLost") == 0) {
                serving.get(1, TimeUnit.MINUTES);
            }
            return outcome.with("stubLateMicros", String.valueOf(broker.maxLatenessMicros()));
        }
    }

    private static Process startSubject(Case measured, Library library, int port)
            throws IOException {
        var command = new ArrayList<String>();
        command.add(ProcessHandle.current().info().command().orElseThrow());
        command.addAll(SUBJECT_JVM_OPTIONS);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(Subject.class.getName());
        command.add("library=" + library.label());
        command.add("port=" + port);
        command.add("filter=" + TOPIC);
        command.add("qos=" + measured.traffic().qos());
        command.add("expected=" + measured.traffic().count());
        command.add("warmup=" + measured.warmup());
        return new ProcessBuilder(command)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
    }

    /// Reads the subject's output until its result line, passing any other line through.
    private static String readResult(Process subject, Library library) throws Exception {
        var lines = new BufferedReader(new InputStreamReader(subject.getInputStream(), UTF_8));
        var reading = new FutureTask<String>(() -> {
            String line;
            while ((line = lines.readLine()) != null) {
                if (line.startsWith(Subject.RESULT_PREFIX)) {
                    return line.substring(Subject.RESULT_PREFIX.length());
                }
                System.out.println(library.label() + ": " + line);
            }
            throw new IllegalStateException(library.label() + " ended without a result");
        });
        Thread.ofPlatform().daemon().start(reading);
        try {
            return reading.get(RUN_TIMEOUT_MINUTES, TimeUnit.MINUTES);
        } catch (Exception e) {
            subject.destroyForcibly();
            throw e;
        }
    }

    private static void printReceive(
            Case receive,
            int runs,
            Map<Library, List<Arguments>> results
    ) {
        Traffic traffic = receive.traffic();
        System.out.printf(
                "%nQoS %d, %d-byte payloads, window %d: median of %d runs of %,d messages,"
                        + " the first %,d not measured%n",
                traffic.qos(),
                traffic.payloadSize(),
                traffic.window(),
                runs,
                traffic.count(),
                receive.warmup()
        );
        // Handled messages per second, but CPU time and allocation per message that came off the
        // wire: a client that drops messages still reads and decodes them. Runs whose connection
        // ended before the warm-up was over measured nothing; "lost" counts the connections that
        // ended in all runs, and NaN marks a figure that no run measured.
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
        results.forEach((library, runResults) -> System.out.printf(
                "%-7s | %,10.0f | %10.2f | %,11.0f | %,9.0f | %4d | %7.0f%n",
                library.label(),
                measuredMedian(runResults, Driver::handledPerSecond),
                measuredMedian(runResults, result -> result.number("cpuNanos") / 1e3
                        / result.number("arrived")),
                measuredMedian(runResults, result -> (double) result.number("allocatedBytes")
                        / result.number("arrived")),
                median(runResults, Driver::dropped),
                lost(runResults),
                measuredMedian(runResults, result -> result.number("peakThreads"))
        ));
    }

    private static void printLatency(Map<Long, Map<Library, List<Arguments>>> byRate) {
        // Latency runs from the time a message was due to the start of its handler. Two columns
        // help to read its tail: "GC ms" is the time that garbage collection took in the measured
        // stretch, and "stub µs" the most that the stub broker sent a message after it was due,
        // which the client's holding back explains only at QoS 1 or when the client stopped
        // reading. The other figures follow the receive table.
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
        byRate.forEach((rate, results) -> results.forEach((library, runResults) -> {
            System.out.printf(
                    "%,9d | %-7s | %,10.0f | %,9.0f | %,9.0f | %,9.0f | %,10.0f | %,9.0f | %4d"
                            + " | %10.2f | %5.0f | %,10.0f%n",
                    rate,
                    library.label(),
                    measuredMedian(runResults, Driver::handledPerSecond),
                    measuredMedian(runResults, result -> result.number("p50Micros")),
                    measuredMedian(runResults, result -> result.number("p99Micros")),
                    measuredMedian(runResults, result -> result.number("p999Micros")),
                    measuredMedian(runResults, result -> result.number("maxMicros")),
                    median(runResults, Driver::dropped),
                    lost(runResults),
                    measuredMedian(runResults, result -> result.number("cpuNanos") / 1e3
                            / result.number("arrived")),
                    measuredMedian(runResults, result -> result.number("gcMillis")),
                    median(runResults, result -> result.number("stubLateMicros"))
            );
            keptUp.putIfAbsent(library, 0L);
            if (runResults.stream().allMatch(result -> keepsUp(result, rate))) {
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

    private static boolean keepsUp(Arguments result, long rate) {
        return dropped(result) == 0
                && result.number("connectionsLost") == 0
                && result.number("measured") > 0
                && handledPerSecond(result) >= KEEPING_UP * rate;
    }

    private static double handledPerSecond(Arguments result) {
        return result.number("measured") * 1e9 / result.number("elapsedNanos");
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
