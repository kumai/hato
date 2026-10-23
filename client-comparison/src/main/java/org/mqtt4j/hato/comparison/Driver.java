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

import static java.nio.charset.StandardCharsets.UTF_8;

/// Runs the comparison and prints its results.
///
/// For each case, the driver runs every library `runs` times. A run starts a [StubBroker] in this
/// JVM and a [Subject] in a JVM of its own, so that what one client leaves behind, such as
/// compiled code, garbage, and threads, never reaches another, and so that the subject's CPU time
/// is its client's. The order of the libraries rotates from run to run, so that none always runs
/// first. The table shows the median of the runs for each library.
///
/// Arguments, as `key=value` words, all optional: `runs`, `messages`, `warmup`, `window`,
/// `libraries`, `qos`, and `sizes`; the last three take comma-separated lists. For example,
/// `./gradlew :client-comparison:run --args="runs=5 qos=1 sizes=16"`.
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

    private Driver() {
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(Arrays.asList(args));
        int runs = (int) arguments.number("runs", 3);
        long messages = arguments.number("messages", 500_000);
        long warmup = arguments.number("warmup", 100_000);
        int window = (int) arguments.number("window", 10);
        List<Library> libraries = arguments.list("libraries", "paho,hato,hivemq").stream()
                .map(Library::of)
                .toList();
        System.out.printf(
                "Receiving from a stub broker on loopback; Java %s, %d processors%n",
                Runtime.version(),
                Runtime.getRuntime().availableProcessors()
        );
        for (String qos : arguments.list("qos", "0,1")) {
            for (String size : arguments.list("sizes", "16,1024")) {
                var receive = new Case(
                        Integer.parseInt(qos),
                        Integer.parseInt(size),
                        window,
                        messages,
                        warmup
                );
                print(receive, runs, measure(receive, libraries, runs));
            }
        }
    }

    /// One configuration of the stub broker and the subscriber.
    private record Case(int qos, int payloadSize, int window, long messages, long warmup) {}

    /// Runs every library `runs` times and returns each library's results in run order.
    private static Map<Library, List<Arguments>> measure(
            Case receive,
            List<Library> libraries,
            int runs
    ) throws Exception {
        // The first run takes the libraries in the given order, which the table keeps.
        var results = new LinkedHashMap<Library, List<Arguments>>();
        for (int run = 0; run < runs; run++) {
            var order = new ArrayList<>(libraries);
            Collections.rotate(order, -run);
            for (Library library : order) {
                results.computeIfAbsent(library, _ -> new ArrayList<>())
                        .add(runOnce(receive, library));
            }
        }
        return results;
    }

    private static Arguments runOnce(Case receive, Library library) throws Exception {
        try (var broker = new StubBroker(
                TOPIC,
                receive.qos(),
                receive.payloadSize(),
                receive.window(),
                receive.messages()
        )) {
            var serving = new FutureTask<Boolean>(() -> {
                broker.serve();
                return true;
            });
            Thread.ofPlatform().name("stub-broker").daemon().start(serving);
            Process subject = startSubject(receive, library, broker.port());
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
            return outcome;
        }
    }

    private static Process startSubject(Case receive, Library library, int port)
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
        command.add("qos=" + receive.qos());
        command.add("expected=" + receive.messages());
        command.add("warmup=" + receive.warmup());
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

    private static void print(Case receive, int runs, Map<Library, List<Arguments>> results) {
        System.out.printf(
                "%nQoS %d, %d-byte payloads, window %d: median of %d runs of %,d messages,"
                        + " the first %,d not measured%n",
                receive.qos(),
                receive.payloadSize(),
                receive.window(),
                runs,
                receive.messages(),
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
                measuredMedian(runResults, result -> result.number("measured") * 1e9
                        / result.number("elapsedNanos")),
                measuredMedian(runResults, result -> result.number("cpuNanos") / 1e3
                        / result.number("arrived")),
                measuredMedian(runResults, result -> (double) result.number("allocatedBytes")
                        / result.number("arrived")),
                median(runResults, result -> result.number("expected")
                        - result.number("handled")),
                runResults.stream().mapToLong(result -> result.number("connectionsLost")).sum(),
                measuredMedian(runResults, result -> result.number("peakThreads"))
        ));
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
