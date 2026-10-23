package org.mqtt4j.hato.comparison;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.RabbitMq;

/// Runs the comparison and prints its results.
///
/// For each case, the driver runs every library `runs` times. A run starts a [Subject] in a JVM of
/// its own, so that what one client leaves behind, such as compiled code, garbage, and threads,
/// never reaches another, and so that the subject's CPU time is its client's. The order of the
/// libraries rotates from run to run, so that none always runs first. The tables show the median
/// of the runs for each library.
///
/// Arguments, as `key=value` words, all optional. `scenario` picks what to measure:
///
/// - `receive`, the default: a [StubBroker] in this JVM sends as fast as each client takes the
///   messages, which shows what a message costs. `messages` and `warmup` count the messages.
/// - `latency`: the stub broker sends at fixed rates, which shows how long messages take to reach
///   the handler and which rates each client keeps up with. `seconds` and `warmupSeconds` set the
///   time at each rate, and `qos0Rates` and `qos1Rates` the rates.
/// - `burst`: RabbitMQ, in Docker, relays QoS 1 messages from a [RawPublisher] at a steady `rate`,
///   which a burst of `burst` messages interrupts after `burstAfterSeconds`, the way devices that
///   reconnect send what they held back; the steady messages go on for `tailSeconds` more. The
///   messages, of `size` bytes, go to `posts` topics, which the subject subscribes to with one
///   wildcard filter. For each of `workMicros`, the handler works that long on each message, in
///   `workMode`, as [Subject] describes it. This shows how long the burst takes to drain, and how
///   much of it waits in the broker.
///
/// All scenarios take `runs`, `libraries`, `verbose=true` to print each run's result as well,
/// `threads=true` to print where each run's CPU time went by thread, and `subjectOptions` for JVM
/// options to add to every subject JVM, such as `-Djdk.virtualThreadScheduler.parallelism=1`.
/// The stub broker's scenarios also take `window`, `qos`, and `sizes`. Lists are comma-separated.
/// For example, `./gradlew :client-comparison:run --args="scenario=latency qos=1 runs=5"`.
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

    private static final Duration RUN_TIMEOUT = Duration.ofMinutes(15);

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
                Boolean.parseBoolean(arguments.text("verbose", "false")),
                Boolean.parseBoolean(arguments.text("threads", "false")),
                arguments.text("subjectOptions", "").isEmpty()
                        ? List.of()
                        : arguments.list("subjectOptions", "")
        );
        System.out.printf(
                "Java %s, %d processors%n",
                Runtime.version(),
                Runtime.getRuntime().availableProcessors()
        );
        String scenario = arguments.text("scenario", "receive");
        switch (scenario) {
            case "receive" -> receive(arguments, settings);
            case "latency" -> latency(arguments, settings);
            case "burst" -> burst(arguments, settings);
            default -> throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
    }

    /// What every scenario takes: the runs per library, the QoS 1 window of the stub broker, the
    /// libraries, whether to print each run's result, whether to print where each run's CPU time
    /// went by thread, and the JVM options to add to every subject JVM.
    private record Settings(
            int runs,
            int window,
            List<Library> libraries,
            boolean verbose,
            boolean threads,
            List<String> subjectOptions
    ) {}

    /// One configuration of the sender and the subscriber: what the sender sends, and how many of
    /// the leading messages the subscriber does not measure.
    private record Case(Traffic traffic, long warmup) {}

    /// A RabbitMQ broker in Docker, and where its MQTT listener is reached from this machine.
    private record Broker(GenericContainer<?> container, String host, int port) {}

    /// One run of one library, the `index`th of the runs.
    @FunctionalInterface
    private interface Run {

        Arguments run(Library library, int index) throws Exception;
    }

    /// Receiving from the stub broker as fast as each client takes the messages.
    private static void receive(Arguments arguments, Settings settings) throws Exception {
        long messages = arguments.number("messages", 500_000);
        long warmup = arguments.number("warmup", 100_000);
        for (String qos : arguments.list("qos", "0,1")) {
            for (String size : arguments.list("sizes", "16,1024")) {
                var traffic = new Traffic(
                        List.of(TOPIC),
                        Integer.parseInt(qos),
                        Integer.parseInt(size),
                        settings.window(),
                        messages,
                        new Schedule.Flood()
                );
                var receive = new Case(traffic, warmup);
                printReceive(
                        receive,
                        settings.runs(),
                        measure(settings, (library, _) -> runOnStub(receive, library, settings))
                );
            }
        }
    }

    /// Receiving from the stub broker at fixed rates, each for `warmupSeconds` and then `seconds`.
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
                        "%nFrom a stub broker on loopback, QoS %d, %d-byte payloads, window %d, at"
                                + " fixed rates: median of %d runs of %d s, after %d s not"
                                + " measured%n",
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
                            List.of(TOPIC),
                            level,
                            payloadSize,
                            settings.window(),
                            rate * (warmupSeconds + seconds),
                            new Schedule.Steady(rate)
                    );
                    var paced = new Case(traffic, rate * warmupSeconds);
                    byRate.put(
                            rate,
                            measure(settings, (library, _) -> runOnStub(paced, library, settings))
                    );
                }
                printLatency(byRate);
            }
        }
    }

    /// Receiving a burst through RabbitMQ, for each amount of handler work.
    private static void burst(Arguments arguments, Settings settings) throws Exception {
        long rate = arguments.number("rate", 200);
        long burstAfterSeconds = arguments.number("burstAfterSeconds", 5);
        long burst = arguments.number("burst", 2_000);
        long tailSeconds = arguments.number("tailSeconds", 10);
        int posts = (int) arguments.number("posts", 100);
        int size = (int) arguments.number("size", 256);
        // Enough unacknowledged publications that the broker's acknowledgements never hold the
        // publisher back.
        int publisherWindow = (int) arguments.number("publisherWindow", 1_000);
        String workMode = arguments.text("workMode", "sleep");
        List<String> topics = IntStream.range(0, posts)
                .mapToObj(post -> "comparison/" + post + "/status")
                .toList();
        long after = rate * burstAfterSeconds;
        var traffic = new Traffic(
                topics,
                1,
                size,
                publisherWindow,
                rate * (burstAfterSeconds + tailSeconds) + burst,
                new Schedule.Burst(rate, after, burst)
        );
        // The steady messages before the burst warm the client up.
        var bursting = new Case(traffic, after);
        try (GenericContainer<?> container = RabbitMq.container()) {
            Docker.start(container);
            var broker = new Broker(
                    container,
                    container.getHost(),
                    container.getMappedPort(RabbitMq.MQTT_PORT)
            );
            for (String work : arguments.list("workMicros", "0,1000")) {
                System.out.printf(
                        "%nThrough RabbitMQ at QoS 1, prefetch 10: %,d msg/s to %d topics,"
                                + " %d-byte payloads, a burst of %,d after %d s, then %d s more;"
                                + " handler %s %s µs: median of %d runs%n",
                        rate,
                        posts,
                        size,
                        burst,
                        burstAfterSeconds,
                        tailSeconds,
                        workMode,
                        work,
                        settings.runs()
                );
                printBurst(measure(settings, (library, index) -> runOnBroker(
                        broker,
                        bursting,
                        List.of("filter=comparison/+/status", "workMicros=" + work,
                                "workMode=" + workMode),
                        library,
                        index,
                        settings
                )));
            }
        }
    }

    /// Runs every library the set number of times and returns each library's results in run
    /// order, printing each result as the subject reported it when the settings ask for it.
    private static Map<Library, List<Arguments>> measure(Settings settings, Run run)
            throws Exception {
        // The first run takes the libraries in the given order, which the tables keep.
        var results = new LinkedHashMap<Library, List<Arguments>>();
        for (int index = 0; index < settings.runs(); index++) {
            var order = new ArrayList<>(settings.libraries());
            Collections.rotate(order, -index);
            for (Library library : order) {
                Arguments result = run.run(library, index);
                if (settings.verbose()) {
                    System.out.println(library.label() + ": " + result.values());
                }
                results.computeIfAbsent(library, _ -> new ArrayList<>()).add(result);
            }
        }
        return results;
    }

    private static Arguments runOnStub(Case measured, Library library, Settings settings)
            throws Exception {
        try (var broker = new StubBroker(measured.traffic())) {
            var serving = new FutureTask<Boolean>(() -> {
                broker.serve();
                return true;
            });
            Thread.ofPlatform().name("stub-broker").daemon().start(serving);
            try (var subject = new SubjectProcess(
                    library,
                    subjectJvmOptions(settings),
                    subjectArguments(
                            measured,
                            settings,
                            List.of("port=" + broker.port(), "filter=" + TOPIC)
                    )
            )) {
                Arguments outcome = subject.awaitResult(RUN_TIMEOUT);
                // A client that ended its connection leaves the stub broker unable to finish,
                // and its result says so. Otherwise the stub broker must have served the run
                // through.
                if (outcome.number("connectionsLost") == 0) {
                    serving.get(1, TimeUnit.MINUTES);
                }
                return outcome.with("stubLateMicros", String.valueOf(broker.maxLatenessMicros()));
            }
        }
    }

    /// Runs one subject against `broker`, publishing the case's traffic once it has subscribed,
    /// and adds the most messages that its queue held to the result. Each run takes a client
    /// identifier of its own, and with it a queue of its own.
    private static Arguments runOnBroker(
            Broker broker,
            Case measured,
            List<String> subjectArguments,
            Library library,
            int index,
            Settings settings
    ) throws Exception {
        String clientId = "comparison-" + library.label() + "-" + index;
        var arguments = Stream.concat(
                Stream.of(
                        "host=" + broker.host(),
                        "port=" + broker.port(),
                        "clientId=" + clientId
                ),
                subjectArguments.stream()
        ).toList();
        try (var subject = new SubjectProcess(
                library,
                subjectJvmOptions(settings),
                subjectArguments(measured, settings, arguments)
        )) {
            subject.awaitReady(Duration.ofMinutes(1));
            try (var probe = new QueueProbe(
                    broker.container(),
                    "mqtt-subscription-" + clientId + "qos1"
            );
                    var publisher = new RawPublisher(
                            broker.host(),
                            broker.port(),
                            "comparison-publisher"
                    )) {
                var publishing = new FutureTask<Long>(() -> publisher.publish(measured.traffic()));
                Thread.ofPlatform().name("publisher").daemon().start(publishing);
                Arguments outcome = subject.awaitResult(RUN_TIMEOUT);
                long late = publishing.get(1, TimeUnit.MINUTES);
                return outcome
                        .with("queuePeak", String.valueOf(probe.peak()))
                        .with("publisherLateMicros", String.valueOf(late));
            }
        }
    }

    private static List<String> subjectJvmOptions(Settings settings) {
        return Stream.concat(SUBJECT_JVM_OPTIONS.stream(), settings.subjectOptions().stream())
                .toList();
    }

    /// The subject's arguments for `measured`, followed by `more`.
    private static List<String> subjectArguments(
            Case measured,
            Settings settings,
            List<String> more
    ) {
        return Stream.concat(
                Stream.of(
                        "qos=" + measured.traffic().qos(),
                        "expected=" + measured.traffic().count(),
                        "warmup=" + measured.warmup(),
                        "threads=" + settings.threads()
                ),
                more.stream()
        ).toList();
    }

    private static void printReceive(
            Case receive,
            int runs,
            Map<Library, List<Arguments>> results
    ) {
        Traffic traffic = receive.traffic();
        System.out.printf(
                "%nFrom a stub broker on loopback, QoS %d, %d-byte payloads, window %d: median of"
                        + " %d runs of %,d messages, the first %,d not measured%n",
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
                measuredMedian(runResults, Driver::cpuMicrosPerMessage),
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
                    measuredMedian(runResults, Driver::cpuMicrosPerMessage),
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

    private static void printBurst(Map<Library, List<Arguments>> results) {
        // Latency runs from the time a message was due at the publisher, through the broker, to
        // the start of its handler, for the messages from the burst on. The burst's last message
        // waits longest, so "max" tells how long the burst took to drain. "queue peak" is the most
        // messages that a sample found in the subscriber's queue in RabbitMQ, ready or awaiting
        // acknowledgement. The other figures follow the receive table.
        System.out.printf(
                "%-7s | %8s | %8s | %8s | %10s | %9s | %4s | %10s%n",
                "client",
                "p50 ms",
                "p99 ms",
                "max ms",
                "queue peak",
                "dropped",
                "lost",
                "CPU µs/msg"
        );
        results.forEach((library, runResults) -> System.out.printf(
                "%-7s | %,8.1f | %,8.1f | %,8.1f | %,10.0f | %,9.0f | %4d | %10.2f%n",
                library.label(),
                measuredMedian(runResults, result -> result.number("p50Micros") / 1e3),
                measuredMedian(runResults, result -> result.number("p99Micros") / 1e3),
                measuredMedian(runResults, result -> result.number("maxMicros") / 1e3),
                median(runResults, result -> result.number("queuePeak")),
                median(runResults, Driver::dropped),
                lost(runResults),
                measuredMedian(runResults, Driver::cpuMicrosPerMessage)
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
