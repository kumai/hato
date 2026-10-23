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
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;

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
/// - `burst`: RabbitMQ relays QoS 1 messages at a steady `rate`, which a burst of `burst` messages
///   interrupts after `burstAfterSeconds`, the way devices that reconnect send what they held
///   back; the steady messages go on for `tailSeconds` more. For each of `workMicros`, the handler
///   works that long on each message, in `workMode`, as [Subject] describes it. This shows how
///   long the burst takes to drain, and how large a backlog it leaves.
/// - `stall`: RabbitMQ relays QoS 1 messages at a steady `rate`, and after `beforeSeconds` the
///   handler blocks once for `stallMillis`, as when a database stops answering; the messages go
///   on for `tailSeconds` after the stall. The clients keep alive at `keepAliveSeconds`, below the
///   stall, so this shows whether a client keeps its connection while its handler blocks, and how
///   long it takes to catch up.
/// - `slowSubscription`: RabbitMQ relays QoS 1 messages at a steady `rate` for `warmupSeconds` and
///   then `seconds`, one in `every` of them to a second subscription on the same connection,
///   whose handler works `slowWorkMicros` on each in `workMode`. For each of `prefetches`, the
///   MQTT prefetch of the broker, this shows how much the slow subscription holds back the other.
///
/// The scenarios through RabbitMQ send `size`-byte messages to `posts` topics, which the subject
/// subscribes to with one wildcard filter, from a [RawPublisher] that keeps at most
/// `publisherWindow` publications unacknowledged. `burst` and `stall` take `prefetch`.
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

    private static final String STATUS_FILTER = "comparison/+/status";

    private static final String RESPONSE_FILTER = "comparison/+/response";

    private static final Duration RUN_TIMEOUT = Duration.ofMinutes(15);

    // How many times a failed run is tried again before the comparison gives up.
    private static final int RETRIES = 2;

    // How long a subject waits for a message through RabbitMQ before it ends its run: longer than
    // on loopback, so that a broker that is still warming up after its start does not end a run.
    private static final long BROKER_IDLE_MILLIS = 10_000;

    // The default rates per second for the latency scenario. With a window of 10, QoS 1 tops out
    // at a few tens of thousands of messages per second on loopback.
    private static final String QOS0_RATES = "50000,100000,200000,300000,400000,600000,800000";

    private static final String QOS1_RATES = "5000,20000,30000,40000,50000";

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
            case "stall" -> stall(arguments, settings);
            case "slowSubscription" -> slowSubscription(arguments, settings);
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

    /// Work done against a running broker.
    @FunctionalInterface
    private interface WithBroker {

        void run(Broker broker) throws Exception;
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
                System.out.printf(
                        "%nFrom a stub broker on loopback, QoS %s, %s-byte payloads, window %d:"
                                + " median of %d runs of %,d messages, the first %,d not"
                                + " measured%n",
                        qos,
                        size,
                        settings.window(),
                        settings.runs(),
                        messages,
                        warmup
                );
                Tables.receive(
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
                Tables.latency(byRate);
            }
        }
    }

    /// Receiving a burst through RabbitMQ, for each amount of handler work.
    private static void burst(Arguments arguments, Settings settings) throws Exception {
        long rate = arguments.number("rate", 200);
        long burstAfterSeconds = arguments.number("burstAfterSeconds", 5);
        long burst = arguments.number("burst", 2_000);
        long tailSeconds = arguments.number("tailSeconds", 10);
        String workMode = arguments.text("workMode", "sleep");
        long after = rate * burstAfterSeconds;
        var traffic = brokerTraffic(
                arguments,
                statusTopics(arguments.number("posts", 100)),
                rate * (burstAfterSeconds + tailSeconds) + burst,
                new Schedule.Burst(rate, after, burst)
        );
        // The steady messages before the burst warm the client up.
        var bursting = new Case(traffic, after);
        withRabbitMq(arguments.number("prefetch", 10), broker -> {
            for (String work : arguments.list("workMicros", "0,1000")) {
                System.out.printf(
                        "%nThrough RabbitMQ at QoS 1: %,d msg/s, a burst of %,d after %d s, then"
                                + " %d s more; handler %s %s µs: median of %d runs%n",
                        rate,
                        burst,
                        burstAfterSeconds,
                        tailSeconds,
                        workMode,
                        work,
                        settings.runs()
                );
                Tables.broker(measure(settings, (library, index) -> runOnBroker(
                        broker,
                        bursting,
                        List.of(
                                "filter=" + STATUS_FILTER,
                                "workMicros=" + work,
                                "workMode=" + workMode,
                                "idleMillis=" + BROKER_IDLE_MILLIS
                        ),
                        library,
                        index,
                        settings
                )));
            }
        });
    }

    /// A handler that blocks once through RabbitMQ, with a keep-alive below the stall.
    private static void stall(Arguments arguments, Settings settings) throws Exception {
        long rate = arguments.number("rate", 200);
        long beforeSeconds = arguments.number("beforeSeconds", 5);
        long stallMillis = arguments.number("stallMillis", 15_000);
        long tailSeconds = arguments.number("tailSeconds", 15);
        long keepAliveSeconds = arguments.number("keepAliveSeconds", 5);
        long stallAt = rate * beforeSeconds;
        var traffic = brokerTraffic(
                arguments,
                statusTopics(arguments.number("posts", 100)),
                rate * ((beforeSeconds + tailSeconds) * 1_000 + stallMillis) / 1_000,
                new Schedule.Steady(rate)
        );
        // The message that stalls the handler is the first measured.
        var stalling = new Case(traffic, stallAt);
        withRabbitMq(arguments.number("prefetch", 10), broker -> {
            System.out.printf(
                    "%nThrough RabbitMQ at QoS 1: %,d msg/s; after %d s the handler blocks once"
                            + " for %,d ms, keep-alive %d s; %d s more after the stall: median of"
                            + " %d runs%n",
                    rate,
                    beforeSeconds,
                    stallMillis,
                    keepAliveSeconds,
                    tailSeconds,
                    settings.runs()
            );
            Tables.broker(measure(settings, (library, index) -> runOnBroker(
                    broker,
                    stalling,
                    List.of(
                            "filter=" + STATUS_FILTER,
                            "stallAt=" + stallAt,
                            "stallMillis=" + stallMillis,
                            "keepAliveSeconds=" + keepAliveSeconds,
                            // No message arrives while the handler blocks.
                            "idleMillis=" + (stallMillis + BROKER_IDLE_MILLIS)
                    ),
                    library,
                    index,
                    settings
            )));
        });
    }

    /// Two subscriptions on one connection through RabbitMQ, one of them slow, for each prefetch.
    private static void slowSubscription(Arguments arguments, Settings settings) throws Exception {
        long rate = arguments.number("rate", 200);
        long warmupSeconds = arguments.number("warmupSeconds", 5);
        long seconds = arguments.number("seconds", 20);
        int every = (int) arguments.number("every", 200);
        long slowWorkMicros = arguments.number("slowWorkMicros", 200_000);
        String workMode = arguments.text("workMode", "sleep");
        // One message in `every` goes to the slow subscription, the others to the fast one.
        List<String> topics = Stream.concat(
                statusTopics(every - 1).stream(),
                Stream.of("comparison/0/response")
        ).toList();
        var traffic = brokerTraffic(
                arguments,
                topics,
                rate * (warmupSeconds + seconds),
                new Schedule.Steady(rate)
        );
        var mixed = new Case(traffic, rate * warmupSeconds);
        for (String prefetch : arguments.list("prefetches", "10,100")) {
            withRabbitMq(Long.parseLong(prefetch), broker -> {
                System.out.printf(
                        "%nThrough RabbitMQ at QoS 1: %,d msg/s, one in %d to a second"
                                + " subscription whose handler %ss %,d µs: median of %d runs of"
                                + " %d s, after %d s not measured%n",
                        rate,
                        every,
                        workMode,
                        slowWorkMicros,
                        settings.runs(),
                        seconds,
                        warmupSeconds
                );
                Tables.broker(measure(settings, (library, index) -> runOnBroker(
                        broker,
                        mixed,
                        List.of(
                                "filter=" + STATUS_FILTER,
                                "slowFilter=" + RESPONSE_FILTER,
                                "slowWorkMicros=" + slowWorkMicros,
                                "workMode=" + workMode,
                                "idleMillis=" + BROKER_IDLE_MILLIS
                        ),
                        library,
                        index,
                        settings
                )));
            });
        }
    }

    /// The topics of `posts` devices that report their status.
    private static List<String> statusTopics(long posts) {
        return IntStream.range(0, Math.toIntExact(posts))
                .mapToObj(post -> "comparison/" + post + "/status")
                .toList();
    }

    /// QoS 1 traffic for the publisher of the scenarios through RabbitMQ.
    private static Traffic brokerTraffic(
            Arguments arguments,
            List<String> topics,
            long count,
            Schedule schedule
    ) {
        // Enough unacknowledged publications that the broker's acknowledgements never hold the
        // publisher back.
        return new Traffic(
                topics,
                1,
                (int) arguments.number("size", 256),
                (int) arguments.number("publisherWindow", 1_000),
                count,
                schedule
        );
    }

    /// Starts RabbitMQ in Docker with `prefetch` as its MQTT prefetch, the most QoS 1 messages it
    /// sends a subscriber unacknowledged, runs `body` against it, and stops it. Prints the
    /// prefetch that the broker reports, so that a run shows the setting took effect.
    private static void withRabbitMq(long prefetch, WithBroker body) throws Exception {
        try (GenericContainer<?> container = RabbitMq.container()
                .withCopyToContainer(
                        Transferable.of("mqtt.prefetch = " + prefetch + "\n"),
                        "/etc/rabbitmq/conf.d/90-comparison.conf"
                )) {
            Docker.start(container);
            String reported = container.execInContainer(
                    "rabbitmqctl",
                    "eval",
                    "application:get_env(rabbitmq_mqtt, prefetch)."
            ).getStdout().trim();
            System.out.printf("%nRabbitMQ reports its MQTT prefetch as %s%n", reported);
            body.run(new Broker(
                    container,
                    container.getHost(),
                    container.getMappedPort(RabbitMq.MQTT_PORT)
            ));
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
                Arguments result = runWithRetries(run, library, index);
                if (settings.verbose()) {
                    System.out.println(library.label() + ": " + result.values());
                }
                results.computeIfAbsent(library, _ -> new ArrayList<>()).add(result);
            }
        }
        return results;
    }

    /// Runs `run`, trying again up to [#RETRIES] times when it fails. A run can fail outside the
    /// client, as when Docker or its port forwarding stops answering for a while. Each failure is
    /// printed, so that one that the client caused shows too, and repeats until the run gives up.
    private static Arguments runWithRetries(Run run, Library library, int index)
            throws Exception {
        for (int attempt = 0; ; attempt++) {
            try {
                return run.run(library, index);
            } catch (Exception e) {
                if (attempt == RETRIES) {
                    throw e;
                }
                System.out.println(library.label() + ": run failed, trying again: " + e);
            }
        }
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
        // The publisher connects before the subject starts. Docker Desktop's port forwarding has
        // been seen to close the broker's side of a subscriber's connection when the publisher's
        // connection came tens of milliseconds after it, while the subscriber's side stayed open;
        // the subject JVM's start keeps the two apart.
        try (var publisher = new RawPublisher(
                broker.host(),
                broker.port(),
                "comparison-publisher"
        )) {
            try (var subject = new SubjectProcess(
                    library,
                    subjectJvmOptions(settings),
                    subjectArguments(measured, settings, arguments)
            )) {
                subject.awaitReady(Duration.ofMinutes(1));
                var publishing = new FutureTask<Long>(() -> publisher.publish(measured.traffic()));
                Thread.ofPlatform().name("publisher").daemon().start(publishing);
                Arguments outcome = subject.awaitResult(RUN_TIMEOUT);
                long late;
                try {
                    late = publishing.get(1, TimeUnit.MINUTES);
                } catch (TimeoutException e) {
                    throw new IllegalStateException(
                            "The publisher did not finish: " + publisher.progress()
                                    + "\nSubject: " + outcome.values()
                                    + "\n" + brokerState(broker),
                            e
                    );
                }
                // At QoS 1 a broker loses no message to a subscriber that keeps its connection.
                // When the port forwarding closes the broker's side all the same, the client
                // waits for nothing; such a run fails, with what the broker shows, and is tried
                // again.
                if (outcome.number("handled") < outcome.number("expected")) {
                    throw new IllegalStateException(String.format(
                            "%s handled %,d of %,d messages%n%s",
                            library.label(),
                            outcome.number("handled"),
                            outcome.number("expected"),
                            brokerState(broker)
                    ));
                }
                return outcome.with("publisherLateMicros", String.valueOf(late));
            }
        }
    }

    /// What RabbitMQ holds now, for a run that went wrong: its queues with their messages and
    /// consumers, its MQTT connections, and the end of its log.
    private static String brokerState(Broker broker) throws Exception {
        GenericContainer<?> container = broker.container();
        String queues = container.execInContainer(
                "rabbitmqctl",
                "-q",
                "list_queues",
                "--no-table-headers",
                "name",
                "messages",
                "consumers"
        ).getStdout();
        String connections = container.execInContainer(
                "rabbitmqctl",
                "-q",
                "list_mqtt_connections",
                "--no-table-headers",
                "client_id"
        ).getStdout();
        List<String> log = container.getLogs().lines().toList();
        return "Broker queues now: " + queues.trim().replaceAll("\\s*\n\\s*", "; ")
                + "\nBroker MQTT connections now: "
                + connections.trim().replaceAll("\\s*\n\\s*", "; ")
                + "\nBroker log:\n"
                + String.join("\n", log.subList(Math.max(0, log.size() - 30), log.size()));
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
}
