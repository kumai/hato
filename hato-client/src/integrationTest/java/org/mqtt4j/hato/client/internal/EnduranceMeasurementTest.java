package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.UnixOperatingSystemMXBean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.ClientStats;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttClientBuilder;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mqtt4j.hato.client.internal.Measurements.heapAfterGc;
import static org.mqtt4j.hato.client.internal.Measurements.print;

/// Long runs that look for leaks. Each keeps a client reconnecting, or overloaded, for
/// `-Phato.measure.minutes` minutes (30 by default) and samples, after GC, the heap in use, the
/// open file descriptors, the platform threads, and the client's own threads, virtual ones
/// included. Runs only with `-Phato.measure=true`, and prints the samples instead of asserting
/// limits: a leak shows as a level that keeps rising from sample to sample.
@EnabledIfSystemProperty(named = "hato.measure", matches = "true")
class EnduranceMeasurementTest {

    private static final Duration DURATION =
            Duration.ofMinutes(Long.getLong("hato.measure.minutes", 30));

    private static final int SAMPLES = 30;

    private static final String CLIENT_ID = "endurance";

    // Every thread that the measured client starts carries this prefix.
    private static final String CLIENT_THREADS = "hato-" + CLIENT_ID + "-";

    private static final Duration WAIT = Duration.ofSeconds(30);

    @Test
    void reconnecting_over_tcp_to_a_restarting_broker_leaks_nothing() throws Exception {
        var address = new InetSocketAddress(InetAddress.getLoopbackAddress(), freePort());
        var broker = new AtomicReference<MqttBroker>(MqttBroker.start(address));
        var events = new LinkedBlockingQueue<String>();
        try (MqttClient sut = builder(URI.create("mqtt://127.0.0.1:" + address.getPort()), events)
                .build()) {
            sut.connect().get(WAIT.toSeconds(), SECONDS);
            await(events, "connected");
            var traffic = new Traffic(sut);
            var sampler = new Sampler("TCP reconnection to a restarting embedded broker");
            run(sampler, () -> {
                traffic.exchange();
                broker.get().close();
                await(events, "disconnected");
                broker.set(MqttBroker.start(address));
                await(events, "connected");
            });
            traffic.close();
        } finally {
            broker.get().close();
        }
    }

    @Test
    void reconnecting_over_websocket_through_cut_connections_leaks_nothing() throws Exception {
        GenericContainer<?> mosquitto = Mosquitto.container();
        Docker.start(mosquitto);
        var events = new LinkedBlockingQueue<String>();
        // The JDK's WebSocket client reports no end for a TCP close that arrives while it hands
        // over the message before it, so a short keep-alive ends such a connection instead.
        try (var relay = new CuttingRelay(new InetSocketAddress(
                mosquitto.getHost(),
                mosquitto.getMappedPort(Mosquitto.WEBSOCKET_PORT)
        ));
                MqttClient sut = builder(
                        URI.create("ws://127.0.0.1:" + relay.port() + "/mqtt"),
                        events
                )
                        .keepAlive(Duration.ofSeconds(1))
                        .build()) {
            sut.connect().get(WAIT.toSeconds(), SECONDS);
            await(events, "connected");
            var traffic = new Traffic(sut);
            var sampler = new Sampler("WS reconnection to Mosquitto through cut connections");
            run(sampler, () -> {
                traffic.exchange();
                relay.cut();
                await(events, "disconnected");
                await(events, "connected");
            });
            traffic.close();
        } finally {
            mosquitto.stop();
        }
    }

    @Test
    void staying_overloaded_leaks_nothing() throws Exception {
        // A flooder publishes faster than the measured client's slow handlers take: QoS 0
        // deliveries overflow and are dropped, a QoS 1 overflow ends the connection, and the
        // measured client's own publications run into its inflight limit.
        var address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        var events = new LinkedBlockingQueue<String>();
        try (MqttBroker broker = MqttBroker.start(address);
                MqttClient sut = MqttClient.builder()
                        .server(URI.create("mqtt://127.0.0.1:" + broker.port()))
                        .clientId(CLIENT_ID)
                        .maxInflightPublishes(8)
                        .reconnectPolicy(ReconnectPolicy.exponentialBackoff(
                                Duration.ofMillis(10),
                                Duration.ofMillis(200)
                        ).withInitialRetry())
                        .connectionListener(listener(events))
                        .build();
                MqttClient flooder = MqttClient.builder()
                        .server(URI.create("mqtt://127.0.0.1:" + broker.port()))
                        .clientId("flooder")
                        .build()) {
            sut.connect().get(WAIT.toSeconds(), SECONDS);
            flooder.connect().get(WAIT.toSeconds(), SECONDS);
            Subscription dropping = sut.subscribe(
                    Subscribe.of(new TopicFilter("overload/zero"), Qos.AT_MOST_ONCE)
                            .withBufferCapacity(32),
                    _ -> sleep(Duration.ofMillis(1))
            );
            Subscription overloading = sut.subscribe(
                    Subscribe.of(new TopicFilter("overload/one"), Qos.AT_LEAST_ONCE)
                            .withBufferCapacity(8),
                    _ -> sleep(Duration.ofMillis(1))
            );
            dropping.established().get(WAIT.toSeconds(), SECONDS);
            overloading.established().get(WAIT.toSeconds(), SECONDS);
            var sampler = new Sampler("Overload: dropped QoS 0, ended QoS 1, refusals");
            MqttMessage zero = message("overload/zero", Qos.AT_MOST_ONCE);
            MqttMessage one = message("overload/one", Qos.AT_LEAST_ONCE);
            MqttMessage own = message("overload/own", Qos.AT_LEAST_ONCE);
            run(sampler, () -> {
                var flood = new ArrayList<CompletableFuture<?>>();
                for (int i = 0; i < 200; i++) {
                    flood.add(flooder.publish(zero));
                    flood.add(flooder.publish(one));
                }
                for (int i = 0; i < 50; i++) {
                    CompletableFuture<?> _ = sut.publish(own);
                }
                for (CompletableFuture<?> published : flood) {
                    try {
                        published.get(WAIT.toSeconds(), SECONDS);
                    } catch (Exception _) {
                        // The flooder's own limits refuse some; the flood goes on.
                    }
                }
                events.clear();
            });
            ClientStats stats = sut.stats().get(WAIT.toSeconds(), SECONDS);
            print(
                    "dropped %s, overloads %s, refusals %s",
                    stats.receiving().dropped(),
                    stats.overloads(),
                    stats.refusals()
            );
        }
    }

    /// Repeats `cycle` for the configured time, sampling at even intervals.
    private static void run(Sampler sampler, Cycle cycle) throws Exception {
        long start = System.nanoTime();
        long interval = DURATION.toNanos() / SAMPLES;
        long cycles = 0;
        sampler.sample(0);
        for (int sample = 1; sample <= SAMPLES; sample++) {
            long next = start + sample * interval;
            while (System.nanoTime() < next) {
                cycle.run();
                cycles++;
            }
            sampler.sample(cycles);
        }
        sampler.print();
    }

    private static MqttClientBuilder builder(URI server, LinkedBlockingQueue<String> events) {
        return MqttClient.builder()
                .server(server)
                .clientId(CLIENT_ID)
                .reconnectPolicy(ReconnectPolicy.exponentialBackoff(
                        Duration.ofMillis(10),
                        Duration.ofMillis(200)
                ).withInitialRetry())
                .connectionListener(listener(events));
    }

    private static ConnectionListener listener(LinkedBlockingQueue<String> events) {
        return new ConnectionListener() {
            @Override
            public void onConnected(ConnAck ack) {
                events.add("connected");
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
                events.add("disconnected");
            }
        };
    }

    /// Waits for the next connection event, which must be `expected`.
    private static void await(LinkedBlockingQueue<String> events, String expected)
            throws InterruptedException {
        String event = events.poll(WAIT.toSeconds(), SECONDS);
        if (!expected.equals(event)) {
            throw new IllegalStateException("Expected " + expected + " but got " + event);
        }
    }

    private static int freePort() throws IOException {
        try (MqttBroker probe = MqttBroker.start(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0)
        )) {
            return probe.port();
        }
    }

    private static MqttMessage message(String topic, Qos qos) {
        return MqttMessage.of(new Topic(topic), new byte[64], qos, false);
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface
    private interface Cycle {

        void run() throws Exception;
    }

    /// Subscriptions of every delivery kind, which a cycle exercises with one publication of
    /// each QoS to each of them.
    private static final class Traffic {

        private static final List<String> TOPICS = List.of(
                "endurance/serial",
                "endurance/striped",
                "endurance/parallel",
                "endurance/handoff",
                "endurance/pull"
        );

        private final MqttClient client;

        private final AtomicLong deliveries = new AtomicLong();

        private final Subscription pull;

        private final Thread receiver;

        Traffic(MqttClient client) throws Exception {
            this.client = client;
            var subscriptions = new ArrayList<Subscription>();
            subscriptions.add(client.subscribe(
                    request("endurance/serial"),
                    _ -> deliveries.incrementAndGet()
            ));
            subscriptions.add(client.subscribe(
                    request("endurance/striped"),
                    DeliveryOptions.striped(4),
                    _ -> deliveries.incrementAndGet()
            ));
            subscriptions.add(client.subscribe(
                    request("endurance/parallel"),
                    DeliveryOptions.parallel(4),
                    _ -> deliveries.incrementAndGet()
            ));
            subscriptions.add(client.handOff(request("endurance/handoff"), _ -> {
                deliveries.incrementAndGet();
                return true;
            }));
            pull = client.subscribe(request("endurance/pull"));
            subscriptions.add(pull);
            for (Subscription subscription : subscriptions) {
                subscription.established().get(WAIT.toSeconds(), SECONDS);
            }
            receiver = Thread.ofVirtual().name("endurance-receiver").start(() -> {
                while (true) {
                    try {
                        ReceiveResult result = pull.receive(Duration.ofSeconds(1));
                        if (result instanceof ReceiveResult.Received) {
                            deliveries.incrementAndGet();
                        } else if (result instanceof ReceiveResult.Ended) {
                            return;
                        }
                    } catch (InterruptedException _) {
                        return;
                    } catch (RuntimeException _) {
                        // A connection's end can fail a waiting receive; the next one waits on.
                    }
                }
            });
        }

        /// Publishes one message of each QoS to each subscription and waits, within a bound,
        /// for their deliveries.
        void exchange() throws Exception {
            long expected = deliveries.get() + (long) TOPICS.size() * Qos.values().length;
            var published = new ArrayList<CompletableFuture<?>>();
            for (String topic : TOPICS) {
                for (Qos qos : Qos.values()) {
                    published.add(client.publish(
                            MqttMessage.of(new Topic(topic), new byte[16], qos, false)
                    ));
                }
            }
            for (CompletableFuture<?> publication : published) {
                publication.get(WAIT.toSeconds(), SECONDS);
            }
            long deadline = System.nanoTime() + WAIT.toNanos();
            while (deliveries.get() < expected && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
        }

        void close() throws InterruptedException {
            pull.close();
            receiver.join();
        }

        private static Subscribe request(String topic) {
            return Subscribe.of(new TopicFilter(topic), Qos.EXACTLY_ONCE);
        }
    }

    /// Samples, after GC, the heap in use, open file descriptors, platform threads, and the
    /// measured client's threads, from a thread dump that includes virtual threads.
    private static final class Sampler {

        private final String name;

        private final List<String> rows = new ArrayList<>();

        private final long start = System.nanoTime();

        Sampler(String name) {
            this.name = name;
        }

        void sample(long cycles) throws Exception {
            long heap = heapAfterGc();
            var os = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
            rows.add(String.format(
                    "%6d s | %8d cycles | %8.1f MiB heap | %5d fds | %4d platform | %5d client",
                    seconds,
                    cycles,
                    heap / (1024.0 * 1024.0),
                    os.getOpenFileDescriptorCount(),
                    ManagementFactory.getThreadMXBean().getThreadCount(),
                    clientThreads()
            ));
        }

        void print() {
            Measurements.print("%s, %s, %d samples", name, DURATION, SAMPLES + 1);
            rows.forEach(System.out::println);
        }

        private static int clientThreads() throws IOException {
            HotSpotDiagnosticMXBean diagnostic =
                    ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
            Path file = Files.createTempFile("hato-threads", ".json");
            try {
                Files.delete(file);
                diagnostic.dumpThreads(
                        file.toString(),
                        HotSpotDiagnosticMXBean.ThreadDumpFormat.JSON
                );
                String dump = Files.readString(file);
                return dump.split("\"name\": \"" + CLIENT_THREADS, -1).length - 1;
            } finally {
                Files.deleteIfExists(file);
            }
        }
    }
}
