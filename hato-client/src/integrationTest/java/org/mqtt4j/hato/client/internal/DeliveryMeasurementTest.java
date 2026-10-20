package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// Measures the delivery modes and prints the results instead of asserting limits: how a slow
/// handler backs up deliveries and their acknowledgements under serial and striped delivery, how
/// long finished deliveries wait behind slower ones in the one receive-order acknowledgement
/// queue, and what handing deliveries from the dispatcher to drain tasks costs. Runs only with
/// `-Phato.measure=true`; the integrationTestSingleCarrier task measures the worst case of one
/// carrier thread.
@EnabledIfSystemProperty(named = "hato.measure", matches = "true")
@Timeout(600)
class DeliveryMeasurementTest {

    private static final int SLOW_MESSAGES = 320;

    private static final int SLOW_TOPICS = 16;

    private static final Duration SLOW_HANDLER = Duration.ofMillis(2);

    // Messages arrive at this pace, four times as fast as one slow handler can take them.
    private static final Duration ARRIVAL = Duration.ofNanos(500_000);

    private static final int MIXED_MESSAGES = 400;

    private static final int MIXED_TOPICS = 8;

    private static final Duration FAST_HANDLER = Duration.ofMillis(1);

    // One delivery in this many takes the slower handler's time instead of the fast one's. The
    // count is odd, so the slower deliveries alternate between QoS 1 and 2 and spread over the
    // topics.
    private static final int SLOW_EVERY = 15;

    private static final Duration SLOWER_HANDLER = Duration.ofMillis(20);

    private static final int HAND_OFFS = 100_000;

    // On average, at most this many deliveries wait in each subscription while the measurement
    // feeds them.
    private static final int WINDOW = 1_000;

    private static final int RUNS = 5;

    @Test
    void a_slow_handler_backs_up_deliveries_and_acknowledgements() throws Exception {
        print(
                "%s: %d QoS 1 messages over %d topics, one every %.1f ms, %d ms per handler",
                carriers(),
                SLOW_MESSAGES,
                SLOW_TOPICS,
                millis(ARRIVAL.toNanos()),
                SLOW_HANDLER.toMillis()
        );
        print("%-12s | %8s | %12s | %s", "mode", "total ms", "most waiting",
                "PUBACK delay ms p50 / p99 / max");
        for (Mode mode : List.of(
                new Mode("serial", DeliveryOptions.serial()),
                new Mode("striped(4)", DeliveryOptions.striped(4)),
                new Mode("striped(16)", DeliveryOptions.striped(16))
        )) {
            SlowRun run = slowRun(mode.options());
            long[] delays = run.delays();
            print(
                    "%-12s | %8.1f | %12d | %.1f / %.1f / %.1f",
                    mode.name(),
                    millis(run.total()),
                    run.mostWaiting(),
                    millis(percentile(delays, 50)),
                    millis(percentile(delays, 99)),
                    millis(delays[delays.length - 1])
            );
        }
    }

    @Test
    void finished_deliveries_wait_in_the_one_acknowledgement_queue() throws Exception {
        print(
                "%s: %d messages, QoS 1 and 2 in turn, over %d topics; handlers take %d ms, one in"
                        + " %d takes %d ms",
                carriers(),
                MIXED_MESSAGES,
                MIXED_TOPICS,
                FAST_HANDLER.toMillis(),
                SLOW_EVERY,
                SLOWER_HANDLER.toMillis()
        );
        print("%-12s | %13s | %s | %s", "mode", "subscriptions",
                "measured wait ms p50 / p99 / max",
                "simulated mean ms: one queue / one queue per type (share removed)");
        for (Mode mode : List.of(
                new Mode("striped(8)", DeliveryOptions.striped(8)),
                new Mode("parallel(8)", DeliveryOptions.parallel(8))
        )) {
            for (int subscriptions : List.of(1, 4)) {
                MixedRun run = mixedRun(mode.options(), subscriptions);
                long[] measured = sorted(run.measured());
                double one = mean(run.oneQueue());
                double perType = mean(run.perType());
                print(
                        "%-12s | %13d | %.1f / %.1f / %.1f | %.2f / %.2f (%.0f%%)",
                        mode.name(),
                        subscriptions,
                        millis(percentile(measured, 50)),
                        millis(percentile(measured, 99)),
                        millis(measured[measured.length - 1]),
                        one / 1e6,
                        perType / 1e6,
                        one == 0 ? 0 : 100 * (one - perType) / one
                );
            }
        }
    }

    @Test
    void handing_deliveries_on_from_the_dispatcher_costs() throws Exception {
        print(
                "%s: nanoseconds per delivery copy, median of %d runs of %d deliveries, no-op"
                        + " handlers",
                carriers(),
                RUNS,
                HAND_OFFS
        );
        print("%-36s | %13s | %s", "execution", "subscriptions", "ns per copy");
        try (ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int subscriptions : List.of(1, 16)) {
                for (Execution execution : List.of(
                        new Execution("serial, in place", new HandlerExecution.Internal()),
                        new Execution(
                                "serial on a virtual-thread executor",
                                new HandlerExecution.OnExecutor(virtualThreads)
                        ),
                        new Execution(
                                "striped(1)",
                                new HandlerExecution.Striped(1, MqttMessage::topic, null)
                        ),
                        new Execution(
                                "striped(8)",
                                new HandlerExecution.Striped(8, MqttMessage::topic, null)
                        ),
                        new Execution("parallel(8)", new HandlerExecution.Parallel(8, null))
                )) {
                    var samples = new long[RUNS];
                    // The first run warms up the code paths.
                    long _ = handOffNanos(execution.execution(), subscriptions);
                    for (int run = 0; run < RUNS; run++) {
                        samples[run] = handOffNanos(execution.execution(), subscriptions);
                    }
                    print("%-36s | %13d | %d", execution.name(), subscriptions,
                            percentile(sorted(samples), 50));
                }
            }
        }
    }

    private record Mode(String name, DeliveryOptions options) {}

    private record Execution(String name, HandlerExecution execution) {}

    private record SlowRun(long total, int mostWaiting, long[] delays) {}

    /// For each delivery, how long its acknowledgement waited after its handlers returned, as
    /// measured and as the one queue and one queue per QoS type would make it, from the same
    /// handler return times.
    private record MixedRun(long[] measured, long[] oneQueue, long[] perType) {}

    /// Sends the messages from a raw peer at a steady pace and takes each PUBACK's delay after
    /// its PUBLISH, and the most deliveries that the client held at once.
    private static SlowRun slowRun(DeliveryOptions options) throws Exception {
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("measure/+"), Qos.AT_LEAST_ONCE),
                    options,
                    _ -> sleep(SLOW_HANDLER)
            );
            establish(peer, subscription, Qos.AT_LEAST_ONCE);
            var actor = (ClientActor) sut;
            var mostWaiting = new AtomicInteger();
            var sampling = new AtomicBoolean(true);
            Thread sampler = Thread.ofPlatform().start(() -> {
                while (sampling.get()) {
                    mostWaiting.accumulateAndGet(actor.usage().deliveries(), Math::max);
                    LockSupport.parkNanos(100_000);
                }
            });
            var sent = new AtomicLongArray(SLOW_MESSAGES + 1);
            var acknowledged = new AtomicLongArray(SLOW_MESSAGES + 1);
            // PUBACKs are read as they arrive, while the messages are still being sent.
            Thread reader = Thread.ofPlatform().start(() -> {
                for (int count = 0; count < SLOW_MESSAGES; count++) {
                    var acknowledgement = (Packet.PubAck) receive(peer);
                    acknowledged.set(acknowledgement.packetId(), System.nanoTime());
                }
            });
            long start = System.nanoTime();
            for (int id = 1; id <= SLOW_MESSAGES; id++) {
                LockSupport.parkNanos(start + id * ARRIVAL.toNanos() - System.nanoTime());
                sent.set(id, System.nanoTime());
                peer.send(new Packet.Publish(
                        new Topic("measure/" + id % SLOW_TOPICS),
                        new byte[] {1},
                        Qos.AT_LEAST_ONCE,
                        false,
                        false,
                        id,
                        EMPTY
                ));
            }
            reader.join();
            long total = System.nanoTime() - start;
            sampling.set(false);
            sampler.join();
            long[] delays = IntStream.rangeClosed(1, SLOW_MESSAGES)
                    .mapToLong(id -> acknowledged.get(id) - sent.get(id))
                    .sorted()
                    .toArray();
            return new SlowRun(total, mostWaiting.get(), delays);
        }
    }

    /// Sends QoS 1 and 2 messages in turn from a raw peer to `subscriptions` overlapping
    /// subscriptions, records when the last handler of each returns and when its PUBACK or
    /// PUBREC arrives, and completes each QoS 2 exchange.
    private static MixedRun mixedRun(DeliveryOptions options, int subscriptions) throws Exception {
        var returned = new AtomicLongArray(MIXED_MESSAGES + 1);
        Consumer<MqttDelivery> handler = delivery -> {
            int id = id(delivery.message().payload());
            sleep(id % SLOW_EVERY == 0 ? SLOWER_HANDLER : FAST_HANDLER);
            returned.accumulateAndGet(id, System.nanoTime(), Math::max);
        };
        var acknowledged = new long[MIXED_MESSAGES + 1];
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            List<String> filters = List.of("mixed/+", "mixed/#", "+/+", "#");
            for (String filter : filters.subList(0, subscriptions)) {
                Subscription subscription = sut.subscribe(
                        Subscribe.of(new TopicFilter(filter), Qos.EXACTLY_ONCE),
                        options,
                        handler
                );
                establish(peer, subscription, Qos.EXACTLY_ONCE);
            }
            for (int id = 1; id <= MIXED_MESSAGES; id++) {
                peer.send(new Packet.Publish(
                        new Topic("mixed/" + id % MIXED_TOPICS),
                        new byte[] {(byte) (id >> 8), (byte) id},
                        qosOf(id),
                        false,
                        false,
                        id,
                        EMPTY
                ));
            }
            int acknowledgements = 0;
            int completions = 0;
            while (acknowledgements < MIXED_MESSAGES || completions < MIXED_MESSAGES / 2) {
                switch (peer.receive()) {
                    case Packet.PubAck acknowledgement -> {
                        acknowledged[acknowledgement.packetId()] = System.nanoTime();
                        acknowledgements++;
                    }
                    case Packet.PubRec received -> {
                        acknowledged[received.packetId()] = System.nanoTime();
                        acknowledgements++;
                        peer.send(
                                new Packet.PubRel(received.packetId(), ReasonCode.SUCCESS, EMPTY)
                        );
                    }
                    case Packet.PubComp _ -> completions++;
                    case Packet other -> throw new AssertionError("Unexpected packet: " + other);
                }
            }
        }
        var measured = new long[MIXED_MESSAGES];
        var oneQueue = new long[MIXED_MESSAGES];
        var perType = new long[MIXED_MESSAGES];
        long latest = 0;
        long[] latestOfType = new long[Qos.values().length];
        for (int id = 1; id <= MIXED_MESSAGES; id++) {
            long done = returned.get(id);
            latest = Math.max(latest, done);
            int type = qosOf(id).ordinal();
            latestOfType[type] = Math.max(latestOfType[type], done);
            measured[id - 1] = acknowledged[id] - done;
            oneQueue[id - 1] = latest - done;
            perType[id - 1] = latestOfType[type] - done;
        }
        return new MixedRun(measured, oneQueue, perType);
    }

    /// Feeds QoS 0 deliveries of 64 topics straight into the delivery lanes, keeping at most
    /// [#WINDOW] copies per subscription waiting in all, and returns the time per delivery copy
    /// until no-op handlers ran them all. Each subscription's buffer holds every waiting copy, so
    /// a subscription that falls behind the others drops none.
    private static long handOffNanos(HandlerExecution execution, int subscriptions) {
        var handled = new AtomicLong();
        var lanes = new DeliveryLanes(
                "measure",
                Long.MAX_VALUE,
                (_, _) -> true,
                _ -> { },
                () -> { },
                _ -> { }
        );
        Subscribe request = Subscribe.of(new TopicFilter("#"), Qos.AT_MOST_ONCE)
                .withBufferCapacity(WINDOW * subscriptions);
        var ids = new ArrayList<Long>();
        for (long id = 1; id <= subscriptions; id++) {
            boolean _ = lanes.register(id, request, execution, _ -> handled.incrementAndGet());
            ids.add(id);
        }
        MqttMessage[] messages = IntStream.range(0, 64)
                .mapToObj(index -> MqttMessage.of(
                        new Topic("devices/" + index),
                        new byte[] {1},
                        Qos.AT_MOST_ONCE,
                        false
                ))
                .toArray(MqttMessage[]::new);
        var token = new DeliveryToken(1, 1, 1, 0);
        long copies = (long) HAND_OFFS * subscriptions;
        long start = System.nanoTime();
        for (int index = 0; index < HAND_OFFS; index++) {
            while (lanes.retainedDeliveries() >= WINDOW * subscriptions) {
                Thread.onSpinWait();
            }
            boolean _ = lanes.deliver(token, ids, messages[index % messages.length], false, 1);
        }
        while (handled.get() < copies) {
            Thread.onSpinWait();
        }
        long elapsed = System.nanoTime() - start;
        lanes.closeHandOffs();
        lanes.close(null);
        return elapsed / copies;
    }

    private static void establish(ScriptedBroker peer, Subscription subscription, Qos qos)
            throws Exception {
        var request = (Packet.Subscribe) peer.receive();
        peer.send(
                new Packet.SubAck(request.packetId(), List.of(ReasonCode.of(qos.level())), EMPTY)
        );
        subscription.established().get(5, SECONDS);
    }

    private static Packet receive(ScriptedBroker peer) {
        try {
            return peer.receive();
        } catch (IOException cause) {
            throw new UncheckedIOException(cause);
        }
    }

    private static Qos qosOf(int id) {
        return id % 2 == 1 ? Qos.AT_LEAST_ONCE : Qos.EXACTLY_ONCE;
    }

    private static int id(byte[] payload) {
        return (payload[0] & 0xFF) << 8 | payload[1] & 0xFF;
    }

    private static String carriers() {
        return "carriers " + System.getProperty(
                "jdk.virtualThreadScheduler.parallelism",
                String.valueOf(Runtime.getRuntime().availableProcessors())
        );
    }

    private static long[] sorted(long[] values) {
        long[] copy = values.clone();
        Arrays.sort(copy);
        return copy;
    }

    /// The value below which `percent` percent of the values in `sorted` lie.
    private static long percentile(long[] sorted, int percent) {
        return sorted[Math.min(sorted.length - 1, sorted.length * percent / 100)];
    }

    private static double mean(long[] values) {
        return Arrays.stream(values).average().orElse(0);
    }

    private static double millis(long nanos) {
        return nanos / 1e6;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException cause) {
            Thread.currentThread().interrupt();
            throw new AssertionError(cause);
        }
    }

    private static void print(String format, Object... arguments) {
        System.out.println(String.format(format, arguments));
    }
}
