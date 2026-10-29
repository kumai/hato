package org.mqtt4j.hato.client.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PullSubscription;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.DeliveryToken.Origin;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mqtt4j.hato.client.internal.Measurements.heapAfterGc;
import static org.mqtt4j.hato.client.internal.Measurements.median;
import static org.mqtt4j.hato.client.internal.Measurements.percentile;
import static org.mqtt4j.hato.client.internal.Measurements.print;
import static org.mqtt4j.hato.client.internal.Measurements.runtime;
import static org.mqtt4j.hato.client.internal.Measurements.sorted;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// Measures the delivery modes and prints the results instead of asserting limits: how a slow
/// handler backs up deliveries and their acknowledgements under serial and striped delivery, how
/// long finished deliveries wait behind slower ones in the one receive-order acknowledgement
/// queue, what handing deliveries from the dispatcher to drain tasks costs, what receiving a
/// publication through the whole client costs for each way of receiving, with and without a
/// metrics listener, and how much heap a delivery takes while it waits behind a blocked handler.
/// Runs only with `-Phato.measure=true`; the integrationTestSingleCarrier task measures the worst
/// case of one carrier thread.
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

    private static final int RECEPTIONS = 100_000;

    // The peer writes this many publications at once and keeps at most RECEIVE_WINDOW of them
    // unfinished, well within the client's receive table and the subscription's buffer.
    private static final int RECEIVE_BATCH = 50;

    private static final int RECEIVE_WINDOW = 500;

    // A blocked handler holds this many deliveries, all within its subscription's buffer and,
    // at QoS 1, within the receipts that the client keeps open for acknowledgement.
    private static final int WAITING = 4_000;

    private static final int WAITING_PAYLOAD_BYTES = 16;

    @Test
    void a_slow_handler_backs_up_deliveries_and_acknowledgements() throws Exception {
        print(
                "%s: %d QoS 1 messages over %d topics, one every %.1f ms, %d ms per handler",
                runtime(),
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
                runtime(),
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
                runtime(),
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
                    long nanos = median(
                            RUNS,
                            () -> handOffNanos(execution.execution(), subscriptions)
                    );
                    print("%-36s | %13d | %d", execution.name(), subscriptions, nanos);
                }
            }
        }
    }

    @Test
    void receiving_publications_through_the_client_costs() throws Exception {
        print(
                "%s: nanoseconds per publication received through the client, median of %d runs"
                        + " of %d publications, no-op handlers",
                runtime(),
                RUNS,
                RECEPTIONS
        );
        print(
                "%-24s | %12s | %13s | %12s",
                "receiver",
                Qos.AT_MOST_ONCE,
                Qos.AT_LEAST_ONCE,
                Qos.EXACTLY_ONCE
        );
        for (Receiver receiver : List.of(
                new Receiver("serial", false, DeliveryMeasurementTest::serial),
                new Receiver("serial, metrics listener", true, DeliveryMeasurementTest::serial),
                new Receiver("striped(4)", false, DeliveryMeasurementTest::striped),
                new Receiver("parallel(4)", false, DeliveryMeasurementTest::parallel),
                new Receiver("hand-off", false, DeliveryMeasurementTest::handOff),
                new Receiver("pull", false, DeliveryMeasurementTest::pull)
        )) {
            var nanos = new long[Qos.values().length];
            for (Qos qos : Qos.values()) {
                nanos[qos.ordinal()] = median(RUNS, () -> receiveNanos(qos, receiver));
            }
            print("%-24s | %12d | %13d | %12d", receiver.name(), nanos[0], nanos[1], nanos[2]);
        }
    }

    @Test
    void each_waiting_delivery_takes_memory() throws Exception {
        print(
                "%s: heap after GC per delivery held behind a blocked handler, %d deliveries of"
                        + " %d-byte payloads over 64 topics",
                runtime(),
                WAITING,
                WAITING_PAYLOAD_BYTES
        );
        print("%-14s | %s", "qos", "bytes per held delivery");
        for (Qos qos : List.of(Qos.AT_MOST_ONCE, Qos.AT_LEAST_ONCE)) {
            print("%-14s | %d", qos, waitingBytes(qos));
        }
    }

    private record Mode(String name, DeliveryOptions options) {}

    private record Execution(String name, HandlerExecution execution) {}

    /// A way of receiving: `subscriber` subscribes, and `metrics` says whether the client reports
    /// its timings to a metrics listener.
    private record Receiver(String name, boolean metrics, Subscriber subscriber) {}

    @FunctionalInterface
    private interface Subscriber {

        /// Subscribes `request` on `client`, counting in `handled` each delivery once application
        /// code has it.
        Subscription subscribe(MqttClient client, Subscribe request, AtomicLong handled);
    }

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
        var latestOfType = new long[Qos.values().length];
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
                256,
                DeliveryLanes.CONDITION_LIMIT,
                (_, _) -> true,
                _ -> { },
                (_, _) -> { },
                _ -> { },
                null,
                new InlineHandlers()
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
        var token = new DeliveryToken(new Origin(1, 1, 1), 0, DeliveryToken.UNTIMED);
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

    /// Sends [#RECEPTIONS] publications at `qos` from a raw peer to a subscription of
    /// `receiver`, and returns the time per publication until each has finished: QoS 0 once
    /// application code has it, QoS 1 at PUBACK, and QoS 2 at PUBCOMP, after the peer has
    /// answered PUBREC. The publications are encoded before the clock starts, so the time is
    /// mostly the client's.
    private static long receiveNanos(Qos qos, Receiver receiver) throws Exception {
        var codec = PacketCodec.of(MqttVersion.V3_1_1);
        var batches = new byte[RECEPTIONS / RECEIVE_BATCH][];
        for (int batch = 0; batch < batches.length; batch++) {
            var bytes = new ByteArrayOutputStream();
            for (int index = batch * RECEIVE_BATCH; index < (batch + 1) * RECEIVE_BATCH; index++) {
                bytes.writeBytes(codec.encode(new Packet.Publish(
                        new Topic("receive/" + index % 64),
                        new byte[] {1},
                        qos,
                        false,
                        false,
                        qos == Qos.AT_MOST_ONCE ? 0 : index % 65_535 + 1,
                        EMPTY
                )));
            }
            batches[batch] = bytes.toByteArray();
        }
        var handled = new AtomicLong();
        var acknowledged = new AtomicLong();
        AtomicLong finished = qos == Qos.AT_MOST_ONCE ? handled : acknowledged;
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = receiver.metrics()
                        ? ScriptedBroker.client(server, Measurements.countingListener())
                        : ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            Subscription subscription = receiver.subscriber().subscribe(
                    sut,
                    Subscribe.of(new TopicFilter("receive/+"), qos),
                    handled
            );
            establish(peer, subscription, qos);
            // The reader answers PUBREC on the socket that the sender writes, so they write in
            // turn.
            Thread reader = Thread.ofPlatform().start(() -> {
                while (qos != Qos.AT_MOST_ONCE && acknowledged.get() < RECEPTIONS) {
                    switch (receive(peer)) {
                        case Packet.PubAck _, Packet.PubComp _ -> acknowledged.incrementAndGet();
                        case Packet.PubRec received -> {
                            var release = new Packet.PubRel(
                                    received.packetId(),
                                    ReasonCode.SUCCESS,
                                    EMPTY
                            );
                            send(peer, codec.encode(release));
                        }
                        case Packet other -> throw new AssertionError(
                                "Unexpected packet: " + other
                        );
                    }
                }
            });
            long start = System.nanoTime();
            for (int batch = 0; batch < batches.length; batch++) {
                long sent = (long) (batch + 1) * RECEIVE_BATCH;
                while (sent - finished.get() > RECEIVE_WINDOW) {
                    Thread.onSpinWait();
                }
                send(peer, batches[batch]);
            }
            while (finished.get() < RECEPTIONS) {
                Thread.onSpinWait();
            }
            long elapsed = System.nanoTime() - start;
            reader.join();
            return elapsed / RECEPTIONS;
        }
    }

    /// Sends [#WAITING] publications at `qos` from a raw peer to a subscription whose handler
    /// blocks on the first of them, and returns the heap that each delivery takes while the client
    /// holds it: the one in the handler and the others waiting in the subscription's buffer, with
    /// their acknowledgements held until the handler returns.
    private static long waitingBytes(Qos qos) throws Exception {
        var release = new CountDownLatch(1);
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("waiting/+"), qos).withBufferCapacity(WAITING),
                    _ -> {
                        try {
                            release.await();
                        } catch (InterruptedException _) {
                            Thread.currentThread().interrupt();
                        }
                    }
            );
            establish(peer, subscription, qos);
            var actor = (ClientActor) sut;
            long before = heapAfterGc();
            for (int id = 1; id <= WAITING; id++) {
                peer.send(new Packet.Publish(
                        new Topic("waiting/" + id % 64),
                        new byte[WAITING_PAYLOAD_BYTES],
                        qos,
                        false,
                        false,
                        qos == Qos.AT_MOST_ONCE ? 0 : id,
                        EMPTY
                ));
            }
            long deadline = System.nanoTime() + SECONDS.toNanos(60);
            while (actor.usage().deliveries() < WAITING) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("Held " + actor.usage().deliveries() + " deliveries");
                }
                Thread.sleep(1);
            }
            long after = heapAfterGc();
            release.countDown();
            return (after - before) / WAITING;
        }
    }

    private static Subscription serial(MqttClient client, Subscribe request, AtomicLong handled) {
        return client.subscribe(request, _ -> handled.incrementAndGet());
    }

    private static Subscription striped(MqttClient client, Subscribe request, AtomicLong handled) {
        return client.subscribe(
                request,
                DeliveryOptions.striped(4),
                _ -> handled.incrementAndGet()
        );
    }

    private static Subscription parallel(
            MqttClient client,
            Subscribe request,
            AtomicLong handled
    ) {
        return client.subscribe(
                request,
                DeliveryOptions.parallel(4),
                _ -> handled.incrementAndGet()
        );
    }

    private static Subscription handOff(MqttClient client, Subscribe request, AtomicLong handled) {
        return client.handOff(request, _ -> {
            handled.incrementAndGet();
            return true;
        });
    }

    /// A pull subscription whose own virtual thread receives every delivery until the
    /// subscription ends.
    private static Subscription pull(MqttClient client, Subscribe request, AtomicLong handled) {
        PullSubscription subscription = client.subscribe(request);
        Thread.ofVirtual().start(() -> {
            try {
                while (true) {
                    switch (subscription.receive(Duration.ofSeconds(1))) {
                        case ReceiveResult.Received _ -> handled.incrementAndGet();
                        case ReceiveResult.TimedOut _ -> {
                            // The next receive waits on.
                        }
                        case ReceiveResult.Ended _ -> {
                            return;
                        }
                    }
                }
            } catch (InterruptedException _) {
                // Nothing interrupts the receiver.
            }
        });
        return subscription;
    }

    private static void send(ScriptedBroker peer, byte[] bytes) {
        synchronized (peer) {
            try {
                peer.send(bytes);
            } catch (IOException cause) {
                throw new UncheckedIOException(cause);
            }
        }
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
}
