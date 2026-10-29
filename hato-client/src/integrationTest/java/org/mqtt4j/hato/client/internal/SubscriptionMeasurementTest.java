package org.mqtt4j.hato.client.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.ClientStats;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mqtt4j.hato.client.internal.Measurements.awaitCount;
import static org.mqtt4j.hato.client.internal.Measurements.heapAfterGc;
import static org.mqtt4j.hato.client.internal.Measurements.median;
import static org.mqtt4j.hato.client.internal.Measurements.percentile;
import static org.mqtt4j.hato.client.internal.Measurements.print;
import static org.mqtt4j.hato.client.internal.Measurements.runtime;
import static org.mqtt4j.hato.client.internal.Measurements.sorted;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// Measures what subscriptions cost and prints the results instead of asserting limits: the
/// time to match a publication against many subscriptions and its share of receiving it, the
/// time to receive the copies that an MQTT 5.0 broker sends for overlapping subscriptions, the
/// memory that each subscription and each held acknowledgement condition takes, the time to
/// make every subscription again on a new session, and the time to take the client's stats.
/// Runs only with `-Phato.measure=true`.
@EnabledIfSystemProperty(named = "hato.measure", matches = "true")
@Timeout(1800)
class SubscriptionMeasurementTest {

    private static final List<Integer> SUBSCRIPTIONS = List.of(1, 256, 10_000, 100_000);

    private static final int RUNS = 5;

    private static final int MATCHES = 1_000_000;

    private static final int RECEPTIONS = 100_000;

    // The peer writes this many publications at once and keeps at most RECEIVE_WINDOW of them
    // unhandled, well within the subscription's buffer.
    private static final int RECEIVE_BATCH = 50;

    private static final int RECEIVE_WINDOW = 500;

    private static final int MEMORY_SUBSCRIPTIONS = 10_000;

    private static final int STATS_CALLS = 51;

    // Each of these receipts stays open, under the receive table's 4,096.
    private static final int OPEN_RECEIPTS = 4_000;

    // Eight levels give 256 distinct filters that all select the topic: each level is either
    // its own name or +.
    private static final String CONDITION_TOPIC = "a/b/c/d/e/f/g/h";

    @Test
    void matching_a_publication_against_many_subscriptions_costs() throws Exception {
        print(
                "%s: one filter of the form devices/<n>/# selects each topic devices/<n>/temp."
                        + " Matching in the filter tree, and receiving a QoS 0 publication"
                        + " through the client over each MQTT version, median of %d runs; under"
                        + " MQTT 5.0 each publication carries the identifier of its subscription",
                runtime(),
                RUNS
        );
        print(
                "%13s | %12s | %22s | %20s",
                "subscriptions",
                "ns per match",
                "ns per 3.1.1 reception",
                "ns per 5.0 reception"
        );
        for (int subscriptions : SUBSCRIPTIONS) {
            long match = median(RUNS, () -> matchNanos(subscriptions));
            long mqtt3 = median(RUNS, () -> receiveNanos(subscriptions, MqttVersion.V3_1_1));
            long mqtt5 = median(RUNS, () -> receiveNanos(subscriptions, MqttVersion.V5));
            print("%13d | %12d | %22d | %20d", subscriptions, match, mqtt3, mqtt5);
        }
    }

    @Test
    void receiving_copies_for_overlapping_subscriptions_by_identifier_costs() throws Exception {
        print(
                "%s: up to 256 distinct filters of %s select it, and the peer sends a copy of"
                        + " each QoS 0 publication for each subscription, carrying its"
                        + " identifier, as an MQTT 5.0 broker that does not merge copies does."
                        + " Time per copy through the client, median of %d runs",
                runtime(),
                CONDITION_TOPIC,
                RUNS
        );
        print("%13s | %s", "subscriptions", "ns per copy");
        for (int subscriptions : List.of(1, 16, 256)) {
            print("%13d | %d", subscriptions, median(RUNS, () -> copyNanos(subscriptions)));
        }
    }

    @Test
    void each_subscription_takes_memory() throws Exception {
        print(
                "%s: heap after GC per established subscription, %d subscriptions of each kind",
                runtime(),
                MEMORY_SUBSCRIPTIONS
        );
        print("%-12s | %s", "kind", "bytes per subscription");
        for (Kind kind : List.of(
                new Kind("serial", (client, request) -> client.subscribe(request, _ -> { })),
                new Kind("striped(16)", (client, request) -> client.subscribe(
                        request,
                        DeliveryOptions.striped(16),
                        _ -> { }
                )),
                new Kind("hand-off", (client, request) -> client.handOff(request, _ -> true)),
                new Kind("pull", MqttClient::subscribe)
        )) {
            print("%-12s | %d", kind.name(), subscriptionBytes(kind));
        }
    }

    @Test
    void each_held_acknowledgement_condition_takes_memory() throws Exception {
        print(
                "%s: %d open QoS 1 receipts, each matched by hand-off subscriptions that accepted"
                        + " it; heap after GC",
                runtime(),
                OPEN_RECEIPTS
        );
        print("%-40s | %9s | %10s | %8s", "conditions", "hand-offs", "held", "heap MiB");
        // A pull subscription that never receives keeps each receipt open, while the hand-offs'
        // conditions are met.
        long met = conditionBytes(63, false);
        long moreMet = conditionBytes(255, false);
        // Unacknowledged MANUAL hand-offs keep the receipts open themselves.
        long accepted = conditionBytes(64, true);
        long moreAccepted = conditionBytes(256, true);
        long perCondition = (long) OPEN_RECEIPTS * (255 - 63);
        print(
                "bytes per condition, from the differences: met %d, accepted and unacknowledged %d",
                (moreMet - met) / perCondition,
                (moreAccepted - accepted) / perCondition
        );
    }

    @Test
    void making_every_subscription_again_on_a_new_session_takes() throws Exception {
        print(
                "%s: from CONNACK of a new session to the last SUBACK, a peer that answers each"
                        + " SUBSCRIBE after the round trip",
                runtime()
        );
        print("%13s | %13s | %10s", "subscriptions", "round trip ms", "total ms");
        for (int subscriptions : List.of(256, 1_000, 10_000)) {
            for (Duration roundTrip : List.of(Duration.ZERO, Duration.ofMillis(10))) {
                long nanos = resubscribeNanos(subscriptions, roundTrip);
                print(
                        "%13d | %13d | %10.1f",
                        subscriptions,
                        roundTrip.toMillis(),
                        nanos / 1e6
                );
            }
        }
    }

    @Test
    void taking_stats_with_many_subscriptions_costs() throws Exception {
        print(
                "%s: time per stats() call with established pull subscriptions, median of %d"
                        + " calls",
                runtime(),
                STATS_CALLS
        );
        print("%13s | %s", "subscriptions", "microseconds per stats()");
        for (int subscriptions : SUBSCRIPTIONS) {
            print("%13d | %.1f", subscriptions, statsNanos(subscriptions) / 1e3);
        }
    }

    /// The time per match of a topic against a filter tree of `filters` filters.
    private static long matchNanos(int filters) {
        var tree = new FilterTree<Integer>();
        for (int index = 0; index < filters; index++) {
            tree.add(new TopicFilter("devices/" + index + "/#"), index);
        }
        var random = new SplittableRandom(1);
        var topics = new Topic[1_024];
        for (int index = 0; index < topics.length; index++) {
            topics[index] = new Topic("devices/" + random.nextInt(filters) + "/temp");
        }
        // Each topic selects one filter. Counting them keeps the matching from being optimized
        // away.
        var selected = new long[1];
        Consumer<Integer> sink = _ -> selected[0]++;
        long start = System.nanoTime();
        for (int index = 0; index < MATCHES; index++) {
            tree.match(topics[index % topics.length], sink);
        }
        long elapsed = System.nanoTime() - start;
        if (selected[0] != MATCHES) {
            throw new AssertionError("Selected " + selected[0] + " filters");
        }
        return elapsed / MATCHES;
    }

    /// Receives [#RECEPTIONS] QoS 0 publications over `version` through a client that holds
    /// `subscriptions` subscriptions, of which only one, with a no-op handler, selects them. The
    /// others are pull subscriptions, which add no delivery thread. Returns the time per
    /// publication.
    private static long receiveNanos(int subscriptions, MqttVersion version) throws Exception {
        var handled = new AtomicLong();
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = client(
                        server,
                        version,
                        subscriptions,
                        new LinkedBlockingQueue<>()
                );
                ScriptedBroker peer = ScriptedBroker.connect(server, sut, version)) {
            var requests = new ArrayList<Subscription>();
            requests.add(sut.subscribe(request("devices/0/#"), _ -> handled.incrementAndGet()));
            for (int index = 1; index < subscriptions; index++) {
                requests.add(sut.subscribe(request("devices/" + index + "/#")));
            }
            // The client sends its SUBSCRIBE packets in the order of the calls, so the first
            // made the subscription that takes the publications.
            List<Packet.Subscribe> made = establish(peer, requests);
            byte[][] batches = batches(
                    PacketCodec.of(version),
                    new Topic("devices/0/temp"),
                    List.of(ScriptedBroker.forwarding(made.getFirst()))
            );
            return feed(peer, batches, handled);
        }
    }

    /// Receives [#RECEPTIONS] QoS 0 copies through an MQTT 5.0 client whose `subscriptions`
    /// subscriptions all select [#CONDITION_TOPIC], each with a no-op handler. Each copy carries
    /// the identifier of one subscription, in turn, so it goes to that subscription alone.
    /// Returns the time per copy.
    private static long copyNanos(int subscriptions) throws Exception {
        var handled = new AtomicLong();
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = client(
                        server,
                        MqttVersion.V5,
                        subscriptions,
                        new LinkedBlockingQueue<>()
                );
                ScriptedBroker peer = ScriptedBroker.connect(server, sut, MqttVersion.V5)) {
            var requests = new ArrayList<Subscription>();
            for (int mask = 0; mask < subscriptions; mask++) {
                requests.add(sut.subscribe(
                        request(conditionFilter(mask)),
                        _ -> handled.incrementAndGet()
                ));
            }
            List<Properties> copies = establish(peer, requests).stream()
                    .map(ScriptedBroker::forwarding)
                    .toList();
            byte[][] batches = batches(
                    PacketCodec.of(MqttVersion.V5),
                    new Topic(CONDITION_TOPIC),
                    copies
            );
            return feed(peer, batches, handled);
        }
    }

    /// The writes that carry [#RECEPTIONS] QoS 0 publications to `topic`, [#RECEIVE_BATCH] in
    /// each, encoded by `codec`. Each publication carries the next entry of `properties`, in
    /// turn.
    private static byte[][] batches(PacketCodec codec, Topic topic, List<Properties> properties) {
        var batches = new byte[RECEPTIONS / RECEIVE_BATCH][];
        for (int batch = 0; batch < batches.length; batch++) {
            var bytes = new ByteArrayOutputStream();
            for (int index = 0; index < RECEIVE_BATCH; index++) {
                int publication = batch * RECEIVE_BATCH + index;
                bytes.writeBytes(codec.encode(new Packet.Publish(
                        topic,
                        new byte[] {1},
                        Qos.AT_MOST_ONCE,
                        false,
                        false,
                        0,
                        properties.get(publication % properties.size())
                )));
            }
            batches[batch] = bytes.toByteArray();
        }
        return batches;
    }

    /// Writes `batches` while at most [#RECEIVE_WINDOW] of their publications wait for their
    /// handlers, and returns the time per publication until `handled` counts all of them.
    private static long feed(ScriptedBroker peer, byte[][] batches, AtomicLong handled)
            throws IOException {
        long start = System.nanoTime();
        for (int batch = 0; batch < batches.length; batch++) {
            awaitCount(handled, (long) (batch + 1) * RECEIVE_BATCH - RECEIVE_WINDOW);
            peer.send(batches[batch]);
        }
        awaitCount(handled, RECEPTIONS);
        return (System.nanoTime() - start) / RECEPTIONS;
    }

    /// The heap per subscription of `kind` that a connected client holds once
    /// [#MEMORY_SUBSCRIPTIONS] of them are established. The client has terminated when this
    /// returns, so that what it held does not count toward the next measurement.
    private static long subscriptionBytes(Kind kind) throws Exception {
        try (ServerSocket server = ScriptedBroker.listen()) {
            MqttClient sut = client(server, MEMORY_SUBSCRIPTIONS, new LinkedBlockingQueue<>());
            try (sut; ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                long before = heapAfterGc();
                var subscriptions = new ArrayList<Subscription>();
                for (int index = 0; index < MEMORY_SUBSCRIPTIONS; index++) {
                    subscriptions.add(
                            kind.subscriber().subscribe(sut, request("memory/" + index))
                    );
                }
                establish(peer, subscriptions);
                long after = heapAfterGc();
                return (after - before) / MEMORY_SUBSCRIPTIONS;
            } finally {
                sut.terminated().get(1, TimeUnit.MINUTES);
            }
        }
    }

    /// Opens [#OPEN_RECEIPTS] QoS 1 receipts, each matched by `handOffs` hand-off subscriptions
    /// that accept at once, prints the conditions held and the heap they add, and returns that
    /// heap in bytes. An accepted hand-off returns its capacity; a push delivery would keep its
    /// capacity until the receipt's PUBACK. With `manual`, the hand-offs acknowledge manually
    /// and never do, so the client keeps each accepted delivery until its `ack()`. Otherwise
    /// their conditions are met at acceptance, and a pull subscription that never receives keeps
    /// the receipts open. The client has terminated when this returns.
    private static long conditionBytes(int handOffs, boolean manual) throws Exception {
        var codec = PacketCodec.of(MqttVersion.V3_1_1);
        var handled = new AtomicLong();
        try (ServerSocket server = ScriptedBroker.listen()) {
            MqttClient sut = client(server, handOffs + 1, new LinkedBlockingQueue<>());
            try (sut; ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                var subscriptions = new ArrayList<Subscription>();
                // Each subscription takes the next filter of the topic.
                int first = 0;
                if (!manual) {
                    subscriptions.add(sut.subscribe(
                            Subscribe.of(new TopicFilter(conditionFilter(0)), Qos.AT_LEAST_ONCE)
                                    .withBufferCapacity(OPEN_RECEIPTS)
                    ));
                    first = 1;
                }
                AckMode ackMode = manual ? AckMode.MANUAL : AckMode.ON_RECEIVE;
                for (int mask = first; mask < first + handOffs; mask++) {
                    subscriptions.add(sut.handOff(
                            Subscribe.of(new TopicFilter(conditionFilter(mask)), Qos.AT_LEAST_ONCE)
                                    .withAckMode(ackMode),
                            _ -> {
                                handled.incrementAndGet();
                                return true;
                            }
                    ));
                }
                establish(peer, subscriptions);
                long before = heapAfterGc();
                for (int id = 1; id <= OPEN_RECEIPTS; id++) {
                    // At most four publications wait for the acceptors, so no hand-off lane holds
                    // more than its buffer.
                    awaitCount(handled, (long) (id - 4) * handOffs);
                    peer.send(codec.encode(new Packet.Publish(
                            new Topic(CONDITION_TOPIC),
                            new byte[16],
                            Qos.AT_LEAST_ONCE,
                            false,
                            false,
                            id,
                            EMPTY
                    )));
                }
                long conditions = (long) OPEN_RECEIPTS * subscriptions.size();
                long deadline = System.nanoTime() + SECONDS.toNanos(60);
                while (heldConditions(sut) < conditions) {
                    if (System.nanoTime() > deadline) {
                        throw new AssertionError(
                                "Only " + heldConditions(sut) + " conditions held"
                        );
                    }
                    Thread.sleep(10);
                }
                long added = heapAfterGc() - before;
                print(
                        "%-40s | %9d | %10d | %8.1f",
                        manual ? "accepted, unacknowledged" : "met, receipts kept open by a pull",
                        handOffs,
                        conditions,
                        added / (1024.0 * 1024.0)
                );
                return added;
            } finally {
                sut.terminated().get(1, TimeUnit.MINUTES);
            }
        }
    }

    /// The median time of a `stats()` call on a client that holds `subscriptions` established
    /// pull subscriptions, after as many calls that warm up the code paths.
    private static long statsNanos(int subscriptions) throws Exception {
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = client(server, subscriptions, new LinkedBlockingQueue<>());
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            var requests = new ArrayList<Subscription>();
            for (int index = 0; index < subscriptions; index++) {
                requests.add(sut.subscribe(request("stats/" + index)));
            }
            establish(peer, requests);
            for (int index = 0; index < STATS_CALLS; index++) {
                ClientStats _ = sut.stats().get(30, SECONDS);
            }
            var samples = new long[STATS_CALLS];
            for (int index = 0; index < STATS_CALLS; index++) {
                long start = System.nanoTime();
                ClientStats _ = sut.stats().get(30, SECONDS);
                samples[index] = System.nanoTime() - start;
            }
            return percentile(sorted(samples), 50);
        }
    }

    /// The time from the CONNACK of a new session to the last SUBACK for `subscriptions`
    /// subscriptions that the client makes again, with each SUBACK sent `roundTrip` after its
    /// SUBSCRIBE arrived.
    private static long resubscribeNanos(int subscriptions, Duration roundTrip) throws Exception {
        var events = new LinkedBlockingQueue<String>();
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = client(server, subscriptions, events)) {
            var requests = new ArrayList<Subscription>();
            try (ScriptedBroker first = ScriptedBroker.connect(server, sut)) {
                for (int index = 0; index < subscriptions; index++) {
                    requests.add(sut.subscribe(request("again/" + index)));
                }
                establish(first, requests);
            }
            await(events, "connected");
            await(events, "disconnected");
            CompletableFuture<ConnAck> reconnected = sut.connect();
            try (ScriptedBroker peer = ScriptedBroker.accept(server);
                    ScheduledExecutorService answers = Executors.newScheduledThreadPool(1)) {
                if (!(peer.receive() instanceof Packet.Connect)) {
                    throw new AssertionError("Expected CONNECT");
                }
                long start = System.nanoTime();
                peer.send(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));
                reconnected.get(5, SECONDS);
                var answered = new CountDownLatch(subscriptions);
                var last = new AtomicLong();
                for (int index = 0; index < subscriptions; index++) {
                    var subscribe = (Packet.Subscribe) peer.receive();
                    Runnable answer = () -> {
                        try {
                            peer.send(subAck(subscribe));
                        } catch (IOException cause) {
                            throw new UncheckedIOException(cause);
                        }
                        last.set(System.nanoTime());
                        answered.countDown();
                    };
                    if (roundTrip.isZero()) {
                        answer.run();
                    } else {
                        answers.schedule(answer, roundTrip.toNanos(), TimeUnit.NANOSECONDS);
                    }
                }
                if (!answered.await(5, TimeUnit.MINUTES)) {
                    throw new AssertionError("Not every SUBSCRIBE was answered");
                }
                return last.get() - start;
            }
        }
    }

    /// Answers SUBSCRIBE packets at once until every one of `subscriptions` is established, and
    /// returns the packets in the order in which they arrived.
    private static List<Packet.Subscribe> establish(
            ScriptedBroker peer,
            List<Subscription> subscriptions
    ) throws Exception {
        var made = new ArrayList<Packet.Subscribe>();
        for (int index = 0; index < subscriptions.size(); index++) {
            var subscribe = (Packet.Subscribe) peer.receive();
            peer.send(subAck(subscribe));
            made.add(subscribe);
        }
        for (Subscription subscription : subscriptions) {
            subscription.established().get(5, SECONDS);
        }
        return made;
    }

    private static Packet.SubAck subAck(Packet.Subscribe subscribe) {
        return new Packet.SubAck(
                subscribe.packetId(),
                subscribe.subscriptions().stream()
                        .map(entry -> ReasonCode.of(entry.qos().level()))
                        .toList(),
                EMPTY
        );
    }

    /// A client that may hold `subscriptions` subscriptions, does not reconnect by itself, and
    /// adds its connection events to `events`.
    private static MqttClient client(
            ServerSocket server,
            int subscriptions,
            LinkedBlockingQueue<String> events
    ) {
        return client(server, MqttVersion.V3_1_1, subscriptions, events);
    }

    /// A client like [#client(ServerSocket, int, LinkedBlockingQueue)] that speaks `version`.
    private static MqttClient client(
            ServerSocket server,
            MqttVersion version,
            int subscriptions,
            LinkedBlockingQueue<String> events
    ) {
        return MqttClient.builder()
                .server(ScriptedBroker.endpoint(server))
                .version(version)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .maxSubscriptions(subscriptions)
                .connectionListener(new ConnectionListener() {
                    @Override
                    public void onConnected(ConnAck ack) {
                        events.add("connected");
                    }

                    @Override
                    public void onDisconnected(Disconnection disconnection) {
                        events.add("disconnected");
                    }
                })
                .build();
    }

    private static Subscribe request(String filter) {
        return Subscribe.of(new TopicFilter(filter), Qos.AT_MOST_ONCE);
    }

    /// The filter of [#CONDITION_TOPIC] whose levels that `mask` names are +.
    private static String conditionFilter(int mask) {
        String[] levels = CONDITION_TOPIC.split("/");
        for (int level = 0; level < levels.length; level++) {
            if ((mask & 1 << level) != 0) {
                levels[level] = "+";
            }
        }
        return String.join("/", levels);
    }

    private static long heldConditions(MqttClient client) throws Exception {
        ClientStats stats = client.stats().get(5, SECONDS);
        return stats.receiving().heldConditions().used();
    }

    private static void await(LinkedBlockingQueue<String> events, String expected)
            throws InterruptedException {
        String event = events.poll(5, SECONDS);
        if (!expected.equals(event)) {
            throw new IllegalStateException("Expected " + expected + " but got " + event);
        }
    }

    /// A kind of subscription: `subscriber` subscribes `request` on a client.
    private record Kind(String name, Subscriber subscriber) {}

    @FunctionalInterface
    private interface Subscriber {

        Subscription subscribe(MqttClient client, Subscribe request);
    }
}
