package org.mqtt4j.hato.broker.internal;

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mqtt4j.hato.broker.internal.BrokerSessionRaceTest.awaitRelease;
import static org.mqtt4j.hato.broker.internal.RawSession.acceptedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.codec;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.GRANTED_QOS_1;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// How the sessions of one broker run at once: each handles the packets of its connections in
/// order, while the others go on.
///
/// A session stalls on a hook that its actor calls as it starts to handle a packet. The hook
/// holds the thread of the actor pool that runs the session, so the other sessions go on only
/// while the pool has another thread.
class BrokerConcurrencyTest {

    @Test
    void a_session_that_stalls_on_a_packet_holds_back_no_other_session() throws Exception {
        var stall = new Stall("stall");
        try (BrokerServer sut = BrokerServer.builder()
                .beforeHandle(stall)
                .start(loopback());
                RawSession watcher = connected(sut.port(), "watcher");
                RawSession stalling = connected(sut.port(), "stalling");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            watcher.subscribe("news", AT_MOST_ONCE);
            stalling.subscribe("news", AT_MOST_ONCE);
            stalling.send(publication("stall", "stop here", AT_MOST_ONCE, 0));
            stall.reached.get(5, SECONDS);

            // Act: the PUBACK follows the deliveries to both subscribers.
            publisher.publish("news", "while stalled", AT_LEAST_ONCE, 1);

            // Assert: the stalled session receives the publication once it goes on.
            assertThat(watcher.expect(Packet.Publish.class))
                    .isEqualTo(publication("news", "while stalled", AT_MOST_ONCE, 0));
            stall.release.countDown();
            assertThat(stalling.expect(Packet.Publish.class))
                    .isEqualTo(publication("news", "while stalled", AT_MOST_ONCE, 0));
        } finally {
            stall.release.countDown();
        }
    }

    @Test
    void a_will_whose_delay_passes_goes_out_while_a_session_that_receives_it_stalls()
            throws Exception {
        var timer = new BrokerDeadlineTest.ManualTimer();
        var stall = new Stall("stall");
        try (BrokerServer sut = BrokerServer.builder()
                .scheduler(timer)
                .beforeHandle(stall)
                .start(loopback());
                RawSession watcher = connected(sut.port(), "watcher");
                RawSession stalling = connected(sut.port(), "stalling")) {
            // Arrange: a client leaves with a Will that waits 10 seconds, and a session that
            // receives it stalls.
            watcher.subscribe("left/#", AT_MOST_ONCE);
            stalling.subscribe("left/#", AT_MOST_ONCE);
            RawSession leaving = RawSession.open(sut.port(), MqttVersion.V5);
            leaving.send(connectWithDelayedWill("leaving", 10));
            assertThat(leaving.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(false));
            leaving.close();
            timer.awaitPendingTasks(1);
            stalling.send(publication("stall", "stop here", AT_MOST_ONCE, 0));
            stall.reached.get(5, SECONDS);

            // Act: the timer only hands the Will to its session, so it waits for no other.
            assertTimeoutPreemptively(
                    Duration.ofSeconds(5),
                    () -> timer.clock.advance(Duration.ofSeconds(10))
            );

            // Assert
            assertThat(watcher.expect(Packet.Publish.class).topic())
                    .isEqualTo(new Topic("left/leaving"));
            stall.release.countDown();
            assertThat(stalling.expect(Packet.Publish.class).topic())
                    .isEqualTo(new Topic("left/leaving"));
        } finally {
            stall.release.countDown();
        }
    }

    @Test
    void publications_from_several_publishers_reach_a_subscriber_in_each_publishers_order()
            throws Exception {
        int publishers = 4;
        int count = 500;
        try (BrokerServer sut = BrokerServer.builder()
                .maxQueued(publishers * count)
                .start(loopback());
                RawSession subscriber = connected(sut.port(), "subscriber");
                ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            // Arrange
            subscriber.subscribe("order", AT_LEAST_ONCE);
            var sessions = new ArrayList<RawSession>();
            for (int index = 0; index < publishers; index++) {
                sessions.add(connected(sut.port(), "publisher-" + index));
            }

            // Act: each publisher sends all of its publications at once, without waiting for
            // their PUBACKs.
            var sent = new ArrayList<Future<?>>();
            for (int index = 0; index < publishers; index++) {
                RawSession publisher = sessions.get(index);
                var burst = new ByteArrayOutputStream();
                for (int number = 1; number <= count; number++) {
                    burst.writeBytes(codec.encode(publication(
                            "order",
                            index + ":" + number,
                            AT_LEAST_ONCE,
                            number
                    )));
                }
                sent.add(tasks.submit(() -> {
                    publisher.sendRaw(burst.toByteArray());
                    for (int number = 1; number <= count; number++) {
                        publisher.expect(Packet.PubAck.class);
                    }
                    return null;
                }));
            }

            // Assert: every publication arrives, each publisher's in the order it sent them.
            var next = new int[publishers];
            for (int received = 0; received < publishers * count; received++) {
                Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
                subscriber.send(new Packet.PubAck(delivery.packetId(), SUCCESS, EMPTY));
                String[] parts = new String(delivery.payload(), UTF_8).split(":");
                int index = Integer.parseInt(parts[0]);
                assertThat(Integer.parseInt(parts[1]))
                        .as("the next publication of publisher %d", index)
                        .isEqualTo(++next[index]);
            }
            for (Future<?> burst : sent) {
                burst.get(5, SECONDS);
            }
            subscriber.ping();
            for (RawSession publisher : sessions) {
                publisher.close();
            }
        }
    }

    @RepeatedTest(10)
    void subscriptions_made_among_retained_publications_receive_each_once_and_in_order()
            throws Exception {
        int subscribers = 8;
        int chunk = 50;
        int count = subscribers * chunk;
        try (BrokerServer sut = BrokerServer.builder()
                .maxQueued(count)
                .start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            var sessions = new ArrayList<RawSession>();
            for (int index = 0; index < subscribers; index++) {
                sessions.add(connected(sut.port(), "subscriber-" + index));
            }

            // Act: the publisher replaces the retained message again and again, and a subscriber
            // subscribes while the broker takes each chunk of the publications.
            for (int index = 0; index < subscribers; index++) {
                var burst = new ByteArrayOutputStream();
                for (int number = index * chunk + 1; number <= (index + 1) * chunk; number++) {
                    burst.writeBytes(codec.encode(new Packet.Publish(
                            new Topic("state"),
                            Integer.toString(number).getBytes(UTF_8),
                            AT_LEAST_ONCE,
                            true,
                            false,
                            number,
                            EMPTY
                    )));
                }
                publisher.sendRaw(burst.toByteArray());
                sessions.get(index).send(new Packet.Subscribe(
                        1,
                        List.of(subscription("state", AT_LEAST_ONCE)),
                        EMPTY
                ));
            }

            // Assert: each subscription begins with the retained message of its moment, or with
            // the first publication if none was retained yet, and then receives every later one,
            // once.
            for (RawSession subscriber : sessions) {
                assertThat(subscriber.expect(Packet.SubAck.class))
                        .isEqualTo(new Packet.SubAck(1, List.of(GRANTED_QOS_1), EMPTY));
                Packet.Publish first = subscriber.expect(Packet.Publish.class);
                subscriber.send(new Packet.PubAck(first.packetId(), SUCCESS, EMPTY));
                int begun = Integer.parseInt(new String(first.payload(), UTF_8));
                assertThat(first.retain() || begun == 1)
                        .as("a live first delivery of %d", begun)
                        .isTrue();
                for (int number = begun + 1; number <= count; number++) {
                    Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
                    subscriber.send(new Packet.PubAck(delivery.packetId(), SUCCESS, EMPTY));
                    assertThat(delivery.retain()).isFalse();
                    assertThat(new String(delivery.payload(), UTF_8))
                            .isEqualTo(Integer.toString(number));
                }
                subscriber.ping();
            }
            for (int number = 1; number <= count; number++) {
                publisher.expect(Packet.PubAck.class);
            }
            for (RawSession subscriber : sessions) {
                subscriber.close();
            }
        }
    }

    @Test
    void a_session_handles_its_packets_one_at_a_time_in_the_order_received() throws Exception {
        int count = 50;
        var handling = new AtomicInteger();
        var most = new AtomicInteger();
        var handled = new ConcurrentLinkedQueue<String>();
        try (BrokerServer sut = BrokerServer.builder()
                .beforeHandle(packet -> {
                    if (packet instanceof Packet.Publish publish
                            && publish.topic().name().equals("sequence")) {
                        most.accumulateAndGet(handling.incrementAndGet(), Math::max);
                        handled.add(new String(publish.payload(), UTF_8));
                        // Long enough for another thread to overlap, if one could.
                        LockSupport.parkNanos(200_000);
                        handling.decrementAndGet();
                    }
                })
                .start(loopback());
                RawSession client = connected(sut.port(), "client")) {
            // Arrange
            var burst = new ByteArrayOutputStream();
            for (int number = 1; number <= count; number++) {
                burst.writeBytes(codec.encode(publication(
                        "sequence",
                        Integer.toString(number),
                        AT_MOST_ONCE,
                        0
                )));
            }

            // Act
            client.sendRaw(burst.toByteArray());
            client.ping();

            // Assert
            assertThat(most).hasValue(1);
            assertThat(handled).containsExactlyElementsOf(
                    IntStream.rangeClosed(1, count).mapToObj(Integer::toString).toList()
            );
        }
    }

    @Test
    void a_session_with_a_long_backlog_lets_a_waiting_session_take_its_turn() throws Exception {
        int workers = 2;
        int backlog = 2_000;
        var busy = new CountDownLatch(workers);
        var release = new CountDownLatch(1);
        var queued = new CountDownLatch(1);
        var handled = new AtomicInteger();
        var handledBefore = new CompletableFuture<Integer>();
        try (BrokerServer sut = BrokerServer.builder()
                .actors(workers)
                .beforeProcess(packet -> {
                    if (packet instanceof Packet.Publish publish
                            && publish.topic().name().equals("queued")) {
                        queued.countDown();
                    }
                })
                .beforeHandle(packet -> {
                    if (!(packet instanceof Packet.Publish publish)) {
                        return;
                    }
                    if (publish.topic().name().equals("backlog")) {
                        if (handled.getAndIncrement() < workers) {
                            busy.countDown();
                            awaitRelease(release);
                        }
                        // Slower than the selector hands the next publications over, so that
                        // the session never runs out of them.
                        LockSupport.parkNanos(50_000);
                    } else if (publish.topic().name().equals("waiting")) {
                        handledBefore.complete(handled.get());
                    }
                })
                .start(loopback());
                RawSession first = connected(sut.port(), "first");
                RawSession second = connected(sut.port(), "second");
                RawSession waiting = connected(sut.port(), "waiting")) {
            // Arrange: each thread of the pool stalls on the first publication of one of two
            // sessions, which both have many more to handle. Their selectors hand the next ones
            // over as the sessions take them, which a request that waits for room for its
            // response would not do before the turn ends. The third session's publication
            // waits for a thread: its selector has handed it over once it decodes the next one.
            var publications = new ByteArrayOutputStream();
            for (int number = 0; number < backlog; number++) {
                publications.writeBytes(codec.encode(publication("backlog", "", AT_MOST_ONCE, 0)));
            }
            first.sendRaw(publications.toByteArray());
            second.sendRaw(publications.toByteArray());
            awaitRelease(busy);
            waiting.send(publication("waiting", "", AT_MOST_ONCE, 0));
            waiting.send(publication("queued", "", AT_MOST_ONCE, 0));
            awaitRelease(queued);

            // Act
            release.countDown();

            // Assert: the publication is handled long before either backlog is through.
            assertThat(handledBefore.get(5, SECONDS)).isLessThan(backlog);
        } finally {
            release.countDown();
        }
    }

    /// The packets whose numbers wait in a session's inbox up to limits of their own: QoS 0
    /// publications, which nothing else holds back, and the others, here acknowledgements of
    /// deliveries that the broker never made, which it ignores.
    static Stream<Arguments> bursts() {
        IntFunction<Packet> publications = number -> publication(
                "sequence",
                Integer.toString(number),
                AT_MOST_ONCE,
                0
        );
        IntFunction<Packet> acknowledgements = number -> new Packet.PubAck(number, SUCCESS, EMPTY);
        return Stream.of(
                Arguments.of(Named.of("QoS 0 publications", publications)),
                Arguments.of(Named.of("acknowledgements", acknowledgements))
        );
    }

    @ParameterizedTest
    @MethodSource("bursts")
    void a_burst_that_overfills_a_sessions_inbox_is_handled_whole_and_in_order(
            IntFunction<Packet> packets
    ) throws Exception {
        List<Packet> burst = IntStream.rangeClosed(1, 400).mapToObj(packets).toList();
        var handled = new ConcurrentLinkedQueue<Packet>();
        try (BrokerServer sut = BrokerServer.builder()
                .beforeHandle(packet -> {
                    if (packet.getClass() == burst.getFirst().getClass()) {
                        handled.add(packet);
                        // Slower than the reader, which fills the inbox and waits for room.
                        LockSupport.parkNanos(100_000);
                    }
                })
                .start(loopback());
                RawSession client = connected(sut.port(), "client")) {
            // Arrange
            var bytes = new ByteArrayOutputStream();
            for (Packet packet : burst) {
                bytes.writeBytes(codec.encode(packet));
            }

            // Act
            client.sendRaw(bytes.toByteArray());
            client.ping();

            // Assert
            assertThat(handled).containsExactlyElementsOf(burst);
        }
    }

    @Test
    void requests_that_wait_for_a_stalled_session_keep_room_for_their_responses()
            throws Exception {
        var writer = new HeldWrite();
        var armed = new AtomicBoolean();
        var decoded = new AtomicInteger();
        var stall = new Stall(Packet.PingReq.class::isInstance);
        try (BrokerServer sut = BrokerServer.builder()
                .holdDeliveryWrite(writer)
                .beforeProcess(packet -> {
                    if (armed.get() && packet instanceof Packet.PingReq) {
                        decoded.incrementAndGet();
                    }
                })
                .beforeHandle(packet -> {
                    if (armed.get()) {
                        stall.accept(packet);
                    }
                })
                .start(loopback());
                RawSession client = connected(sut.port(), "client");
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession observer = connected(sut.port(), "observer")) {
            // Arrange: the write of a delivery is kept back, so that 120 PINGRESPs wait in the
            // send queue, 8 short of the 128 responses that it keeps room for.
            client.subscribe("deliveries", AT_MOST_ONCE);
            observer.subscribe("processed", AT_MOST_ONCE);
            writer.hold();
            publisher.publish("deliveries", "held", AT_MOST_ONCE, 0);
            writer.awaitHeld();
            var answered = new ByteArrayOutputStream();
            for (int request = 0; request < 120; request++) {
                answered.writeBytes(codec.encode(new Packet.PingReq()));
            }
            Packet.Publish marker = publication("processed", "answered", AT_MOST_ONCE, 0);
            answered.writeBytes(codec.encode(marker));
            client.sendRaw(answered.toByteArray());
            assertThat(observer.expect(Packet.Publish.class)).isEqualTo(marker);

            // Act: the session stalls on the next request, and the reader takes those after it
            // until their responses would fill the room.
            armed.set(true);
            var waiting = new ByteArrayOutputStream();
            for (int request = 0; request < 21; request++) {
                waiting.writeBytes(codec.encode(new Packet.PingReq()));
            }
            client.sendRaw(waiting.toByteArray());
            stall.reached.get(5, SECONDS);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(decoded).hasValueGreaterThanOrEqualTo(9));
            stall.release.countDown();
            writer.release();

            // Assert: no response finds the room full, which would end the connection.
            assertThat(client.expect(Packet.Publish.class))
                    .isEqualTo(publication("deliveries", "held", AT_MOST_ONCE, 0));
            for (int response = 0; response < 141; response++) {
                client.expect(Packet.PingResp.class);
            }
            client.ping();
        } finally {
            stall.release.countDown();
            writer.release();
        }
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    /// An MQTT 5.0 CONNECT whose session outlives the connection for a minute and whose QoS 0
    /// Will on `left/` and the identifier waits `delay` seconds.
    private static Packet.Connect connectWithDelayedWill(String clientId, long delay) {
        var will = new Packet.Will(
                new Topic("left/" + clientId),
                "gone".getBytes(UTF_8),
                AT_MOST_ONCE,
                false,
                new Properties(List.of(
                        new Property.FourByteValue(PropertyId.WILL_DELAY_INTERVAL, delay)
                ))
        );
        return new Packet.Connect(
                clientId,
                MqttVersion.V5,
                true,
                0,
                will,
                null,
                null,
                new Properties(List.of(
                        new Property.FourByteValue(PropertyId.SESSION_EXPIRY_INTERVAL, 60)
                ))
        );
    }

    /// The hook that stalls the first packet that it matches until the test releases it.
    private static final class Stall implements Consumer<Packet> {

        final CompletableFuture<Thread> reached = new CompletableFuture<>();

        final CountDownLatch release = new CountDownLatch(1);

        private final Predicate<Packet> matches;

        /// Stalls the first PUBLISH to `topic`.
        Stall(String topic) {
            this(packet -> packet instanceof Packet.Publish publish
                    && publish.topic().name().equals(topic));
        }

        Stall(Predicate<Packet> matches) {
            this.matches = matches;
        }

        @Override
        public void accept(Packet packet) {
            if (matches.test(packet) && reached.complete(Thread.currentThread())) {
                awaitRelease(release);
            }
        }
    }
}
