package org.mqtt4j.hato.broker.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.broker.internal.common.concurrent.Timer;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.RawSession.codec;
import static org.mqtt4j.hato.broker.internal.RawSession.connect;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

class BrokerSessionRaceTest {

    @ParameterizedTest
    @CsvSource({"1, 64", "128, 64", "128, 127"})
    void a_control_response_burst_keeps_fifo_order_with_a_held_delivery_writer(
            int deliveries,
            int acknowledgements
    ) throws Exception {
        // Arrange: one active delivery holds the writer and the remaining deliveries queue.
        var heldWriter = new CompletableFuture<Thread>();
        var release = new CountDownLatch(1);
        try (BrokerServer sut = builder(new BrokerDeadlineTest.ManualTimer())
                .beforeDeliveryWrite(() -> {
                    if (heldWriter.complete(Thread.currentThread())) {
                        awaitRelease(release);
                    }
                })
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                var reads = Executors.newVirtualThreadPerTaskExecutor();
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connected(sut.port(), "publisher");
                var observer = connected(sut.port(), "observer")) {
            subscriber.subscribe("deliveries", AT_MOST_ONCE);
            observer.subscribe("processed", AT_MOST_ONCE);
            for (int i = 0; i < deliveries; i++) {
                publisher.publish("deliveries", "queued-" + i, AT_MOST_ONCE, 0);
            }
            heldWriter.get(5, SECONDS);
            publisher.ping();
            var firstDelivery = reads.submit(() -> subscriber.expect(Packet.Publish.class));
            var markerReceived = reads.submit(() -> observer.expect(Packet.Publish.class));
            Packet.Publish marker = publication(
                    "processed",
                    "all responses queued",
                    AT_MOST_ONCE,
                    0
            );
            var burst = new ByteArrayOutputStream();
            for (int id = 1; id <= acknowledgements; id++) {
                burst.writeBytes(codec.encode(publication(
                        "requests",
                        "request",
                        AT_LEAST_ONCE,
                        id
                )));
            }
            burst.writeBytes(codec.encode(new Packet.PingReq()));
            burst.writeBytes(codec.encode(marker));

            // Act: all requests enter without waiting for their responses.
            subscriber.sendRaw(burst.toByteArray());

            // Assert: the marker follows response admission; an early EOF also ends this wait.
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> firstDelivery.isDone() || markerReceived.isDone());
            assertThat(firstDelivery).as("writer remains held before the marker").isNotDone();
            assertThat(markerReceived.get(5, SECONDS)).isEqualTo(marker);
            release.countDown();
            for (int i = 0; i < deliveries; i++) {
                Packet.Publish delivery = i == 0
                        ? firstDelivery.get(5, SECONDS)
                        : subscriber.expect(Packet.Publish.class);
                assertThat(delivery).isEqualTo(publication(
                        "deliveries",
                        "queued-" + i,
                        AT_MOST_ONCE,
                        0
                ));
            }
            for (int id = 1; id <= acknowledgements; id++) {
                assertThat(subscriber.expect(Packet.PubAck.class))
                        .isEqualTo(new Packet.PubAck(id, SUCCESS, EMPTY));
            }
            subscriber.expect(Packet.PingResp.class);
            subscriber.ping();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_delivery_taken_over_before_the_writer_starts_keeps_dup_clear() throws Exception {
        var heldWriter = new CompletableFuture<Thread>();
        var release = new CountDownLatch(1);
        var timer = new BrokerDeadlineTest.ManualTimer();
        try (BrokerServer sut = builder(timer)
                .beforeDeliveryWrite(() -> {
                    if (heldWriter.complete(Thread.currentThread())) {
                        awaitRelease(release);
                    }
                })
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                var first = connected(sut.port(), "subscriber", false, false);
                var publisher = connected(sut.port(), "publisher")) {
            first.subscribe("kept/#", AT_LEAST_ONCE);
            publisher.publish("kept/topic", "not written", AT_LEAST_ONCE, 7);
            Thread writer = heldWriter.get(5, SECONDS);

            try (var replacement = connected(sut.port(), "subscriber", false, true)) {
                first.expectClosed();
                Packet.Publish resumed = replacement.expect(Packet.Publish.class);
                assertThat(resumed.packetId()).isPositive();
                assertThat(resumed).isEqualTo(publication(
                        "kept/topic", "not written", AT_LEAST_ONCE, resumed.packetId()
                ));
                release.countDown();
                assertThat(writer.join(Duration.ofSeconds(5))).isTrue();

                replacement.send(new Packet.PubAck(resumed.packetId(), SUCCESS, EMPTY));
                replacement.ping();
                publisher.publish("kept/topic", "after old writer stopped", AT_LEAST_ONCE, 8);
                Packet.Publish next = replacement.expect(Packet.Publish.class);
                assertThat(next.packetId()).isPositive();
                assertThat(next).isEqualTo(publication(
                        "kept/topic", "after old writer stopped", AT_LEAST_ONCE, next.packetId()
                ));
                replacement.send(new Packet.PubAck(next.packetId(), SUCCESS, EMPTY));
                replacement.ping();
                assertThat(timer.clock.pendingTaskCount()).isZero();
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void deliveries_waiting_for_send_space_resume_when_the_writer_frees_it() throws Exception {
        // Arrange: the held writer keeps three deliveries in a 1 KiB queue; two more wait.
        var heldWriter = new CompletableFuture<Thread>();
        var release = new CountDownLatch(1);
        String payload = "x".repeat(300);
        try (BrokerServer sut = builder(new BrokerDeadlineTest.ManualTimer())
                .sendBytes(1024)
                .maxInflightBytes(1024)
                .beforeDeliveryWrite(() -> {
                    if (heldWriter.complete(Thread.currentThread())) {
                        awaitRelease(release);
                    }
                })
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("queued", AT_MOST_ONCE);
            for (int i = 0; i < 5; i++) {
                publisher.publish("queued", payload + i, AT_MOST_ONCE, 0);
            }
            heldWriter.get(5, SECONDS);
            // The broker routes all five publications before answering this ping.
            publisher.ping();

            // Act: no acknowledgement or later publication can resume the waiting deliveries.
            release.countDown();

            // Assert
            for (int i = 0; i < 5; i++) {
                assertThat(subscriber.expect(Packet.Publish.class))
                        .isEqualTo(publication("queued", payload + i, AT_MOST_ONCE, 0));
            }
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void acknowledged_and_queued_messages_survive_legal_event_orders(int seed) throws Exception {
        var timer = new BrokerDeadlineTest.ManualTimer();
        var events = new ArrayList<>(List.of(Event.values()));
        Collections.shuffle(events, new SplittableRandom(seed));
        RawSession subscriber = null;
        try (BrokerServer sut = start(timer, packet -> { });
                var publisher = connected(sut.port(), "publisher")) {
            subscriber = persistent(sut.port(), false);
            subscriber.subscribe("kept/#", AT_LEAST_ONCE);
            publisher.publish("kept/topic", "first", AT_LEAST_ONCE, 7);
            Packet.Publish original = subscriber.expect(Packet.Publish.class);
            Packet.Publish inflight = original;
            boolean acknowledged = false;
            boolean published = false;

            for (Event event : events) {
                switch (event) {
                    case ACK -> {
                        subscriber.send(new Packet.PubAck(original.packetId(), SUCCESS, EMPTY));
                        acknowledged = true;
                        inflight = published ? subscriber.expect(Packet.Publish.class) : null;
                        if (inflight != null) {
                            assertThat(inflight).isEqualTo(publication(
                                    "kept/topic", "second", AT_LEAST_ONCE, inflight.packetId()
                            ));
                        }
                    }
                    case PUBLISH -> {
                        publisher.publish("kept/topic", "second", AT_LEAST_ONCE, 8);
                        published = true;
                        if (acknowledged) {
                            inflight = subscriber.expect(Packet.Publish.class);
                            assertThat(inflight).isEqualTo(publication(
                                    "kept/topic", "second", AT_LEAST_ONCE, inflight.packetId()
                            ));
                        }
                    }
                    case TAKEOVER, TIMEOUT -> {
                        try (RawSession old = subscriber) {
                            if (event == Event.TIMEOUT) {
                                timer.clock.advance(Duration.ofMillis(1500));
                                old.expectClosed();
                            }
                            subscriber = persistent(sut.port(), true);
                            if (event == Event.TAKEOVER) {
                                old.expectClosed();
                            }
                            if (inflight != null) {
                                Packet.Publish replay = subscriber.expect(Packet.Publish.class);
                                assertThat(replay).isEqualTo(new Packet.Publish(
                                        inflight.topic(), inflight.payload(), AT_LEAST_ONCE,
                                        false, true, inflight.packetId(), EMPTY
                                ));
                                inflight = replay;
                            }
                        }
                    }
                }
                // An unexpected replay or premature queued delivery fails this wire barrier.
                subscriber.ping();
                assertThat(timer.clock.pendingTaskCount()).isEqualTo(1);
            }

            assertThat(inflight).isNotNull();
            subscriber.send(new Packet.PubAck(inflight.packetId(), SUCCESS, EMPTY));
            subscriber.ping();
            subscriber.send(new Packet.Disconnect(SUCCESS, EMPTY));
            subscriber.expectClosed();
            assertThat(timer.clock.pendingTaskCount()).isZero();
        } finally {
            if (subscriber != null) {
                subscriber.close();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(OldPacket.class)
    void a_packet_decoded_before_takeover_cannot_change_the_replacement_session(OldPacket kind)
            throws Exception {
        var heldReader = new CompletableFuture<Thread>();
        var release = new CountDownLatch(1);
        var timer = new BrokerDeadlineTest.ManualTimer();
        try (BrokerServer sut = start(timer, packet -> {
            if (kind.matches(packet) && heldReader.complete(Thread.currentThread())) {
                awaitRelease(release);
            }
        }); var first = connected(sut.port(), "subscriber", false, false);
                var publisher = connected(sut.port(), "publisher")) {
            first.subscribe("kept/#", AT_LEAST_ONCE);
            publisher.publish("kept/topic", "unacknowledged", AT_LEAST_ONCE, 7);
            Packet.Publish original = first.expect(Packet.Publish.class);
            first.send(kind.packet(original.packetId()));
            Thread reader = heldReader.get(5, SECONDS);

            try (var replacement = connected(sut.port(), "subscriber", false, true)) {
                first.expectClosed();
                Packet.Publish replay = replacement.expect(Packet.Publish.class);
                assertThat(replay).isEqualTo(new Packet.Publish(
                        original.topic(), original.payload(), AT_LEAST_ONCE,
                        false, true, original.packetId(), EMPTY
                ));
                release.countDown();
                assertThat(reader.join(Duration.ofSeconds(5))).isTrue();

                publisher.publish("kept/topic", "after takeover", AT_LEAST_ONCE, 8);
                // The old PUBACK must not release the one-message window.
                replacement.ping();
                replacement.send(new Packet.PubAck(replay.packetId(), SUCCESS, EMPTY));
                Packet.Publish next = replacement.expect(Packet.Publish.class);
                assertThat(next).isEqualTo(publication(
                        "kept/topic", "after takeover", AT_LEAST_ONCE, next.packetId()
                ));
                replacement.send(new Packet.PubAck(next.packetId(), SUCCESS, EMPTY));
                replacement.ping();
                assertThat(timer.clock.pendingTaskCount()).isZero();
            }
        } finally {
            release.countDown();
        }
    }

    private static BrokerServer start(Timer timer, Consumer<Packet> beforeProcess)
            throws IOException {
        return builder(timer)
                .beforeProcess(beforeProcess)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    private static BrokerServer.Builder builder(Timer timer) {
        return BrokerServer.builder()
                .connectTimeout(Duration.ofSeconds(10))
                .sendBytes(1024 * 1024L)
                .maxInflight(1)
                .maxInflightBytes(4096L)
                .maxQueued(16)
                .maxQueuedBytes(65536L)
                .maxSessions(16)
                .scheduler(timer);
    }

    private static RawSession persistent(int port, boolean sessionPresent) throws Exception {
        RawSession session = RawSession.open(port);
        try {
            session.send(connect("subscriber", false, 1));
            assertThat(session.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(sessionPresent, SUCCESS, EMPTY));
            return session;
        } catch (Exception | AssertionError failure) {
            session.close();
            throw failure;
        }
    }

    private static IntStream seeds() {
        return IntStream.range(0, 100);
    }

    private static void awaitRelease(CountDownLatch release) {
        try {
            assertThat(release.await(5, SECONDS)).isTrue();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    private enum OldPacket {
        PUBACK, UNSUBSCRIBE, DISCONNECT;

        boolean matches(Packet packet) {
            return switch (this) {
                case PUBACK -> packet instanceof Packet.PubAck;
                case UNSUBSCRIBE -> packet instanceof Packet.Unsubscribe;
                case DISCONNECT -> packet instanceof Packet.Disconnect;
            };
        }

        Packet packet(int packetId) {
            return switch (this) {
                case PUBACK -> new Packet.PubAck(packetId, SUCCESS, EMPTY);
                case UNSUBSCRIBE -> new Packet.Unsubscribe(
                        9, List.of(new TopicFilter("kept/#")), EMPTY
                );
                case DISCONNECT -> new Packet.Disconnect(SUCCESS, EMPTY);
            };
        }
    }

    private enum Event { ACK, PUBLISH, TAKEOVER, TIMEOUT }
}
