package org.mqtt4j.hato.broker.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.internal.BrokerQos2Test.acknowledge;
import static org.mqtt4j.hato.broker.internal.BrokerQos2Test.receive;
import static org.mqtt4j.hato.broker.internal.BrokerQos2Test.withDup;
import static org.mqtt4j.hato.broker.internal.BrokerSessionRaceTest.awaitRelease;
import static org.mqtt4j.hato.broker.internal.RawSession.codec;
import static org.mqtt4j.hato.broker.internal.RawSession.connect;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// PUBREL that waits for space in a connection's send queue. The broker's writer is held on a
/// delivery so that the queue stays full while the client's packets are processed.
class BrokerQos2RaceTest {

    // A connection's regular send queue holds 128 sends, as many as the default inflight window.
    private static final int SEND_QUEUE = 128;

    // Responses to the client's requests have their own room for 128 sends.
    private static final int RESPONSES = 128;

    // The broker routes this only after processing every packet its sender sent before it.
    private static final Packet.Publish MARKER = publication(
            "processed",
            "everything before was processed",
            AT_MOST_ONCE,
            0
    );

    @ParameterizedTest
    @ValueSource(ints = {1, 256})
    void deferred_pubrels_keep_the_order_of_first_receipts_and_merge_duplicates(int receipts)
            throws Exception {
        // Arrange: two QoS 2 deliveries reach the subscriber, then its send queue fills up.
        var writer = new HeldWriter();
        try (
                BrokerServer sut = broker(writer);
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession observer = connected(sut.port(), "observer")
        ) {
            subscriber.subscribe("exactly-once", EXACTLY_ONCE);
            subscriber.subscribe("held", AT_MOST_ONCE);
            observer.subscribe("processed", AT_MOST_ONCE);
            publisher.publish("exactly-once", "first", EXACTLY_ONCE, 7);
            Packet.Publish first = subscriber.expect(Packet.Publish.class);
            publisher.publish("exactly-once", "second", EXACTLY_ONCE, 7);
            Packet.Publish second = subscriber.expect(Packet.Publish.class);
            fillSendQueue(writer, publisher);
            var burst = new ByteArrayOutputStream();
            write(burst, new Packet.PubRec(first.packetId(), SUCCESS, EMPTY));
            write(burst, new Packet.PubRec(second.packetId(), SUCCESS, EMPTY));
            // A later duplicate of the first receipt must not move its PUBREL after the second.
            for (int receipt = 1; receipt < receipts; receipt++) {
                write(burst, new Packet.PubRec(first.packetId(), SUCCESS, EMPTY));
            }
            // The waiting PUBRELs must leave the whole room for responses to these requests.
            for (int id = 1; id <= RESPONSES; id++) {
                write(burst, publication("requests", "request", AT_LEAST_ONCE, id));
            }
            write(burst, MARKER);

            // Act: PUBREC must wait for space without closing the subscriber's connection.
            subscriber.sendRaw(burst.toByteArray());

            // Assert
            assertThat(observer.expect(Packet.Publish.class)).isEqualTo(MARKER);
            writer.release();
            expectHeldDeliveries(subscriber);
            for (int id = 1; id <= RESPONSES; id++) {
                assertThat(subscriber.expect(Packet.PubAck.class))
                        .isEqualTo(new Packet.PubAck(id, SUCCESS, EMPTY));
            }
            for (Packet.Publish delivery : List.of(first, second)) {
                assertThat(subscriber.expect(Packet.PubRel.class))
                        .isEqualTo(new Packet.PubRel(delivery.packetId(), SUCCESS, EMPTY));
                subscriber.send(new Packet.PubComp(delivery.packetId(), SUCCESS, EMPTY));
            }
            // An extra deferred PUBREL would arrive instead of this PINGRESP.
            subscriber.ping();
        } finally {
            writer.release();
        }
    }

    @Test
    void pubcomp_drops_a_repeated_pubrel_still_waiting_for_space() throws Exception {
        // Arrange: the subscriber has received PUBREL, then its send queue fills up.
        var writer = new HeldWriter();
        try (
                BrokerServer sut = broker(writer);
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession observer = connected(sut.port(), "observer")
        ) {
            subscriber.subscribe("exactly-once", EXACTLY_ONCE);
            subscriber.subscribe("held", AT_MOST_ONCE);
            observer.subscribe("processed", AT_MOST_ONCE);
            publisher.publish("exactly-once", "message", EXACTLY_ONCE, 7);
            Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
            receive(subscriber, delivery.packetId());
            fillSendQueue(writer, publisher);
            var burst = new ByteArrayOutputStream();
            write(burst, new Packet.PubRec(delivery.packetId(), SUCCESS, EMPTY));
            write(burst, new Packet.PubComp(delivery.packetId(), SUCCESS, EMPTY));
            write(burst, MARKER);

            // Act: the repeated PUBREC queues a PUBREL that the PUBCOMP then makes obsolete.
            subscriber.sendRaw(burst.toByteArray());

            // Assert
            assertThat(observer.expect(Packet.Publish.class)).isEqualTo(MARKER);
            writer.release();
            expectHeldDeliveries(subscriber);
            // The completed exchange's PUBREL would arrive instead of this PINGRESP.
            subscriber.ping();
        } finally {
            writer.release();
        }
    }

    @Test
    void requests_sent_while_a_resumed_pubrel_window_waits_are_answered() throws Exception {
        // Arrange: a persistent subscriber leaves one PUBLISH and 127 PUBRELs unacknowledged.
        var writer = new HeldWriter();
        try (
                BrokerServer sut = broker(writer);
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession observer = connected(sut.port(), "observer")
        ) {
            observer.subscribe("processed", AT_MOST_ONCE);
            var deliveries = new ArrayList<Packet.Publish>();
            try (RawSession previous = connected(sut.port(), "subscriber", false, false)) {
                previous.subscribe("exactly-once", EXACTLY_ONCE);
                for (int index = 0; index < SEND_QUEUE; index++) {
                    publisher.publish("exactly-once", "window", EXACTLY_ONCE, 7);
                    deliveries.add(previous.expect(Packet.Publish.class));
                }
                for (Packet.Publish delivery : deliveries.subList(1, SEND_QUEUE)) {
                    receive(previous, delivery.packetId());
                }
                previous.disconnect();
            }

            try (RawSession resumed = RawSession.open(sut.port())) {
                // The writer sends CONNACK and is then held on the replayed PUBLISH.
                writer.hold();
                resumed.send(connect("subscriber", false, 0));
                writer.awaitHeld();
                var burst = new ByteArrayOutputStream();
                for (int id = 1; id <= RESPONSES; id++) {
                    write(burst, publication("requests", "request", AT_LEAST_ONCE, id));
                }
                write(burst, MARKER);

                // Act: the replayed PUBRELs wait in the regular queue, not in the responses' room.
                resumed.sendRaw(burst.toByteArray());

                // Assert
                assertThat(observer.expect(Packet.Publish.class)).isEqualTo(MARKER);
                writer.release();
                assertThat(resumed.expect(Packet.ConnAck.class))
                        .isEqualTo(new Packet.ConnAck(true, SUCCESS, EMPTY));
                assertThat(resumed.expect(Packet.Publish.class))
                        .isEqualTo(withDup(deliveries.getFirst(), true));
                for (Packet.Publish delivery : deliveries.subList(1, SEND_QUEUE)) {
                    assertThat(resumed.expect(Packet.PubRel.class))
                            .isEqualTo(new Packet.PubRel(delivery.packetId(), SUCCESS, EMPTY));
                }
                for (int id = 1; id <= RESPONSES; id++) {
                    assertThat(resumed.expect(Packet.PubAck.class))
                            .isEqualTo(new Packet.PubAck(id, SUCCESS, EMPTY));
                }
                for (Packet.Publish delivery : deliveries.subList(1, SEND_QUEUE)) {
                    resumed.send(new Packet.PubComp(delivery.packetId(), SUCCESS, EMPTY));
                }
                acknowledge(resumed, deliveries.getFirst());
                resumed.ping();
            }
        } finally {
            writer.release();
        }
    }

    private static BrokerServer broker(HeldWriter writer) throws IOException {
        return BrokerServer.builder()
                .beforeDeliveryWrite(writer)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    /// Holds the subscriber's writer on a QoS 0 delivery to `held`, then fills the rest of its
    /// regular send queue with more of them.
    private static void fillSendQueue(HeldWriter writer, RawSession publisher) throws Exception {
        writer.hold();
        publisher.publish("held", "writer paused", AT_MOST_ONCE, 0);
        writer.awaitHeld();
        for (int filler = 1; filler < SEND_QUEUE; filler++) {
            publisher.publish("held", "filler", AT_MOST_ONCE, 0);
        }
        // The broker has routed every publication sent before this PINGRESP.
        publisher.ping();
    }

    private static void expectHeldDeliveries(RawSession subscriber) throws Exception {
        assertThat(subscriber.expect(Packet.Publish.class))
                .isEqualTo(publication("held", "writer paused", AT_MOST_ONCE, 0));
        for (int filler = 1; filler < SEND_QUEUE; filler++) {
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("held", "filler", AT_MOST_ONCE, 0));
        }
    }

    private static void write(ByteArrayOutputStream burst, Packet packet) {
        burst.writeBytes(codec.encode(packet));
    }

    /// Holds the first broker writer that writes a delivery after [#hold()], until released.
    private static final class HeldWriter implements Runnable {

        private final AtomicBoolean holding = new AtomicBoolean();

        private final CompletableFuture<Thread> held = new CompletableFuture<>();

        private final CountDownLatch released = new CountDownLatch(1);

        @Override
        public void run() {
            if (holding.get() && held.complete(Thread.currentThread())) {
                awaitRelease(released);
            }
        }

        void hold() {
            holding.set(true);
        }

        void awaitHeld() throws Exception {
            held.get(5, SECONDS);
        }

        void release() {
            released.countDown();
        }
    }
}
