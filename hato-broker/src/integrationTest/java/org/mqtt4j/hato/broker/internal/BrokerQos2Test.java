package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketType;
import org.mqtt4j.hato.broker.internal.common.value.Qos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.internal.RawSession.codec;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

class BrokerQos2Test {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_pending_identifier_suppresses_duplicates_until_pubrel_regardless_of_dup(boolean dup)
            throws Exception {
        // Arrange
        try (MqttBroker sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("exactly-once", AT_MOST_ONCE);
            Packet.Publish first = publication("exactly-once", "first", EXACTLY_ONCE, 7);

            // Act & Assert: routing happens on PUBLISH, before PUBREL arrives.
            publisher.send(first);
            expectPubRec(publisher, 7);
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("exactly-once", "first", AT_MOST_ONCE, 0));
            Packet.Publish next = publication("exactly-once", "next", EXACTLY_ONCE, 7);
            publisher.send(withDup(next, dup));
            expectPubRec(publisher, 7);
            subscriber.ping();

            release(publisher, 7);
            release(publisher, 7);
            publisher.send(withDup(next, dup));
            expectPubRec(publisher, 7);
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("exactly-once", "next", AT_MOST_ONCE, 0));
            release(publisher, 7);
            subscriber.ping();
        }
    }

    @Test
    void qos_two_publications_are_routed_and_acknowledged_in_receive_order() throws Exception {
        // Arrange
        try (MqttBroker sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "echo")) {
            publisher.subscribe("echo", AT_MOST_ONCE);

            // Act
            for (int id : List.of(9, 3, 7)) {
                publisher.send(publication("echo", Integer.toString(id), EXACTLY_ONCE, id));
            }

            // Assert: each self-delivery enters the FIFO before its PUBREC.
            for (int id : List.of(9, 3, 7)) {
                assertThat(publisher.expect(Packet.Publish.class))
                        .isEqualTo(publication("echo", Integer.toString(id), AT_MOST_ONCE, 0));
                expectPubRec(publisher, id);
            }
            for (int id : List.of(9, 3, 7)) {
                release(publisher, id);
            }
            publisher.ping();
        }
    }

    @Test
    void unknown_acknowledgements_are_ignored_and_unknown_pubrel_receives_pubcomp()
            throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession client = connected(sut.port(), "unknown")) {
            client.send(new Packet.PubAck(7, SUCCESS, EMPTY));
            client.send(new Packet.PubRec(8, SUCCESS, EMPTY));
            client.send(new Packet.PubComp(9, SUCCESS, EMPTY));
            client.ping();
            release(client, 10);
            release(client, 10);
            client.ping();
        }
    }

    @ParameterizedTest
    @CsvSource({
            "AT_LEAST_ONCE, false, PUBREC",
            "AT_LEAST_ONCE, false, PUBCOMP",
            "EXACTLY_ONCE, false, PUBACK",
            "EXACTLY_ONCE, false, PUBCOMP",
            "EXACTLY_ONCE, true, PUBACK"
    })
    void an_acknowledgement_for_the_wrong_stage_closes_the_subscriber(
            Qos qos,
            boolean received,
            PacketType acknowledgement
    ) throws Exception {
        // Arrange
        try (MqttBroker sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("wrong-stage", qos);
            publisher.publish("wrong-stage", "message", qos, 7);
            Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
            if (received) {
                receive(subscriber, delivery.packetId());
            }
            Packet wrong = switch (acknowledgement) {
                case PUBACK -> new Packet.PubAck(delivery.packetId(), SUCCESS, EMPTY);
                case PUBREC -> new Packet.PubRec(delivery.packetId(), SUCCESS, EMPTY);
                case PUBCOMP -> new Packet.PubComp(delivery.packetId(), SUCCESS, EMPTY);
                default -> throw new IllegalArgumentException("Expected a publication response");
            };

            // Act
            subscriber.send(wrong);

            // Assert
            subscriber.expectClosed();
            publisher.ping();
        }
    }

    @Test
    void qos_one_reusing_an_identifier_waiting_for_pubrel_ends_that_wait() throws Exception {
        // Arrange: a persistent publisher loses the QoS 2 exchange it started, as after a crash.
        try (MqttBroker sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("reused", AT_MOST_ONCE);
            try (RawSession previous = connected(sut.port(), "publisher", false, false)) {
                previous.send(publication("reused", "abandoned", EXACTLY_ONCE, 7));
                expectPubRec(previous, 7);
                assertThat(subscriber.expect(Packet.Publish.class))
                        .isEqualTo(publication("reused", "abandoned", AT_MOST_ONCE, 0));
            }

            // Act & Assert: the resumed publisher reuses the identifier without sending PUBREL.
            try (RawSession resumed = connected(sut.port(), "publisher", false, true)) {
                resumed.publish("reused", "qos one", AT_LEAST_ONCE, 7);
                assertThat(subscriber.expect(Packet.Publish.class))
                        .isEqualTo(publication("reused", "qos one", AT_MOST_ONCE, 0));
                // The wait has ended, so a new QoS 2 publication is forwarded, not suppressed.
                resumed.publish("reused", "qos two", EXACTLY_ONCE, 7);
                assertThat(subscriber.expect(Packet.Publish.class))
                        .isEqualTo(publication("reused", "qos two", AT_MOST_ONCE, 0));
                subscriber.ping();
            }
        }
    }

    @Test
    void incoming_and_outgoing_qos_two_exchanges_can_use_the_same_identifier() throws Exception {
        // Arrange
        try (MqttBroker sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("outgoing", EXACTLY_ONCE);
            publisher.subscribe("incoming", AT_MOST_ONCE);
            publisher.publish("outgoing", "outbound", EXACTLY_ONCE, 7);
            Packet.Publish outgoing = expectDelivery(subscriber, "outgoing", "outbound");
            int id = outgoing.packetId();

            // Act & Assert
            subscriber.send(publication("incoming", "inbound", EXACTLY_ONCE, id));
            expectPubRec(subscriber, id);
            assertThat(publisher.expect(Packet.Publish.class))
                    .isEqualTo(publication("incoming", "inbound", AT_MOST_ONCE, 0));
            receive(subscriber, id);
            release(subscriber, id);
            subscriber.send(new Packet.PubComp(id, SUCCESS, EMPTY));
            subscriber.ping();
            publisher.ping();
        }
    }

    @Test
    void a_resumed_receiver_keeps_duplicate_suppression_until_pubrel() throws Exception {
        // Arrange
        try (MqttBroker sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("resumed", AT_MOST_ONCE);
            Packet.Publish message = publication("resumed", "once", EXACTLY_ONCE, 7);
            try (RawSession previous = connected(sut.port(), "publisher", false, false)) {
                previous.send(message);
                expectPubRec(previous, 7);
                assertThat(subscriber.expect(Packet.Publish.class))
                        .isEqualTo(publication("resumed", "once", AT_MOST_ONCE, 0));
                previous.disconnect();
            }

            // Act & Assert
            try (RawSession resumed = connected(sut.port(), "publisher", false, true)) {
                resumed.send(withDup(message, true));
                expectPubRec(resumed, 7);
                subscriber.ping();
                release(resumed, 7);
                resumed.disconnect();
            }
            // A PUBCOMP lost at disconnect must be recoverable in another connection.
            try (RawSession resumed = connected(sut.port(), "publisher", false, true)) {
                release(resumed, 7);
                resumed.publish("resumed", "next", EXACTLY_ONCE, 7);
                assertThat(subscriber.expect(Packet.Publish.class))
                        .isEqualTo(publication("resumed", "next", AT_MOST_ONCE, 0));
                subscriber.ping();
            }
        }
    }

    @Test
    void cleaning_a_session_discards_its_pending_incoming_identifiers() throws Exception {
        // Arrange
        try (MqttBroker sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            subscriber.subscribe("cleaned", AT_MOST_ONCE);
            try (RawSession previous = connected(sut.port(), "publisher", false, false)) {
                previous.send(publication("cleaned", "previous", EXACTLY_ONCE, 7));
                expectPubRec(previous, 7);
                assertThat(subscriber.expect(Packet.Publish.class))
                        .isEqualTo(publication("cleaned", "previous", AT_MOST_ONCE, 0));
                previous.disconnect();
            }

            // Act & Assert
            try (RawSession replacement = connected(sut.port(), "publisher", true, false)) {
                replacement.publish("cleaned", "replacement", EXACTLY_ONCE, 7);
                assertThat(subscriber.expect(Packet.Publish.class))
                        .isEqualTo(publication("cleaned", "replacement", AT_MOST_ONCE, 0));
                subscriber.ping();
            }
        }
    }

    @Test
    void pubcomp_releases_the_count_window_while_pubrec_keeps_the_slot() throws Exception {
        // Arrange
        try (BrokerServer sut = broker(1, 1024);
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("bounded", EXACTLY_ONCE);
            publisher.publish("bounded", "first", EXACTLY_ONCE, 7);
            Packet.Publish first = expectDelivery(subscriber, "bounded", "first");
            publisher.publish("bounded", "queued", EXACTLY_ONCE, 7);
            subscriber.ping();

            // Act & Assert
            receive(subscriber, first.packetId());
            subscriber.ping();
            // A repeated PUBREC repeats PUBREL, without freeing the occupied slot.
            receive(subscriber, first.packetId());
            subscriber.ping();
            subscriber.send(new Packet.PubComp(first.packetId(), SUCCESS, EMPTY));
            Packet.Publish next = expectDelivery(subscriber, "bounded", "queued");
            acknowledge(subscriber, next);
            subscriber.ping();
        }
    }

    @Test
    void pubrec_releases_payload_bytes_before_pubcomp() throws Exception {
        // Arrange: two identifier slots, but payload space for only one publication.
        long bytes = codec.encode(publication("bytes", "payload", EXACTLY_ONCE, 1)).length;
        try (BrokerServer sut = broker(2, bytes);
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("bytes", EXACTLY_ONCE);
            publisher.publish("bytes", "payload", EXACTLY_ONCE, 7);
            Packet.Publish first = expectDelivery(subscriber, "bytes", "payload");
            publisher.publish("bytes", "payload", EXACTLY_ONCE, 7);
            subscriber.ping();

            // Act
            receive(subscriber, first.packetId());

            // Assert
            Packet.Publish second = expectDelivery(subscriber, "bytes", "payload");
            subscriber.send(new Packet.PubComp(first.packetId(), SUCCESS, EMPTY));
            acknowledge(subscriber, second);
            subscriber.ping();
        }
    }

    @Test
    void resumption_replays_unacknowledged_publish_before_offline_deliveries() throws Exception {
        // Arrange
        try (BrokerServer sut = broker(1, 1024);
                RawSession publisher = connected(sut.port(), "publisher")) {
            Packet.Publish first;
            try (RawSession previous = connected(sut.port(), "subscriber", false, false)) {
                previous.subscribe("offline", EXACTLY_ONCE);
                publisher.publish("offline", "inflight", EXACTLY_ONCE, 7);
                first = expectDelivery(previous, "offline", "inflight");
                previous.disconnect();
            }
            publisher.publish("offline", "first queued", EXACTLY_ONCE, 7);
            publisher.publish("offline", "second queued", EXACTLY_ONCE, 7);

            // Act & Assert: connected() requires CONNACK to precede every resumed frame.
            try (RawSession resumed = connected(sut.port(), "subscriber", false, true)) {
                assertThat(resumed.expect(Packet.Publish.class)).isEqualTo(withDup(first, true));
                resumed.ping();
                acknowledge(resumed, first);
                for (String payload : List.of("first queued", "second queued")) {
                    acknowledge(resumed, expectDelivery(resumed, "offline", payload));
                }
                resumed.ping();
            }
        }
    }

    @Test
    void resumption_preserves_publish_order_and_the_order_of_received_pubrecs() throws Exception {
        // Arrange
        try (BrokerServer sut = broker(3, 1024);
                RawSession publisher = connected(sut.port(), "publisher")) {
            var deliveries = new ArrayList<Packet.Publish>();
            try (RawSession previous = connected(sut.port(), "subscriber", false, false)) {
                previous.subscribe("ordered", EXACTLY_ONCE);
                for (String payload : List.of("first", "second", "third")) {
                    publisher.publish("ordered", payload, EXACTLY_ONCE, 7);
                    deliveries.add(expectDelivery(previous, "ordered", payload));
                }
                // PUBREL order follows PUBREC receipt, even when it reverses PUBLISH order.
                receive(previous, deliveries.get(1).packetId());
                receive(previous, deliveries.get(0).packetId());
                previous.ping();
                previous.disconnect();
            }

            // Act & Assert
            try (RawSession resumed = connected(sut.port(), "subscriber", false, true)) {
                assertThat(resumed.expect(Packet.Publish.class))
                        .isEqualTo(withDup(deliveries.get(2), true));
                expectPubRel(resumed, deliveries.get(1).packetId());
                expectPubRel(resumed, deliveries.get(0).packetId());
                resumed.ping();
                // After PUBREC, only PUBREL may be replayed for the first two publications.
                for (int index : List.of(1, 0)) {
                    resumed.send(new Packet.PubComp(
                            deliveries.get(index).packetId(),
                            SUCCESS,
                            EMPTY
                    ));
                }
                acknowledge(resumed, deliveries.get(2));
                resumed.ping();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 128})
    void a_full_pubrel_window_resumes_before_queued_deliveries_without_closing(int window)
            throws Exception {
        // Arrange
        try (BrokerServer sut = broker(window, 64 * 1024);
                RawSession publisher = connected(sut.port(), "publisher")) {
            var pending = new ArrayList<Integer>();
            try (RawSession previous = connected(sut.port(), "subscriber", false, false)) {
                previous.subscribe("full", EXACTLY_ONCE);
                for (int index = 0; index < window; index++) {
                    publisher.publish("full", "inflight", EXACTLY_ONCE, 7);
                    Packet.Publish delivery = expectDelivery(previous, "full", "inflight");
                    pending.add(delivery.packetId());
                    receive(previous, delivery.packetId());
                }
                assertThat(pending).doesNotHaveDuplicates();
                publisher.publish("full", "queued", EXACTLY_ONCE, 7);
                previous.ping();
                previous.disconnect();
            }

            // Act & Assert
            try (RawSession resumed = connected(sut.port(), "subscriber", false, true)) {
                for (int id : pending) {
                    expectPubRel(resumed, id);
                }
                resumed.ping();
                resumed.send(new Packet.PubComp(pending.getFirst(), SUCCESS, EMPTY));
                Packet.Publish queued = expectDelivery(resumed, "full", "queued");
                for (int id : pending.subList(1, pending.size())) {
                    resumed.send(new Packet.PubComp(id, SUCCESS, EMPTY));
                }
                acknowledge(resumed, queued);
                resumed.ping();
            }
        }
    }

    private static BrokerServer broker(int maxInflight, long maxInflightBytes) throws IOException {
        return BrokerServer.builder()
                .maxInflight(maxInflight)
                .maxInflightBytes(maxInflightBytes)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    static Packet.Publish withDup(Packet.Publish publication, boolean dup) {
        return new Packet.Publish(
                publication.topic(),
                publication.payload(),
                publication.qos(),
                publication.retain(),
                dup,
                publication.packetId(),
                publication.properties()
        );
    }

    private static Packet.Publish expectDelivery(RawSession receiver, String topic, String payload)
            throws Exception {
        Packet.Publish delivery = receiver.expect(Packet.Publish.class);
        assertThat(delivery.packetId()).isPositive();
        assertThat(delivery)
                .isEqualTo(publication(topic, payload, EXACTLY_ONCE, delivery.packetId()));
        return delivery;
    }

    private static void expectPubRec(RawSession receiver, int packetId) throws Exception {
        assertThat(receiver.expect(Packet.PubRec.class))
                .isEqualTo(new Packet.PubRec(packetId, SUCCESS, EMPTY));
    }

    private static void expectPubRel(RawSession receiver, int packetId) throws Exception {
        assertThat(receiver.expect(Packet.PubRel.class))
                .isEqualTo(new Packet.PubRel(packetId, SUCCESS, EMPTY));
    }

    static void receive(RawSession receiver, int packetId) throws Exception {
        receiver.send(new Packet.PubRec(packetId, SUCCESS, EMPTY));
        expectPubRel(receiver, packetId);
    }

    static void acknowledge(RawSession receiver, Packet.Publish publication)
            throws Exception {
        receive(receiver, publication.packetId());
        receiver.send(new Packet.PubComp(publication.packetId(), SUCCESS, EMPTY));
    }

    private static void release(RawSession publisher, int packetId) throws Exception {
        publisher.send(new Packet.PubRel(packetId, SUCCESS, EMPTY));
        assertThat(publisher.expect(Packet.PubComp.class))
                .isEqualTo(new Packet.PubComp(packetId, SUCCESS, EMPTY));
    }
}
