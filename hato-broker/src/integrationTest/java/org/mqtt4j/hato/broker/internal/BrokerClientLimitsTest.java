package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.internal.RawSession.acceptedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// The limits that an MQTT 5.0 client announces in CONNECT, which the broker keeps to: its
/// Receive Maximum and its Maximum Packet Size. The broker sends a connection's packets in
/// order, so a PINGRESP that arrives where a PUBLISH would shows that the PUBLISH waits.
class BrokerClientLimitsTest {

    @Test
    void deliveries_beyond_the_receive_maximum_wait_for_an_answer_while_other_packets_go_on()
            throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            accepted(subscriber, connect("subscriber", true, receiveMaximum(2)), false);
            subscriber.subscribe("limited/#", AT_LEAST_ONCE);

            // Act: each publication is routed before the publisher's PUBACK.
            for (int index = 1; index <= 3; index++) {
                publisher.publish("limited/" + index, "message", AT_LEAST_ONCE, index);
            }

            // Assert
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("limited/1", "message", AT_LEAST_ONCE, 1));
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("limited/2", "message", AT_LEAST_ONCE, 2));
            subscriber.ping();
            subscriber.send(new Packet.PubAck(1, SUCCESS, EMPTY));
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("limited/3", "message", AT_LEAST_ONCE, 3));
        }
    }

    @Test
    void a_pubrel_goes_out_while_its_exchange_holds_the_receive_maximum() throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            accepted(subscriber, connect("subscriber", true, receiveMaximum(1)), false);
            subscriber.subscribe("limited/#", EXACTLY_ONCE);
            publisher.publish("limited/1", "message", EXACTLY_ONCE, 1);
            publisher.publish("limited/2", "message", EXACTLY_ONCE, 2);
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("limited/1", "message", EXACTLY_ONCE, 1));

            // Act
            subscriber.send(new Packet.PubRec(1, SUCCESS, EMPTY));

            // Assert: the exchange keeps its place in the Receive Maximum until PUBCOMP.
            assertThat(subscriber.expect(Packet.PubRel.class))
                    .isEqualTo(new Packet.PubRel(1, SUCCESS, EMPTY));
            subscriber.ping();
            subscriber.send(new Packet.PubComp(1, SUCCESS, EMPTY));
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("limited/2", "message", EXACTLY_ONCE, 2));
        }
    }

    @Test
    void a_resumed_session_resends_within_the_receive_maximum_of_the_new_connection()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: three deliveries that the first connection left unanswered.
            try (var first = RawSession.open(sut.port(), MqttVersion.V5)) {
                accepted(
                        first,
                        connect("resumed", true, sessionExpiry(), receiveMaximum(3)),
                        false
                );
                first.subscribe("resumed/#", AT_LEAST_ONCE);
                for (int index = 1; index <= 3; index++) {
                    publisher.publish("resumed/" + index, "message", AT_LEAST_ONCE, index);
                    first.expect(Packet.Publish.class);
                }
            }
            try (var second = RawSession.open(sut.port(), MqttVersion.V5)) {
                // Act
                accepted(
                        second,
                        connect("resumed", false, sessionExpiry(), receiveMaximum(1)),
                        true
                );

                // Assert: the broker resends them one at a time, in their order.
                for (int index = 1; index <= 3; index++) {
                    assertThat(second.expect(Packet.Publish.class))
                            .isEqualTo(resent("resumed/" + index, index));
                    second.ping();
                    second.send(new Packet.PubAck(index, SUCCESS, EMPTY));
                }
            }
        }
    }

    @Test
    void an_exchange_that_continues_from_an_earlier_connection_takes_no_place_in_the_maximum()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a QoS 2 delivery that the first connection left waiting for PUBCOMP.
            try (var first = RawSession.open(sut.port(), MqttVersion.V5)) {
                accepted(
                        first,
                        connect("resumed", true, sessionExpiry(), receiveMaximum(1)),
                        false
                );
                first.subscribe("resumed/#", EXACTLY_ONCE);
                publisher.publish("resumed/first", "message", EXACTLY_ONCE, 1);
                first.expect(Packet.Publish.class);
                first.send(new Packet.PubRec(1, SUCCESS, EMPTY));
                first.expect(Packet.PubRel.class);
            }
            try (var second = RawSession.open(sut.port(), MqttVersion.V5)) {
                accepted(
                        second,
                        connect("resumed", false, sessionExpiry(), receiveMaximum(1)),
                        true
                );
                assertThat(second.expect(Packet.PubRel.class))
                        .isEqualTo(new Packet.PubRel(1, SUCCESS, EMPTY));

                // Act
                publisher.publish("resumed/second", "message", AT_LEAST_ONCE, 2);

                // Assert
                assertThat(second.expect(Packet.Publish.class))
                        .isEqualTo(publication("resumed/second", "message", AT_LEAST_ONCE, 2));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Qos.class)
    void a_delivery_larger_than_the_client_takes_is_discarded_and_the_next_one_arrives(Qos qos)
            throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            accepted(subscriber, connect("subscriber", true, maximumPacketSize(64)), false);
            subscriber.subscribe("sized/#", EXACTLY_ONCE);
            boolean acknowledged = qos != AT_MOST_ONCE;

            // Act
            publisher.publish("sized/large", "x".repeat(100), qos, acknowledged ? 1 : 0);
            publisher.publish("sized/small", "small", qos, acknowledged ? 2 : 0);

            // Assert: the discarded delivery took no packet identifier.
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("sized/small", "small", qos, acknowledged ? 1 : 0));
        }
    }

    @Test
    void a_retained_message_larger_than_the_client_takes_is_not_sent() throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: at QoS 1, the broker has stored each one once its PUBACK arrives.
            publisher.publish(retained("kept/large", "x".repeat(100), AT_LEAST_ONCE, 1));
            publisher.publish(retained("kept/small", "small", AT_LEAST_ONCE, 2));
            accepted(subscriber, connect("subscriber", true, maximumPacketSize(64)), false);

            // Act
            subscriber.subscribe("kept/#", AT_MOST_ONCE);

            // Assert
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(retained("kept/small", "small", AT_MOST_ONCE, 0));
            subscriber.ping();
        }
    }

    @Test
    void a_delivery_too_large_to_resend_ends_as_if_the_client_had_answered_it()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a large delivery that the first connection left unanswered.
            try (var first = RawSession.open(sut.port(), MqttVersion.V5)) {
                accepted(first, connect("resumed", true, sessionExpiry()), false);
                first.subscribe("resumed/#", AT_LEAST_ONCE);
                publisher.publish("resumed/large", "x".repeat(100), AT_LEAST_ONCE, 1);
                first.expect(Packet.Publish.class);
            }
            try (var second = RawSession.open(sut.port(), MqttVersion.V5)) {
                // Act
                accepted(
                        second,
                        connect("resumed", false, sessionExpiry(), maximumPacketSize(64)),
                        true
                );

                // Assert
                second.ping();
            }
            // Assert: a later connection that takes it does not get it either.
            try (var third = RawSession.open(sut.port(), MqttVersion.V5)) {
                accepted(third, connect("resumed", false, sessionExpiry()), true);
                third.ping();
            }
        }
    }

    @Test
    void a_delivery_beyond_the_client_and_the_broker_limits_leaves_the_session_alone()
            throws Exception {
        try (BrokerServer sut = brokerWithSendBudget(256);
                var subscriber = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            accepted(subscriber, connect("subscriber", true, maximumPacketSize(64)), false);
            subscriber.subscribe("sized/#", AT_LEAST_ONCE);

            // Act: beyond the send budget, which alone ends the subscriber's connection, and
            // beyond what the subscriber takes.
            publisher.publish("sized/large", "x".repeat(512), AT_LEAST_ONCE, 1);
            publisher.publish("sized/small", "small", AT_LEAST_ONCE, 2);

            // Assert
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("sized/small", "small", AT_LEAST_ONCE, 1));
            subscriber.ping();
        }
    }

    /// Sends `connect` and checks that the broker accepts it, resuming a session or not.
    private static void accepted(RawSession client, Packet.Connect connect, boolean sessionPresent)
            throws Exception {
        client.send(connect);
        assertThat(client.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(sessionPresent));
    }

    /// An MQTT 5.0 CONNECT with `properties`; without Clean Start, it resumes the session.
    private static Packet.Connect connect(
            String clientId,
            boolean cleanStart,
            Property... properties
    ) {
        return new Packet.Connect(
                clientId,
                MqttVersion.V5,
                cleanStart,
                0,
                null,
                null,
                null,
                new Properties(List.of(properties))
        );
    }

    private static Property receiveMaximum(int value) {
        return new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, value);
    }

    private static Property maximumPacketSize(int value) {
        return new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, value);
    }

    /// Keeps the session for a minute after its connection ends.
    private static Property sessionExpiry() {
        return new Property.FourByteValue(PropertyId.SESSION_EXPIRY_INTERVAL, 60);
    }

    /// A QoS 1 delivery sent again with the DUP flag.
    private static Packet.Publish resent(String topic, int packetId) {
        return new Packet.Publish(
                new Topic(topic),
                "message".getBytes(UTF_8),
                AT_LEAST_ONCE,
                false,
                true,
                packetId,
                EMPTY
        );
    }

    private static Packet.Publish retained(String topic, String payload, Qos qos, int packetId) {
        return new Packet.Publish(
                new Topic(topic),
                payload.getBytes(UTF_8),
                qos,
                true,
                false,
                packetId,
                EMPTY
        );
    }

    /// A broker whose send queue, window, and delivery queue each hold `bytes`.
    private static BrokerServer brokerWithSendBudget(long bytes) throws IOException {
        return BrokerServer.builder()
                .sendBytes(bytes)
                .maxInflightBytes(bytes)
                .maxQueuedBytes(bytes)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }
}
