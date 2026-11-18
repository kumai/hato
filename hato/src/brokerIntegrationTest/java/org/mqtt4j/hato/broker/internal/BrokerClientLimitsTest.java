package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Properties;
import org.mqtt4j.hato.internal.protocol.Property;
import org.mqtt4j.hato.internal.protocol.PropertyId;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.ReasonCode.IMPLEMENTATION_SPECIFIC_ERROR;
import static org.mqtt4j.hato.ReasonCode.PACKET_TOO_LARGE;
import static org.mqtt4j.hato.ReasonCode.SUCCESS;
import static org.mqtt4j.hato.broker.internal.BrokerSessionRaceTest.awaitRelease;
import static org.mqtt4j.hato.broker.internal.RawSession.acceptedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.deliverySize;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.internal.protocol.Properties.EMPTY;

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
    void a_qos_2_delivery_too_large_to_resend_goes_on_to_its_pubrel() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a large QoS 2 delivery that the first connection received and left
            // unanswered, so that the client may hold its identifier.
            int id;
            try (var first = RawSession.open(sut.port(), MqttVersion.V5)) {
                accepted(first, connect("resumed", true, sessionExpiry()), false);
                first.subscribe("resumed/#", EXACTLY_ONCE);
                publisher.publish("resumed/large", "x".repeat(100), EXACTLY_ONCE, 1);
                id = first.expect(Packet.Publish.class).packetId();
            }
            try (var second = RawSession.open(sut.port(), MqttVersion.V5)) {
                // Act
                accepted(
                        second,
                        connect("resumed", false, sessionExpiry(), maximumPacketSize(64)),
                        true
                );

                // Assert: the message does not come again, and PUBREL frees the identifier.
                assertThat(second.expect(Packet.PubRel.class))
                        .isEqualTo(new Packet.PubRel(id, SUCCESS, EMPTY));
                second.send(new Packet.PubComp(id, SUCCESS, EMPTY));
                second.ping();
            }
        }
    }

    @Test
    void a_delivery_beyond_the_client_and_the_broker_limits_leaves_the_session_alone()
            throws Exception {
        // Arrange: a budget that holds the small delivery exactly.
        long budget = deliverySize(publication("sized/small", "small", AT_LEAST_ONCE, 1));
        try (BrokerServer sut = brokerWithSendBudget(budget);
                var subscriber = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession publisher = connected(sut.port(), "publisher")) {
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

    @Test
    void a_client_that_takes_no_connack_is_refused_and_leaves_the_connection_of_its_identifier()
            throws Exception {
        try (var sut = MqttBroker.start();
                var first = RawSession.open(sut.port(), MqttVersion.V5);
                var second = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Arrange: the CONNACK that accepts a client announces the broker's limits in 15
            // bytes.
            accepted(first, connect("robot", true), false);

            // Act
            second.send(connect("robot", true, maximumPacketSize(10)));

            // Assert: a refusal of 5 bytes, and the connection that it would have taken over
            // goes on.
            assertThat(second.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, IMPLEMENTATION_SPECIFIC_ERROR, EMPTY));
            second.expectClosed();
            first.ping();
        }
    }

    @Test
    void a_client_that_takes_not_even_a_refusal_loses_its_connection_without_connack()
            throws Exception {
        try (var sut = MqttBroker.start();
                var client = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Act: a CONNACK takes 5 bytes at least.
            client.send(connect("robot", true, maximumPacketSize(4)));

            // Assert
            client.expectClosed();
        }
    }

    @Test
    void a_shutdown_that_refuses_a_client_as_its_connect_arrives_keeps_to_its_limit()
            throws Exception {
        // Arrange: the selector holds the CONNECT of a client that takes 4 bytes at most, while
        // a shutdown refuses the client and waits for its connection to close.
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (
                BrokerServer sut = BrokerServer.builder()
                        .beforeProcess(packet -> {
                            if (packet instanceof Packet.Connect) {
                                held.countDown();
                                awaitRelease(release);
                            }
                        })
                        .start(loopback());
                var client = RawSession.open(sut.port(), MqttVersion.V5)
        ) {
            client.send(connect("robot", true, maximumPacketSize(4)));
            assertThat(held.await(5, SECONDS)).isTrue();
            Thread shuttingDown = Thread.ofPlatform().start(() -> sut.shutdown(Duration.ZERO));
            await().until(() -> LockSupport.getBlocker(shuttingDown) == sut);

            // Act
            release.countDown();

            // Assert: no CONNACK, which takes 5 bytes at least.
            client.expectClosed();
            shuttingDown.join();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_subscribe_whose_suback_the_client_does_not_take_ends_the_connection_unsubscribed()
            throws Exception {
        try (BrokerServer sut = BrokerServer.builder().start(loopback());
                var client = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Arrange: a session that outlives the connection.
            accepted(client, connect("robot", true, maximumPacketSize(64), sessionExpiry()), false);

            // Act: the SUBACK for 64 filters takes 69 bytes.
            client.send(new Packet.Subscribe(1, subscriptions(0, 64), EMPTY));

            // Assert
            assertThat(client.expect(Packet.Disconnect.class))
                    .isEqualTo(new Packet.Disconnect(PACKET_TOO_LARGE, EMPTY));
            client.expectClosed();
            assertThat(sut.indexedFor(new Topic("topic/0"))).isEmpty();
        }
    }

    @Test
    void an_unsubscribe_whose_unsuback_the_client_does_not_take_ends_the_connection_subscribed()
            throws Exception {
        try (BrokerServer sut = BrokerServer.builder().start(loopback());
                var client = RawSession.open(sut.port(), MqttVersion.V5)) {
            // Arrange: a session that outlives the connection, with 64 subscriptions made 32 at
            // a time, whose SUBACKs take 37 bytes.
            accepted(client, connect("robot", true, maximumPacketSize(64), sessionExpiry()), false);
            client.send(new Packet.Subscribe(1, subscriptions(0, 32), EMPTY));
            client.expect(Packet.SubAck.class);
            client.send(new Packet.Subscribe(2, subscriptions(32, 32), EMPTY));
            client.expect(Packet.SubAck.class);

            // Act: the UNSUBACK for all 64 takes 69 bytes.
            List<TopicFilter> filters = subscriptions(0, 64).stream()
                    .map(Packet.Subscription::filter)
                    .toList();
            client.send(new Packet.Unsubscribe(3, filters, EMPTY));

            // Assert
            assertThat(client.expect(Packet.Disconnect.class))
                    .isEqualTo(new Packet.Disconnect(PACKET_TOO_LARGE, EMPTY));
            client.expectClosed();
            assertThat(sut.indexedFor(new Topic("topic/0"))).containsExactly("robot");
        }
    }

    /// QoS 0 subscriptions to `count` topics, from topic/`first` on.
    private static List<Packet.Subscription> subscriptions(int first, int count) {
        var subscriptions = new ArrayList<Packet.Subscription>();
        for (int index = first; index < first + count; index++) {
            subscriptions.add(subscription("topic/" + index, AT_MOST_ONCE));
        }
        return subscriptions;
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
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
