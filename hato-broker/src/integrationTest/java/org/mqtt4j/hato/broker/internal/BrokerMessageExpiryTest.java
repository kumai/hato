package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

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
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// How the broker counts down the Message Expiry Interval of the messages that wait in it, on a
/// clock that the test advances. A subscriber that takes one unanswered QoS 1 delivery at a time
/// keeps a second one waiting in the broker while the clock moves.
class BrokerMessageExpiryTest {

    private final BrokerDeadlineTest.ManualTimer timer = new BrokerDeadlineTest.ManualTimer();

    @Test
    void a_message_that_waits_in_the_broker_carries_what_is_left_of_its_lifetime()
            throws Exception {
        try (BrokerServer sut = broker(128);
                RawSession subscriber = oneAtATime(sut);
                RawSession publisher = connectedV5(sut.port(), "publisher")) {
            // Arrange
            publisher.publish("expiring/first", "first", AT_LEAST_ONCE, 1);
            publisher.publish(expiring("expiring/second", AT_LEAST_ONCE, 2, 10));
            Packet.Publish first = subscriber.expect(Packet.Publish.class);
            timer.clock.advance(Duration.ofMillis(3_500));

            // Act
            subscriber.send(new Packet.PubAck(first.packetId(), SUCCESS, EMPTY));

            // Assert: 10 seconds less the 3 whole seconds waited.
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(expiring("expiring/second", AT_LEAST_ONCE, 2, 7));
        }
    }

    @Test
    void a_message_whose_lifetime_passed_before_it_could_be_sent_is_dropped() throws Exception {
        try (BrokerServer sut = broker(128);
                RawSession subscriber = oneAtATime(sut);
                RawSession publisher = connectedV5(sut.port(), "publisher")) {
            // Arrange
            publisher.publish("expiring/first", "first", AT_LEAST_ONCE, 1);
            publisher.publish(expiring("expiring/second", AT_LEAST_ONCE, 2, 10));
            Packet.Publish first = subscriber.expect(Packet.Publish.class);
            timer.clock.advance(Duration.ofSeconds(10));

            // Act
            subscriber.send(new Packet.PubAck(first.packetId(), SUCCESS, EMPTY));

            // Assert
            subscriber.ping();
        }
    }

    @Test
    void a_message_without_any_lifetime_reaches_no_subscriber() throws Exception {
        try (BrokerServer sut = broker(128);
                RawSession subscriber = oneAtATime(sut);
                RawSession publisher = connectedV5(sut.port(), "publisher")) {
            // Act
            publisher.publish(expiring("expiring/none", AT_MOST_ONCE, 0, 0));

            // Assert
            subscriber.ping();
        }
    }

    @Test
    void a_delivery_that_started_in_time_is_resent_with_no_lifetime_left() throws Exception {
        try (BrokerServer sut = broker(128);
                RawSession watcher = connectedV5(sut.port(), "watcher");
                RawSession publisher = connectedV5(sut.port(), "publisher")) {
            // Arrange: a delivery that the subscriber leaves unanswered as it goes.
            watcher.subscribe("left/#", AT_MOST_ONCE);
            try (var subscriber = RawSession.open(sut.port(), MqttVersion.V5)) {
                subscriber.send(connect("resending", true, false));
                assertThat(subscriber.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(false));
                subscriber.subscribe("expiring/#", AT_LEAST_ONCE);
                publisher.publish(expiring("expiring/started", AT_LEAST_ONCE, 1, 5));
                subscriber.expect(Packet.Publish.class);
            }
            // The broker publishes the Will once it has detached the session.
            assertThat(watcher.expect(Packet.Publish.class).topic())
                    .isEqualTo(new Topic("left/resending"));
            timer.clock.advance(Duration.ofSeconds(6));

            // Act
            try (var resumed = RawSession.open(sut.port(), MqttVersion.V5)) {
                resumed.send(connect("resending", false, false));
                assertThat(resumed.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(true));

                // Assert
                Packet.Publish resent = resumed.expect(Packet.Publish.class);
                var none = new Property.FourByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, 0);
                assertThat(resent.dup()).isTrue();
                assertThat(resent.properties().find(PropertyId.MESSAGE_EXPIRY_INTERVAL))
                        .contains(none);
            }
        }
    }

    @Test
    void an_expired_retained_message_is_not_sent_and_frees_its_place() throws Exception {
        // Arrange: a broker that keeps a single retained message.
        try (BrokerServer sut = broker(128, 1);
                RawSession publisher = connectedV5(sut.port(), "publisher");
                RawSession early = connectedV5(sut.port(), "early");
                RawSession late = connectedV5(sut.port(), "late");
                RawSession last = connectedV5(sut.port(), "last")) {
            publisher.publish(retained("kept/expiring", 1, 5));
            timer.clock.advance(Duration.ofSeconds(4));
            early.subscribe("kept/#", AT_MOST_ONCE);
            Packet.Publish before = early.expect(Packet.Publish.class);

            // Act
            timer.clock.advance(Duration.ofSeconds(1));
            late.subscribe("kept/#", AT_MOST_ONCE);

            // Assert: 1 second was left before and nothing after, and the expired message no
            // longer takes the place of another one.
            assertThat(before.properties().find(PropertyId.MESSAGE_EXPIRY_INTERVAL))
                    .contains(new Property.FourByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, 1));
            late.ping();
            publisher.publish(retained("kept/lasting", 2, 60));
            last.subscribe("kept/#", AT_MOST_ONCE);
            assertThat(last.expect(Packet.Publish.class).topic())
                    .isEqualTo(new Topic("kept/lasting"));
        }
    }

    @Test
    void an_mqtt_3_1_1_subscriber_does_not_receive_an_expired_message() throws Exception {
        // Arrange: a window of one unanswered delivery holds the second one back.
        try (BrokerServer sut = broker(1);
                RawSession subscriber = RawSession.connected(sut.port(), "classic");
                RawSession publisher = connectedV5(sut.port(), "publisher")) {
            subscriber.subscribe("expiring/#", AT_LEAST_ONCE);
            publisher.publish("expiring/first", "first", AT_LEAST_ONCE, 1);
            publisher.publish(expiring("expiring/second", AT_LEAST_ONCE, 2, 10));
            Packet.Publish first = subscriber.expect(Packet.Publish.class);
            timer.clock.advance(Duration.ofSeconds(10));

            // Act
            subscriber.send(new Packet.PubAck(first.packetId(), SUCCESS, EMPTY));

            // Assert
            subscriber.ping();
        }
    }

    private BrokerServer broker(int maxInflight) throws IOException {
        return broker(maxInflight, 128);
    }

    private BrokerServer broker(int maxInflight, int maxRetained) throws IOException {
        return BrokerServer.builder()
                .connectTimeout(Duration.ofSeconds(5))
                .scheduler(timer)
                .maxInflight(maxInflight)
                .maxRetained(maxRetained)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    /// An MQTT 5.0 subscriber to `expiring/#` at QoS 1 that takes one unanswered delivery at a
    /// time, with its Receive Maximum.
    private static RawSession oneAtATime(BrokerServer broker) throws Exception {
        RawSession subscriber = RawSession.open(broker.port(), MqttVersion.V5);
        subscriber.send(connect("subscriber", true, true));
        assertThat(subscriber.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(false));
        subscriber.subscribe("expiring/#", AT_LEAST_ONCE);
        return subscriber;
    }

    /// An MQTT 5.0 CONNECT that keeps the session for a minute, with a Will that announces the
    /// end of the connection, and optionally a Receive Maximum of 1.
    private static Packet.Connect connect(String clientId, boolean cleanStart, boolean single) {
        var expiry = new Property.FourByteValue(PropertyId.SESSION_EXPIRY_INTERVAL, 60);
        var receiveMaximum = new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, 1);
        List<Property> properties = single ? List.of(expiry, receiveMaximum) : List.of(expiry);
        var will = new Packet.Will(
                new Topic("left/" + clientId),
                "gone".getBytes(UTF_8),
                AT_MOST_ONCE,
                false,
                EMPTY
        );
        return new Packet.Connect(
                clientId,
                MqttVersion.V5,
                cleanStart,
                0,
                will,
                null,
                null,
                new Properties(properties)
        );
    }

    /// A retained publication at QoS 1, which the broker has stored once its PUBACK arrives.
    private static Packet.Publish retained(String topic, int packetId, long lifetime) {
        Packet.Publish publication = expiring(topic, AT_LEAST_ONCE, packetId, lifetime);
        return new Packet.Publish(
                publication.topic(),
                publication.payload(),
                AT_LEAST_ONCE,
                true,
                false,
                packetId,
                publication.properties()
        );
    }

    /// A publication of `topic`, which is also its payload, with a Message Expiry Interval of
    /// `lifetime` seconds.
    private static Packet.Publish expiring(String topic, Qos qos, int packetId, long lifetime) {
        var expiry = new Property.FourByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, lifetime);
        return new Packet.Publish(
                new Topic(topic),
                topic.getBytes(UTF_8),
                qos,
                false,
                false,
                packetId,
                new Properties(List.of(expiry))
        );
    }
}
