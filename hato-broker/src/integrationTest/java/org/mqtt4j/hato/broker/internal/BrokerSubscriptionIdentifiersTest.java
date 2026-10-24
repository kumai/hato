package org.mqtt4j.hato.broker.internal;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mqtt4j.hato.broker.internal.RawSession.acceptedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// The Subscription Identifiers that MQTT 5.0 subscriptions carry, and the deliveries that
/// return them to the subscriber.
class BrokerSubscriptionIdentifiersTest {

    @Test
    void a_subscription_identifier_reaches_the_deliveries_of_its_subscription() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession subscriber = connectedV5(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            subscribe(subscriber, 1, subscription("ids/#", AT_MOST_ONCE), 7);

            // Act
            publisher.publish("ids/a", "message", AT_MOST_ONCE, 0);

            // Assert
            assertThat(identifiersOf(subscriber.expect(Packet.Publish.class))).containsExactly(7);
        }
    }

    @Test
    void one_copy_for_overlapping_subscriptions_carries_the_identifiers_of_them_all()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession subscriber = connectedV5(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: two subscriptions with identifiers, and one without.
            subscribe(subscriber, 1, subscription("ids/#", AT_MOST_ONCE), 1);
            subscribe(subscriber, 2, subscription("ids/+", AT_MOST_ONCE), 2);
            subscribe(subscriber, 3, subscription("ids/a", AT_MOST_ONCE), 0);

            // Act
            publisher.publish("ids/a", "message", AT_MOST_ONCE, 0);

            // Assert
            assertThat(identifiersOf(subscriber.expect(Packet.Publish.class)))
                    .containsExactlyInAnyOrder(1, 2);
            subscriber.ping();
        }
    }

    @Test
    void copies_split_by_the_retain_flag_carry_the_identifiers_of_their_subscriptions()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession subscriber = connectedV5(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: only the first subscription keeps the publisher's retain flag.
            subscribe(subscriber, 1, subscription("ids/#", AT_MOST_ONCE, false, true), 1);
            subscribe(subscriber, 2, subscription("ids/+", AT_MOST_ONCE, false, false), 2);

            // Act
            publisher.publish(retained("ids/a"));

            // Assert
            List<Packet.Publish> copies = List.of(
                    subscriber.expect(Packet.Publish.class),
                    subscriber.expect(Packet.Publish.class)
            );
            assertThat(copies)
                    .extracting(
                            Packet.Publish::retain,
                            BrokerSubscriptionIdentifiersTest::identifiersOf
                    )
                    .containsExactlyInAnyOrder(tuple(true, List.of(1)), tuple(false, List.of(2)));
        }
    }

    @Test
    void a_subscription_made_again_carries_its_new_identifier_or_none() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession subscriber = connectedV5(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            subscribe(subscriber, 1, subscription("ids/#", AT_MOST_ONCE), 1);

            // Act & Assert
            subscribe(subscriber, 2, subscription("ids/#", AT_MOST_ONCE), 2);
            publisher.publish("ids/a", "message", AT_MOST_ONCE, 0);
            assertThat(identifiersOf(subscriber.expect(Packet.Publish.class))).containsExactly(2);
            subscribe(subscriber, 3, subscription("ids/#", AT_MOST_ONCE), 0);
            publisher.publish("ids/a", "message", AT_MOST_ONCE, 0);
            assertThat(identifiersOf(subscriber.expect(Packet.Publish.class))).isEmpty();
        }
    }

    @Test
    void retained_messages_for_a_new_subscription_carry_its_identifier() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession subscriber = connectedV5(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            publisher.publish(retained("kept/a"));

            // Act
            subscribe(subscriber, 1, subscription("kept/#", AT_MOST_ONCE), 5);

            // Assert
            Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
            assertThat(delivery.retain()).isTrue();
            assertThat(identifiersOf(delivery)).containsExactly(5);
        }
    }

    @Test
    void subscriptions_that_share_an_identifier_repeat_it_in_their_copy() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession subscriber = connectedV5(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            subscribe(subscriber, 1, subscription("ids/#", AT_MOST_ONCE), 3);
            subscribe(subscriber, 2, subscription("ids/+", AT_MOST_ONCE), 3);

            // Act
            publisher.publish("ids/a", "message", AT_MOST_ONCE, 0);

            // Assert
            assertThat(identifiersOf(subscriber.expect(Packet.Publish.class)))
                    .containsExactly(3, 3);
            subscriber.ping();
        }
    }

    @Test
    void a_delivery_keeps_the_identifiers_that_it_was_matched_with() throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: the subscriber takes one QoS 1 delivery at a time, so the second one
            // waits while the subscription is made again with another identifier.
            var receiveMaximum = new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, 1);
            var connect = new Packet.Connect(
                    "subscriber",
                    MqttVersion.V5,
                    true,
                    0,
                    null,
                    null,
                    null,
                    new Properties(List.of(receiveMaximum))
            );
            subscriber.send(connect);
            assertThat(subscriber.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(false));
            subscribe(subscriber, 1, subscription("ids/#", AT_LEAST_ONCE), 1);
            publisher.publish("ids/first", "message", AT_LEAST_ONCE, 1);
            publisher.publish("ids/second", "message", AT_LEAST_ONCE, 2);
            Packet.Publish first = subscriber.expect(Packet.Publish.class);
            subscribe(subscriber, 2, subscription("ids/#", AT_LEAST_ONCE), 2);

            // Act
            subscriber.send(new Packet.PubAck(first.packetId(), SUCCESS, EMPTY));

            // Assert
            Packet.Publish second = subscriber.expect(Packet.Publish.class);
            assertThat(second.topic()).isEqualTo(new Topic("ids/second"));
            assertThat(identifiersOf(second)).containsExactly(1);
        }
    }

    /// Subscribes with `identifier`, or with none for 0, and checks that the broker grants the
    /// requested QoS.
    private static void subscribe(
            RawSession client,
            int packetId,
            Packet.Subscription subscription,
            int identifier
    ) throws Exception {
        Properties properties = (identifier == 0) ? EMPTY : identified(identifier);
        client.send(new Packet.Subscribe(packetId, List.of(subscription), properties));
        ReasonCode granted = switch (subscription.qos()) {
            case AT_MOST_ONCE -> SUCCESS;
            case AT_LEAST_ONCE -> ReasonCode.GRANTED_QOS_1;
            case EXACTLY_ONCE -> ReasonCode.GRANTED_QOS_2;
        };
        assertThat(client.expect(Packet.SubAck.class))
                .isEqualTo(new Packet.SubAck(packetId, List.of(granted), EMPTY));
    }

    private static Properties identified(int identifier) {
        var property = new Property.VariableByteValue(
                PropertyId.SUBSCRIPTION_IDENTIFIER,
                identifier
        );
        return new Properties(List.of(property));
    }

    private static List<Integer> identifiersOf(Packet.Publish publication) {
        return publication.properties().entries().stream()
                .filter(property -> property.id() == PropertyId.SUBSCRIPTION_IDENTIFIER)
                .map(Property.VariableByteValue.class::cast)
                .map(Property.VariableByteValue::value)
                .toList();
    }

    /// A retained publication at QoS 1, which the broker has stored once its PUBACK arrives.
    private static Packet.Publish retained(String topic) {
        return new Packet.Publish(
                new Topic(topic),
                "kept".getBytes(UTF_8),
                AT_LEAST_ONCE,
                true,
                false,
                1,
                EMPTY
        );
    }
}
