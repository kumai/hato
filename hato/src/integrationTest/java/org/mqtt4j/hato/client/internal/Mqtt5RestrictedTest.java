package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The client against brokers configured to announce smaller MQTT 5.0 limits than their
/// defaults, shared by Mosquitto, EMQX, HiveMQ CE, RabbitMQ, and the embedded broker: Maximum
/// QoS 1, a Maximum Packet Size of 512 bytes, and no Topic Aliases from the client. The client
/// refuses what the limits forbid and keeps to what they allow, so the connection goes on. A
/// broker that would end the connection over a violation, as most of them do, shows that the
/// client sent none. NanoMQ 0.25.7 cannot take part: it has no setting for the Maximum QoS, and
/// it announces the client's own Maximum Packet Size in place of the one that it enforces.
@Timeout(30)
abstract class Mqtt5RestrictedTest {

    @Test
    void the_connack_reports_the_limits_that_the_broker_announces() throws Exception {
        try (MqttClient sut = client()) {
            // Act
            ConnAck ack = sut.connect().get(5, SECONDS);

            // Assert
            assertThat(ack).isEqualTo(ConnAck.of(false, 1)
                    .withMaximumQos(Qos.AT_LEAST_ONCE)
                    .withRetainAvailable(retainAvailable())
                    .withWildcardSubscriptionAvailable(wildcardSubscriptionAvailable())
                    .withSharedSubscriptionAvailable(sharedSubscriptionAvailable())
                    .withMaximumPacketSize(512));
        }
    }

    @Test
    void a_qos_2_publication_is_refused_and_the_connection_goes_on() throws Exception {
        assertRefusedWhileTheConnectionGoesOn(
                MqttMessage.of(new Topic(uniqueTopic()), new byte[] {1}, Qos.EXACTLY_ONCE, false),
                ReasonCode.QOS_NOT_SUPPORTED
        );
    }

    @Test
    void a_publication_beyond_the_maximum_packet_size_is_refused_and_the_connection_goes_on()
            throws Exception {
        assertRefusedWhileTheConnectionGoesOn(
                MqttMessage.of(new Topic(uniqueTopic()), new byte[600], Qos.AT_LEAST_ONCE, false),
                ReasonCode.PACKET_TOO_LARGE
        );
    }

    @Test
    void a_subscription_beyond_the_maximum_packet_size_is_refused_and_the_connection_goes_on()
            throws Exception {
        assertRefusedWhileTheConnectionGoesOn(
                uniqueTopic() + "/" + "a".repeat(600),
                ReasonCode.PACKET_TOO_LARGE
        );
    }

    @Test
    void repeated_publications_on_one_topic_go_without_topic_aliases() throws Exception {
        var topic = new Topic(uniqueTopic());
        var received = new LinkedBlockingQueue<MqttMessage>();
        try (MqttClient subscriber = client(); MqttClient publisher = client()) {
            // Arrange: the broker takes no Topic Aliases from the client.
            subscriber.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            subscriber.subscribe(
                    Subscribe.of(new TopicFilter(topic.name()), Qos.AT_LEAST_ONCE),
                    delivery -> received.add(delivery.message())
            ).established().get(5, SECONDS);
            List<MqttMessage> messages = List.of(
                    MqttMessage.of(topic, new byte[] {1}, Qos.AT_LEAST_ONCE, false),
                    MqttMessage.of(topic, new byte[] {2}, Qos.AT_LEAST_ONCE, false),
                    MqttMessage.of(topic, new byte[] {3}, Qos.AT_LEAST_ONCE, false)
            );

            // Act
            for (MqttMessage message : messages) {
                publisher.publish(message).get(5, SECONDS);
            }

            // Assert: a broker that saw an alias would end the connection.
            for (MqttMessage message : messages) {
                assertThat(received.poll(5, SECONDS)).isEqualTo(message);
            }
        }
    }

    /// Whether the broker announces retained messages as available, as recorded for it.
    abstract boolean retainAvailable();

    /// Whether the broker announces wildcard subscriptions as available, as recorded for it.
    abstract boolean wildcardSubscriptionAvailable();

    /// Whether the broker announces shared subscriptions as available, as recorded for it.
    abstract boolean sharedSubscriptionAvailable();

    abstract URI endpoint();

    MqttClient client() {
        return MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V5)
                .clientId("hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19))
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }

    /// Refusing `message` with `reason` leaves the connection to a QoS 1 publication.
    void assertRefusedWhileTheConnectionGoesOn(MqttMessage message, ReasonCode reason)
            throws Exception {
        try (MqttClient sut = client()) {
            // Arrange
            sut.connect().get(5, SECONDS);

            // Act & Assert
            assertThatThrownBy(() -> sut.publish(message).get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOfSatisfying(
                            MqttPublicationRefusedException.class,
                            refusal -> assertThat(refusal.reasonCode()).isEqualTo(reason)
                    );
            assertThat(sut.publish(MqttMessage.of(
                    new Topic(uniqueTopic()),
                    new byte[] {1},
                    Qos.AT_LEAST_ONCE,
                    false
            ))).succeedsWithin(Duration.ofSeconds(5));
        }
    }

    /// Refusing a subscription to `filter` with `reason` leaves the connection to another one.
    void assertRefusedWhileTheConnectionGoesOn(String filter, ReasonCode reason)
            throws Exception {
        try (MqttClient sut = client()) {
            // Arrange
            sut.connect().get(5, SECONDS);

            // Act
            Subscription refused = sut.subscribe(
                    Subscribe.of(new TopicFilter(filter), Qos.AT_LEAST_ONCE)
            );

            // Assert
            assertThatThrownBy(() -> refused.established().get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOfSatisfying(
                            MqttSubscriptionRefusedException.class,
                            refusal -> assertThat(refusal.reasonCode()).isEqualTo(reason)
                    );
            assertThat(sut.subscribe(
                    Subscribe.of(new TopicFilter(uniqueTopic()), Qos.AT_LEAST_ONCE)
            ).established()).succeedsWithin(Duration.ofSeconds(5));
        }
    }
}
