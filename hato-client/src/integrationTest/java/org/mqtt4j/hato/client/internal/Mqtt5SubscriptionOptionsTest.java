package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The MQTT 5.0 subscription options, shared by Mosquitto, EMQX, and HiveMQ CE: No Local, Retain
/// As Published, and Retain Handling. The embedded broker ignores the options and keeps no
/// retained messages, so it runs none of these.
@Timeout(30)
abstract class Mqtt5SubscriptionOptionsTest {

    @Test
    void a_no_local_subscription_receives_no_publication_of_its_own_client() throws Exception {
        String topic = uniqueTopic();
        try (
                MqttClient sut = connected(Duration.ZERO);
                MqttClient other = connected(Duration.ZERO)
        ) {
            // Arrange
            Subscription subscription = subscribe(sut, request(topic).withNoLocal(true));

            // Act: a broker has matched a publication with the subscriptions once it
            // acknowledges it.
            sut.publish(message(topic, 1)).get(5, SECONDS);
            other.publish(message(topic, 2)).get(5, SECONDS);

            // Assert: the other client's publication is the first to arrive.
            assertThat(receive(subscription).payload()).containsExactly(2);
        }
    }

    @Test
    void retain_as_published_keeps_the_retain_flag_that_the_publisher_set() throws Exception {
        String topic = uniqueTopic();
        try (
                MqttClient sut = connected(Duration.ZERO);
                MqttClient plain = connected(Duration.ZERO);
                MqttClient publisher = connected(Duration.ZERO)
        ) {
            // Arrange
            Subscription asPublished = subscribe(sut, request(topic).withRetainAsPublished(true));
            Subscription cleared = subscribe(plain, request(topic));

            // Act
            publisher.publish(message(topic, 1).withRetain(true)).get(5, SECONDS);

            // Assert: without the option, the broker clears the flag of a live publication.
            assertThat(receive(asPublished).retain()).isTrue();
            assertThat(receive(cleared).retain()).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(value = RetainHandling.class, names = {"SEND", "SEND_IF_NEW_SUBSCRIPTION"})
    void a_new_subscription_receives_the_retained_message_when_its_retain_handling_sends_it(
            RetainHandling handling
    ) throws Exception {
        String topic = uniqueTopic();
        try (
                MqttClient sut = connected(Duration.ZERO);
                MqttClient publisher = connected(Duration.ZERO)
        ) {
            // Arrange
            publisher.publish(message(topic, 1).withRetain(true)).get(5, SECONDS);

            // Act
            Subscription subscription = subscribe(
                    sut,
                    request(topic).withRetainHandling(handling)
            );

            // Assert: a retained delivery carries the retain flag.
            MqttMessage retained = receive(subscription);
            assertThat(retained.payload()).containsExactly(1);
            assertThat(retained.retain()).isTrue();
        }
    }

    @Test
    void a_subscription_that_withholds_retained_messages_receives_only_later_publications()
            throws Exception {
        String topic = uniqueTopic();
        try (
                MqttClient sut = connected(Duration.ZERO);
                MqttClient publisher = connected(Duration.ZERO)
        ) {
            // Arrange
            publisher.publish(message(topic, 1).withRetain(true)).get(5, SECONDS);

            // Act
            Subscription subscription = subscribe(
                    sut,
                    request(topic).withRetainHandling(RetainHandling.DO_NOT_SEND)
            );
            publisher.publish(message(topic, 2)).get(5, SECONDS);

            // Assert
            assertThat(receive(subscription).payload()).containsExactly(2);
        }
    }

    abstract URI endpoint();

    /// A connected MQTT 5.0 client with its own identifier and `sessionExpiry`.
    MqttClient connected(Duration sessionExpiry) throws Exception {
        MqttClient client = MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V5)
                .clientId("hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19))
                .sessionExpiry(sessionExpiry)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
        client.connect().get(5, SECONDS);
        return client;
    }

    static Subscribe request(String topic) {
        return Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE);
    }

    /// A QoS 1 message on `topic` with one payload byte.
    static MqttMessage message(String topic, int payload) {
        return MqttMessage.of(
                new Topic(topic),
                new byte[] {(byte) payload},
                Qos.AT_LEAST_ONCE,
                false
        );
    }

    static Subscription subscribe(MqttClient client, Subscribe request) throws Exception {
        Subscription subscription = client.subscribe(request);
        subscription.established().get(5, SECONDS);
        return subscription;
    }

    static MqttMessage receive(Subscription subscription) throws Exception {
        ReceiveResult.Received received = assertInstanceOf(
                ReceiveResult.Received.class,
                subscription.receive(Duration.ofSeconds(5))
        );
        return received.delivery().message();
    }
}
