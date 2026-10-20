package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The MQTT 5.0 subscription options on Mosquitto, and when a subscription that the client
/// makes again counts as new.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class Mosquitto5SubscriptionOptionsTest extends Mqtt5SubscriptionOptionsTest {

    private static final GenericContainer<?> broker = Mosquitto.container();

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @BeforeAll
    static void start() {
        Docker.start(broker);
    }

    @AfterAll
    static void stop() {
        broker.stop();
    }

    @Test
    void a_subscription_made_again_for_a_new_session_receives_the_retained_message_again()
            throws Exception {
        String topic = uniqueTopic();
        try (
                MqttClient sut = connected(Duration.ZERO);
                MqttClient publisher = connected(Duration.ZERO)
        ) {
            // Arrange
            publisher.publish(message(topic, 1).withRetain(true)).get(5, SECONDS);
            Subscription subscription = subscribe(
                    sut,
                    request(topic).withRetainHandling(RetainHandling.SEND_IF_NEW_SUBSCRIPTION)
            );
            assertThat(receive(subscription).payload()).containsExactly(1);
            sut.disconnect().get(5, SECONDS);

            // Act: the session ended with the connection, so the client makes the subscription
            // again.
            ConnAck reconnected = sut.connect().get(5, SECONDS);

            // Assert
            assertThat(reconnected.sessionPresent()).isFalse();
            MqttMessage retained = receive(subscription);
            assertThat(retained.payload()).containsExactly(1);
            assertThat(retained.retain()).isTrue();
        }
    }

    @Test
    void a_resumed_session_keeps_the_subscription_without_sending_the_retained_message_again()
            throws Exception {
        String topic = uniqueTopic();
        try (
                MqttClient sut = connected(Duration.ofMinutes(1));
                MqttClient publisher = connected(Duration.ZERO)
        ) {
            // Arrange
            publisher.publish(message(topic, 1).withRetain(true)).get(5, SECONDS);
            Subscription subscription = subscribe(
                    sut,
                    request(topic).withRetainHandling(RetainHandling.SEND_IF_NEW_SUBSCRIPTION)
            );
            assertThat(receive(subscription).payload()).containsExactly(1);
            sut.disconnect().get(5, SECONDS);

            // Act
            ConnAck resumed = sut.connect().get(5, SECONDS);
            publisher.publish(message(topic, 2)).get(5, SECONDS);

            // Assert: the later publication is the first to arrive.
            assertThat(resumed.sessionPresent()).isTrue();
            assertThat(receive(subscription).payload()).containsExactly(2);
        }
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(Mosquitto.MQTT_PORT)
        );
    }
}
