package org.mqtt4j.hato.client.internal;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// Retain Handling, with the other MQTT 5.0 subscription options, shared by Mosquitto, EMQX, and
/// HiveMQ CE. The embedded broker keeps no retained messages, so it runs only the other options.
abstract class Mqtt5RetainHandlingTest extends Mqtt5SubscriptionOptionsTest {

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
}
