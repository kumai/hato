package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.HiveMq;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The client against HiveMQ CE configured to announce smaller MQTT 5.0 limits.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class HiveMq5RestrictedTest extends Mqtt5RestrictedSubscriptionsTest {

    private static final GenericContainer<?> broker = HiveMq.restrictedContainer();

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
    void a_subscription_goes_without_a_subscription_identifier() throws Exception {
        var topic = new Topic(uniqueTopic());
        var received = new LinkedBlockingQueue<MqttMessage>();
        try (MqttClient sut = client(); MqttClient publisher = client()) {
            // Arrange: the broker takes no Subscription Identifiers.
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            var message = MqttMessage.of(topic, new byte[] {1}, Qos.AT_LEAST_ONCE, false);

            // Act
            sut.subscribe(
                    Subscribe.of(new TopicFilter(topic.name()), Qos.AT_LEAST_ONCE),
                    delivery -> received.add(delivery.message())
            ).established().get(5, SECONDS);
            publisher.publish(message).get(5, SECONDS);

            // Assert: a broker that saw an identifier would end the connection.
            assertThat(received.poll(5, SECONDS)).isEqualTo(message);
        }
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(HiveMq.MQTT_PORT)
        );
    }
}
