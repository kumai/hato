package org.mqtt4j.mqtj.client.internal;

import java.net.URI;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.mqtj.*;
import org.mqtt4j.mqtj.testsupport.Mosquitto;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class MosquittoMessagingTest extends MessagingTest {

    private static final GenericContainer<?> broker = Mosquitto.container();

    static boolean dockerIsAvailableOrRequired() {
        return Mosquitto.availableOrRequired();
    }

    @BeforeAll
    static void start() {
        Mosquitto.start(broker);
    }

    @AfterAll
    static void stop() {
        broker.stop();
    }

    @Test
    void a_new_subscription_receives_the_retained_message() throws Exception {
        // Arrange
        String topic = uniqueTopic();
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var retained = new MqttMessage(new Topic(topic), new byte[] {9}, Qos.AT_MOST_ONCE, true);
        try (MqttClient sut = client()) {
            sut.connect().get(5, SECONDS);
            sut.publish(retained).get(5, SECONDS);

            // Act: the same connection preserves publish-before-subscribe order at the broker.
            sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_MOST_ONCE),
                    received::add
            ).get(5, SECONDS);

            // Assert
            assertThat(requireNonNull(received.poll(5, SECONDS)).message()).isEqualTo(retained);

            // Remove retained state before finishing the connection.
            sut.publish(new MqttMessage(new Topic(topic), new byte[0], Qos.AT_MOST_ONCE, true))
                    .get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void broker_copies_for_overlapping_filters_reach_each_matching_subscription() throws Exception {
        // Arrange
        String base = uniqueTopic();
        var wildcard = new LinkedBlockingQueue<MqttDelivery>();
        var exact = new LinkedBlockingQueue<MqttDelivery>();
        MqttMessage publication = message(base + "/data", (byte) 3);
        try (MqttClient sut = client()) {
            sut.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/#"), Qos.AT_MOST_ONCE),
                    wildcard::add
            ).get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/data"), Qos.AT_MOST_ONCE),
                    exact::add
            ).get(5, SECONDS);

            // Act: Mosquitto 2.1's default sends a copy for each matching subscription.
            sut.publish(publication).get(5, SECONDS);
            sut.publish(message(base + "/barrier", (byte) 4)).get(5, SECONDS);

            // Assert: 3.1.1 carries no identifier to route a copy to only its intended filter.
            for (int i = 0; i < 2; i++) {
                assertThat(requireNonNull(wildcard.poll(5, SECONDS)).message())
                        .isEqualTo(publication);
                assertThat(requireNonNull(exact.poll(5, SECONDS)).message())
                        .isEqualTo(publication);
            }

            // FIFO delivery of the barrier proves no third copy preceded it in the wildcard lane.
            assertThat(requireNonNull(wildcard.poll(5, SECONDS)).message())
                    .isEqualTo(message(base + "/barrier", (byte) 4));
            assertThat(exact).isEmpty();
        }
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(Mosquitto.MQTT_PORT)
        );
    }
}
