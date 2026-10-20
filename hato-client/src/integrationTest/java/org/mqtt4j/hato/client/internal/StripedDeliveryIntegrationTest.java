package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// Real brokers complete the QoS 1 and 2 exchanges of a striped subscription whose deliveries
/// finish out of receive order, because the client still acknowledges them in receive order.
@Timeout(30)
class StripedDeliveryIntegrationTest {

    private static final int STRIPES = 4;

    private static final int MESSAGES = 24;

    private final String clientId = "hato" + UUID.randomUUID().toString().replace("-", "")
            .substring(0, 19);

    @Test
    void the_embedded_broker_completes_striped_deliveries_that_finish_out_of_order()
            throws Exception {
        try (MqttBroker broker = MqttBroker.start()) {
            outOfOrderRoundTrip(URI.create("mqtt://localhost:" + broker.port()));
        }
    }

    @Test
    @Timeout(60)
    @EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
    void mosquitto_completes_striped_deliveries_that_finish_out_of_order() throws Exception {
        try (GenericContainer<?> broker = Mosquitto.container()) {
            Mosquitto.start(broker);
            outOfOrderRoundTrip(
                    URI.create(
                            "mqtt://" + broker.getHost() + ":"
                                    + broker.getMappedPort(Mosquitto.MQTT_PORT)
                    )
            );
        }
    }

    static boolean dockerIsAvailableOrRequired() {
        return Mosquitto.availableOrRequired();
    }

    /// QoS 1 and 2 messages alternate over one topic per stripe. The first message is
    /// acknowledged only after the second, which is on another stripe.
    private void outOfOrderRoundTrip(URI endpoint) throws Exception {
        // Arrange
        String prefix = uniqueTopic();
        List<String> topics = topicsOnEachStripe(prefix);
        var secondAcknowledged = new CountDownLatch(1);
        var handled = new LinkedBlockingQueue<MqttMessage>();
        try (MqttClient sut = MqttClient.builder()
                .server(endpoint)
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .clientId(clientId)
                .cleanStart(false)
                .sessionExpiry(Duration.ofSeconds(30))
                .build();
                MqttClient publisher = MqttClient.builder()
                        .server(endpoint)
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .build()) {
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(prefix + "/+"), Qos.EXACTLY_ONCE)
                            .withAckMode(AckMode.MANUAL),
                    DeliveryOptions.striped(STRIPES),
                    delivery -> {
                        if (delivery.message().payload()[0] == 0) {
                            waitFor(secondAcknowledged);
                        }
                        delivery.ack();
                        handled.add(delivery.message());
                        if (delivery.message().payload()[0] == 1) {
                            secondAcknowledged.countDown();
                        }
                    }
            );
            subscription.established().get(5, SECONDS);

            // Act
            for (int index = 0; index < MESSAGES; index++) {
                Qos qos = index % 2 == 0 ? Qos.AT_LEAST_ONCE : Qos.EXACTLY_ONCE;
                publisher.publish(message(topics.get(index % STRIPES), index, qos)).get(5, SECONDS);
            }

            // Assert: every message ran once, and each topic kept its receive order.
            var received = new int[STRIPES];
            for (int count = 0; count < MESSAGES; count++) {
                MqttMessage message = requireNonNull(handled.poll(5, SECONDS));
                int topic = topics.indexOf(message.topic().name());
                assertThat(message.payload()[0])
                        .isEqualTo((byte) (received[topic] * STRIPES + topic));
                received[topic]++;
            }
            // The broker kept nothing unacknowledged: after a reconnection the marker arrives
            // first, and no message is delivered again.
            sut.disconnect().get(5, SECONDS);
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(true, 2));
            publisher.publish(message(topics.getFirst(), 100, Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            assertThat(requireNonNull(handled.poll(5, SECONDS)).payload()).containsExactly(100);
            sut.disconnect().get(5, SECONDS);
            publisher.disconnect().get(5, SECONDS);
        }
    }

    /// One topic under `prefix` for each stripe, in stripe order, so that the default striped
    /// key, the topic, sends each to a stripe of its own.
    private static List<String> topicsOnEachStripe(String prefix) {
        var topics = new String[STRIPES];
        int found = 0;
        for (int index = 0; found < STRIPES; index++) {
            String topic = prefix + "/" + index;
            int stripe = DeliveryLanes.stripeOf(new Topic(topic), STRIPES);
            if (topics[stripe] == null) {
                topics[stripe] = topic;
                found++;
            }
        }
        return List.of(topics);
    }

    private static MqttMessage message(String topic, int value, Qos qos) {
        return MqttMessage.of(new Topic(topic), new byte[] {(byte) value}, qos, false);
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, SECONDS)).isTrue();
        } catch (InterruptedException cause) {
            Thread.currentThread().interrupt();
            throw new AssertionError(cause);
        }
    }
}
