package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// Both brokers complete the QoS 1 and 2 exchanges of deliveries that striped and parallel
/// subscriptions finish out of receive order, because the client still acknowledges them in
/// receive order.
@Timeout(30)
abstract class ConcurrentDeliveryIntegrationTest {

    private static final int HANDLERS = 4;

    private static final int MESSAGES = 24;

    private static final byte MARKER = 100;

    abstract URI endpoint();

    @Test
    void striped_deliveries_finished_out_of_order_complete_in_each_topics_order()
            throws Exception {
        // Arrange: one topic for each stripe.
        String prefix = uniqueTopic();
        List<String> topics = topicsOnEachStripe(prefix);

        // Act
        RoundTrip result = outOfOrderRoundTrip(DeliveryOptions.striped(HANDLERS), prefix, topics);

        // Assert: every message ran once, in its topic's receive order, and the broker kept
        // nothing unacknowledged.
        var received = new int[HANDLERS];
        for (MqttMessage message : result.handled()) {
            int topic = topics.indexOf(message.topic().name());
            assertThat(message.payload()[0])
                    .isEqualTo((byte) (received[topic] * HANDLERS + topic));
            received[topic]++;
        }
        assertThat(received).containsOnly(MESSAGES / HANDLERS);
        assertThat(result.redelivered()).isEmpty();
    }

    @Test
    void parallel_deliveries_finished_out_of_order_complete() throws Exception {
        // Arrange
        String prefix = uniqueTopic();
        List<String> topics = topicsOnEachStripe(prefix);

        // Act
        RoundTrip result = outOfOrderRoundTrip(DeliveryOptions.parallel(HANDLERS), prefix, topics);

        // Assert: every message ran once, and the broker kept nothing unacknowledged.
        assertThat(result.handled())
                .extracting(message -> (int) message.payload()[0])
                .containsExactlyInAnyOrderElementsOf(
                        IntStream.range(0, MESSAGES).boxed().toList()
                );
        assertThat(result.redelivered()).isEmpty();
    }

    /// The messages that the handler took, and those that came again after a reconnection,
    /// before a later marker.
    private record RoundTrip(List<MqttMessage> handled, List<MqttMessage> redelivered) {}

    /// QoS 1 and 2 messages alternate over `topics` to a persistent client. Its handler
    /// acknowledges the first message only after the second, which runs at the same time on
    /// another stripe or handler. The client then reconnects, and a marker follows.
    private RoundTrip outOfOrderRoundTrip(
            DeliveryOptions options,
            String prefix,
            List<String> topics
    ) throws Exception {
        URI endpoint = endpoint();
        var secondAcknowledged = new CountDownLatch(1);
        var handled = new LinkedBlockingQueue<MqttMessage>();
        try (MqttClient sut = MqttClient.builder()
                .server(endpoint)
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .clientId("hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19))
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
                    options,
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
            for (int index = 0; index < MESSAGES; index++) {
                Qos qos = index % 2 == 0 ? Qos.AT_LEAST_ONCE : Qos.EXACTLY_ONCE;
                publisher.publish(message(topics.get(index % HANDLERS), index, qos))
                        .get(5, SECONDS);
            }
            var taken = new ArrayList<MqttMessage>();
            for (int count = 0; count < MESSAGES; count++) {
                taken.add(requireNonNull(handled.poll(5, SECONDS)));
            }

            sut.disconnect().get(5, SECONDS);
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(true, 2));
            publisher.publish(message(topics.getFirst(), MARKER, Qos.AT_LEAST_ONCE))
                    .get(5, SECONDS);
            var redelivered = new ArrayList<MqttMessage>();
            while (true) {
                MqttMessage next = requireNonNull(handled.poll(5, SECONDS));
                if (next.payload()[0] == MARKER) {
                    break;
                }
                redelivered.add(next);
            }
            sut.disconnect().get(5, SECONDS);
            publisher.disconnect().get(5, SECONDS);
            return new RoundTrip(taken, redelivered);
        }
    }

    /// One topic under `prefix` for each stripe, in stripe order, so that the default striped
    /// key, the topic, sends each to a stripe of its own.
    private static List<String> topicsOnEachStripe(String prefix) {
        var topics = new String[HANDLERS];
        int found = 0;
        for (int index = 0; found < HANDLERS; index++) {
            String topic = prefix + "/" + index;
            int stripe = DeliveryLanes.stripeOf(new Topic(topic), HANDLERS);
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
