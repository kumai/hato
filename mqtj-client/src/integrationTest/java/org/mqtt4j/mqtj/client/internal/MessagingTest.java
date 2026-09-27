package org.mqtt4j.mqtj.client.internal;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.mqtt4j.mqtj.*;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/// Public messaging contracts shared by the embedded broker and Mosquitto.
abstract class MessagingTest {

    @Test
    void public_api_publishes_subscribes_and_reuses_a_filter_after_unsubscribe() throws Exception {
        // Arrange
        String topic = uniqueTopic();
        var firstDeliveries = new LinkedBlockingQueue<MqttDelivery>();
        var secondDeliveries = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            sut.connect().get(5, SECONDS);

            // Act: establish the first subscription.
            Subscription first = sut.subscribe(
                    new Subscribe(new TopicFilter(topic), Qos.AT_MOST_ONCE),
                    firstDeliveries::add
            ).get(5, SECONDS);

            // Assert
            assertThat(first.filter()).isEqualTo(new TopicFilter(topic));
            assertThat(first.grantedQos()).isEqualTo(Qos.AT_MOST_ONCE);

            // Act: deliver a message before removing the subscription.
            sut.publish(message(topic, (byte) 1)).get(5, SECONDS);
            MqttDelivery received = requireNonNull(firstDeliveries.poll(5, SECONDS));

            // Assert
            assertThat(received.message()).isEqualTo(message(topic, (byte) 1));
            assertThat(received.dup()).isFalse();

            // Act: remove the first subscription, then reuse its filter.
            first.unsubscribe().get(5, SECONDS);
            Subscription second = sut.subscribe(
                    new Subscribe(new TopicFilter(topic), Qos.AT_MOST_ONCE),
                    secondDeliveries::add
            ).get(5, SECONDS);
            sut.publish(message(topic, (byte) 2)).get(5, SECONDS);

            // Assert: only the replacement receives the next publication.
            assertThat(requireNonNull(secondDeliveries.poll(5, SECONDS)).message())
                    .isEqualTo(message(topic, (byte) 2));
            assertThat(firstDeliveries).isEmpty();

            // Act & Assert: removal and graceful disconnection also finish within the deadline.
            second.unsubscribe().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void one_subscription_runs_handlers_in_publication_order() throws Exception {
        // Arrange
        String topic = uniqueTopic();
        var firstStarted = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var received = new LinkedBlockingQueue<Integer>();
        try (MqttClient sut = client()) {
            sut.connect().get(5, SECONDS);
            sut.subscribe(
                    new Subscribe(new TopicFilter(topic), Qos.AT_MOST_ONCE),
                    delivery -> {
                        int number = delivery.message().payload()[0];
                        if (number == 0) {
                            firstStarted.countDown();
                            try {
                                assertThat(releaseFirst.await(5, SECONDS)).isTrue();
                            } catch (InterruptedException cause) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError(cause);
                            }
                        }
                        received.add(number);
                    }
            ).get(5, SECONDS);

            // Act: later publications arrive while the first handler occupies the lane.
            sut.publish(message(topic, (byte) 0)).get(5, SECONDS);
            assertThat(firstStarted.await(5, SECONDS)).isTrue();
            for (int i = 1; i < 8; i++) {
                sut.publish(message(topic, (byte) i)).get(5, SECONDS);
            }
            releaseFirst.countDown();

            // Assert
            for (int i = 0; i < 8; i++) {
                assertThat(received.poll(5, SECONDS)).isEqualTo(i);
            }
        } finally {
            releaseFirst.countDown();
        }
    }

    @Test
    void overlapping_shared_subscription_is_rejected_without_stopping_ordinary_delivery()
            throws Exception {
        // Arrange: the ordinary subscription already covers the shared filter's topics.
        String base = uniqueTopic();
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var shared = new Subscribe(
                new TopicFilter("$share/workers/" + base + "/+"),
                Qos.AT_MOST_ONCE
        );
        try (MqttClient sut = client()) {
            sut.connect().get(5, SECONDS);
            Subscription ordinary = sut.subscribe(
                    new Subscribe(new TopicFilter(base + "/#"), Qos.AT_MOST_ONCE),
                    received::add
            ).get(5, SECONDS);

            // Act & Assert: rejecting the conflicting request leaves ordinary delivery usable.
            assertThatThrownBy(() -> sut.subscribe(shared, _ -> { }).get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            sut.publish(message(base + "/job", (byte) 1)).get(5, SECONDS);
            assertThat(requireNonNull(received.poll(5, SECONDS)).message())
                    .isEqualTo(message(base + "/job", (byte) 1));

            // Act & Assert: acknowledged removal permits the previously conflicting filter.
            ordinary.unsubscribe().get(5, SECONDS);
            Subscription replacement = sut.subscribe(shared, _ -> { }).get(5, SECONDS);
            replacement.unsubscribe().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void a_handler_failure_is_reported_without_a_pending_operation() throws Exception {
        // Arrange
        String topic = uniqueTopic();
        var failure = new IllegalStateException("handler failed");
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = client(ended::add); MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            sut.subscribe(
                    new Subscribe(new TopicFilter(topic), Qos.AT_MOST_ONCE),
                    _ -> {
                        throw failure;
                    }
            ).get(5, SECONDS);

            // Act: the subscriber has no pending operation to carry this failure.
            publisher.publish(message(topic, (byte) 1)).get(5, SECONDS);
            Disconnection disconnection = requireNonNull(ended.poll(5, SECONDS));

            // Assert
            assertThat(disconnection.cause()).isSameAs(failure);
            assertThat(disconnection.userRequested()).isFalse();
        }
    }

    @Test
    void a_late_handler_failure_does_not_end_the_new_connection() throws Exception {
        // Arrange: keep an old connection's handler running across a reconnect.
        String topic = uniqueTopic();
        var failure = new IllegalStateException("old handler failed");
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var worker = new AtomicReference<Thread>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client(ended::add)) {
            sut.connect().get(5, SECONDS);
            sut.subscribe(
                    new Subscribe(new TopicFilter(topic), Qos.AT_MOST_ONCE),
                    _ -> {
                        worker.set(Thread.currentThread());
                        entered.countDown();
                        try {
                            assertThat(release.await(30, SECONDS)).isTrue();
                        } catch (InterruptedException cause) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(cause);
                        }
                        throw failure;
                    }
            ).get(5, SECONDS);
            sut.publish(message(topic, (byte) 1)).get(5, SECONDS);
            assertThat(entered.await(5, SECONDS)).isTrue();
            sut.disconnect().get(5, SECONDS);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
            sut.connect().get(5, SECONDS);
            sut.subscribe(
                    new Subscribe(new TopicFilter(topic), Qos.AT_MOST_ONCE),
                    received::add
            ).get(5, SECONDS);

            // Act: the old handler fails after the new connection is ready.
            release.countDown();
            await().until(() -> !requireNonNull(worker.get()).isAlive());

            // Act & Assert: new delivery and the next ordered notification remain normal.
            sut.publish(message(topic, (byte) 2)).get(5, SECONDS);
            assertThat(requireNonNull(received.poll(5, SECONDS)).message())
                    .isEqualTo(message(topic, (byte) 2));
            sut.disconnect().get(5, SECONDS);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
            assertThat(ended).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void receive_overload_closes_without_waiting_for_the_running_handler() throws Exception {
        // Arrange
        String topic = uniqueTopic();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var returned = new CountDownLatch(1);
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = client(ended::add); MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            sut.subscribe(
                    new Subscribe(new TopicFilter(topic), Qos.AT_MOST_ONCE, 1),
                    _ -> {
                        entered.countDown();
                        try {
                            assertThat(release.await(10, SECONDS)).isTrue();
                        } catch (InterruptedException cause) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(cause);
                        } finally {
                            returned.countDown();
                        }
                    }
            ).get(5, SECONDS);
            publisher.publish(message(topic, (byte) 1)).get(5, SECONDS);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act: the running handler still owns the only receive slot.
            publisher.publish(message(topic, (byte) 2)).get(5, SECONDS);
            Disconnection disconnection = requireNonNull(ended.poll(5, SECONDS));

            // Assert: shutdown progresses while application code is blocked.
            assertThat(disconnection.cause()).isInstanceOf(MqttCapacityException.class);
            assertThat(disconnection.userRequested()).isFalse();
            assertThat(returned.getCount()).isEqualTo(1);

            // Act & Assert: the handler can return normally after connection termination.
            release.countDown();
            assertThat(returned.await(5, SECONDS)).isTrue();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_handler_can_wait_for_a_publication_on_the_same_client() throws Exception {
        // Arrange
        String base = uniqueTopic();
        var replies = new LinkedBlockingQueue<MqttDelivery>();
        var handled = new CompletableFuture<Void>();
        try (MqttClient sut = client()) {
            sut.connect().get(5, SECONDS);
            sut.subscribe(
                    new Subscribe(new TopicFilter(base + "/reply"), Qos.AT_MOST_ONCE),
                    replies::add
            ).get(5, SECONDS);
            sut.subscribe(
                    new Subscribe(new TopicFilter(base + "/request"), Qos.AT_MOST_ONCE),
                    delivery -> {
                        sut.publish(message(base + "/reply", delivery.message().payload())).join();
                        handled.complete(null);
                    }
            ).get(5, SECONDS);

            // Act
            sut.publish(message(base + "/request", (byte) 7)).get(5, SECONDS);

            // Assert
            handled.get(5, SECONDS);
            assertThat(requireNonNull(replies.poll(5, SECONDS)).message())
                    .isEqualTo(message(base + "/reply", (byte) 7));
        }
    }

    MqttClient client() {
        return client(_ -> {});
    }

    MqttClient client(ConnectionListener listener) {
        return MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
                .connectionListener(listener)
                .build();
    }

    static String uniqueTopic() {
        return "mqtj/client/" + UUID.randomUUID();
    }

    static MqttMessage message(String topic, byte... payload) {
        return new MqttMessage(new Topic(topic), payload, Qos.AT_MOST_ONCE, false);
    }

    abstract URI endpoint();
}
