package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.spi.Transport;
import org.mqtt4j.hato.testkit.FaultyTransport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/// Public messaging contracts shared by the embedded broker and Mosquitto.
abstract class MessagingTest {

    static final ClientActor.Usage NOTHING_HELD = new ClientActor.Usage(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0
    );

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_subscription_registered_before_connect_receives_on_its_original_handle(boolean pull)
            throws Exception {
        // Arrange: both brokers accept registrations made before any connection exists.
        String topic = uniqueTopic();
        var handled = new LinkedBlockingQueue<MqttDelivery>();
        var request = Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE);
        try (MqttClient sut = client()) {
            Subscription subscription = pull ? sut.subscribe(request)
                    : sut.subscribe(request, handled::add);
            CompletableFuture<Qos> established = subscription.established();
            assertThat(established).isNotDone();

            // Act
            sut.connect().get(5, SECONDS);
            assertThat(established.get(5, SECONDS)).isEqualTo(Qos.AT_LEAST_ONCE);
            MqttMessage message = MqttMessage.of(
                    new Topic(topic),
                    new byte[] {7},
                    Qos.AT_LEAST_ONCE,
                    false
            );
            sut.publish(message).get(5, SECONDS);

            // Assert
            MqttDelivery delivery = pull ? received(subscription.receive(Duration.ofSeconds(5)))
                    : requireNonNull(handled.poll(5, SECONDS));
            assertThat(delivery.message()).isEqualTo(message);
            subscription.unsubscribe().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(30)
    void a_hundred_subscriptions_can_close_together_and_register_the_same_filters_again(
            boolean pull
    ) throws Exception {
        // Arrange
        String base = uniqueTopic();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = client(ended::add)) {
            sut.connect().get(5, SECONDS);
            var subscriptions = new ArrayList<Subscription>();
            for (int i = 0; i < 100; i++) {
                var request = Subscribe.of(new TopicFilter(base + "/" + i), Qos.AT_LEAST_ONCE);
                Subscription subscription = pull ? sut.subscribe(request)
                        : sut.subscribe(request, _ -> { });
                subscription.established().get(5, SECONDS);
                subscriptions.add(subscription);
            }

            // Act: disposal does not require admission of 100 public operations at once.
            subscriptions.forEach(Subscription::close);
            for (Subscription subscription : subscriptions) {
                subscription.unsubscribe().get(5, SECONDS);
            }

            // Assert: UNSUBACK released every filter, and new subscriptions can receive QoS 1.
            for (Subscription subscription : subscriptions) {
                Subscription replacement = sut.subscribe(
                        Subscribe.of(subscription.filter(), Qos.AT_LEAST_ONCE)
                );
                replacement.established().get(5, SECONDS);
                var message = MqttMessage.of(new Topic(subscription.filter().pattern()),
                        new byte[] {7}, Qos.AT_LEAST_ONCE, false);
                sut.publish(message).get(5, SECONDS);
                assertThat(received(replacement.receive(Duration.ofSeconds(5))).message())
                        .isEqualTo(message);
                replacement.unsubscribe().get(5, SECONDS);
            }
            assertThat(ended).isEmpty();
            sut.disconnect().get(5, SECONDS);
        }
    }

    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_MOST_ONCE", "AT_LEAST_ONCE"})
    @Timeout(30)
    void publications_round_trip_through_the_default_pull_subscription(Qos qos) throws Exception {
        // Arrange
        String topic = uniqueTopic();
        var publication = MqttMessage.of(new Topic(topic), new byte[] {1, 2, 3}, qos, false);
        try (MqttClient sut = client()) {
            sut.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(Subscribe.of(new TopicFilter(topic), qos));
            subscription.established().get(5, SECONDS);

            // Act
            sut.publish(publication).get(5, SECONDS);
            MqttDelivery delivery = received(subscription.receive(Duration.ofSeconds(5)));

            // Assert
            assertThat(delivery.message()).isEqualTo(publication);
            subscription.unsubscribe().get(5, SECONDS);
            assertThat(subscription.receive()).isEqualTo(new ReceiveResult.Ended());
            sut.disconnect().get(5, SECONDS);
        }
    }

    @ParameterizedTest
    @EnumSource(value = AckMode.class, names = {"ON_RECEIVE", "MANUAL"})
    void eight_virtual_receivers_share_five_hundred_twelve_publications_once_each(AckMode mode)
            throws Exception {
        // Arrange
        String topic = uniqueTopic();
        var received = ConcurrentHashMap.<Integer>newKeySet();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = client(ended::add); MqttClient publisher = client(ended::add);
                var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE)
                            .withAckMode(mode)
                            .withBufferCapacity(64)
            );
            subscription.established().get(5, SECONDS);
            var jobs = new ArrayList<Future<?>>();
            for (int worker = 0; worker < 8; worker++) {
                jobs.add(workers.submit(() -> {
                    for (int item = 0; item < 64; item++) {
                        MqttDelivery delivery = received(
                                subscription.receive(Duration.ofSeconds(10))
                        );
                        var payload = new String(delivery.message().payload(), UTF_8);
                        int value = Integer.parseInt(payload);
                        assertThat(received.add(value)).isTrue();
                        delivery.ack();
                    }
                    return null;
                }));
            }

            // Act: publisher completions keep outgoing operations bounded while receivers race.
            for (int value = 0; value < 512; value++) {
                publisher.publish(MqttMessage.of(new Topic(topic),
                        Integer.toString(value).getBytes(UTF_8), Qos.AT_LEAST_ONCE, false))
                        .get(5, SECONDS);
            }
            for (Future<?> job : jobs) {
                job.get(15, SECONDS);
            }

            // Assert
            assertThat(received).containsExactlyInAnyOrderElementsOf(
                    IntStream.range(0, 512).boxed().toList()
            );
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            assertThat(ended).isEmpty();
            subscription.unsubscribe().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);
            publisher.disconnect().get(5, SECONDS);
        }
    }

    @ParameterizedTest
    @EnumSource(AckMode.class)
    void qos_one_publications_round_trip_with_each_acknowledgement_mode(AckMode ackMode)
            throws Exception {
        // Arrange
        String topic = uniqueTopic();
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var publication = MqttMessage.of(
                new Topic(topic), new byte[] {1, 2, 3}, Qos.AT_LEAST_ONCE, false
        );
        try (MqttClient sut = client()) {
            sut.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE).withAckMode(ackMode),
                    received::add
            );
            subscription.established().get(5, SECONDS);

            // Act
            PublishResult result = sut.publish(publication).get(5, SECONDS);
            MqttDelivery delivery = requireNonNull(received.poll(5, SECONDS));
            delivery.ack();

            // Assert
            assertThat(result).isEqualTo(PublishResult.of());
            assertThat(subscription.grantedQos()).isEqualTo(Qos.AT_LEAST_ONCE);
            assertThat(delivery.message()).isEqualTo(publication);
            assertThat(delivery.dup()).isFalse();
            subscription.unsubscribe().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);
        }
    }

    @ParameterizedTest
    @EnumSource(AckMode.class)
    void five_hundred_qos_one_publications_arrive_in_order_without_disconnect(AckMode ackMode)
            throws Exception {
        // Arrange
        String topic = uniqueTopic();
        var received = new LinkedBlockingQueue<MqttMessage>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = client(ended::add); MqttClient publisher = client(ended::add)) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE).withAckMode(ackMode),
                    delivery -> {
                        received.add(delivery.message());
                        if (ackMode == AckMode.MANUAL) {
                            delivery.ack();
                        }
                    }
            );
            subscription.established().get(5, SECONDS);

            // Act: bound publisher operations while the subscriber continuously handles input.
            for (int i = 0; i < 500; i++) {
                publisher.publish(MqttMessage.of(
                        new Topic(topic),
                        Integer.toString(i).getBytes(UTF_8),
                        Qos.AT_LEAST_ONCE,
                        false
                )).get(5, SECONDS);
            }

            // Assert
            for (int i = 0; i < 500; i++) {
                MqttMessage message = requireNonNull(received.poll(5, SECONDS));
                assertThat(message.payload()).containsExactly(Integer.toString(i).getBytes(UTF_8));
                assertThat(message.qos()).isEqualTo(Qos.AT_LEAST_ONCE);
            }
            assertThat(received).isEmpty();
            subscription.unsubscribe().get(5, SECONDS);
            assertThat(ended).isEmpty();
            sut.disconnect().get(5, SECONDS);
            publisher.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void a_connection_lost_before_puback_leaves_the_publication_outcome_unknown()
            throws Exception {
        // Arrange: this scheduler controls only delayed inbound transport bytes.
        var scheduler = new ManualTimerScheduler();
        var transport = new AtomicReference<FaultyTransport>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        var failure = new IOException("Connection cut before PUBACK reached the client");
        try (MqttClient sut = MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .connectionListener(ended::add)
                .transportFactory(id -> {
                    var faulty = new FaultyTransport(Transport.tcp(id), scheduler);
                    transport.set(faulty);
                    return faulty;
                })
                .build()) {
            sut.connect().get(5, SECONDS);
            FaultyTransport faulty = requireNonNull(transport.get());
            faulty.receiveDelay(Duration.ofSeconds(30));
            CompletableFuture<PublishResult> publication = sut.publish(MqttMessage.of(
                    new Topic(uniqueTopic()), new byte[] {1}, Qos.AT_LEAST_ONCE, false
            ));
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));
            assertThat(publication).isNotDone();

            // Act: the broker's PUBACK is present but has not crossed the delay boundary.
            faulty.disconnect(failure);

            // Assert
            assertThatThrownBy(() -> publication.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class)
                    .hasRootCause(failure);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(scheduler.pendingTaskCount()).isZero();
            scheduler.advance(Duration.ofSeconds(30));
            assertThat(ended).isEmpty();
        }
    }

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
                    Subscribe.of(new TopicFilter(topic), Qos.AT_MOST_ONCE),
                    firstDeliveries::add
            );
            first.established().get(5, SECONDS);

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
                    Subscribe.of(new TopicFilter(topic), Qos.AT_MOST_ONCE),
                    secondDeliveries::add
            );
            second.established().get(5, SECONDS);
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
                    Subscribe.of(new TopicFilter(topic), Qos.AT_MOST_ONCE),
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
            ).established().get(5, SECONDS);

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
    void unsubscribe_runs_the_deliveries_it_accepted_after_the_broker_confirms_removal()
            throws Exception {
        // Arrange: a handler holds the first of four queued publications.
        String topic = uniqueTopic();
        var release = new CountDownLatch(1);
        var handled = new LinkedBlockingQueue<MqttMessage>();
        try (MqttClient sut = client()) {
            sut.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE),
                    delivery -> {
                        handled.add(delivery.message());
                        waitFor(release);
                    }
            );
            subscription.established().get(5, SECONDS);
            List<MqttMessage> published = publishAndWaitUntilHeld(sut, topic, 4);

            // Act: UNSUBACK confirms removal while the first handler still runs.
            subscription.unsubscribe().get(5, SECONDS);
            CompletableFuture<Void> terminated = subscription.terminated();
            assertThat(terminated).isNotDone();
            release.countDown();

            // Assert: every accepted delivery runs in order, and termination follows the last.
            terminated.get(5, SECONDS);
            assertThat(handled).containsExactlyElementsOf(published);
            await().untilAsserted(() -> assertThat(usage(sut)).isEqualTo(NOTHING_HELD));
            sut.disconnect().get(5, SECONDS);
        } finally {
            release.countDown();
        }
    }

    @Test
    void unsubscribe_leaves_accepted_deliveries_receivable_after_the_broker_confirms_removal()
            throws Exception {
        // Arrange: four publications wait in a pull subscription.
        String topic = uniqueTopic();
        try (MqttClient sut = client()) {
            sut.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE)
            );
            subscription.established().get(5, SECONDS);
            List<MqttMessage> published = publishAndWaitUntilHeld(sut, topic, 4);

            // Act: UNSUBACK confirms removal before any of them is received.
            subscription.unsubscribe().get(5, SECONDS);
            CompletableFuture<Void> terminated = subscription.terminated();

            // Assert: all stay receivable in order; receiving the last one ends the subscription.
            for (MqttMessage message : published) {
                assertThat(terminated).isNotDone();
                assertThat(received(subscription.receive(Duration.ZERO)).message())
                        .isEqualTo(message);
            }
            terminated.get(5, SECONDS);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
            await().untilAsserted(() -> assertThat(usage(sut)).isEqualTo(NOTHING_HELD));
            sut.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void overlapping_shared_subscription_is_rejected_without_stopping_ordinary_delivery()
            throws Exception {
        // Arrange: the ordinary subscription already covers the shared filter's topics.
        String base = uniqueTopic();
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var shared = Subscribe.of(
                new TopicFilter("$share/workers/" + base + "/+"),
                Qos.AT_MOST_ONCE
        );
        try (MqttClient sut = client()) {
            sut.connect().get(5, SECONDS);
            Subscription ordinary = sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/#"), Qos.AT_MOST_ONCE),
                    received::add
            );
            ordinary.established().get(5, SECONDS);

            // Act & Assert: rejecting the conflicting request leaves ordinary delivery usable.
            assertThatThrownBy(() -> sut.subscribe(shared, _ -> { }).established().get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            sut.publish(message(base + "/job", (byte) 1)).get(5, SECONDS);
            assertThat(requireNonNull(received.poll(5, SECONDS)).message())
                    .isEqualTo(message(base + "/job", (byte) 1));

            // Act & Assert: acknowledged removal permits the previously conflicting filter.
            ordinary.unsubscribe().get(5, SECONDS);
            Subscription replacement = sut.subscribe(shared, _ -> { });
            replacement.established().get(5, SECONDS);
            replacement.unsubscribe().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void an_established_subscription_resumes_after_explicit_reconnect(boolean pull, boolean failure)
            throws Exception {
        // Arrange
        String topic = uniqueTopic();
        var ended = new LinkedBlockingQueue<Disconnection>();
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var transport = new AtomicReference<FaultyTransport>();
        try (MqttClient sut = MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .connectionListener(ended::add)
                .transportFactory(id -> {
                    var faulty = new FaultyTransport(Transport.tcp(id), new ManualTimerScheduler());
                    transport.set(faulty);
                    return faulty;
                })
                .build()) {
            sut.connect().get(5, SECONDS);
            var request = Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE);
            Subscription subscription = pull ? sut.subscribe(request)
                    : sut.subscribe(request, received::add);
            subscription.established().get(5, SECONDS);
            sut.publish(message(topic, (byte) 1)).get(5, SECONDS);
            MqttDelivery first = pull ? received(subscription.receive(Duration.ofSeconds(5)))
                    : requireNonNull(received.poll(5, SECONDS));
            assertThat(first.message().payload()).containsExactly((byte) 1);

            // Act: keep the same handle while its connection ends and is explicitly reopened.
            var cause = new IOException("connection lost");
            if (failure) {
                requireNonNull(transport.get()).disconnect(cause);
            } else {
                sut.disconnect().get(5, SECONDS);
            }
            assertThat(ended.poll(5, SECONDS))
                    .isEqualTo(Disconnection.of(failure ? cause : null, !failure, 1));
            sut.connect().get(5, SECONDS);
            var next = MqttMessage.of(new Topic(topic), new byte[] {2}, Qos.AT_LEAST_ONCE, false);
            // One restoration fits the send capacity, before this same connection's PUBLISH.
            sut.publish(next).get(5, SECONDS);

            // Assert: the original registration receives again without another subscribe call.
            MqttDelivery resumed = pull ? received(subscription.receive(Duration.ofSeconds(5)))
                    : requireNonNull(received.poll(5, SECONDS));
            assertThat(resumed.message()).isEqualTo(next);
            assertThat(subscription.filter()).isEqualTo(new TopicFilter(topic));
            subscription.unsubscribe().get(5, SECONDS);
            if (pull) {
                assertThat(subscription.receive()).isEqualTo(new ReceiveResult.Ended());
            }
            assertThat(ended).isEmpty();
        }
    }

    @Test
    void a_handler_failure_is_reported_and_the_subscription_resumes_after_reconnect()
            throws Exception {
        // Arrange
        String topic = uniqueTopic();
        var failure = new IllegalStateException("handler failed");
        var ended = new LinkedBlockingQueue<Disconnection>();
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client(ended::add); MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE),
                    delivery -> {
                        if (delivery.message().payload()[0] == 1) {
                            throw failure;
                        }
                        received.add(delivery);
                    }
            );
            subscription.established().get(5, SECONDS);

            // Act: the subscriber has no pending operation to carry this failure. The QoS 1
            // delivery holds back its acknowledgement until the handler returns, so the
            // failure stops the client.
            publisher.publish(message(topic, (byte) 1).withQos(Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            sut.connect().get(5, SECONDS);
            // This PUBLISH follows the single restored SUBSCRIBE on the same connection.
            sut.publish(message(topic, (byte) 2)).get(5, SECONDS);

            // Assert: failure did not remove the handler or its subscription handle.
            assertThat(requireNonNull(received.poll(5, SECONDS)).message().payload())
                    .containsExactly((byte) 2);
            subscription.unsubscribe().get(5, SECONDS);
            assertThat(ended).isEmpty();
        }
    }

    @Test
    void a_qos_0_handler_failure_is_skipped_and_the_connection_continues() throws Exception {
        // Arrange: a QoS 0 delivery holds back no acknowledgement for its failure to protect.
        String topic = uniqueTopic();
        var failure = new IllegalStateException("handler failed");
        var ended = new LinkedBlockingQueue<Disconnection>();
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client(ended::add); MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_MOST_ONCE),
                    delivery -> {
                        if (delivery.message().payload()[0] == 1) {
                            throw failure;
                        }
                        received.add(delivery);
                    }
            ).established().get(5, SECONDS);

            // Act
            publisher.publish(message(topic, (byte) 1)).get(5, SECONDS);
            publisher.publish(message(topic, (byte) 2)).get(5, SECONDS);

            // Assert: the default policy skips the failure, so the next message still arrives.
            assertThat(requireNonNull(received.poll(5, SECONDS)).message().payload())
                    .containsExactly((byte) 2);
            assertThat(ended).isEmpty();
        }
    }

    @Test
    void skip_continues_after_a_failed_qos_1_delivery() throws Exception {
        // Arrange: the subscription skips failures, and the first QoS 1 delivery fails.
        String topic = uniqueTopic();
        var failure = new IllegalStateException("handler failed");
        var ended = new LinkedBlockingQueue<Disconnection>();
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client(ended::add); MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE)
                            .withFailurePolicy(FailurePolicy.SKIP),
                    delivery -> {
                        if (delivery.message().payload()[0] == 1) {
                            throw failure;
                        }
                        received.add(delivery);
                    }
            ).established().get(5, SECONDS);

            // Act
            for (byte value = 1; value <= 3; value++) {
                publisher.publish(message(topic, value).withQos(Qos.AT_LEAST_ONCE))
                        .get(5, SECONDS);
            }

            // Assert: the failed delivery is skipped, and the later ones arrive on the same
            // connection.
            assertThat(requireNonNull(received.poll(5, SECONDS)).message().payload())
                    .containsExactly((byte) 2);
            assertThat(requireNonNull(received.poll(5, SECONDS)).message().payload())
                    .containsExactly((byte) 3);
            assertThat(ended).isEmpty();
        }
    }

    @Test
    void a_late_handler_failure_does_not_end_the_new_connection_or_stop_its_lane()
            throws Exception {
        // Arrange: keep the same subscription's handler running across a reconnect.
        String topic = uniqueTopic();
        var failure = new IllegalStateException("old handler failed");
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var ended = new LinkedBlockingQueue<Disconnection>();
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client(ended::add)) {
            sut.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_MOST_ONCE),
                    delivery -> {
                        if (delivery.message().payload()[0] == 1) {
                            entered.countDown();
                            waitFor(release);
                            throw failure;
                        }
                        received.add(delivery);
                    }
            );
            subscription.established().get(5, SECONDS);
            sut.publish(message(topic, (byte) 1)).get(5, SECONDS);
            assertThat(entered.await(5, SECONDS)).isTrue();
            sut.disconnect().get(5, SECONDS);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            sut.connect().get(5, SECONDS);
            // This PUBLISH follows the single restored SUBSCRIBE on the same connection.
            sut.publish(message(topic, (byte) 2)).get(5, SECONDS);
            await().untilAsserted(() -> assertThat(usage(sut).deliveries()).isEqualTo(2));
            assertThat(received).isEmpty();

            // Act: the old handler fails with the current delivery already waiting in its lane.
            release.countDown();

            // Assert: handlers stay serial, and the old failure does not strand the queued work.
            assertThat(requireNonNull(received.poll(5, SECONDS)).message().payload())
                    .containsExactly((byte) 2);
            subscription.unsubscribe().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 2));
            assertThat(ended).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void receive_overload_closes_without_waiting_for_the_running_handler() throws Exception {
        // Arrange: QoS 1 messages, which overflow cannot drop as it drops QoS 0.
        String topic = uniqueTopic();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var returned = new CountDownLatch(1);
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = client(ended::add); MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE).withBufferCapacity(1),
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
            ).established().get(5, SECONDS);
            publisher.publish(message(topic, (byte) 1).withQos(Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act: the running handler still owns the only receive slot.
            publisher.publish(message(topic, (byte) 2).withQos(Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            Disconnection disconnection = requireNonNull(ended.poll(5, SECONDS));

            // Assert: shutdown progresses while application code is blocked.
            assertThat(disconnection.cause().orElseThrow())
                    .isInstanceOf(MqttCapacityException.class);
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
    void receive_overload_reconnects_and_the_subscription_continues() throws Exception {
        // Arrange: a client on manual time whose handler blocks with room for one QoS 1
        // delivery.
        String topic = uniqueTopic();
        var clock = new ManualTimerScheduler();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var ended = new LinkedBlockingQueue<Disconnection>();
        var payloads = new LinkedBlockingQueue<Byte>();
        try (MqttClient sut = MqttClient.builder()
                        .server(endpoint())
                        .version(MqttVersion.V3_1_1)
                        .timerScheduler(clock)
                        .reconnectPolicy(ReconnectPolicy.exponentialBackoff(
                                Duration.ofSeconds(1),
                                Duration.ofSeconds(1)
                        ))
                        .connectionListener(ended::add)
                        .build();
                MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE).withBufferCapacity(1),
                    delivery -> {
                        payloads.add(delivery.message().payload()[0]);
                        if (entered.getCount() > 0) {
                            entered.countDown();
                            waitFor(release);
                        }
                    }
            );
            subscription.established().get(5, SECONDS);
            publisher.publish(message(topic, (byte) 1).withQos(Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act: the second delivery overloads the subscription, and the delay passes.
            publisher.publish(message(topic, (byte) 2).withQos(Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            Disconnection end = requireNonNull(ended.poll(5, SECONDS));
            release.countDown();
            clock.advance(Duration.ofSeconds(1));

            // Assert: the overload announces a reconnection, which keeps the subscription.
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class);
            assertThat(end.reconnectDelay()).isPresent();
            assertThat(sut.whenConnected().get(5, SECONDS).connectionAttempt()).isEqualTo(2);
            subscription.established().get(5, SECONDS);
            publisher.publish(message(topic, (byte) 3)).get(5, SECONDS);
            await().untilAsserted(() -> assertThat(payloads).contains((byte) 3));
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
                    Subscribe.of(new TopicFilter(base + "/reply"), Qos.AT_MOST_ONCE),
                    replies::add
            ).established().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/request"), Qos.AT_MOST_ONCE),
                    delivery -> {
                        sut.publish(message(base + "/reply", delivery.message().payload())).join();
                        handled.complete(null);
                    }
            ).established().get(5, SECONDS);

            // Act
            sut.publish(message(base + "/request", (byte) 7)).get(5, SECONDS);

            // Assert
            handled.get(5, SECONDS);
            assertThat(requireNonNull(replies.poll(5, SECONDS)).message())
                    .isEqualTo(message(base + "/reply", (byte) 7));
        }
    }

    @Test
    @Timeout(60)
    @SuppressWarnings("try") // Explicit close verifies final release of the retained subscriptions.
    void a_blocked_handler_drops_qos_0_messages_beyond_its_default_queue() throws Exception {
        // Arrange: the handler holds the first publication before any other one is sent.
        String base = uniqueTopic();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var barrier = new CountDownLatch(1);
        var handled = new AtomicInteger();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = client(ended::add); MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/barrier"), Qos.AT_MOST_ONCE),
                    _ -> barrier.countDown()
            ).established().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/work"), Qos.AT_MOST_ONCE),
                    _ -> {
                        handled.incrementAndGet();
                        entered.countDown();
                        waitFor(release);
                    }
            ).established().get(5, SECONDS);
            publisher.publish(message(base + "/work", (byte) 0)).get(5, SECONDS);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act: more publications than the default queue holds arrive behind the handler, as
            // QoS 0 for the QoS 0 subscription. Each QoS 1 publication waits for the broker, so
            // none piles up there instead. The barrier follows them through the broker and the
            // client.
            var publication = MqttMessage.of(new Topic(base + "/work"), new byte[] {1},
                    Qos.AT_LEAST_ONCE, false);
            for (int i = 0; i < 2_000; i++) {
                publisher.publish(publication).get(5, SECONDS);
            }
            publisher.publish(message(base + "/barrier", (byte) 0)).get(5, SECONDS);
            assertThat(barrier.await(5, SECONDS)).isTrue();

            // Assert: the queue holds 1,024 deliveries, the running one included, and drops the
            // rest without ending the connection. They all run once the handler returns, and a
            // later message fits again.
            await().untilAsserted(() -> assertThat(usage(sut).deliveries()).isEqualTo(1_024));
            release.countDown();
            await().untilAsserted(() -> assertThat(handled).hasValue(1_024));
            await().untilAsserted(() -> assertThat(usage(sut))
                    .isEqualTo(new ClientActor.Usage(0, 0, 0, 0, 0, 0, 2, 0, 0, 0)));
            publisher.publish(message(base + "/work", (byte) 2)).get(5, SECONDS);
            await().untilAsserted(() -> assertThat(handled).hasValue(1_025));
            assertThat(ended).isEmpty();
            sut.close();
            await().untilAsserted(() -> assertThat(usage(sut)).isEqualTo(NOTHING_HELD));
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close verifies final release of the retained subscriptions.
    void a_handler_can_wait_for_its_own_publication_while_its_queue_is_full() throws Exception {
        // Arrange: a handler keeps its four-slot queue full until it may publish and wait.
        String base = uniqueTopic();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var barrier = new CountDownLatch(1);
        var handled = new LinkedBlockingQueue<Integer>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = client(ended::add); MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/barrier"), Qos.AT_MOST_ONCE),
                    _ -> barrier.countDown()
            ).established().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/work"), Qos.AT_MOST_ONCE)
                            .withBufferCapacity(4),
                    delivery -> {
                        int number = delivery.message().payload()[0];
                        if (number == 0) {
                            entered.countDown();
                            waitFor(release);
                            sut.publish(MqttMessage.of(new Topic(base + "/reply"),
                                    new byte[] {0}, Qos.AT_LEAST_ONCE, false)).join();
                        }
                        handled.add(number);
                    }
            ).established().get(5, SECONDS);
            publisher.publish(message(base + "/work", (byte) 0)).get(5, SECONDS);
            assertThat(entered.await(5, SECONDS)).isTrue();
            for (int i = 1; i < 4; i++) {
                publisher.publish(message(base + "/work", (byte) i)).get(5, SECONDS);
            }
            // The barrier follows the queued work through the broker and the client.
            publisher.publish(message(base + "/barrier", (byte) 0)).get(5, SECONDS);
            assertThat(barrier.await(5, SECONDS)).isTrue();

            // Act: the PUBACK arrives while every queue slot is taken.
            release.countDown();

            // Assert: the publication completes, the queue drains, and the connection remains.
            for (int i = 0; i < 4; i++) {
                assertThat(handled.poll(5, SECONDS)).isEqualTo(i);
            }
            assertThat(ended).isEmpty();
            sut.disconnect().get(5, SECONDS);
            await().untilAsserted(() -> assertThat(usage(sut))
                    .isEqualTo(new ClientActor.Usage(0, 0, 0, 0, 0, 0, 2, 0, 0, 0)));
            sut.close();
            await().untilAsserted(() -> assertThat(usage(sut)).isEqualTo(NOTHING_HELD));
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close verifies final release of the retained subscriptions.
    void overload_releases_a_handler_waiting_for_its_own_publication() throws Exception {
        // Arrange: delayed input lets an overflowing QoS 1 publication precede the handler's
        // PUBACK. The work subscription takes QoS 1 so that publication arrives as QoS 1.
        String base = uniqueTopic();
        var scheduler = new ManualTimerScheduler();
        var transport = new AtomicReference<FaultyTransport>();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var barrier = new CountDownLatch(1);
        var replies = new LinkedBlockingQueue<MqttDelivery>();
        var outcome = new CompletableFuture<Throwable>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (
                MqttClient sut = MqttClient.builder()
                        .server(endpoint())
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .connectionListener(ended::add)
                        .transportFactory(id -> {
                            var faulty = new FaultyTransport(Transport.tcp(id), scheduler);
                            transport.set(faulty);
                            return faulty;
                        })
                        .build();
                MqttClient publisher = client()
        ) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            publisher.subscribe(
                    Subscribe.of(new TopicFilter(base + "/reply"), Qos.AT_MOST_ONCE),
                    replies::add
            ).established().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/barrier"), Qos.AT_MOST_ONCE),
                    _ -> barrier.countDown()
            ).established().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/work"), Qos.AT_LEAST_ONCE)
                            .withBufferCapacity(4),
                    delivery -> {
                        if (delivery.message().payload()[0] == 0) {
                            entered.countDown();
                            waitFor(release);
                            try {
                                sut.publish(MqttMessage.of(new Topic(base + "/reply"),
                                        new byte[] {0}, Qos.AT_LEAST_ONCE, false)).join();
                                outcome.complete(null);
                            } catch (CompletionException failure) {
                                outcome.complete(failure.getCause());
                            }
                        }
                    }
            ).established().get(5, SECONDS);
            publisher.publish(message(base + "/work", (byte) 0)).get(5, SECONDS);
            assertThat(entered.await(5, SECONDS)).isTrue();
            for (int i = 1; i < 4; i++) {
                publisher.publish(message(base + "/work", (byte) i)).get(5, SECONDS);
            }
            publisher.publish(message(base + "/barrier", (byte) 0)).get(5, SECONDS);
            assertThat(barrier.await(5, SECONDS)).isTrue();
            requireNonNull(transport.get()).receiveDelay(Duration.ofSeconds(30));
            publisher.publish(message(base + "/work", (byte) 4).withQos(Qos.AT_LEAST_ONCE))
                    .get(5, SECONDS);
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));
            release.countDown();
            // The broker has the handler's publication, so the handler now waits for PUBACK.
            assertThat(replies.poll(5, SECONDS)).isNotNull();

            // Act: delayed input delivers the overflowing publication before the PUBACK.
            scheduler.advance(Duration.ofSeconds(30));

            // Assert: no circular wait remains; only the subscriptions survive connection failure.
            Disconnection end = requireNonNull(ended.poll(5, SECONDS));
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class);
            assertThat(outcome.get(5, SECONDS))
                    .isInstanceOf(MqttConnectionLostException.class)
                    .hasCause(end.cause().orElseThrow());
            await().untilAsserted(() -> assertThat(usage(sut))
                    .isEqualTo(new ClientActor.Usage(0, 0, 0, 0, 0, 0, 2, 0, 0, 0)));
            sut.close();
            await().untilAsserted(() -> assertThat(usage(sut)).isEqualTo(NOTHING_HELD));
        } finally {
            release.countDown();
        }
    }

    MqttClient client() {
        return client(_ -> {});
    }

    MqttClient client(ConnectionListener listener) {
        return MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .connectionListener(listener)
                .build();
    }

    static String uniqueTopic() {
        return "hato/client/" + UUID.randomUUID();
    }

    static MqttMessage message(String topic, byte... payload) {
        return MqttMessage.of(new Topic(topic), payload, Qos.AT_MOST_ONCE, false);
    }

    abstract URI endpoint();

    /// Publishes `count` QoS 1 messages that the client's own subscription on `topic` receives,
    /// and returns once the client holds every one of their deliveries.
    private static List<MqttMessage> publishAndWaitUntilHeld(
            MqttClient client,
            String topic,
            int count
    ) throws Exception {
        var published = new ArrayList<MqttMessage>();
        for (int i = 0; i < count; i++) {
            var message = MqttMessage.of(
                    new Topic(topic),
                    new byte[] {(byte) i},
                    Qos.AT_LEAST_ONCE,
                    false
            );
            client.publish(message).get(5, SECONDS);
            published.add(message);
        }
        await().untilAsserted(() -> assertThat(usage(client).deliveries()).isEqualTo(count));
        return published;
    }

    static ClientActor.Usage usage(MqttClient client) {
        return ((ClientActor) client).usage();
    }

    static void waitFor(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, SECONDS)).isTrue();
        } catch (InterruptedException cause) {
            Thread.currentThread().interrupt();
            throw new AssertionError(cause);
        }
    }

    private static MqttDelivery received(ReceiveResult result) {
        return assertInstanceOf(ReceiveResult.Received.class, result).delivery();
    }
}
