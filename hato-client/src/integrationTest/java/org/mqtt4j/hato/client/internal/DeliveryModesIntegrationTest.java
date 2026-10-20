package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttDelivery;
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
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// Both brokers carry each delivery mode: the push delivery options and hand-off keep their
/// order, their bound on handlers at once, their buffers, their failure policy, and their
/// termination. Deliveries that striped and parallel subscriptions finish out of receive order
/// still complete, because the client acknowledges them in receive order.
@Timeout(30)
abstract class DeliveryModesIntegrationTest {

    private static final int HANDLERS = 4;

    private static final int MESSAGES = 24;

    private static final byte MARKER = 100;

    /// The push delivery options under test, with the most handlers that each runs at once.
    enum Mode {
        SERIAL_ON_EXECUTOR(1),
        LANES(2),
        STRIPED(2),
        PARALLEL(2);

        final int atOnce;

        Mode(int atOnce) {
            this.atOnce = atOnce;
        }
    }

    abstract URI endpoint();

    @ParameterizedTest
    @EnumSource(Mode.class)
    void each_mode_runs_every_message_once_within_its_bound(Mode mode) throws Exception {
        // Arrange: QoS 0, 1, and 2 messages take turns over four topics, and each handler takes
        // a moment so that handlers can overlap.
        String prefix = uniqueTopic();
        var executors = new ArrayList<ExecutorService>();
        var running = new AtomicInteger();
        var mostAtOnce = new AtomicInteger();
        var handled = new LinkedBlockingQueue<MqttMessage>();
        var taken = new ArrayList<MqttMessage>();
        List<MqttMessage> redelivered;
        try (MqttClient sut = persistentClient(_ -> { });
                MqttClient publisher = client()) {
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(prefix + "/+"), Qos.EXACTLY_ONCE),
                    options(mode, executors),
                    delivery -> {
                        mostAtOnce.accumulateAndGet(running.incrementAndGet(), Math::max);
                        pause();
                        running.decrementAndGet();
                        handled.add(delivery.message());
                    }
            );
            subscription.established().get(5, SECONDS);

            // Act
            for (int index = 0; index < MESSAGES; index++) {
                Qos qos = Qos.values()[index % 3];
                publisher.publish(message(prefix + "/" + index % HANDLERS, index, qos))
                        .get(5, SECONDS);
            }
            for (int count = 0; count < MESSAGES; count++) {
                taken.add(requireNonNull(handled.poll(5, SECONDS)));
            }
            redelivered = redeliveredAfterReconnect(sut, publisher, prefix + "/0", handled);
        } finally {
            executors.forEach(ExecutorService::close);
        }

        // Assert: every message ran once, never more handlers at once than the mode allows, and
        // the broker kept nothing unacknowledged.
        assertThat(taken)
                .extracting(message -> (int) message.payload()[0])
                .containsExactlyInAnyOrderElementsOf(IntStream.range(0, MESSAGES).boxed().toList());
        assertThat(mostAtOnce.get()).isBetween(1, mode.atOnce);
        assertThat(redelivered).isEmpty();
    }

    @Test
    void serial_delivery_on_an_executor_keeps_the_receive_order() throws Exception {
        // Arrange
        String prefix = uniqueTopic();
        var executors = new ArrayList<ExecutorService>();
        var handled = new LinkedBlockingQueue<Integer>();
        try (MqttClient sut = client();
                MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(prefix + "/+"), Qos.AT_LEAST_ONCE),
                    options(Mode.SERIAL_ON_EXECUTOR, executors),
                    delivery -> handled.add((int) delivery.message().payload()[0])
            );
            subscription.established().get(5, SECONDS);

            // Act
            for (int index = 0; index < MESSAGES; index++) {
                String topic = prefix + "/" + index % HANDLERS;
                publisher.publish(message(topic, index, Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            }
            await().until(() -> handled.size() == MESSAGES);
        } finally {
            executors.forEach(ExecutorService::close);
        }

        // Assert
        assertThat(handled)
                .containsExactlyElementsOf(IntStream.range(0, MESSAGES).boxed().toList());
    }

    @Test
    void lanes_keep_each_lanes_receive_order() throws Exception {
        // Arrange: each topic always takes the same of two lanes.
        String prefix = uniqueTopic();
        var executors = new ArrayList<ExecutorService>();
        var handled = new LinkedBlockingQueue<MqttMessage>();
        try (MqttClient sut = client();
                MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(prefix + "/+"), Qos.AT_LEAST_ONCE),
                    options(Mode.LANES, executors),
                    delivery -> {
                        pause();
                        handled.add(delivery.message());
                    }
            );
            subscription.established().get(5, SECONDS);

            // Act
            for (int index = 0; index < MESSAGES; index++) {
                String topic = prefix + "/" + index % HANDLERS;
                publisher.publish(message(topic, index, Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            }
            await().until(() -> handled.size() == MESSAGES);
        } finally {
            executors.forEach(ExecutorService::close);
        }

        // Assert: within each topic, and so within each lane, the messages kept their order.
        for (int topic = 0; topic < HANDLERS; topic++) {
            String name = prefix + "/" + topic;
            assertThat(handled)
                    .filteredOn(message -> message.topic().name().equals(name))
                    .extracting(message -> (int) message.payload()[0])
                    .isSorted()
                    .hasSize(MESSAGES / HANDLERS);
        }
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void a_stopping_failure_is_delivered_again_after_connect(Mode mode) throws Exception {
        // Arrange: the handler fails the first time it sees a QoS 1 message, which stops the
        // client under the default policy.
        String prefix = uniqueTopic();
        var failure = new IllegalStateException("handler failed");
        var executors = new ArrayList<ExecutorService>();
        var attempts = new AtomicInteger();
        var handled = new LinkedBlockingQueue<MqttMessage>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = persistentClient(ended::add);
                MqttClient publisher = client()) {
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(prefix + "/+"), Qos.AT_LEAST_ONCE),
                    options(mode, executors),
                    delivery -> {
                        if (attempts.incrementAndGet() == 1) {
                            throw failure;
                        }
                        handled.add(delivery.message());
                    }
            );
            subscription.established().get(5, SECONDS);
            publisher.publish(message(prefix + "/0", 1, Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));

            // Act
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(true, 2));

            // Assert: the broker's retransmission ran this time, and nothing is left
            // unacknowledged.
            MqttMessage again = requireNonNull(handled.poll(5, SECONDS));
            assertThat(again.payload()).containsExactly(1);
            assertThat(redeliveredAfterReconnect(sut, publisher, prefix + "/0", handled)).isEmpty();
        } finally {
            executors.forEach(ExecutorService::close);
        }
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void unsubscribe_runs_the_accepted_messages_before_the_subscription_terminates(Mode mode)
            throws Exception {
        // Arrange: the handler holds the first of three messages until removal is confirmed.
        String prefix = uniqueTopic();
        var executors = new ArrayList<ExecutorService>();
        var release = new CountDownLatch(1);
        var handled = new LinkedBlockingQueue<Integer>();
        try (MqttClient sut = client();
                MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(prefix + "/+"), Qos.AT_LEAST_ONCE),
                    options(mode, executors),
                    delivery -> {
                        if (delivery.message().payload()[0] == 0) {
                            waitFor(release);
                        }
                        handled.add((int) delivery.message().payload()[0]);
                    }
            );
            subscription.established().get(5, SECONDS);
            for (int index = 0; index < 3; index++) {
                publisher.publish(message(prefix + "/" + index, index, Qos.AT_LEAST_ONCE))
                        .get(5, SECONDS);
            }
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 3);

            // Act
            CompletableFuture<Void> removing = subscription.unsubscribe();
            removing.get(5, SECONDS);
            release.countDown();

            // Assert
            subscription.terminated().get(5, SECONDS);
            assertThat(handled).containsExactlyInAnyOrder(0, 1, 2);
        } finally {
            release.countDown();
            executors.forEach(ExecutorService::close);
        }
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void a_full_buffer_drops_qos_0_messages_and_the_connection_goes_on(Mode mode)
            throws Exception {
        // Arrange: a one-message buffer, which the first message's handler holds.
        String prefix = uniqueTopic();
        var executors = new ArrayList<ExecutorService>();
        var release = new CountDownLatch(1);
        var handled = new LinkedBlockingQueue<Integer>();
        var markers = new LinkedBlockingQueue<MqttDelivery>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = client(ended::add);
                MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(prefix + "/+"), Qos.AT_MOST_ONCE)
                            .withBufferCapacity(1),
                    options(mode, executors),
                    delivery -> {
                        handled.add((int) delivery.message().payload()[0]);
                        if (delivery.message().payload()[0] == 0) {
                            waitFor(release);
                        }
                    }
            );
            Subscription marker = sut.subscribe(
                    Subscribe.of(new TopicFilter(prefix + "-marker"), Qos.AT_MOST_ONCE),
                    markers::add
            );
            subscription.established().get(5, SECONDS);
            marker.established().get(5, SECONDS);
            publisher.publish(message(prefix + "/0", 0, Qos.AT_MOST_ONCE)).get(5, SECONDS);
            assertThat(handled.poll(5, SECONDS)).isZero();

            // Act: two more messages arrive while the buffer is full, and a marker follows them
            // on the same connection.
            publisher.publish(message(prefix + "/1", 1, Qos.AT_MOST_ONCE)).get(5, SECONDS);
            publisher.publish(message(prefix + "/2", 2, Qos.AT_MOST_ONCE)).get(5, SECONDS);
            publisher.publish(message(prefix + "-marker", MARKER, Qos.AT_MOST_ONCE))
                    .get(5, SECONDS);
            assertThat(markers.poll(5, SECONDS)).isNotNull();
            release.countDown();
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 0);
            publisher.publish(message(prefix + "/3", 3, Qos.AT_MOST_ONCE)).get(5, SECONDS);

            // Assert: the full buffer dropped both, and the subscription and connection go on.
            assertThat(handled.poll(5, SECONDS)).isEqualTo(3);
            assertThat(handled).isEmpty();
            assertThat(ended).isEmpty();
        } finally {
            release.countDown();
            executors.forEach(ExecutorService::close);
        }
    }

    @Test
    void hand_off_offers_the_messages_of_several_subscriptions_in_receive_order() throws Exception {
        // Arrange: two hand-off subscriptions share one mailbox.
        String prefix = uniqueTopic();
        var mailbox = new LinkedBlockingQueue<MqttDelivery>();
        List<MqttMessage> redelivered;
        var offered = new ArrayList<MqttMessage>();
        try (MqttClient sut = persistentClient(_ -> { });
                MqttClient publisher = client()) {
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            publisher.connect().get(5, SECONDS);
            Subscription first = sut.handOff(
                    Subscribe.of(new TopicFilter(prefix + "/a"), Qos.EXACTLY_ONCE),
                    mailbox::offer
            );
            Subscription second = sut.handOff(
                    Subscribe.of(new TopicFilter(prefix + "/b"), Qos.EXACTLY_ONCE),
                    mailbox::offer
            );
            first.established().get(5, SECONDS);
            second.established().get(5, SECONDS);

            // Act
            for (int index = 0; index < MESSAGES; index++) {
                String topic = prefix + (index % 2 == 0 ? "/a" : "/b");
                publisher.publish(message(topic, index, Qos.values()[index % 3])).get(5, SECONDS);
            }
            for (int count = 0; count < MESSAGES; count++) {
                offered.add(requireNonNull(mailbox.poll(5, SECONDS)).message());
            }
            var handled = new LinkedBlockingQueue<MqttMessage>();
            Subscription replays = sut.subscribe(
                    Subscribe.of(new TopicFilter(prefix + "/replay"), Qos.AT_LEAST_ONCE),
                    delivery -> handled.add(delivery.message())
            );
            replays.established().get(5, SECONDS);
            redelivered = redeliveredAfterReconnect(sut, publisher, prefix + "/replay", handled);
        }

        // Assert: the mailbox saw one receive order across both subscriptions, and the broker
        // kept nothing unacknowledged.
        assertThat(offered)
                .extracting(message -> (int) message.payload()[0])
                .containsExactlyElementsOf(IntStream.range(0, MESSAGES).boxed().toList());
        assertThat(redelivered).isEmpty();
    }

    @Test
    void an_acceptor_failure_is_offered_again_after_connect() throws Exception {
        // Arrange: the acceptor fails the first time it sees a QoS 1 message, which stops the
        // client under the default policy.
        String prefix = uniqueTopic();
        var failure = new IllegalStateException("acceptor failed");
        var attempts = new AtomicInteger();
        var mailbox = new LinkedBlockingQueue<MqttDelivery>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = persistentClient(ended::add);
                MqttClient publisher = client()) {
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.handOff(
                    Subscribe.of(new TopicFilter(prefix + "/+"), Qos.AT_LEAST_ONCE),
                    delivery -> {
                        if (attempts.incrementAndGet() == 1) {
                            throw failure;
                        }
                        return mailbox.offer(delivery);
                    }
            );
            subscription.established().get(5, SECONDS);
            publisher.publish(message(prefix + "/0", 1, Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));

            // Act
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(true, 2));

            // Assert: the broker's retransmission is offered again and accepted this time.
            MqttDelivery again = requireNonNull(mailbox.poll(5, SECONDS));
            assertThat(again.message().payload()).containsExactly(1);
            assertThat(again.connectionAttempt()).isEqualTo(2);
            var handled = new LinkedBlockingQueue<MqttMessage>();
            Subscription replays = sut.subscribe(
                    Subscribe.of(new TopicFilter(prefix + "-replay"), Qos.AT_LEAST_ONCE),
                    delivery -> handled.add(delivery.message())
            );
            replays.established().get(5, SECONDS);
            assertThat(redeliveredAfterReconnect(sut, publisher, prefix + "-replay", handled))
                    .isEmpty();
            assertThat(mailbox).isEmpty();
        }
    }

    @Test
    void unsubscribe_hands_off_the_accepted_messages_before_the_subscription_terminates()
            throws Exception {
        // Arrange: the acceptor holds the first of three messages until removal is confirmed.
        String prefix = uniqueTopic();
        var release = new CountDownLatch(1);
        var offered = new LinkedBlockingQueue<Integer>();
        try (MqttClient sut = client();
                MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.handOff(
                    Subscribe.of(new TopicFilter(prefix + "/+"), Qos.AT_LEAST_ONCE),
                    delivery -> {
                        if (delivery.message().payload()[0] == 0) {
                            waitFor(release);
                        }
                        return offered.offer((int) delivery.message().payload()[0]);
                    }
            );
            subscription.established().get(5, SECONDS);
            for (int index = 0; index < 3; index++) {
                publisher.publish(message(prefix + "/" + index, index, Qos.AT_LEAST_ONCE))
                        .get(5, SECONDS);
            }
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 3);

            // Act
            subscription.unsubscribe().get(5, SECONDS);
            release.countDown();

            // Assert
            subscription.terminated().get(5, SECONDS);
            assertThat(offered).containsExactly(0, 1, 2);
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_full_mailbox_refuses_qos_0_messages_and_the_connection_goes_on() throws Exception {
        // Arrange: a mailbox with room for one message that nothing drains yet.
        String prefix = uniqueTopic();
        var mailbox = new ArrayBlockingQueue<MqttDelivery>(1);
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = client(ended::add);
                MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.handOff(
                    Subscribe.of(new TopicFilter(prefix + "/+"), Qos.AT_MOST_ONCE),
                    mailbox::offer
            );
            subscription.established().get(5, SECONDS);

            // Act: two messages arrive, then the application drains its mailbox, and a third
            // message follows.
            publisher.publish(message(prefix + "/0", 0, Qos.AT_MOST_ONCE)).get(5, SECONDS);
            publisher.publish(message(prefix + "/1", 1, Qos.AT_MOST_ONCE)).get(5, SECONDS);
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 0);
            MqttDelivery kept = requireNonNull(mailbox.poll(5, SECONDS));
            publisher.publish(message(prefix + "/2", 2, Qos.AT_MOST_ONCE)).get(5, SECONDS);

            // Assert: the full mailbox refused the second message, and the connection goes on.
            assertThat(kept.message().payload()).containsExactly(0);
            assertThat(requireNonNull(mailbox.poll(5, SECONDS)).message().payload())
                    .containsExactly(2);
            assertThat(ended).isEmpty();
        }
    }

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
        var secondAcknowledged = new CountDownLatch(1);
        var handled = new LinkedBlockingQueue<MqttMessage>();
        try (MqttClient sut = persistentClient(_ -> { });
                MqttClient publisher = client()) {
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
            List<MqttMessage> redelivered =
                    redeliveredAfterReconnect(sut, publisher, topics.getFirst(), handled);
            return new RoundTrip(taken, redelivered);
        }
    }

    /// Reconnects `sut`, whose earlier connection was its first, and publishes a marker to
    /// `topic`, which one of its subscriptions puts in `handled`. Returns what `handled` took
    /// before the marker: the messages that the broker delivered again because it kept them
    /// unacknowledged.
    private static List<MqttMessage> redeliveredAfterReconnect(
            MqttClient sut,
            MqttClient publisher,
            String topic,
            LinkedBlockingQueue<MqttMessage> handled
    ) throws Exception {
        sut.disconnect().get(5, SECONDS);
        assertThat(sut.connect().get(5, SECONDS).sessionPresent()).isTrue();
        publisher.publish(message(topic, MARKER, Qos.AT_LEAST_ONCE)).get(5, SECONDS);
        var redelivered = new ArrayList<MqttMessage>();
        while (true) {
            MqttMessage next = requireNonNull(handled.poll(5, SECONDS));
            if (next.payload()[0] == MARKER) {
                return redelivered;
            }
            redelivered.add(next);
        }
    }

    /// Delivery options for `mode`, on executors that the test adds to `executors` and closes.
    private static DeliveryOptions options(Mode mode, List<ExecutorService> executors) {
        return switch (mode) {
            case SERIAL_ON_EXECUTOR -> {
                ExecutorService pool = Executors.newFixedThreadPool(2);
                executors.add(pool);
                yield DeliveryOptions.serial().withExecutor(pool);
            }
            case LANES -> {
                ExecutorService even = Executors.newSingleThreadExecutor();
                ExecutorService odd = Executors.newSingleThreadExecutor();
                executors.add(even);
                executors.add(odd);
                yield DeliveryOptions.lanes(message ->
                        Math.floorMod(message.topic().name().hashCode(), 2) == 0 ? even : odd);
            }
            case STRIPED -> DeliveryOptions.striped(2);
            case PARALLEL -> DeliveryOptions.parallel(2);
        };
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

    private MqttClient persistentClient(ConnectionListener listener) {
        return MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .clientId("hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19))
                .sessionExpiry(Duration.ofSeconds(30))
                .connectionListener(listener)
                .build();
    }

    private MqttClient client() {
        return client(_ -> { });
    }

    private MqttClient client(ConnectionListener listener) {
        return MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .connectionListener(listener)
                .build();
    }

    private static MqttMessage message(String topic, int value, Qos qos) {
        return MqttMessage.of(new Topic(topic), new byte[] {(byte) value}, qos, false);
    }

    /// Lets handlers overlap: each takes a moment, as real work would.
    private static void pause() {
        try {
            Thread.sleep(2);
        } catch (InterruptedException cause) {
            Thread.currentThread().interrupt();
            throw new AssertionError(cause);
        }
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
