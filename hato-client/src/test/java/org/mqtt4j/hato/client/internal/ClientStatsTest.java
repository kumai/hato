package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.CapacityLimit;
import org.mqtt4j.hato.ClientStats;
import org.mqtt4j.hato.ClientStats.ConnectionState;
import org.mqtt4j.hato.ClientStats.DropReason;
import org.mqtt4j.hato.ClientStats.OverloadCause;
import org.mqtt4j.hato.ClientStats.SubscriptionStats;
import org.mqtt4j.hato.ClientStats.Usage;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttClientBuilder;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.PullSubscription;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.TimerScheduler;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class ClientStatsTest {

    private final ManualTimerScheduler scheduler = new ManualTimerScheduler();
    private final LinkedBlockingQueue<ScriptTransport> transports = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Disconnection> ends = new LinkedBlockingQueue<>();

    // Runs before each clock read on the client's actor; tests use it to hold the actor or to
    // fail its clock.
    private volatile Runnable beforeActorTime = () -> { };

    @Test
    void calls_made_while_a_snapshot_is_pending_share_it_within_the_observation_limit()
            throws Exception {
        // Arrange: the actor is held in the snapshot that the first call requested.
        var release = new CountDownLatch(1);
        try (MqttClient sut = builder().build()) {
            var pending = new ArrayList<CompletableFuture<ClientStats>>();
            pending.add(holdActorInStats(sut, release));

            // Act
            for (int i = 1; i < ((ClientActor) sut).waiterLimit(); i++) {
                pending.add(sut.stats());
            }
            CompletableFuture<ClientStats> refused = sut.stats();
            CompletableFuture<Void> refusedTermination = sut.terminated();
            pending.getFirst().cancel(false);
            release.countDown();

            // Assert: each call has its own future, so cancelling one leaves the others.
            for (CompletableFuture<?> refusal : List.of(refused, refusedTermination)) {
                assertThatThrownBy(() -> refusal.getNow(null))
                        .hasCauseInstanceOf(MqttCapacityException.class);
            }
            ClientStats shared = pending.get(1).get(5, SECONDS);
            for (CompletableFuture<ClientStats> call : pending.subList(1, pending.size())) {
                assertThat(call.get(5, SECONDS)).isEqualTo(shared);
            }
            assertThat(shared.refusals()).containsEntry(CapacityLimit.OBSERVATIONS, 2L);
            assertThat(stats(sut).refusals()).containsEntry(CapacityLimit.OBSERVATIONS, 2L);
        } finally {
            release.countDown();
        }
    }

    @Test
    void each_subscription_shows_its_waiting_running_and_unacknowledged_deliveries()
            throws Exception {
        // Arrange: the handler holds each delivery until the test lets it return, without ack().
        var started = new LinkedBlockingQueue<MqttDelivery>();
        var returns = new Semaphore(0);
        try (MqttClient sut = builder().build()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("readings"), Qos.AT_LEAST_ONCE)
                            .withAckMode(AckMode.MANUAL)
                            .withBufferCapacity(8),
                    delivery -> {
                        started.add(delivery);
                        returns.acquireUninterruptibly();
                    }
            );
            Subscription idle = sut.subscribe(
                    Subscribe.of(new TopicFilter("idle"), Qos.AT_LEAST_ONCE),
                    _ -> { }
            );
            acknowledgeSubscribes(transport, 2);
            for (int id = 1; id <= 3; id++) {
                transport.receive(qosOnePublish("readings", id));
            }
            MqttDelivery first = requireNonNull(started.poll(5, SECONDS));

            // Act
            ClientStats running = stats(sut);

            // Assert
            SubscriptionStats values = find(running, subscription);
            assertThat(values.waiting()).isEqualTo(2);
            assertThat(values.running()).isOne();
            assertThat(values.awaitingAck()).isZero();
            assertThat(values.filter()).isEqualTo(new TopicFilter("readings"));
            assertThat(values.bufferCapacity()).isEqualTo(8);
            assertThat(find(running, idle).waiting()).isZero();
            assertThat(running.receiving().deliveries()).isEqualTo(3);
            assertThat(running.receiving().heldDeliveries()).isEqualTo(new Usage(3, 65_536));
            assertThat(running.receiving().heldConditions()).isEqualTo(new Usage(3, 1_048_576));

            // Act: the first handler returns without ack(), and the second one starts.
            returns.release();
            requireNonNull(started.poll(5, SECONDS));

            // Assert
            SubscriptionStats processed = find(stats(sut), subscription);
            assertThat(processed.waiting()).isOne();
            assertThat(processed.running()).isOne();
            assertThat(processed.awaitingAck()).isOne();

            // Act
            first.ack();

            // Assert
            assertThat(find(stats(sut), subscription).awaitingAck()).isZero();
        } finally {
            returns.release(3);
        }
    }

    @Test
    void dropped_deliveries_are_counted_by_reason_for_their_subscription_and_the_client()
            throws Exception {
        // Arrange: the handler holds its first delivery and fails on a payload of 0.
        var holding = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = builder().build()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("readings"), Qos.AT_MOST_ONCE)
                            .withBufferCapacity(1),
                    delivery -> {
                        if (delivery.message().payload()[0] == 0) {
                            throw new IllegalStateException("handler failure");
                        }
                        holding.countDown();
                        waitFor(release);
                    }
            );
            Subscription idle = sut.subscribe(
                    Subscribe.of(new TopicFilter("idle"), Qos.AT_MOST_ONCE),
                    _ -> { }
            );
            acknowledgeSubscribes(transport, 2);
            transport.receive(publish("readings", 1));
            assertThat(holding.await(5, SECONDS)).isTrue();

            // Act: two arrivals find the buffer full, and a later one fails its handler.
            transport.receive(publish("readings", 2));
            transport.receive(publish("readings", 3));
            await().untilAsserted(() -> assertThat(find(stats(sut), subscription).dropped())
                    .containsEntry(DropReason.SUBSCRIPTION_FULL, 2L));
            release.countDown();
            await().untilAsserted(() -> assertThat(find(stats(sut), subscription).running())
                    .isZero());
            transport.receive(publish("readings", 0));

            // Assert
            await().untilAsserted(() -> assertThat(find(stats(sut), subscription).dropped())
                    .containsEntry(DropReason.FAILED, 1L));
            ClientStats stats = stats(sut);
            Map<DropReason, Long> expected = Map.of(
                    DropReason.FAILED, 1L,
                    DropReason.CONNECTION_ENDED, 0L,
                    DropReason.SUBSCRIPTION_STOPPED, 0L,
                    DropReason.SUBSCRIPTION_FULL, 2L,
                    DropReason.CLIENT_FULL, 0L,
                    DropReason.REFUSED, 0L
            );
            assertThat(find(stats, subscription).dropped()).isEqualTo(expected);
            assertThat(find(stats, idle).dropped()).containsOnlyKeys(DropReason.values())
                    .allSatisfy((_, count) -> assertThat(count).isZero());
            assertThat(stats.receiving().dropped()).isEqualTo(expected);
            assertThat(stats.receiving().deliveries()).isEqualTo(2);
            assertThat(stats.receiving().publishes(Qos.AT_MOST_ONCE)).isEqualTo(4);
        } finally {
            release.countDown();
        }
    }

    @Test
    void deliveries_waiting_on_their_stripe_count_as_waiting() throws Exception {
        // Arrange: the handler holds the first of three deliveries on one topic, so the other
        // two wait on the same stripe.
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = builder().build()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("readings"), Qos.AT_LEAST_ONCE),
                    DeliveryOptions.striped(2),
                    _ -> {
                        started.countDown();
                        waitFor(release);
                    }
            );
            acknowledgeSubscribes(transport, 1);
            subscription.established().get(5, SECONDS);

            // Act
            for (int id = 1; id <= 3; id++) {
                transport.receive(qosOnePublish("readings", id));
            }
            assertThat(started.await(5, SECONDS)).isTrue();

            // Assert
            await().untilAsserted(() -> {
                SubscriptionStats values = find(stats(sut), subscription);
                assertThat(values.waiting()).isEqualTo(2);
                assertThat(values.running()).isOne();
            });
        } finally {
            release.countDown();
        }
    }

    @Test
    void an_accepted_hand_off_delivery_awaits_its_ack_without_holding_the_buffer()
            throws Exception {
        // Arrange
        var accepted = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = builder().build()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.handOff(
                    Subscribe.of(new TopicFilter("orders"), Qos.AT_LEAST_ONCE)
                            .withAckMode(AckMode.MANUAL),
                    accepted::add
            );
            acknowledgeSubscribes(transport, 1);
            subscription.established().get(5, SECONDS);

            // Act
            transport.receive(qosOnePublish("orders", 1));
            MqttDelivery delivery = requireNonNull(accepted.poll(5, SECONDS));

            // Assert
            await().untilAsserted(() -> assertThat(find(stats(sut), subscription).awaitingAck())
                    .isOne());
            ClientStats stats = stats(sut);
            assertThat(find(stats, subscription).running()).isZero();
            assertThat(stats.receiving().heldDeliveries().used()).isZero();
            assertThat(stats.receiving().awaitingConditions()).isOne();

            // Act
            delivery.ack();

            // Assert
            await().untilAsserted(() -> assertThat(stats(sut).receiving().awaitingConditions())
                    .isZero());
            assertThat(find(stats(sut), subscription).awaitingAck()).isZero();
        }
    }

    @Test
    void a_subscription_is_listed_from_the_return_of_its_handle_before_its_registration()
            throws Exception {
        // Arrange: the registration waits behind the snapshot that holds the actor.
        var release = new CountDownLatch(1);
        try (MqttClient sut = builder().build()) {
            CompletableFuture<ClientStats> pending = holdActorInStats(sut, release);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("readings"), Qos.AT_LEAST_ONCE)
                            .withBufferCapacity(8),
                    _ -> { }
            );

            // Act
            release.countDown();
            ClientStats stats = pending.get(5, SECONDS);

            // Assert
            SubscriptionStats values = find(stats, subscription);
            assertThat(values.waiting()).isZero();
            assertThat(values.bufferCapacity()).isEqualTo(8);
            assertThat(values.dropped()).containsOnlyKeys(DropReason.values());
            assertThat(stats.subscribing().registrations().used()).isZero();
            assertThat(stats(sut).subscribing().pending()).isOne();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_closed_subscription_leaves_the_list_while_its_removal_continues() throws Exception {
        // Arrange
        try (MqttClient sut = builder().build()) {
            ScriptTransport transport = connect(sut);
            Subscription closed = subscribe(sut, transport, "closed");
            Subscription removed = subscribe(sut, transport, "removed");

            // Act
            closed.close();
            CompletableFuture<Void> removal = removed.unsubscribe();
            var first = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            var second = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));

            // Assert: the unsubscribed handle stays listed until UNSUBACK.
            ClientStats removing = stats(sut);
            assertThat(removing.subscriptions())
                    .extracting(SubscriptionStats::subscription)
                    .containsExactly(removed);
            assertThat(removing.subscribing().removing()).isEqualTo(2);
            assertThat(removing.subscribing().registrations())
                    .isEqualTo(new Usage(2, 4_096));

            // Act
            transport.receive(new Packet.UnsubAck(first.packetId(), List.of(), EMPTY));
            transport.receive(new Packet.UnsubAck(second.packetId(), List.of(), EMPTY));
            removal.get(5, SECONDS);

            // Assert
            ClientStats removedStats = stats(sut);
            assertThat(removedStats.subscriptions()).isEmpty();
            assertThat(removedStats.subscribing().registrations().used()).isZero();
            assertThat(removedStats.subscribing().removing()).isZero();
        }
    }

    @Test
    void send_values_show_the_operation_capacity_and_the_queued_writes() throws Exception {
        // Arrange: the transport holds the write of the publication.
        try (MqttClient sut = builder().build()) {
            ScriptTransport transport = connect(sut);
            transport.completeSends = false;
            CompletableFuture<PublishResult> published = sut.publish(MqttMessage.of(
                    new Topic("readings"),
                    new byte[] {1},
                    Qos.AT_LEAST_ONCE,
                    false
            ));
            var publish = (Packet.Publish) requireNonNull(transport.sent.poll(5, SECONDS));

            // Act
            ClientStats.Sending queued = stats(sut).sending();

            // Assert
            assertThat(queued.operations()).isEqualTo(new Usage(1, 64));
            assertThat(queued.queuedWrites()).isEqualTo(new Usage(1, 144));
            assertThat(queued.queuedBytes()).isEqualTo(new Usage(
                    PacketCodec.of(MqttVersion.V3_1_1).encode(publish).length,
                    17L * 1024 * 1024
            ));
            assertThat(queued.awaitingPubAck()).isOne();
            assertThat(queued.oldestResponseWait()).isEmpty();
            assertThat(queued.quota()).isEmpty();

            // Act: the write completes, and PUBACK follows 5 ms later.
            transport.completeSend();
            await().untilAsserted(() -> assertThat(stats(sut).sending().oldestResponseWait())
                    .isPresent());
            scheduler.advance(Duration.ofMillis(5));
            ClientStats.Sending written = stats(sut).sending();
            transport.receive(new Packet.PubAck(publish.packetId(), ReasonCode.SUCCESS, EMPTY));
            published.get(5, SECONDS);

            // Assert
            assertThat(written.queuedWrites().used()).isZero();
            assertThat(written.oldestResponseWait()).contains(Duration.ofMillis(5));
            await().untilAsserted(() -> assertThat(stats(sut).sending().operations().used())
                    .isZero());
            assertThat(stats(sut).sending().awaitingPubAck()).isZero();
        }
    }

    @Test
    @SuppressWarnings("try") // The closed state is under test; the resource closes on failure.
    void connection_values_follow_its_state_and_attempt_numbers() throws Exception {
        // Arrange
        try (MqttClient sut = builder()
                .reconnectPolicy(ReconnectPolicy.exponentialBackoff(
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1)
                ))
                .build()) {
            assertThat(connection(sut)).isEqualTo(List.of(ConnectionState.DISCONNECTED, 0L, 0L));

            // Act & Assert: the first attempt establishes a connection, which then drops.
            CompletableFuture<ConnAck> connected = sut.connect();
            ScriptTransport first = nextTransport();
            assertThat(connection(sut)).isEqualTo(List.of(ConnectionState.CONNECTING, 1L, 0L));
            first.accept();
            connected.get(5, SECONDS);
            assertThat(connection(sut)).isEqualTo(List.of(ConnectionState.CONNECTED, 1L, 1L));
            first.finishClose();
            Disconnection lost = requireNonNull(ends.poll(5, SECONDS));
            assertThat(connection(sut))
                    .isEqualTo(List.of(ConnectionState.WAITING_TO_RECONNECT, 1L, 1L));

            // Act & Assert: the second attempt establishes the next connection.
            scheduler.advance(lost.reconnectDelay().orElseThrow());
            ScriptTransport second = nextTransport();
            assertThat(connection(sut)).isEqualTo(List.of(ConnectionState.CONNECTING, 2L, 1L));
            second.accept();
            await().untilAsserted(() -> assertThat(connection(sut))
                    .isEqualTo(List.of(ConnectionState.CONNECTED, 2L, 2L)));

            // Act & Assert: a disconnection waits for its DISCONNECT to be written.
            second.completeSends = false;
            CompletableFuture<Void> disconnected = sut.disconnect();
            assertThat(second.sent.poll(5, SECONDS)).isInstanceOf(Packet.Disconnect.class);
            assertThat(connection(sut))
                    .isEqualTo(List.of(ConnectionState.DISCONNECTING, 2L, 2L));
            second.completeSend();
            disconnected.get(5, SECONDS);
            assertThat(connection(sut)).isEqualTo(List.of(ConnectionState.DISCONNECTED, 2L, 2L));

            // Act & Assert
            sut.close();
            assertThat(connection(sut)).isEqualTo(List.of(ConnectionState.CLOSED, 2L, 2L));
        }
    }

    @Test
    void notifications_dropped_by_a_slow_listener_are_counted() throws Exception {
        // Arrange: the listener holds its first notification, so 64 later ones wait for it.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var slow = new ConnectionListener() {
            @Override
            public void onConnected(ConnAck ack) {
                entered.countDown();
                waitFor(release);
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
            }
        };
        try (MqttClient sut = builder().connectionListener(slow).build()) {
            connect(sut);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act: 39 disconnections and connections notify 78 times more.
            for (int i = 0; i < 39; i++) {
                sut.disconnect().get(5, SECONDS);
                connect(sut);
            }

            // Assert
            assertThat(stats(sut).connection().droppedNotifications()).isEqualTo(78 - 64);
        } finally {
            release.countDown();
        }
    }

    @Test
    void refused_operations_are_counted_by_the_limit_that_refused_them() throws Exception {
        // Arrange: one publication and one subscription fill the configured limits, and QoS 0
        // publications whose writes are held take the rest of the 64 operation slots.
        try (MqttClient sut = builder()
                .maxInflightPublishes(1)
                .maxSubscriptions(1)
                .build()) {
            ScriptTransport transport = connect(sut);
            subscribe(sut, transport, "readings");
            sut.publish(qosOneMessage());
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            transport.completeSends = false;
            var operations = new ArrayList<CompletableFuture<PublishResult>>();
            for (int i = 0; i < 63; i++) {
                operations.add(sut.publish(qosZeroMessage()));
            }
            // The first write waits in the transport, and the others wait behind it.
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);

            // Act
            CompletableFuture<PublishResult> overOperations = sut.publish(qosZeroMessage());
            transport.completeSends = true;
            transport.completeSend();
            await().until(() -> operations.stream().allMatch(CompletableFuture::isDone));
            CompletableFuture<PublishResult> overInflight = sut.publish(qosOneMessage());
            Subscription overSubscriptions = sut.subscribe(
                    Subscribe.of(new TopicFilter("more"), Qos.AT_LEAST_ONCE),
                    _ -> { }
            );
            // The registration queue holds as many registrations as the client holds
            // subscriptions, one here, so the next one waits until this one is refused.
            CompletableFuture<Qos> refusedSubscription = overSubscriptions.established();
            await().until(refusedSubscription::isDone);
            PullSubscription overPulled = sut.subscribe(
                    Subscribe.of(new TopicFilter("pulled"), Qos.AT_LEAST_ONCE)
            );

            // Assert: each refusal names the limit that refused it, also to a receiver.
            assertRefusedBy(overOperations, CapacityLimit.OPERATIONS);
            assertRefusedBy(overInflight, CapacityLimit.INFLIGHT_PUBLISHES);
            assertRefusedBy(refusedSubscription, CapacityLimit.SUBSCRIPTIONS);
            assertRefusedBy(overPulled.established(), CapacityLimit.SUBSCRIPTIONS);
            assertThatThrownBy(() -> overPulled.receive(Duration.ZERO))
                    .isInstanceOfSatisfying(MqttCapacityException.class, refusal ->
                            assertThat(refusal.limit()).contains(CapacityLimit.SUBSCRIPTIONS));
            ClientStats stats = stats(sut);
            assertThat(stats.sending().publications()).isEqualTo(new Usage(1, 1));
            assertThat(stats.subscribing().registrations()).isEqualTo(new Usage(1, 1));
            Map<CapacityLimit, Long> refusals = stats.refusals();
            assertThat(refusals).containsOnlyKeys(CapacityLimit.values());
            assertThat(refusals).containsEntry(CapacityLimit.OPERATIONS, 1L)
                    .containsEntry(CapacityLimit.INFLIGHT_PUBLISHES, 1L)
                    .containsEntry(CapacityLimit.SUBSCRIPTIONS, 2L)
                    .containsEntry(CapacityLimit.SEND_QUEUE, 0L);
        }
    }

    @Test
    @SuppressWarnings("try") // Counts after close are under test; the resource closes on failure.
    void connections_ended_for_overload_are_counted_by_cause_for_the_clients_lifetime()
            throws Exception {
        // Arrange: the handler holds the only delivery that its buffer has room for, and the
        // acceptor refuses every delivery.
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = builder().build()) {
            ScriptTransport first = connect(sut);
            sut.subscribe(
                    Subscribe.of(new TopicFilter("readings"), Qos.AT_LEAST_ONCE)
                            .withBufferCapacity(1),
                    _ -> {
                        started.countDown();
                        waitFor(release);
                    }
            );
            sut.handOff(Subscribe.of(new TopicFilter("orders"), Qos.AT_LEAST_ONCE), _ -> false);
            acknowledgeSubscribes(first, 2);
            first.receive(qosOnePublish("readings", 1));
            // A delivery that still waited would end with the connection; a running one stays.
            assertThat(started.await(5, SECONDS)).isTrue();

            // Act: a second delivery overloads the first connection, and a third the next one.
            first.receive(qosOnePublish("readings", 2));
            assertThat(requireNonNull(ends.poll(5, SECONDS)).cause())
                    .containsInstanceOf(MqttCapacityException.class);
            ClientStats once = stats(sut);
            ScriptTransport second = connect(sut);
            acknowledgeSubscribes(second, 2);
            second.receive(qosOnePublish("readings", 3));
            requireNonNull(ends.poll(5, SECONDS));
            ScriptTransport third = connect(sut);
            acknowledgeSubscribes(third, 2);
            third.receive(qosOnePublish("orders", 4));
            requireNonNull(ends.poll(5, SECONDS));
            release.countDown();
            sut.close();
            sut.terminated().get(5, SECONDS);

            // Assert: the counts outlive each connection and the client.
            assertThat(once.overloads()).containsEntry(OverloadCause.DELIVERIES, 1L);
            assertThat(sut.stats().get(5, SECONDS).overloads()).isEqualTo(Map.of(
                    OverloadCause.RECEPTIONS, 0L,
                    OverloadCause.DELIVERIES, 2L,
                    OverloadCause.HAND_OFF, 1L,
                    OverloadCause.EVENTS, 0L,
                    OverloadCause.RESERVED_SENDS, 0L
            ));
        } finally {
            release.countDown();
        }
    }

    @Test
    void registrations_beyond_the_registration_queue_are_refused_and_counted() throws Exception {
        // Arrange: a client of 256 subscriptions, whose 256 registrations wait behind the
        // snapshot that holds the actor.
        var release = new CountDownLatch(1);
        try (MqttClient sut = builder().maxSubscriptions(256).build()) {
            CompletableFuture<ClientStats> pending = holdActorInStats(sut, release);
            for (int i = 0; i < 256; i++) {
                sut.subscribe(
                        Subscribe.of(new TopicFilter("burst/" + i), Qos.AT_LEAST_ONCE),
                        _ -> { }
                );
            }

            // Act
            Subscription refused = sut.subscribe(
                    Subscribe.of(new TopicFilter("over"), Qos.AT_LEAST_ONCE),
                    _ -> { }
            );
            release.countDown();

            // Assert
            assertThatThrownBy(() -> refused.established().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class);
            assertThat(pending.get(5, SECONDS).refusals())
                    .containsEntry(CapacityLimit.REGISTRATION_QUEUE, 1L);
        } finally {
            release.countDown();
        }
    }

    @Test
    void packets_beyond_the_incoming_queue_end_the_connection_as_an_overload() throws Exception {
        // Arrange: the broker's packets pile up while the actor is held.
        var release = new CountDownLatch(1);
        try (MqttClient sut = builder().build()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<ClientStats> pending = holdActorInStats(sut, release);

            // Act: one PINGRESP more than the 65,536 packets that the client queues.
            for (int i = 0; i <= 65_536; i++) {
                transport.receive(new Packet.PingResp());
            }
            release.countDown();

            // Assert
            pending.get(5, SECONDS);
            assertThat(requireNonNull(ends.poll(5, SECONDS)).cause())
                    .containsInstanceOf(MqttCapacityException.class);
            assertThat(stats(sut).overloads()).containsEntry(OverloadCause.EVENTS, 1L);
        } finally {
            release.countDown();
        }
    }

    @Test
    void qos_0_publications_beyond_the_incoming_queue_are_dropped_and_counted() throws Exception {
        // Arrange: the broker's publications pile up while the actor is held.
        var release = new CountDownLatch(1);
        try (MqttClient sut = builder().build()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<ClientStats> pending = holdActorInStats(sut, release);

            // Act: one QoS 0 publication more than the 65,536 that the client queues.
            for (int i = 0; i <= 65_536; i++) {
                transport.receive(publish("readings", 1));
            }
            release.countDown();

            // Assert: unlike the packets above, the one that did not fit leaves the connection
            // up; it counts as received, and the rest as matching no subscription.
            pending.get(5, SECONDS);
            ClientStats stats = stats(sut);
            assertThat(stats.receiving().unrouted()).isOne();
            assertThat(stats.receiving().publishes(Qos.AT_MOST_ONCE)).isEqualTo(65_537);
            assertThat(stats.receiving().unmatched()).isEqualTo(65_536);
            assertThat(stats.overloads()).containsEntry(OverloadCause.EVENTS, 0L);
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource closes on failure.
    void after_close_calls_get_the_snapshot_taken_when_the_client_stopped() throws Exception {
        // Arrange
        try (MqttClient sut = builder().build()) {
            ScriptTransport transport = connect(sut);
            subscribe(sut, transport, "readings");
            transport.receive(publish("readings", 1));
            await().untilAsserted(() -> assertThat(stats(sut).receiving().deliveries()).isOne());

            // Act
            sut.close();
            sut.terminated().get(5, SECONDS);
            CompletableFuture<ClientStats> last = sut.stats();

            // Assert
            assertThat(last).isDone();
            ClientStats stats = last.get();
            assertThat(stats.connection().state()).isEqualTo(ConnectionState.CLOSED);
            assertThat(stats.receiving().publishes(Qos.AT_MOST_ONCE)).isOne();
            assertThat(stats.subscriptions()).isEmpty();
            assertThat(sut.stats().get(5, SECONDS)).isEqualTo(stats);
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource closes on failure.
    void a_call_made_after_close_never_gets_a_snapshot_taken_before_the_client_stopped()
            throws Exception {
        // Arrange: the actor is held in a snapshot that it began before close().
        var release = new CountDownLatch(1);
        try (MqttClient sut = builder().build()) {
            CompletableFuture<ClientStats> before = holdActorInStats(sut, release);
            sut.close();
            CompletableFuture<ClientStats> after = sut.stats();

            // Act
            release.countDown();

            // Assert: both calls get the snapshot taken when the client stopped.
            assertThat(after.get(5, SECONDS).connection().state())
                    .isEqualTo(ConnectionState.CLOSED);
            assertThat(before.get(5, SECONDS)).isEqualTo(after.get());
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_clock_failure_fails_the_pending_calls_and_ends_the_connection() throws Exception {
        // Arrange: the next clock read on the actor fails.
        var failure = new IllegalStateException("Clock unavailable");
        var failing = new AtomicBoolean();
        try (MqttClient sut = builder().build()) {
            connect(sut);
            beforeActorTime = () -> {
                if (failing.getAndSet(false)) {
                    throw failure;
                }
            };
            failing.set(true);

            // Act
            CompletableFuture<ClientStats> failed = sut.stats();

            // Assert
            assertThatThrownBy(() -> failed.get(5, SECONDS)).hasCause(failure);
            assertThat(requireNonNull(ends.poll(5, SECONDS)).cause()).contains(failure);
            assertThat(stats(sut).connection().state()).isEqualTo(ConnectionState.DISCONNECTED);
        }
    }

    @Test
    void qos_0_publications_routed_before_a_clock_failure_are_discarded_with_the_connection()
            throws Exception {
        // Arrange: the handler holds the first publication, so the ones routed after it wait.
        var started = new CountDownLatch(1);
        var returns = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var handled = new LinkedBlockingQueue<Byte>();
        var failure = new IllegalStateException("Clock unavailable");
        try (MqttClient sut = builder().build()) {
            ScriptTransport transport = connect(sut);
            // The transport never reports the SUBSCRIBE written: the actor reads the clock when
            // it learns of a write, and that read could fall among the ones counted below.
            transport.completeSends = false;
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("readings"), Qos.AT_MOST_ONCE),
                    delivery -> {
                        handled.add(delivery.message().payload()[0]);
                        started.countDown();
                        waitFor(returns);
                    }
            );
            acknowledgeSubscribes(transport, 1);
            subscription.established().get(5, SECONDS);
            transport.receive(publish("readings", 0));
            assertThat(started.await(5, SECONDS)).isTrue();
            // Three more wait for the held actor, which takes them as one run; the clock read
            // for the third fails.
            CompletableFuture<ClientStats> held = holdActorInStats(sut, release);
            for (int value = 1; value <= 3; value++) {
                transport.receive(publish("readings", value));
            }
            var reads = new AtomicInteger();
            beforeActorTime = () -> {
                if (reads.incrementAndGet() == 3) {
                    throw failure;
                }
            };

            // Act
            release.countDown();

            // Assert: the end of the connection discards the two that the run had routed, as it
            // discards every delivery that waits, and the handler gets neither.
            assertThat(requireNonNull(ends.poll(5, SECONDS)).cause()).contains(failure);
            held.get(5, SECONDS);
            assertThat(find(stats(sut), subscription).dropped())
                    .containsEntry(DropReason.CONNECTION_ENDED, 2L);
            returns.countDown();
            await().untilAsserted(() -> assertThat(find(stats(sut), subscription).running())
                    .isZero());
            assertThat(handled).containsExactly((byte) 0);
        } finally {
            release.countDown();
            returns.countDown();
        }
    }

    /// Holds the actor in the clock read of the snapshot that a stats() call requests, until
    /// `release`. Returns that call's future.
    private CompletableFuture<ClientStats> holdActorInStats(
            MqttClient sut,
            CountDownLatch release
    ) throws InterruptedException {
        var entered = new CountDownLatch(1);
        beforeActorTime = () -> {
            entered.countDown();
            waitFor(release);
        };
        CompletableFuture<ClientStats> held = sut.stats();
        assertThat(entered.await(5, SECONDS)).isTrue();
        return held;
    }

    private MqttClientBuilder builder() {
        var clock = new TimerScheduler() {
            private volatile @Nullable Thread actor;

            @Override
            public long nanoTime() {
                // The client's first clock read is on its actor thread. A thread of the delivery
                // lanes reads the clock only while it runs the client's events in place; the
                // reads for writes are independent.
                if (actor == null) {
                    actor = Thread.currentThread();
                }
                if (actor == Thread.currentThread() || ofTheLanes(Thread.currentThread())) {
                    beforeActorTime.run();
                }
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                return scheduler.schedule(delay, task);
            }
        };
        return MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .keepAlive(Duration.ZERO)
                .timerScheduler(clock)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .connectionListener(ends::add)
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                });
    }

    private ScriptTransport connect(MqttClient client) throws Exception {
        CompletableFuture<ConnAck> connected = client.connect();
        ScriptTransport transport = nextTransport();
        transport.accept();
        connected.get(5, SECONDS);
        return transport;
    }

    private ScriptTransport nextTransport() throws Exception {
        ScriptTransport transport = requireNonNull(transports.poll(5, SECONDS));
        assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Connect.class);
        return transport;
    }

    private static ClientStats stats(MqttClient client) throws Exception {
        return client.stats().get(5, SECONDS);
    }

    /// The connection's state, attempt number, and established attempt number.
    private static List<Object> connection(MqttClient client) throws Exception {
        ClientStats.Connection connection = stats(client).connection();
        return List.of(
                connection.state(),
                connection.attempt(),
                connection.establishedAttempt()
        );
    }

    private static SubscriptionStats find(ClientStats stats, Subscription subscription) {
        return stats.subscriptions().stream()
                .filter(values -> values.subscription() == subscription)
                .findFirst()
                .orElseThrow();
    }

    private static void assertRefusedBy(CompletableFuture<?> refused, CapacityLimit limit) {
        assertThatThrownBy(() -> refused.get(5, SECONDS))
                .cause()
                .isInstanceOfSatisfying(MqttCapacityException.class, refusal ->
                        assertThat(refusal.limit()).contains(limit));
    }

    private static Subscription subscribe(
            MqttClient client,
            ScriptTransport transport,
            String filter
    ) throws Exception {
        Subscription subscription = client.subscribe(
                Subscribe.of(new TopicFilter(filter), Qos.AT_LEAST_ONCE),
                _ -> { }
        );
        acknowledgeSubscribes(transport, 1);
        subscription.established().get(5, SECONDS);
        return subscription;
    }

    /// Answers the next `count` SUBSCRIBE packets with the QoS that each requested.
    private static void acknowledgeSubscribes(ScriptTransport transport, int count)
            throws Exception {
        for (int i = 0; i < count; i++) {
            var packet = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    packet.packetId(),
                    List.of(ReasonCode.of(packet.subscriptions().getFirst().qos().level())),
                    EMPTY
            ));
        }
    }

    private static MqttMessage qosZeroMessage() {
        return MqttMessage.of(new Topic("out"), new byte[] {1}, Qos.AT_MOST_ONCE, false);
    }

    private static MqttMessage qosOneMessage() {
        return MqttMessage.of(new Topic("out"), new byte[] {1}, Qos.AT_LEAST_ONCE, false);
    }

    private static Packet.Publish publish(String topic, int value) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {(byte) value},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        );
    }

    private static Packet.Publish qosOnePublish(String topic, int packetId) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {1},
                Qos.AT_LEAST_ONCE,
                false,
                false,
                packetId,
                EMPTY
        );
    }

    /// Whether `thread` is one of the delivery lanes' own, a dispatcher or the hand-off worker,
    /// which may run the client's events in place between deliveries.
    private static boolean ofTheLanes(Thread thread) {
        String name = thread.getName();
        return name.contains("-delivery-") || name.endsWith("-hand-off");
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            if (!latch.await(30, SECONDS)) {
                throw new AssertionError("Test failed to release the client");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }
}
