package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.usage;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.client;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.endpoint;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.listen;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// Explicit broker replies expose subscription lifetime independently of reconnection timing.
@Timeout(30)
class SubscriptionContinuityTest {

    @Test
    void a_refused_recreation_notifies_the_original_handle_and_only_ends_that_subscription()
            throws Exception {
        // Arrange
        var ended = new LinkedBlockingQueue<Disconnection>();
        var refused = new LinkedBlockingQueue<ResubscribeFailure>();
        var listener = new ConnectionListener() {
            @Override
            public void onDisconnected(Disconnection disconnection) {
                ended.add(disconnection);
            }

            @Override
            public void onResubscribeFailed(
                    Subscription subscription,
                    MqttSubscriptionRefusedException failure
            ) {
                refused.add(new ResubscribeFailure(subscription, failure));
            }
        };
        try (ServerSocket server = listen();
                ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
                MqttClient sut = MqttClient.builder()
                        .server(endpoint(server))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .connectionListener(listener)
                        .build()) {
            Subscription rejected;
            Subscription survivor;
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                rejected = peer.subscribePull(sut, "refused", AckMode.ON_RECEIVE, 4);
                survivor = peer.subscribePull(sut, "survivor", AckMode.ON_RECEIVE, 4);
                sut.disconnect().get(5, SECONDS);
            }
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            var receiving = new CountDownLatch(1);
            var received = new CompletableFuture<ReceiveResult>();
            executor.submit(() -> {
                receiving.countDown();
                try {
                    received.complete(rejected.receive());
                } catch (Exception failure) {
                    received.completeExceptionally(failure);
                }
            });
            assertThat(receiving.await(5, SECONDS)).isTrue();

            // Act: one re-creation is refused while another succeeds on the same connection.
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                Packet.Subscribe first = assertInstanceOf(Packet.Subscribe.class, peer.receive());
                Packet.Subscribe second = assertInstanceOf(Packet.Subscribe.class, peer.receive());
                assertThat(List.of(first, second))
                        .extracting(request -> request.subscriptions().getFirst().filter())
                        .containsExactlyInAnyOrder(
                                new TopicFilter("refused"),
                                new TopicFilter("survivor")
                        );
                peer.roundTripBarrier(sut);
                assertThat(received).isNotDone();
                for (Packet.Subscribe request : List.of(first, second)) {
                    ReasonCode reason = request.subscriptions().getFirst().filter()
                            .equals(new TopicFilter("refused"))
                            ? ReasonCode.UNSPECIFIED_ERROR : ReasonCode.GRANTED_QOS_1;
                    peer.send(new Packet.SubAck(request.packetId(), List.of(reason), EMPTY));
                }

                // Assert: the listener and receiver share the original handle's refusal.
                ResubscribeFailure notification = requireNonNull(refused.poll(5, SECONDS));
                assertThat(notification.subscription()).isSameAs(rejected);
                assertThat(notification.failure().reasonCode())
                        .isEqualTo(ReasonCode.UNSPECIFIED_ERROR);
                assertThat(received.handle((_, failure) -> failure).get(5, SECONDS))
                        .isInstanceOfSatisfying(
                                MqttSubscriptionRefusedException.class,
                                failure -> assertThat(failure.reasonCode())
                                        .isEqualTo(notification.failure().reasonCode())
                        );
                assertThatThrownBy(() -> rejected.receive(Duration.ZERO))
                        .isInstanceOfSatisfying(
                                MqttSubscriptionRefusedException.class,
                                failure -> assertThat(failure.reasonCode())
                                        .isEqualTo(notification.failure().reasonCode())
                        );
                peer.publish("survivor", 7);
                assertThat(received(survivor.receive(Duration.ofSeconds(5))).message().payload())
                        .containsExactly((byte) 1);
                peer.expectAcknowledgements(7, 1);
                peer.roundTripBarrier(sut);
                sut.disconnect().get(5, SECONDS);
                assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 2));
                assertThat(refused).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_waiting_receiver_stays_with_its_subscription_across_a_connection_end(boolean failure)
            throws Exception {
        // Arrange
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen();
                var executor = Executors.newVirtualThreadPerTaskExecutor();
                MqttClient sut = MqttClient.builder()
                .server(endpoint(server))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .connectionListener(ended::add)
                .build()) {
            Subscription subscription;
            var receiving = new CountDownLatch(1);
            CompletableFuture<ReceiveResult> received = new CompletableFuture<>();
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                subscription = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 4);
                executor.submit(() -> {
                    receiving.countDown();
                    try {
                        received.complete(subscription.receive());
                    } catch (Exception cause) {
                        received.completeExceptionally(cause);
                    }
                });
                assertThat(receiving.await(5, SECONDS)).isTrue();
                if (!failure) {
                    sut.disconnect().get(5, SECONDS);
                }
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();

            // Act: CONNACK alone cannot complete the pending receive, nor can delayed SUBACK.
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                var request = assertInstanceOf(Packet.Subscribe.class, peer.receive());
                peer.roundTripBarrier(sut);
                assertThat(received).isNotDone();
                peer.send(new Packet.SubAck(request.packetId(),
                        List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
                peer.publish("data", 7);

                // Assert: the original receive claims the new connection's delivery.
                assertThat(received(received.get(5, SECONDS)).message().payload())
                        .containsExactly((byte) 1);
                peer.expectAcknowledgements(7, 1);
            } finally {
                subscription.close();
            }
        }
    }

    @Test
    void disconnect_discards_buffered_input_and_old_ack_cannot_acknowledge_a_reused_packet_id()
            throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            Subscription subscription;
            MqttDelivery previous;
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                subscription = peer.subscribePull(sut, "data", AckMode.MANUAL, 4);
                peer.publish("data", 7);
                previous = received(subscription.receive(Duration.ofSeconds(5)));
                peer.publish("data", 8);
                peer.roundTripBarrier(sut);
                sut.disconnect().get(5, SECONDS);
            }

            // Act: recreate the registration and reuse the old connection's packet id.
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                var request = assertInstanceOf(Packet.Subscribe.class, peer.receive());
                peer.send(new Packet.SubAck(request.packetId(),
                        List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
                peer.roundTripBarrier(sut);
                assertThat(subscription.receive(Duration.ZERO))
                        .isEqualTo(new ReceiveResult.TimedOut());
                peer.publish("data", 7);
                MqttDelivery current = received(subscription.receive(Duration.ofSeconds(5)));
                previous.ack();

                // Assert: only the current handle can release the reused wire transaction.
                peer.expectPublicationBarrier(sut);
                current.ack();
                peer.expectAcknowledgements(7, 1);
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void a_recreated_subscription_exposes_the_new_granted_qos_on_its_existing_handle()
            throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            Subscription subscription;
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                subscription = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 4);
                assertThat(subscription.grantedQos()).isEqualTo(Qos.AT_LEAST_ONCE);
                sut.disconnect().get(5, SECONDS);
            }

            // Act
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                var request = assertInstanceOf(Packet.Subscribe.class, peer.receive());
                assertThat(request.subscriptions().getFirst().qos()).isEqualTo(Qos.AT_LEAST_ONCE);
                peer.send(new Packet.SubAck(request.packetId(),
                        List.of(ReasonCode.SUCCESS), EMPTY));
                peer.roundTripBarrier(sut);

                // Assert
                assertThat(subscription.grantedQos()).isEqualTo(Qos.AT_MOST_ONCE);
                var publication = new Packet.Publish(new Topic("data"), new byte[] {2},
                        Qos.AT_MOST_ONCE, false, false, 0, EMPTY);
                peer.send(publication);
                assertThat(received(subscription.receive(Duration.ofSeconds(5))).message().qos())
                        .isEqualTo(Qos.AT_MOST_ONCE);
            }
        }
    }

    @Test
    void removal_observers_survive_socket_loss_without_holding_operation_capacity()
            throws Exception {
        // Arrange: observe one persistent removal as often as the ordinary operation limit.
        var ended = new LinkedBlockingQueue<Disconnection>();
        var observers = new ArrayList<CompletableFuture<Void>>();
        try (ServerSocket server = listen(); MqttClient sut = MqttClient.builder()
                .server(endpoint(server))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .clientId("pending-removal")
                .sessionExpiry(Duration.ofMinutes(1))
                .connectionListener(ended::add)
                .build()) {
            Subscription subscription;
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                subscription = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 4);
                // The connect completion thread returns its operation slot after the future.
                await().pollDelay(Duration.ZERO)
                        .untilAsserted(() -> assertThat(usage(sut).operations()).isZero());
                subscription.close();
                Packet.Unsubscribe removal = assertInstanceOf(
                        Packet.Unsubscribe.class,
                        peer.receive()
                );
                assertThat(removal.filters()).containsExactly(new TopicFilter("data"));
                for (int i = 0; i < 64; i++) {
                    observers.add(subscription.unsubscribe());
                }
                assertThat(observers).allSatisfy(observer -> assertThat(observer).isNotDone());
                assertThat(usage(sut).operations()).isZero();
                await().untilAsserted(() -> assertThat(usage(sut).mailboxEvents()).isZero());

                // Act: leaving this scope closes the socket without acknowledging UNSUBSCRIBE.
            }

            // Assert: local close remains permanent while every observer awaits confirmation.
            assertThat(ended.poll(5, SECONDS)).isNotNull();
            assertThat(observers).allSatisfy(observer -> assertThat(observer).isNotDone());
            assertThat(usage(sut).operations()).isZero();
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());

            // Act: the retained broker session requires UNSUBACK before confirming removal.
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut, true, _ -> { })) {
                Packet.Unsubscribe removal = assertInstanceOf(
                        Packet.Unsubscribe.class,
                        peer.receive()
                );
                assertThat(removal.filters()).containsExactly(new TopicFilter("data"));
                peer.send(new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY));
                for (CompletableFuture<Void> observer : observers) {
                    observer.get(5, SECONDS);
                }
                peer.expectPublicationBarrier(sut);
                Subscription replacement = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 4);
                peer.publish("data", 7);
                assertThat(replacement.receive(Duration.ofSeconds(5)))
                        .isInstanceOf(ReceiveResult.Received.class);
                peer.expectAcknowledgements(7, 1);

                assertThat(observers).allSatisfy(observer -> assertThat(observer).isCompleted());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void removal_waits_for_the_pending_resuback_before_sending_unsubscribe(boolean close)
            throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            Subscription subscription;
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                subscription = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 4);
                sut.disconnect().get(5, SECONDS);
            }
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                var request = assertInstanceOf(Packet.Subscribe.class, peer.receive());
                if (close) {
                    subscription.close();
                }

                // Act
                CompletableFuture<Void> removed = subscription.unsubscribe();
                peer.roundTripBarrier(sut);
                assertThat(removed).isNotDone();
                peer.send(new Packet.SubAck(request.packetId(),
                        List.of(ReasonCode.GRANTED_QOS_1), EMPTY));

                // Assert: the broker first completes recreation, then acknowledges its removal.
                var removal = assertInstanceOf(Packet.Unsubscribe.class, peer.receive());
                assertThat(removal.filters()).containsExactly(new TopicFilter("data"));
                peer.send(new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY));
                removed.get(5, SECONDS);
                assertThat(subscription.receive()).isEqualTo(new ReceiveResult.Ended());
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @ParameterizedTest(name = "closed before unsubscribe: {0}")
    @ValueSource(booleans = {false, true})
    void resuback_rejection_confirms_a_requested_removal_of_a_pull_subscription(boolean close)
            throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            Subscription subscription;
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                subscription = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 4);
                sut.disconnect().get(5, SECONDS);
            }
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                var request = assertInstanceOf(Packet.Subscribe.class, peer.receive());
                if (close) {
                    subscription.close();
                }
                CompletableFuture<Void> removed = subscription.unsubscribe();
                peer.roundTripBarrier(sut);

                // Act: the broker has no registration left to remove.
                peer.send(new Packet.SubAck(request.packetId(),
                        List.of(ReasonCode.UNSPECIFIED_ERROR), EMPTY));
                removed.get(5, SECONDS);

                // Assert: the requested removal owns the result; the refusal only confirms it.
                assertThat(subscription.receive()).isEqualTo(new ReceiveResult.Ended());
                peer.expectPublicationBarrier(sut);
                Subscription replacement = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 4);
                subscription.close();
                peer.publish("data", 7);
                assertThat(replacement.receive(Duration.ofSeconds(5)))
                        .isInstanceOf(ReceiveResult.Received.class);
                peer.expectAcknowledgements(7, 1);
            }
        }
    }

    private static MqttDelivery received(ReceiveResult result) {
        return assertInstanceOf(ReceiveResult.Received.class, result).delivery();
    }

    private record ResubscribeFailure(
            Subscription subscription,
            MqttSubscriptionRefusedException failure
    ) {}
}
