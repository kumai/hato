package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.endpoint;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.listen;
import static org.mqtt4j.hato.client.internal.SessionPersistenceIntegrationTest.failureOf;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

@Timeout(30)
class SubscriptionRegistrationTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_registered_subscription_receives_before_suback(boolean pull) throws Exception {
        // Arrange
        var handled = new LinkedBlockingQueue<MqttDelivery>();
        try (ServerSocket server = listen();
                MqttClient sut = ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            var request = Subscribe.of(new TopicFilter("data"), Qos.AT_LEAST_ONCE);
            Subscription subscription = pull ? sut.subscribe(request)
                    : sut.subscribe(request, handled::add);
            CompletableFuture<Qos> established = subscription.established();
            Packet.Subscribe subscribe = assertInstanceOf(Packet.Subscribe.class, peer.receive());

            // Act: MQTT permits the broker to deliver before confirming the subscription.
            peer.publish("data", 7);
            MqttDelivery delivery = pull ? receive(subscription)
                    : requireNonNull(handled.poll(5, SECONDS));

            // Assert: the already returned handle can consume the delivery without a SUBACK.
            assertThat(delivery.message().payload()).containsExactly((byte) 1);
            assertThat(established).isNotDone();
            peer.expectAcknowledgements(7, 1);
            acknowledge(peer, subscribe, ReasonCode.GRANTED_QOS_1);
            assertThat(established.get(5, SECONDS)).isEqualTo(Qos.AT_LEAST_ONCE);
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void a_pending_registration_survives_connection_end_until_a_later_suback(
            boolean disconnect,
            boolean sessionPresent
    ) throws Exception {
        // Arrange: establish a persistent MQTT session, but withhold the subscription's reply.
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen(); MqttClient sut = persistentClient(server, ended)) {
            Subscription subscription;
            CompletableFuture<Qos> established;
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                subscription = sut.subscribe(
                        Subscribe.of(new TopicFilter("data"), Qos.AT_LEAST_ONCE)
                );
                established = subscription.established();
                assertSubscription(peer, "data");

                // Act: either an explicit DISCONNECT or EOF ends the connection before SUBACK.
                if (disconnect) {
                    disconnect(peer, sut);
                }
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();
            assertThat(established).isNotDone();

            // Assert: the same registration and original waiter resume on either session result.
            try (ScriptedBroker peer = ScriptedBroker.connect(
                    server,
                    sut,
                    sessionPresent,
                    _ -> { }
            )) {
                Packet.Subscribe repeated = assertSubscription(peer, "data");
                acknowledge(peer, repeated, ReasonCode.GRANTED_QOS_1);
                assertThat(established.get(5, SECONDS)).isEqualTo(Qos.AT_LEAST_ONCE);
                peer.publish("data", 8);
                assertThat(receive(subscription).message().payload()).containsExactly((byte) 1);
                peer.expectAcknowledgements(8, 1);
                peer.roundTripBarrier(sut);
            }
        }
    }

    @Test
    void a_registration_survives_a_failed_connection_attempt() throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = ScriptedBroker.client(server)) {
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("data"), Qos.AT_LEAST_ONCE)
            );
            CompletableFuture<Qos> established = subscription.established();

            // Act: a broker refusal fails only this connection attempt.
            CompletableFuture<ConnAck> connected = sut.connect();
            try (ScriptedBroker peer = ScriptedBroker.accept(server)) {
                assertThat(peer.receive()).isInstanceOf(Packet.Connect.class);
                peer.send(new Packet.ConnAck(false, ReasonCode.SERVER_UNAVAILABLE, EMPTY));
                assertThat(failureOf(connected)).isInstanceOf(MqttConnectException.class);
                peer.expectClosed();
            }
            assertThat(established).isNotDone();

            // Assert
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                acknowledge(peer, assertSubscription(peer, "data"), ReasonCode.GRANTED_QOS_1);
                assertThat(established.get(5, SECONDS)).isEqualTo(Qos.AT_LEAST_ONCE);
                peer.publish("data", 7);
                assertThat(receive(subscription).message().payload()).containsExactly((byte) 1);
                peer.expectAcknowledgements(7, 1);
            }
        }
    }

    @Test
    void an_offline_persistent_removal_is_confirmed_by_a_new_broker_session() throws Exception {
        // Arrange
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen(); MqttClient sut = persistentClient(server, ended)) {
            Subscription subscription = subscribeThenDisconnect(server, sut, ended);

            // Act
            CompletableFuture<Void> removed = subscription.unsubscribe();
            assertThat(removed).isNotDone();

            // Assert: a CONNACK without Session Present confirms that nothing remains.
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                removed.get(5, SECONDS);
                assertThat(subscription.receive(Duration.ZERO))
                        .isEqualTo(new ReceiveResult.Ended());
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void an_offline_persistent_removal_waits_for_unsuback_when_the_session_continues()
            throws Exception {
        // Arrange
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen(); MqttClient sut = persistentClient(server, ended)) {
            Subscription subscription = subscribeThenDisconnect(server, sut, ended);

            // Act
            CompletableFuture<Void> removed = subscription.unsubscribe();
            assertThat(removed).isNotDone();

            // Assert: the continued session needs the broker's UNSUBACK.
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut, true, _ -> { })) {
                Packet.Unsubscribe request = assertInstanceOf(
                        Packet.Unsubscribe.class,
                        peer.receive()
                );
                assertThat(request.filters()).containsExactly(new TopicFilter("data"));
                peer.roundTripBarrier(sut);
                assertThat(removed).isNotDone();
                peer.send(new Packet.UnsubAck(request.packetId(), List.of(), EMPTY));
                removed.get(5, SECONDS);
                assertThat(subscription.receive(Duration.ZERO))
                        .isEqualTo(new ReceiveResult.Ended());
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void an_unacknowledged_removal_survives_socket_loss_and_a_new_session_confirms_it()
            throws Exception {
        // Arrange
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen(); MqttClient sut = persistentClient(server, ended)) {
            PendingRemoval pending = removeThenLoseUnsuback(server, sut, ended);

            // Act & Assert: a CONNACK without Session Present confirms that nothing remains.
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                pending.removed().get(5, SECONDS);
                assertThat(pending.subscription().receive(Duration.ZERO))
                        .isEqualTo(new ReceiveResult.Ended());
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void an_unacknowledged_removal_survives_socket_loss_and_is_repeated_when_the_session_continues()
            throws Exception {
        // Arrange
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen(); MqttClient sut = persistentClient(server, ended)) {
            PendingRemoval pending = removeThenLoseUnsuback(server, sut, ended);

            // Act & Assert: the continued session repeats UNSUBSCRIBE and waits for UNSUBACK.
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut, true, _ -> { })) {
                Packet.Unsubscribe repeated = assertInstanceOf(
                        Packet.Unsubscribe.class,
                        peer.receive()
                );
                assertThat(repeated.filters()).containsExactly(new TopicFilter("data"));
                peer.send(new Packet.UnsubAck(repeated.packetId(), List.of(), EMPTY));
                pending.removed().get(5, SECONDS);
                assertThat(pending.subscription().receive(Duration.ZERO))
                        .isEqualTo(new ReceiveResult.Ended());
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void offline_removal_in_a_clean_session_needs_no_broker_exchange(boolean previouslyConnected)
            throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = ScriptedBroker.client(server)) {
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("data"), Qos.AT_LEAST_ONCE)
            );
            if (previouslyConnected) {
                try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                    acknowledge(peer, assertSubscription(peer, "data"), ReasonCode.GRANTED_QOS_1);
                    subscription.established().get(5, SECONDS);
                    disconnect(peer, sut);
                }
            }

            // Act
            subscription.unsubscribe().get(5, SECONDS);

            // Assert
            assertThat(subscription.receive(Duration.ZERO))
                    .isEqualTo(new ReceiveResult.Ended());
            assertThat(failureOf(subscription.established()))
                    .isInstanceOf(IllegalStateException.class);
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void a_resumed_subscription_reports_its_last_granted_qos_without_another_subscribe()
            throws Exception {
        // Arrange: the broker granted less than the originally requested QoS.
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen(); MqttClient sut = persistentClient(server, ended)) {
            Subscription subscription;
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                subscription = sut.subscribe(
                        Subscribe.of(new TopicFilter("data"), Qos.AT_LEAST_ONCE)
                );
                acknowledge(peer, assertSubscription(peer, "data"), ReasonCode.SUCCESS);
                assertThat(subscription.established().get(5, SECONDS)).isEqualTo(Qos.AT_MOST_ONCE);
                disconnect(peer, sut);
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();

            // Act
            CompletableFuture<Qos> established = subscription.established();
            assertThat(established).isNotDone();

            // Assert: CONNACK resumes the old grant, so no SUBACK is required or expected.
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut, true, _ -> { })) {
                assertThat(established.get(5, SECONDS)).isEqualTo(Qos.AT_MOST_ONCE);
                assertThat(subscription.established().get(5, SECONDS)).isEqualTo(Qos.AT_MOST_ONCE);
                assertThat(subscription.grantedQos()).isEqualTo(Qos.AT_MOST_ONCE);
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void a_broker_refusal_ends_the_returned_handle_and_its_establishment_waiters()
            throws Exception {
        // Arrange
        try (ServerSocket server = listen();
                MqttClient sut = ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("data"), Qos.AT_LEAST_ONCE)
            );
            CompletableFuture<Qos> established = subscription.established();

            // Act
            acknowledge(peer, assertSubscription(peer, "data"), ReasonCode.UNSPECIFIED_ERROR);

            // Assert
            Throwable failure = failureOf(established);
            assertThat(failure).isInstanceOf(MqttSubscribeException.class);
            assertThat(failureOf(subscription.established())).isSameAs(failure);
            assertThatThrownBy(() -> subscription.receive(Duration.ZERO))
                    .isInstanceOfSatisfying(
                            MqttSubscribeException.class,
                            cause -> assertThat(cause.reasonCode())
                                    .isEqualTo(ReasonCode.UNSPECIFIED_ERROR)
                    );
            peer.roundTripBarrier(sut);
        }
    }

    /// Establishes a persistent pull subscription, then ends its connection with DISCONNECT.
    private static Subscription subscribeThenDisconnect(
            ServerSocket server,
            MqttClient sut,
            LinkedBlockingQueue<Disconnection> ended
    ) throws Exception {
        Subscription subscription;
        try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            subscription = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 4);
            disconnect(peer, sut);
        }
        assertThat(ended.poll(5, SECONDS)).isNotNull();
        return subscription;
    }

    /// Sends UNSUBSCRIBE for a persistent pull subscription, then loses the connection by EOF
    /// before UNSUBACK, which does not cancel the removal.
    private static PendingRemoval removeThenLoseUnsuback(
            ServerSocket server,
            MqttClient sut,
            LinkedBlockingQueue<Disconnection> ended
    ) throws Exception {
        PendingRemoval pending;
        try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            Subscription subscription = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 4);
            pending = new PendingRemoval(subscription, subscription.unsubscribe());
            assertThat(peer.receive()).isInstanceOf(Packet.Unsubscribe.class);
        }
        assertThat(ended.poll(5, SECONDS)).isNotNull();
        assertThat(pending.removed()).isNotDone();
        return pending;
    }

    private static MqttClient persistentClient(
            ServerSocket server,
            LinkedBlockingQueue<Disconnection> ended
    ) {
        return MqttClient.builder()
                .server(endpoint(server))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .clientId("registration")
                .cleanStart(false)
                .sessionExpiry(Duration.ofMinutes(1))
                .connectionListener(ended::add)
                .build();
    }

    private static Packet.Subscribe assertSubscription(ScriptedBroker peer, String topic)
            throws Exception {
        Packet.Subscribe request = assertInstanceOf(Packet.Subscribe.class, peer.receive());
        assertThat(request.subscriptions()).singleElement().satisfies(
                item -> assertThat(item.filter()).isEqualTo(new TopicFilter(topic))
        );
        return request;
    }

    private static void acknowledge(
            ScriptedBroker peer,
            Packet.Subscribe request,
            ReasonCode reason
    ) throws Exception {
        peer.send(new Packet.SubAck(request.packetId(), List.of(reason), EMPTY));
    }

    private static void disconnect(ScriptedBroker peer, MqttClient sut) throws Exception {
        CompletableFuture<Void> disconnected = sut.disconnect();
        assertThat(peer.receive()).isInstanceOf(Packet.Disconnect.class);
        disconnected.get(5, SECONDS);
        peer.expectClosed();
    }

    private static MqttDelivery receive(Subscription subscription) throws Exception {
        ReceiveResult.Received received = assertInstanceOf(
                ReceiveResult.Received.class,
                subscription.receive(Duration.ofSeconds(5))
        );
        return received.delivery();
    }

    private record PendingRemoval(Subscription subscription, CompletableFuture<Void> removed) {
    }
}
