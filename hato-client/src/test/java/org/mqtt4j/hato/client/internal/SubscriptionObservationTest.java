package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class SubscriptionObservationTest {

    private static final Subscribe REQUEST = Subscribe.of(
            new TopicFilter("readings"),
            Qos.AT_LEAST_ONCE
    );

    private final LinkedBlockingQueue<ScriptTransport> transports = new LinkedBlockingQueue<>();

    @Test
    void a_registration_burst_can_fill_all_subscription_slots_before_the_actor_catches_up()
            throws Exception {
        // Arrange: hold connection startup so no registration can leave the mailbox.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = builder(() -> {
            entered.countDown();
            waitFor(release);
        }).maxSubscriptions(256).build()) {
            CompletableFuture<ConnAck> connected = sut.connect();
            assertThat(entered.await(5, SECONDS)).isTrue();
            var subscriptions = new ArrayList<Subscription>();

            // Act
            for (int i = 0; i < 256; i++) {
                subscriptions.add(sut.subscribe(Subscribe.of(
                        new TopicFilter("burst/" + i),
                        Qos.AT_LEAST_ONCE
                )));
            }

            // Assert: all 256 handles await acceptance, while further queueing remains bounded.
            for (Subscription subscription : subscriptions) {
                assertThat(subscription.established()).isNotDone();
            }
            Subscription overflow = sut.subscribe(REQUEST);
            assertThatThrownBy(() -> overflow.established().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class);
            release.countDown();
            ScriptTransport transport = requireNonNull(transports.poll(5, SECONDS));
            accept(transport, connected);
            for (Subscription subscription : subscriptions) {
                Packet.Subscribe packet = assertInstanceOf(Packet.Subscribe.class, next(transport));
                assertThat(packet.subscriptions()).singleElement().satisfies(request ->
                        assertThat(request.filter()).isEqualTo(subscription.filter()));
                transport.receive(new Packet.SubAck(
                        packet.packetId(),
                        List.of(ReasonCode.GRANTED_QOS_1),
                        EMPTY
                ));
                assertThat(subscription.established().get(5, SECONDS))
                        .isEqualTo(Qos.AT_LEAST_ONCE);
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_pull_handle_can_receive_and_close_before_its_registration_reaches_the_actor()
            throws Exception {
        // Arrange: hold the actor inside start to expose the queued registration deterministically.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client(() -> {
            entered.countDown();
            waitFor(release);
        })) {
            CompletableFuture<ConnAck> connected = sut.connect();
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act & Assert
            PullSubscription subscription = sut.subscribe(REQUEST);
            assertThat(subscription.receive(Duration.ZERO))
                    .isInstanceOf(ReceiveResult.TimedOut.class);
            assertThat(subscription.grantedQos()).isEmpty();
            subscription.close();
            assertThat(subscription.receive(Duration.ZERO)).isInstanceOf(ReceiveResult.Ended.class);
            assertThatThrownBy(() -> subscription.established().get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            release.countDown();
            ScriptTransport transport = requireNonNull(transports.poll(5, SECONDS));
            accept(transport, connected);
            CompletableFuture<Void> disconnected = sut.disconnect();
            assertThat(next(transport)).isInstanceOf(Packet.Disconnect.class);
            disconnected.get(5, SECONDS);
            assertThat(transport.sent).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void registration_rejection_reaches_both_observers_and_immediate_pull_receivers()
            throws Exception {
        // Arrange
        try (MqttClient sut = client(() -> { })) {
            sut.subscribe(REQUEST);

            // Act
            PullSubscription rejected = sut.subscribe(REQUEST);

            // Assert
            assertThatThrownBy(() -> rejected.established().get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> rejected.receive(Duration.ZERO))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already exists");
        }
    }

    @Test
    void establishment_observations_are_independent_and_remain_bounded_through_continuations()
            throws Exception {
        // Arrange
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient client = client(() -> { })) {
            var sut = (ClientActor) client;
            Subscription subscription = sut.subscribe(REQUEST);
            var observers = new ArrayList<CompletableFuture<Qos>>();
            for (int i = 0; i < 1024; i++) {
                observers.add(subscription.established());
            }
            CompletableFuture<Qos> overwritten = observers.getFirst();
            overwritten.complete(Qos.EXACTLY_ONCE);
            CompletableFuture<Void> continuation = observers.get(1).thenRun(() -> {
                entered.countDown();
                waitFor(release);
            });

            // Act & Assert: caller completion neither grants QoS nor frees Hato's pending slot.
            assertThatThrownBy(() -> subscription.established().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class);
            assertThat(subscription.grantedQos()).isEmpty();
            assertThat(sut.usage().operations()).isZero();
            CompletableFuture<ConnAck> connected = sut.connect();
            ScriptTransport transport = requireNonNull(transports.poll(5, SECONDS));
            accept(transport, connected);
            Packet.Subscribe packet = assertInstanceOf(Packet.Subscribe.class, next(transport));
            transport.receive(new Packet.SubAck(
                    packet.packetId(),
                    List.of(ReasonCode.GRANTED_QOS_1),
                    EMPTY
            ));
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(observers.get(2).get(5, SECONDS)).isEqualTo(Qos.AT_LEAST_ONCE);
            assertThat(overwritten.join()).isEqualTo(Qos.EXACTLY_ONCE);
            assertThat(subscription.grantedQos()).contains(Qos.AT_LEAST_ONCE);
            await().untilAsserted(() -> assertThat(sut.usage().waiters()).isEqualTo(1));
            assertThat(continuation).isNotDone();
            release.countDown();
            continuation.get(5, SECONDS);
            await().untilAsserted(() -> assertThat(sut.usage().waiters()).isZero());
        } finally {
            release.countDown();
        }
    }

    @Test
    void establishment_observation_during_disconnect_waits_for_another_connection()
            throws Exception {
        // Arrange
        try (MqttClient sut = client(() -> { })) {
            Subscription subscription = sut.subscribe(REQUEST);
            CompletableFuture<ConnAck> connected = sut.connect();
            ScriptTransport transport = requireNonNull(transports.poll(5, SECONDS));
            accept(transport, connected);
            Packet.Subscribe packet = assertInstanceOf(Packet.Subscribe.class, next(transport));
            transport.receive(new Packet.SubAck(
                    packet.packetId(),
                    List.of(ReasonCode.GRANTED_QOS_1),
                    EMPTY
            ));
            subscription.established().get(5, SECONDS);
            transport.completeSends = false;

            // Act: DISCONNECT is queued, but its write has not finished yet.
            CompletableFuture<Void> disconnected = sut.disconnect();
            assertThat(next(transport)).isInstanceOf(Packet.Disconnect.class);
            CompletableFuture<Qos> next = subscription.established();

            // Assert
            assertThat(next).isNotDone();
            transport.completeSend();
            disconnected.get(5, SECONDS);
            assertThat(next).isNotDone();
            assertThat(subscription.grantedQos()).contains(Qos.AT_LEAST_ONCE);
            CompletableFuture<ConnAck> reconnected = sut.connect();
            ScriptTransport second = requireNonNull(transports.poll(5, SECONDS));
            accept(second, reconnected);
            Packet.Subscribe recreated = assertInstanceOf(Packet.Subscribe.class, next(second));
            second.receive(new Packet.SubAck(
                    recreated.packetId(),
                    List.of(ReasonCode.GRANTED_QOS_1),
                    EMPTY
            ));
            assertThat(next.get(5, SECONDS)).isEqualTo(Qos.AT_LEAST_ONCE);
        }
    }

    private MqttClient client(Runnable onStart) {
        return builder(onStart).build();
    }

    private MqttClientBuilder builder(Runnable onStart) {
        return MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .timerScheduler(new ManualTimerScheduler())
                .reconnectPolicy(ReconnectPolicy.disabled())
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    transport.onStart = onStart;
                    transports.add(transport);
                    return transport;
                });
    }

    private static void accept(ScriptTransport transport, CompletableFuture<ConnAck> connected)
            throws Exception {
        assertThat(next(transport)).isInstanceOf(Packet.Connect.class);
        transport.accept();
        connected.get(5, SECONDS);
    }

    private static Packet next(ScriptTransport transport) throws InterruptedException {
        return requireNonNull(transport.sent.poll(5, SECONDS));
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, SECONDS)).isTrue();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }
}
