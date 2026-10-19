package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.spi.Transport;
import org.mqtt4j.hato.testkit.FaultyTransport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.endpoint;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.listen;
import static org.mqtt4j.hato.client.internal.SessionPersistenceIntegrationTest.failureOf;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// Automatic reconnection against raw peers and real brokers. Clients that reconnect on manual
/// time wait until a test advances their clock past the one-second delay; fault injection uses
/// a separate clock.
@Timeout(30)
class AutomaticReconnectionIntegrationTest {

    private static final Duration DELAY = Duration.ofSeconds(1);

    private static final Duration HOLD = Duration.ofSeconds(30);

    private final String clientId = "hato" + UUID.randomUUID().toString().replace("-", "")
            .substring(0, 19);

    private final ManualTimerScheduler clock = new ManualTimerScheduler();

    private final ManualTimerScheduler faults = new ManualTimerScheduler();

    private final LinkedBlockingQueue<FaultyTransport> transports = new LinkedBlockingQueue<>();

    private final LinkedBlockingQueue<Disconnection> ends = new LinkedBlockingQueue<>();

    private final LinkedBlockingQueue<Disconnection> failures = new LinkedBlockingQueue<>();

    private final ConnectionListener listener = new ConnectionListener() {
        @Override
        public void onDisconnected(Disconnection disconnection) {
            ends.add(disconnection);
        }

        @Override
        public void onConnectFailed(Disconnection failure) {
            failures.add(failure);
        }
    };

    @Test
    void automatic_attempts_retry_missing_authorization_but_stop_at_an_identifier_rejection()
            throws Exception {
        // Arrange: the client reconnects on real time within ten milliseconds.
        try (ServerSocket server = listen();
                MqttClient sut = MqttClient.builder()
                        .server(endpoint(server))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.exponentialBackoff(
                                Duration.ofMillis(10),
                                Duration.ofMillis(10)
                        ))
                        .connectionListener(listener)
                        .build()) {
            ScriptedBroker.connect(server, sut).close();
            assertThat(requireNonNull(ends.poll(5, SECONDS)).reconnectDelay()).isPresent();

            // Act & Assert: the broker refuses authorization, and then the identifier.
            refuse(server, ReasonCode.NOT_AUTHORIZED);
            Disconnection unauthorized = requireNonNull(failures.poll(5, SECONDS));
            assertThat(unauthorized.connectionAttempt()).isEqualTo(2);
            assertThat(unauthorized.reconnectDelay()).isPresent();
            refuse(server, ReasonCode.CLIENT_IDENTIFIER_NOT_VALID);
            Disconnection rejected = requireNonNull(failures.poll(5, SECONDS));
            assertThat(rejected.connectionAttempt()).isEqualTo(3);
            assertThat(rejected.reconnectDelay()).isEmpty();
            assertThat(rejected.cause()).get()
                    .isInstanceOfSatisfying(MqttConnectException.class, refusal ->
                            assertThat(refusal.reasonCode())
                                    .contains(ReasonCode.CLIENT_IDENTIFIER_NOT_VALID));
            assertThat(sut.whenConnected()).isNotDone();
        }
    }

    @Test
    void a_missing_puback_ends_the_connection_despite_pingresp_and_the_session_resends_it()
            throws Exception {
        // Arrange: on manual time, keep-alive pings after one second, and PUBACK may take 1.5.
        try (ServerSocket server = listen();
                MqttClient sut = MqttClient.builder()
                        .server(endpoint(server))
                        .version(MqttVersion.V3_1_1)
                        .clientId(clientId)
                        .cleanStart(false)
                        .sessionExpiry(Duration.ofSeconds(30))
                        .keepAlive(Duration.ofSeconds(1))
                        .responseTimeout(Duration.ofMillis(1500))
                        .timerScheduler(clock)
                        .reconnectPolicy(ReconnectPolicy.exponentialBackoff(
                                Duration.ofMillis(10),
                                Duration.ofMillis(10)
                        ))
                        .connectionListener(listener)
                        .build()) {
            CompletableFuture<PublishResult> publication;
            Packet.Publish original;
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                publication = sut.publish(message("unanswered", Qos.AT_LEAST_ONCE));
                original = assertInstanceOf(Packet.Publish.class, peer.receive());
                // The write starts the response deadline beside the keep-alive and stable timers.
                await().until(() -> clock.pendingTaskCount() == 3);

                // Act: the broker answers the ping but never the publication.
                clock.advance(Duration.ofSeconds(1));
                assertThat(peer.receive()).isInstanceOf(Packet.PingReq.class);
                // The ping timer joins the others until PINGRESP cancels it.
                await().until(() -> clock.pendingTaskCount() == 4);
                peer.send(new Packet.PingResp());
                await().until(() -> clock.pendingTaskCount() == 3);
                clock.advance(Duration.ofMillis(500));

                // Assert: the deadline still counts from the PUBLISH write.
                Disconnection end = requireNonNull(ends.poll(5, SECONDS));
                assertThat(end.cause()).get().isInstanceOfSatisfying(
                        TimeoutException.class,
                        timeout -> assertThat(timeout).hasMessageContaining("PUBACK")
                );
                assertThat(end.reconnectDelay()).isPresent();
                peer.expectClosed();
            }
            clock.advance(Duration.ofMillis(10));
            try (ScriptedBroker next = ScriptedBroker.accept(server)) {
                Packet.Connect connect = assertInstanceOf(Packet.Connect.class, next.receive());
                assertThat(connect.cleanStart()).isFalse();
                next.send(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
                Packet.Publish replay = assertInstanceOf(Packet.Publish.class, next.receive());
                assertThat(replay.packetId()).isEqualTo(original.packetId());
                assertThat(replay.dup()).isTrue();
                assertThat(publication).isNotDone();
                next.send(new Packet.PubAck(replay.packetId(), ReasonCode.SUCCESS, EMPTY));
                assertThat(publication.get(5, SECONDS)).isEqualTo(PublishResult.of());
            }
        }
    }

    @Test
    void the_embedded_broker_continues_a_session_across_automatic_reconnection() throws Exception {
        try (MqttBroker broker = MqttBroker.start()) {
            continueAcrossAutomaticReconnection(brokerEndpoint(broker));
        }
    }

    @Test
    @Timeout(60)
    @EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
    void mosquitto_continues_a_session_across_automatic_reconnection() throws Exception {
        try (GenericContainer<?> broker = Mosquitto.container()) {
            Mosquitto.start(broker);
            continueAcrossAutomaticReconnection(mosquittoEndpoint(broker));
        }
    }

    @Test
    void a_restarted_embedded_broker_loses_the_session_and_the_registration_is_sent_again()
            throws Exception {
        // Arrange: a persistent client and an unfinished publication whose write is held.
        var address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        MqttBroker broker = MqttBroker.start(address);
        try (MqttClient sut = reconnectingClient(brokerEndpoint(broker))) {
            int port = broker.port();
            assertThat(sut.connect().get(5, SECONDS).sessionPresent()).isFalse();
            String topic = uniqueTopic();
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE)
            );
            subscription.established().get(5, SECONDS);
            requireNonNull(transports.poll(5, SECONDS)).sendDelay(HOLD);
            CompletableFuture<PublishResult> unfinished = sut.publish(
                    message("unfinished", Qos.AT_LEAST_ONCE)
            );

            // Act: the broker restarts on the same port without the session. Its close does not
            // wait for a blocked accept to release the listener, so binding again can fail first.
            broker.close();
            assertThat(requireNonNull(ends.poll(5, SECONDS)).reconnectDelay()).isPresent();
            var restarted = new InetSocketAddress(address.getAddress(), port);
            broker = await().atMost(Duration.ofSeconds(5))
                    .ignoreException(BindException.class)
                    .until(() -> MqttBroker.start(restarted), value -> value != null);
            clock.advance(DELAY);

            // Assert: the outcome stays unknown, and the subscription is created again.
            assertThat(sut.whenConnected().get(5, SECONDS))
                    .isEqualTo(ConnAck.of(false, 2));
            assertThat(failureOf(unfinished)).isInstanceOf(MqttConnectionLostException.class);
            subscription.established().get(5, SECONDS);
            try (MqttClient publisher = plainClient(brokerEndpoint(broker))) {
                publisher.connect().get(5, SECONDS);
                publisher.publish(message(topic, Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            }
            assertThat(subscription.receive(Duration.ofSeconds(5)))
                    .isInstanceOf(ReceiveResult.Received.class);
        } finally {
            broker.close();
        }
    }

    @Test
    void a_handler_failure_stops_reconnecting_until_the_application_connects_again()
            throws Exception {
        try (MqttBroker broker = MqttBroker.start()) {
            stopUntilConnectedAgain(brokerEndpoint(broker));
        }
    }

    @Test
    @Timeout(60)
    @EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
    void mosquitto_sees_no_reconnection_after_a_handler_failure_until_the_application_connects()
            throws Exception {
        try (GenericContainer<?> broker = Mosquitto.container()) {
            Mosquitto.start(broker);
            stopUntilConnectedAgain(mosquittoEndpoint(broker));
        }
    }

    @Test
    void disconnect_while_waiting_completes_at_once_and_no_attempt_follows() throws Exception {
        try (MqttBroker broker = MqttBroker.start();
                MqttClient sut = reconnectingClient(brokerEndpoint(broker))) {
            // Arrange: the connection is cut, and the client waits to reconnect.
            sut.connect().get(5, SECONDS);
            requireNonNull(transports.poll(5, SECONDS)).disconnect(new IOException("cut"));
            assertThat(requireNonNull(ends.poll(5, SECONDS)).reconnectDelay()).isPresent();

            // Act
            sut.disconnect().get(5, SECONDS);

            // Assert: the wait ends without a timer, so its delay passes without an attempt.
            assertThat(clock.pendingTaskCount()).isZero();
            clock.advance(DELAY);
            assertThat(transports).isEmpty();
            assertThat(sut.whenConnected()).isNotDone();
        }
    }

    @Test
    void a_blocked_listener_delays_neither_reconnection_nor_connect() throws Exception {
        // Arrange: the listener blocks in its first ending.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var blocking = new ConnectionListener() {
            @Override
            public void onDisconnected(Disconnection disconnection) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        try (MqttBroker broker = MqttBroker.start();
                MqttClient sut = reconnectingClient(brokerEndpoint(broker), blocking)) {
            sut.connect().get(5, SECONDS);
            requireNonNull(transports.poll(5, SECONDS)).disconnect(new IOException("cut"));
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act
            clock.advance(DELAY);

            // Assert
            assertThat(sut.whenConnected().get(5, SECONDS).connectionAttempt()).isEqualTo(2);
            assertThat(sut.connect().get(5, SECONDS).connectionAttempt()).isEqualTo(2);
        } finally {
            release.countDown();
        }
    }

    static boolean dockerIsAvailableOrRequired() {
        return Mosquitto.availableOrRequired();
    }

    /// A persistent client's QoS 1 and QoS 2 publications and its subscription continue after
    /// its connection is cut, without subscribing again.
    private void continueAcrossAutomaticReconnection(URI broker) throws Exception {
        String outbound = uniqueTopic();
        String inbound = uniqueTopic();
        try (MqttClient subscriber = plainClient(broker);
                MqttClient other = plainClient(broker);
                MqttClient sut = reconnectingClient(broker)) {
            // Arrange: a subscriber of the client's publications, and a subscription of its own.
            subscriber.connect().get(5, SECONDS);
            Subscription received = subscriber.subscribe(
                    Subscribe.of(new TopicFilter(outbound), Qos.EXACTLY_ONCE)
            );
            received.established().get(5, SECONDS);
            other.connect().get(5, SECONDS);
            assertThat(sut.connect().get(5, SECONDS).sessionPresent()).isFalse();
            Subscription inbox = sut.subscribe(
                    Subscribe.of(new TopicFilter(inbound), Qos.EXACTLY_ONCE)
            );
            inbox.established().get(5, SECONDS);
            FaultyTransport first = requireNonNull(transports.poll(5, SECONDS));
            first.receiveDelay(HOLD);
            CompletableFuture<PublishResult> atLeastOnce = sut.publish(
                    message(outbound, Qos.AT_LEAST_ONCE, (byte) 1)
            );
            CompletableFuture<PublishResult> exactlyOnce = sut.publish(
                    message(outbound, Qos.EXACTLY_ONCE, (byte) 2)
            );
            // The broker has processed the QoS 1 publication once its copy arrives.
            var seen = new ArrayList<Byte>();
            receiveUntil(received, seen, (byte) 1);

            // Act: the connection is cut with both acknowledgements outstanding, and another
            // client publishes to the client's subscription while it is disconnected.
            first.disconnect(new IOException("cut"));
            assertThat(requireNonNull(ends.poll(5, SECONDS)).reconnectDelay()).isPresent();
            assertThat(atLeastOnce).isNotDone();
            assertThat(exactlyOnce).isNotDone();
            other.publish(message(inbound, Qos.AT_LEAST_ONCE, (byte) 3)).get(5, SECONDS);
            other.publish(message(inbound, Qos.EXACTLY_ONCE, (byte) 4)).get(5, SECONDS);
            clock.advance(DELAY);

            // Assert: the held futures complete, and the subscription resumes unchanged.
            assertThat(sut.whenConnected().get(5, SECONDS)).isEqualTo(ConnAck.of(true, 2));
            assertThat(atLeastOnce.get(5, SECONDS)).isEqualTo(PublishResult.of());
            assertThat(exactlyOnce.get(5, SECONDS)).isEqualTo(PublishResult.of());
            sut.publish(message(outbound, Qos.EXACTLY_ONCE, (byte) 5)).get(5, SECONDS);
            // The marker follows every QoS 2 copy from the same publisher; QoS 1 may repeat.
            receiveUntil(received, seen, (byte) 5);
            assertThat(seen).filteredOn(payload -> payload == 2).hasSize(1);
            var delivered = new ArrayList<Byte>();
            while (delivered.size() < 2) {
                MqttMessage message = received(inbox.receive(Duration.ofSeconds(5))).message();
                delivered.add(message.payload()[0]);
            }
            assertThat(delivered).containsExactlyInAnyOrder((byte) 3, (byte) 4);
        }
    }

    /// A failing handler stops the client. An explicit connection receives the redelivery and
    /// restores automatic reconnection.
    private void stopUntilConnectedAgain(URI broker) throws Exception {
        try (MqttClient publisher = plainClient(broker);
                MqttClient sut = reconnectingClient(broker)) {
            // Arrange: the handler fails once; the redelivery after reconnection succeeds.
            String topic = uniqueTopic();
            var failure = new IllegalStateException("handler failed");
            var calls = new AtomicInteger();
            publisher.connect().get(5, SECONDS);
            sut.connect().get(5, SECONDS);
            requireNonNull(transports.poll(5, SECONDS));
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE),
                    _ -> {
                        if (calls.getAndIncrement() == 0) {
                            throw failure;
                        }
                    }
            );
            subscription.established().get(5, SECONDS);

            // Act
            publisher.publish(message(topic, Qos.AT_LEAST_ONCE)).get(5, SECONDS);

            // Assert: the stop announces no reconnection and schedules none.
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(clock.pendingTaskCount()).isZero();

            // Act & Assert: an explicit connection receives the redelivery and restores
            // automatic reconnection.
            assertThat(sut.connect().get(5, SECONDS).connectionAttempt()).isEqualTo(2);
            await().untilAsserted(() -> assertThat(calls.get()).isGreaterThanOrEqualTo(2));
            requireNonNull(transports.poll(5, SECONDS)).disconnect(new IOException("cut"));
            assertThat(requireNonNull(ends.poll(5, SECONDS)).reconnectDelay()).isPresent();
            clock.advance(DELAY);
            assertThat(sut.whenConnected().get(5, SECONDS).connectionAttempt()).isEqualTo(3);
        }
    }

    private void refuse(ServerSocket server, ReasonCode reason) throws Exception {
        try (ScriptedBroker peer = ScriptedBroker.accept(server)) {
            assertThat(peer.receive()).isInstanceOf(Packet.Connect.class);
            peer.send(new Packet.ConnAck(false, reason, EMPTY));
        }
    }

    private MqttClient reconnectingClient(URI broker) {
        return reconnectingClient(broker, listener);
    }

    private MqttClient reconnectingClient(URI broker, ConnectionListener notifications) {
        return MqttClient.builder()
                .server(broker)
                .version(MqttVersion.V3_1_1)
                .clientId(clientId)
                .cleanStart(false)
                .sessionExpiry(Duration.ofSeconds(30))
                .timerScheduler(clock)
                .reconnectPolicy(ReconnectPolicy.exponentialBackoff(DELAY, DELAY))
                .connectionListener(notifications)
                .transportFactory(id -> {
                    var transport = new FaultyTransport(Transport.tcp(id), faults);
                    transports.add(transport);
                    return transport;
                })
                .build();
    }

    private static MqttClient plainClient(URI broker) {
        return MqttClient.builder()
                .server(broker)
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }

    private static URI brokerEndpoint(MqttBroker broker) {
        return URI.create("mqtt://localhost:" + broker.port());
    }

    private static URI mosquittoEndpoint(GenericContainer<?> broker) {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(Mosquitto.MQTT_PORT)
        );
    }

    private static MqttMessage message(String topic, Qos qos) {
        return message(topic, qos, (byte) 0);
    }

    private static MqttMessage message(String topic, Qos qos, byte value) {
        return MqttMessage.of(new Topic(topic), new byte[] {value}, qos, false);
    }

    private static MqttDelivery received(ReceiveResult result) {
        return assertInstanceOf(ReceiveResult.Received.class, result).delivery();
    }

    /// Receives into `seen` until `payload` arrives.
    private static void receiveUntil(Subscription subscription, List<Byte> seen, byte payload)
            throws Exception {
        while (!seen.contains(payload)) {
            MqttMessage message = received(subscription.receive(Duration.ofSeconds(5))).message();
            seen.add(message.payload()[0]);
        }
    }
}
