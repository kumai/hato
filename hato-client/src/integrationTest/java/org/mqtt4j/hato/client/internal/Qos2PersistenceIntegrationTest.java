package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.testkit.FaultyTransport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;
import static org.mqtt4j.hato.client.internal.SessionPersistenceIntegrationTest.failureOf;

@Timeout(30)
class Qos2PersistenceIntegrationTest {

    private static final Duration DELAY = Duration.ofSeconds(30);

    enum Cut {
        BEFORE_PUBLISH, BEFORE_PUBREC, BEFORE_PUBREL, BEFORE_PUBCOMP
    }

    @ParameterizedTest
    @EnumSource(Cut.class)
    void the_embedded_broker_completes_a_continued_exchange_once(Cut cut) throws Exception {
        try (MqttBroker broker = MqttBroker.start()) {
            exchangeAcrossDisconnection(URI.create("mqtt://localhost:" + broker.port()), cut, true);
        }
    }

    @ParameterizedTest
    @EnumSource(Cut.class)
    void a_missing_embedded_session_discards_the_exchange(Cut cut) throws Exception {
        try (MqttBroker broker = MqttBroker.start()) {
            exchangeAcrossDisconnection(
                    URI.create("mqtt://localhost:" + broker.port()),
                    cut,
                    false
            );
        }
    }

    @ParameterizedTest
    @EnumSource(Cut.class)
    @Timeout(60)
    @EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
    void mosquitto_completes_a_continued_exchange_once(Cut cut) throws Exception {
        try (GenericContainer<?> broker = Mosquitto.container()) {
            Docker.start(broker);
            exchangeAcrossDisconnection(
                    URI.create(
                            "mqtt://" + broker.getHost() + ":"
                                    + broker.getMappedPort(Mosquitto.MQTT_PORT)
                    ),
                    cut,
                    true
            );
        }
    }

    @ParameterizedTest
    @EnumSource(Cut.class)
    void the_embedded_broker_completes_a_held_exchange_after_automatic_reconnection(Cut cut)
            throws Exception {
        try (MqttBroker broker = MqttBroker.start()) {
            heldExchangeAcrossReconnection(
                    URI.create("mqtt://localhost:" + broker.port()),
                    cut,
                    true
            );
        }
    }

    @ParameterizedTest
    @EnumSource(Cut.class)
    void a_missing_embedded_session_fails_a_held_exchange_after_automatic_reconnection(Cut cut)
            throws Exception {
        try (MqttBroker broker = MqttBroker.start()) {
            heldExchangeAcrossReconnection(
                    URI.create("mqtt://localhost:" + broker.port()),
                    cut,
                    false
            );
        }
    }

    @ParameterizedTest
    @EnumSource(Cut.class)
    @Timeout(60)
    @EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
    void mosquitto_completes_a_held_exchange_after_automatic_reconnection(Cut cut)
            throws Exception {
        try (GenericContainer<?> broker = Mosquitto.container()) {
            Docker.start(broker);
            heldExchangeAcrossReconnection(
                    URI.create(
                            "mqtt://" + broker.getHost() + ":"
                                    + broker.getMappedPort(Mosquitto.MQTT_PORT)
                    ),
                    cut,
                    true
            );
        }
    }

    @Test
    @Timeout(60)
    @EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
    void mosquitto_drops_a_clean_sessions_publication_when_pubrel_never_arrives() throws Exception {
        // Arrange: Mosquitto 2.1.2 was observed to forward QoS 2 only after PUBREL.
        var scheduler = new ManualTimerScheduler();
        var transports = new LinkedBlockingQueue<FaultyTransport>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        String topic = uniqueTopic();
        try (GenericContainer<?> broker = Mosquitto.container()) {
            Docker.start(broker);
            var endpoint = URI.create(
                    "mqtt://" + broker.getHost() + ":"
                            + broker.getMappedPort(Mosquitto.MQTT_PORT)
            );
            try (MqttClient subscriber = client(endpoint);
                    MqttClient sut = MqttClient.builder()
                            .server(endpoint)
                            .version(MqttVersion.V3_1_1)
                            .reconnectPolicy(ReconnectPolicy.disabled())
                            .connectionListener(ended::add)
                            .transportFactory(id -> {
                                var transport = new FaultyTransport(Transport.tcp(id), scheduler);
                                transports.add(transport);
                                return transport;
                            })
                            .build()) {
                subscriber.connect().get(5, SECONDS);
                Subscription subscription = subscriber.subscribe(
                        Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE)
                );
                subscription.established().get(5, SECONDS);
                sut.connect().get(5, SECONDS);
                FaultyTransport transport = requireNonNull(transports.poll(5, SECONDS));
                CompletableFuture<PublishResult> publication = publishUntil(
                        sut,
                        transport,
                        scheduler,
                        topic,
                        Cut.BEFORE_PUBREL
                );

                // Act
                transport.disconnect(new IOException("cut before PUBREL"));
                assertThat(ended.poll(5, SECONDS)).isNotNull();
                Throwable failure = failureOf(publication);
                assertThat(failure).isInstanceOf(MqttConnectionLostException.class);
                assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 2));
                sut.publish(message(topic, (byte) 2)).get(5, SECONDS);

                // Assert: the same publisher, topic and QoS put the marker after any old delivery.
                assertPayload(subscription, (byte) 2);
                assertThat(subscription.receive(Duration.ZERO))
                        .isEqualTo(new ReceiveResult.TimedOut());
                assertThat(failureOf(publication)).isSameAs(failure);
            }
        }
    }

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    private static void exchangeAcrossDisconnection(
            URI endpoint,
            Cut cut,
            boolean sessionPresent
    ) throws Exception {
        // Arrange: only the fault injector uses virtual time; the client keeps its own clock.
        String clientId = "hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19);
        String topic = uniqueTopic();
        var scheduler = new ManualTimerScheduler();
        var transports = new LinkedBlockingQueue<FaultyTransport>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient subscriber = client(endpoint);
                MqttClient sut = MqttClient.builder()
                        .server(endpoint)
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .clientId(clientId)
                        .sessionExpiry(Duration.ofSeconds(30))
                        .connectionListener(ended::add)
                        .transportFactory(id -> {
                            var transport = new FaultyTransport(Transport.tcp(id), scheduler);
                            transports.add(transport);
                            return transport;
                        })
                        .build()) {
            subscriber.connect().get(5, SECONDS);
            Subscription subscription = subscriber.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE)
            );
            subscription.established().get(5, SECONDS);
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            FaultyTransport transport = requireNonNull(transports.poll(5, SECONDS));
            CompletableFuture<PublishResult> publication = publishUntil(
                    sut,
                    transport,
                    scheduler,
                    topic,
                    cut
            );

            // Act: the first connection fails even if the retained exchange later completes.
            var cause = new IOException("cut " + cut);
            transport.disconnect(cause);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(cause, false, 1));
            Throwable failure = failureOf(publication);
            assertThat(failure).isInstanceOf(MqttConnectionLostException.class).hasCause(cause);
            assertThat(scheduler.pendingTaskCount()).isZero();
            if (!sessionPresent) {
                try (MqttClient replacement = MqttClient.builder()
                        .server(endpoint)
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .clientId(clientId)
                        .build()) {
                    replacement.connect().get(5, SECONDS);
                    replacement.disconnect().get(5, SECONDS);
                }
            }
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(sessionPresent, 2));
            sut.publish(message(topic, (byte) 2)).get(5, SECONDS);

            // Assert: the embedded broker forwards on PUBLISH; both brokers resume exactly once.
            // Mosquitto 2.1.2 replay-before-marker order was observed before relying on it here.
            if (sessionPresent || cut != Cut.BEFORE_PUBLISH) {
                assertPayload(subscription, (byte) 1);
            }
            assertPayload(subscription, (byte) 2);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            assertThat(failureOf(publication)).isSameAs(failure);
            sut.disconnect().get(5, SECONDS);
        }
    }

    /// Like [#exchangeAcrossDisconnection], but the client reconnects by itself, which holds the
    /// publication's future for the next connection to decide.
    private static void heldExchangeAcrossReconnection(
            URI endpoint,
            Cut cut,
            boolean sessionPresent
    ) throws Exception {
        // Arrange: the client reconnects on its own manual clock, apart from fault injection.
        String clientId = "hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19);
        String topic = uniqueTopic();
        var scheduler = new ManualTimerScheduler();
        var clock = new ManualTimerScheduler();
        var transports = new LinkedBlockingQueue<FaultyTransport>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient subscriber = client(endpoint);
                MqttClient sut = MqttClient.builder()
                        .server(endpoint)
                        .version(MqttVersion.V3_1_1)
                        .clientId(clientId)
                        .sessionExpiry(Duration.ofSeconds(30))
                        .timerScheduler(clock)
                        .reconnectPolicy(ReconnectPolicy.exponentialBackoff(
                                Duration.ofSeconds(1),
                                Duration.ofSeconds(1)
                        ))
                        .connectionListener(ended::add)
                        .transportFactory(id -> {
                            var transport = new FaultyTransport(Transport.tcp(id), scheduler);
                            transports.add(transport);
                            return transport;
                        })
                        .build()) {
            subscriber.connect().get(5, SECONDS);
            Subscription subscription = subscriber.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE)
            );
            subscription.established().get(5, SECONDS);
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            FaultyTransport transport = requireNonNull(transports.poll(5, SECONDS));
            CompletableFuture<PublishResult> publication = publishUntil(
                    sut,
                    transport,
                    scheduler,
                    topic,
                    cut
            );

            // Act: the cut schedules a reconnection, and the next connection decides the future.
            var cause = new IOException("cut " + cut);
            transport.disconnect(cause);
            Disconnection end = requireNonNull(ended.poll(5, SECONDS));
            assertThat(end.cause()).containsSame(cause);
            assertThat(end.reconnectDelay()).isPresent();
            assertThat(publication).isNotDone();
            if (!sessionPresent) {
                try (MqttClient replacement = MqttClient.builder()
                        .server(endpoint)
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .clientId(clientId)
                        .build()) {
                    replacement.connect().get(5, SECONDS);
                    replacement.disconnect().get(5, SECONDS);
                }
            }
            clock.advance(Duration.ofSeconds(1));
            assertThat(sut.whenConnected().get(5, SECONDS))
                    .isEqualTo(ConnAck.of(sessionPresent, 2));
            sut.publish(message(topic, (byte) 2)).get(5, SECONDS);

            // Assert: a continued exchange succeeds once at PUBCOMP; a lost one stays unknown.
            if (sessionPresent || cut != Cut.BEFORE_PUBLISH) {
                assertPayload(subscription, (byte) 1);
            }
            assertPayload(subscription, (byte) 2);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            if (sessionPresent) {
                assertThat(publication.get(5, SECONDS)).isEqualTo(PublishResult.of());
            } else {
                assertThat(failureOf(publication)).isInstanceOf(MqttConnectionLostException.class);
            }
            sut.disconnect().get(5, SECONDS);
        }
    }

    private static CompletableFuture<PublishResult> publishUntil(
            MqttClient sut,
            FaultyTransport transport,
            ManualTimerScheduler scheduler,
            String topic,
            Cut cut
    ) {
        if (cut == Cut.BEFORE_PUBLISH) {
            transport.sendDelay(DELAY);
        } else {
            transport.receiveDelay(DELAY);
        }
        CompletableFuture<PublishResult> publication = sut.publish(message(topic, (byte) 1));
        await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));
        switch (cut) {
            case BEFORE_PUBLISH, BEFORE_PUBREC -> { }
            case BEFORE_PUBREL, BEFORE_PUBCOMP -> {
                if (cut == Cut.BEFORE_PUBREL) {
                    transport.sendDelay(DELAY);
                }
                // Release PUBREC. The next timer holds PUBREL or the returned PUBCOMP.
                scheduler.advance(DELAY);
                await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));
            }
        }
        assertThat(publication).isNotDone();
        return publication;
    }

    private static MqttClient client(URI endpoint) {
        return MqttClient.builder()
                .server(endpoint)
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }

    private static MqttMessage message(String topic, byte value) {
        return MqttMessage.of(new Topic(topic), new byte[] {value}, Qos.EXACTLY_ONCE, false);
    }

    private static void assertPayload(Subscription subscription, byte expected) throws Exception {
        ReceiveResult.Received received = assertInstanceOf(
                ReceiveResult.Received.class,
                subscription.receive(Duration.ofSeconds(5))
        );
        assertThat(received.delivery().message().payload()).containsExactly(expected);
        assertThat(received.delivery().message().qos()).isEqualTo(Qos.AT_LEAST_ONCE);
    }
}
