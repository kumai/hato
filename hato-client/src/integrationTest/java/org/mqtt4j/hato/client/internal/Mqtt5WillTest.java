package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.testkit.FaultyTransport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The Will of MQTT 5.0 sessions, shared by Mosquitto, EMQX, and HiveMQ CE: its properties, its
/// delay, and how a reconnection or a new session for the same identifier affects it. The
/// embedded broker refuses a delayed Will, which most of these use, so it runs none of them;
/// hato-broker's integration tests check its Will.
@Timeout(60)
@SuppressWarnings("try") // Explicit close is the abnormal MQTT termination under test.
abstract class Mqtt5WillTest {

    // Brokers count the delay in whole seconds, so it must be long enough to tell a delayed Will
    // from one published at once.
    private static final Duration DELAY = Duration.ofSeconds(2);

    @Test
    void closing_without_disconnect_publishes_the_will_with_its_properties() throws Exception {
        String topic = uniqueTopic();
        MqttMessage will = willOn(topic);
        try (
                MqttClient subscriber = subscriber();
                MqttClient sut = builder(clientId(), Duration.ZERO).will(will).build()
        ) {
            // Arrange
            PullSubscription subscription = subscribe(subscriber, topic);
            sut.connect().get(5, SECONDS);

            // Act
            sut.close();

            // Assert
            assertThat(receive(subscription, Duration.ofSeconds(5))).isEqualTo(will);
        }
    }

    @Test
    void a_session_without_expiry_publishes_a_delayed_will_as_recorded_for_the_broker()
            throws Exception {
        String topic = uniqueTopic();
        try (
                MqttClient subscriber = subscriber();
                MqttClient sut = builder(clientId(), Duration.ZERO)
                        .will(willOn(topic))
                        .willDelay(Duration.ofMinutes(1))
                        .build()
        ) {
            // Arrange
            PullSubscription subscription = subscribe(subscriber, topic);
            sut.connect().get(5, SECONDS);

            // Act: closing without DISCONNECT ends the connection and a session without expiry.
            sut.close();

            // Assert: MQTT 5.0 publishes the Will when the delay passes or the session ends,
            // whichever comes first, but a broker can keep the session until the delay passes.
            ReceiveResult result = subscription.receive(Duration.ofSeconds(3));
            assertThat(result instanceof ReceiveResult.Received)
                    .isEqualTo(endsTheDelayWithTheSession());
        }
    }

    @Test
    void a_lasting_session_publishes_the_will_once_the_delay_has_passed() throws Exception {
        String topic = uniqueTopic();
        MqttMessage will = willOn(topic);
        try (
                MqttClient subscriber = subscriber();
                MqttClient sut = builder(clientId(), Duration.ofMinutes(1))
                        .will(will)
                        .willDelay(DELAY)
                        .build()
        ) {
            // Arrange
            PullSubscription subscription = subscribe(subscriber, topic);
            sut.connect().get(5, SECONDS);

            // Act
            sut.close();

            // Assert
            assertThat(subscription.receive(Duration.ofSeconds(1)))
                    .isEqualTo(new ReceiveResult.TimedOut());
            assertThat(receive(subscription, Duration.ofSeconds(10))).isEqualTo(will);
        }
    }

    @Test
    void a_reconnection_within_the_delay_publishes_no_will() throws Exception {
        String topic = uniqueTopic();
        var transport = new AtomicReference<FaultyTransport>();
        var connections = new LinkedBlockingQueue<ConnAck>();
        var clock = new ManualTimerScheduler();
        try (
                MqttClient subscriber = subscriber();
                MqttClient sut = builder(clientId(), Duration.ofMinutes(1))
                        .will(willOn(topic))
                        .willDelay(DELAY)
                        .reconnectPolicy(ReconnectPolicy.exponentialBackoff(
                                Duration.ofMillis(100),
                                Duration.ofMillis(100)
                        ))
                        .connectionListener(new ConnectionListener() {
                            @Override
                            public void onConnected(ConnAck connAck) {
                                connections.add(connAck);
                            }

                            @Override
                            public void onDisconnected(Disconnection disconnection) {
                            }
                        })
                        .transportFactory(id -> {
                            var faulty = new FaultyTransport(Transport.tcp(id), clock);
                            transport.set(faulty);
                            return faulty;
                        })
                        .build()
        ) {
            // Arrange
            PullSubscription subscription = subscribe(subscriber, topic);
            sut.connect().get(5, SECONDS);
            assertThat(connections.poll(5, SECONDS)).isNotNull();

            // Act: the network fails, and the client resumes its session within the delay.
            requireNonNull(transport.get()).disconnect(new IOException("network lost"));
            ConnAck resumed = connections.poll(5, SECONDS);

            // Assert: nothing arrives after the delay has passed.
            assertThat(resumed).isNotNull();
            assertThat(resumed.sessionPresent()).isTrue();
            assertThat(subscription.receive(DELAY.plusSeconds(1)))
                    .isEqualTo(new ReceiveResult.TimedOut());
            sut.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void a_new_session_within_the_delay_treats_the_old_will_as_recorded_for_the_broker()
            throws Exception {
        String topic = uniqueTopic();
        String clientId = clientId();
        MqttMessage will = willOn(topic);
        try (
                MqttClient subscriber = subscriber();
                MqttClient previous = builder(clientId, Duration.ofMinutes(1))
                        .will(will)
                        .willDelay(Duration.ofMinutes(1))
                        .build();
                MqttClient sut = builder(clientId, Duration.ofMinutes(1)).build()
        ) {
            // Arrange: the previous client object, as before an application restart.
            PullSubscription subscription = subscribe(subscriber, topic);
            previous.connect().get(5, SECONDS);
            previous.close();

            // Act: a new client object has no local state, so it starts a new session.
            ConnAck started = sut.connect().get(5, SECONDS);

            // Assert: MQTT 5.0 can be read as publishing the old session's Will at once, since the
            // session ends, or as discarding it, since a connection opened within the delay.
            assertThat(started.sessionPresent()).isFalse();
            ReceiveResult result = subscription.receive(Duration.ofSeconds(3));
            assertThat(result instanceof ReceiveResult.Received)
                    .isEqualTo(publishesTheWillOfAReplacedSession());
            sut.disconnect().get(5, SECONDS);
        }
    }

    /// Whether the broker publishes a delayed Will as the connection of a session without expiry
    /// ends, rather than when the delay has passed, as recorded for the pinned image.
    abstract boolean endsTheDelayWithTheSession();

    /// Whether the broker publishes the Will of a session at once when a new client with the same
    /// identifier starts a new session within the Will's delay, rather than discarding it, as
    /// recorded for the pinned image.
    abstract boolean publishesTheWillOfAReplacedSession();

    abstract URI endpoint();

    private MqttClientBuilder builder(String clientId, Duration sessionExpiry) {
        return MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V5)
                .clientId(clientId)
                .sessionExpiry(sessionExpiry)
                .reconnectPolicy(ReconnectPolicy.disabled());
    }

    private MqttClient subscriber() throws Exception {
        MqttClient subscriber = builder(clientId(), Duration.ZERO).build();
        subscriber.connect().get(5, SECONDS);
        return subscriber;
    }

    /// A QoS 1 Will on `topic` with message properties.
    private static MqttMessage willOn(String topic) {
        return MqttMessage.of(
                new Topic(topic),
                "offline".getBytes(StandardCharsets.UTF_8),
                Qos.AT_LEAST_ONCE,
                false
        )
                .withPayloadUtf8(true)
                .withContentType("text/plain")
                .withUserProperties(List.of(new UserProperty("reason", "connection lost")));
    }

    private static PullSubscription subscribe(MqttClient subscriber, String topic)
            throws Exception {
        PullSubscription subscription = subscriber.subscribe(
                Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE)
        );
        subscription.established().get(5, SECONDS);
        return subscription;
    }

    private static MqttMessage receive(PullSubscription subscription, Duration timeout)
            throws Exception {
        ReceiveResult.Received received = assertInstanceOf(
                ReceiveResult.Received.class,
                subscription.receive(timeout)
        );
        return received.delivery().message();
    }

    private static String clientId() {
        return "hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19);
    }
}
