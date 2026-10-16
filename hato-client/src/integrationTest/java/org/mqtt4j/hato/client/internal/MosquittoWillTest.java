package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.spi.Transport;
import org.mqtt4j.hato.testkit.FaultyTransport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;
import static org.mqtt4j.hato.client.internal.SessionPersistenceIntegrationTest.failureOf;

@EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
@SuppressWarnings("try") // Explicit close is the abnormal MQTT termination under test.
class MosquittoWillTest {

    private static final GenericContainer<?> broker = Mosquitto.container();

    static boolean dockerIsAvailableOrRequired() {
        return Mosquitto.availableOrRequired();
    }

    @BeforeAll
    static void start() {
        Mosquitto.start(broker);
    }

    @AfterAll
    static void stop() {
        broker.stop();
    }

    @ParameterizedTest
    @EnumSource(Qos.class)
    void closing_publishes_the_will_and_retains_it_for_later_subscribers(Qos qos) throws Exception {
        // Arrange
        String topic = uniqueTopic();
        MqttMessage will = MqttMessage.of(new Topic(topic), new byte[] {0, (byte) 0xFF}, qos, true);
        try (MqttClient subscriber = client();
                MqttClient later = client();
                MqttClient sut = MqttClient.builder()
                        .server(endpoint())
                        .version(MqttVersion.V3_1_1)
                        .will(will)
                        .build()) {
            subscriber.connect().get(5, SECONDS);
            Subscription subscription = subscribe(subscriber, topic);
            sut.connect().get(5, SECONDS);

            // Act
            sut.close();

            // Assert: a current subscriber receives the Will at its QoS with RETAIN cleared.
            assertThat(receive(subscription)).isEqualTo(MqttMessage.of(
                    will.topic(),
                    will.payload(),
                    qos,
                    false
            ));
            later.connect().get(5, SECONDS);
            Subscription lateSubscription = subscribe(later, topic);
            assertThat(receive(lateSubscription)).isEqualTo(will);
        }
    }

    @Test
    void a_network_failure_publishes_the_will() throws Exception {
        // Arrange
        String topic = uniqueTopic();
        MqttMessage will = MqttMessage.of(
                new Topic(topic),
                new byte[] {1},
                Qos.AT_LEAST_ONCE,
                false
        );
        var transport = new AtomicReference<FaultyTransport>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        var clock = new ManualTimerScheduler();
        try (MqttClient subscriber = client();
                MqttClient sut = MqttClient.builder()
                        .server(endpoint())
                        .version(MqttVersion.V3_1_1)
                        .will(will)
                        .connectionListener(ended::add)
                        .transportFactory(id -> {
                            var faulty = new FaultyTransport(Transport.tcp(id), clock);
                            transport.set(faulty);
                            return faulty;
                        })
                        .build()) {
            subscriber.connect().get(5, SECONDS);
            Subscription subscription = subscribe(subscriber, topic);
            sut.connect().get(5, SECONDS);

            // Act
            var cause = new IOException("network lost");
            requireNonNull(transport.get()).disconnect(cause);

            // Assert
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(cause, false));
            assertThat(receive(subscription)).isEqualTo(will);
        }
    }

    @Test
    void disconnecting_suppresses_the_will() throws Exception {
        // Arrange
        String topic = uniqueTopic();
        String clientId = clientId();
        try (MqttClient subscriber = client();
                MqttClient sut = MqttClient.builder()
                        .server(endpoint())
                        .version(MqttVersion.V3_1_1)
                        .clientId(clientId)
                        .will(message(topic, (byte) 1))
                        .build()) {
            subscriber.connect().get(5, SECONDS);
            Subscription subscription = subscribe(subscriber, topic);
            sut.connect().get(5, SECONDS);

            // Act
            sut.disconnect().get(5, SECONDS);

            // Assert
            assertNoWillAfterReplacement(clientId, topic, subscription);
        }
    }

    @Test
    void refusing_an_unknown_broker_session_suppresses_the_will() throws Exception {
        // Arrange: leave a broker session behind, then connect without its local state.
        String topic = uniqueTopic();
        String clientId = clientId();
        try (MqttClient previous = MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
                .clientId(clientId)
                .cleanStart(false)
                .sessionExpiry(Duration.ofMinutes(1))
                .build()) {
            previous.connect().get(5, SECONDS);
            previous.disconnect().get(5, SECONDS);
        }
        try (MqttClient subscriber = client();
                MqttClient sut = MqttClient.builder()
                        .server(endpoint())
                        .version(MqttVersion.V3_1_1)
                        .clientId(clientId)
                        .cleanStart(false)
                        .sessionExpiry(Duration.ofMinutes(1))
                        .will(message(topic, (byte) 1))
                        .build()) {
            subscriber.connect().get(5, SECONDS);
            Subscription subscription = subscribe(subscriber, topic);

            // Act & Assert: refusing Session Present must still send DISCONNECT to the broker.
            assertThat(failureOf(sut.connect())).isInstanceOf(MqttConnectException.class);
            assertNoWillAfterReplacement(clientId, topic, subscription);
        }
    }

    private static void assertNoWillAfterReplacement(
            String clientId,
            String topic,
            Subscription subscription
    ) throws Exception {
        // Reusing the id makes Mosquitto finish the previous connection before accepting this
        // one. Its publication follows any Will caused by that connection's termination.
        try (MqttClient replacement = MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
                .clientId(clientId)
                .build()) {
            replacement.connect().get(5, SECONDS);
            MqttMessage marker = message(topic, (byte) 2);
            replacement.publish(marker).get(5, SECONDS);
            assertThat(receive(subscription)).isEqualTo(marker);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            replacement.disconnect().get(5, SECONDS);
        }
    }

    private static MqttMessage message(String topic, byte value) {
        return MqttMessage.of(new Topic(topic), new byte[] {value}, Qos.AT_LEAST_ONCE, false);
    }

    private static MqttMessage receive(Subscription subscription) throws Exception {
        ReceiveResult.Received received = assertInstanceOf(
                ReceiveResult.Received.class,
                subscription.receive(Duration.ofSeconds(5))
        );
        return received.delivery().message();
    }

    private static Subscription subscribe(MqttClient subscriber, String topic) throws Exception {
        Subscription subscription = subscriber.subscribe(
                Subscribe.of(new TopicFilter(topic), Qos.EXACTLY_ONCE)
        );
        subscription.established().get(5, SECONDS);
        return subscription;
    }

    private static MqttClient client() {
        return MqttClient.builder().server(endpoint()).version(MqttVersion.V3_1_1).build();
    }

    private static String clientId() {
        return "hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19);
    }

    private static URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(Mosquitto.MQTT_PORT)
        );
    }
}
