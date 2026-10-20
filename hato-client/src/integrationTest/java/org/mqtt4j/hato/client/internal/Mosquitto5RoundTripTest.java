package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/// The client's MQTT 5.0 connections to Mosquitto: publications at every QoS, a persistent
/// session that keeps messages while the client is away, and the limits that Mosquitto
/// announces.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
@Timeout(30)
class Mosquitto5RoundTripTest {

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

    @Test
    void the_connack_reports_the_limits_that_mosquitto_announces() throws Exception {
        try (MqttClient sut = client("mqtt5-limits", Duration.ZERO)) {
            // Act
            ConnAck ack = sut.connect().get(5, SECONDS);

            // Assert: recorded from the pinned Mosquitto 2.1.2 image, whose only announced limit
            // that ConnAck reports is a maximum packet size of 2,000,000 bytes.
            assertThat(ack).isEqualTo(
                    ConnAck.of(false, 1, Qos.EXACTLY_ONCE, true, true, true, 2_000_000)
            );
            sut.disconnect().get(5, SECONDS);
        }
    }

    @ParameterizedTest
    @EnumSource(Qos.class)
    void publications_round_trip_at_every_qos(Qos qos) throws Exception {
        var topic = new Topic("hato/mqtt5/" + qos.level());
        var received = new LinkedBlockingQueue<MqttMessage>();
        try (
                MqttClient subscriber = client("mqtt5-sub-" + qos.level(), Duration.ZERO);
                MqttClient publisher = client("mqtt5-pub-" + qos.level(), Duration.ZERO)
        ) {
            // Arrange
            subscriber.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            subscriber.subscribe(
                    Subscribe.of(new TopicFilter(topic.name()), Qos.EXACTLY_ONCE),
                    delivery -> received.add(delivery.message())
            ).established().get(5, SECONDS);
            var message = MqttMessage.of(topic, new byte[] {0, (byte) 0xFF, 42}, qos, false);

            // Act
            publisher.publish(message).get(5, SECONDS);

            // Assert
            assertThat(received.poll(5, SECONDS)).isEqualTo(message);
        }
    }

    @Test
    void a_publication_without_subscribers_succeeds() throws Exception {
        try (MqttClient sut = client("mqtt5-unmatched", Duration.ZERO)) {
            // Arrange
            sut.connect().get(5, SECONDS);

            // Act & Assert: Mosquitto answers No matching subscribers.
            assertThat(sut.publish(MqttMessage.of(
                    new Topic("hato/mqtt5/nobody"),
                    new byte[] {1},
                    Qos.AT_LEAST_ONCE,
                    false
            )).get(5, SECONDS)).isEqualTo(PublishResult.of());
        }
    }

    @Test
    void publications_beyond_the_receive_maximum_wait_instead_of_being_refused() throws Exception {
        var topic = new Topic("hato/mqtt5/receive-maximum");
        try (MqttClient sut = client("mqtt5-receive-maximum", Duration.ZERO)) {
            // Arrange: Mosquitto takes 20 publications at once and refuses more with Quota
            // exceeded. QoS 2 keeps that room until PUBCOMP, whereas Mosquitto frees it at once
            // for QoS 1, so only a QoS 2 burst runs into the limit.
            sut.connect().get(5, SECONDS);

            // Act: publish all that the client admits at once.
            List<CompletableFuture<PublishResult>> publications = IntStream.range(0, 64)
                    .mapToObj(i -> MqttMessage.of(
                            topic,
                            new byte[] {(byte) i},
                            Qos.EXACTLY_ONCE,
                            false
                    ))
                    .map(sut::publish)
                    .toList();

            // Assert
            for (CompletableFuture<PublishResult> publication : publications) {
                assertThat(publication.get(5, SECONDS)).isEqualTo(PublishResult.of());
            }
        }
    }

    @Test
    void a_persistent_session_keeps_messages_while_the_client_is_away() throws Exception {
        var topic = new Topic("hato/mqtt5/persistent");
        var received = new LinkedBlockingQueue<MqttMessage>();
        try (
                MqttClient subscriber = client("mqtt5-persistent", Duration.ofMinutes(1));
                MqttClient publisher = client("mqtt5-persistent-publisher", Duration.ZERO)
        ) {
            // Arrange: the first connection starts the session with Clean Start.
            assertThat(subscriber.connect().get(5, SECONDS).sessionPresent()).isFalse();
            subscriber.subscribe(
                    Subscribe.of(new TopicFilter(topic.name()), Qos.AT_LEAST_ONCE),
                    delivery -> received.add(delivery.message())
            ).established().get(5, SECONDS);
            subscriber.disconnect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            var message = MqttMessage.of(topic, new byte[] {7}, Qos.AT_LEAST_ONCE, false);
            publisher.publish(message).get(5, SECONDS);

            // Act
            ConnAck resumed = subscriber.connect().get(5, SECONDS);

            // Assert
            assertThat(resumed.sessionPresent()).isTrue();
            assertThat(received.poll(5, SECONDS)).isEqualTo(message);
        }
    }

    private static MqttClient client(String clientId, Duration sessionExpiry) {
        var endpoint = URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(Mosquitto.MQTT_PORT)
        );
        return Mqtt5Clients.client(endpoint, clientId, sessionExpiry);
    }
}
