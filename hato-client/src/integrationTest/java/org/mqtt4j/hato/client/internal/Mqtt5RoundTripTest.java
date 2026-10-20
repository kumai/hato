package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/// The client's MQTT 5.0 connections, shared by Mosquitto and the embedded broker: publications
/// at every QoS, a persistent session that keeps messages while the client is away, and a
/// session that another client takes over.
@Timeout(30)
abstract class Mqtt5RoundTripTest {

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

            // Act & Assert: Mosquitto answers No matching subscribers, and the embedded
            // broker answers success.
            assertThat(sut.publish(MqttMessage.of(
                    new Topic("hato/mqtt5/nobody"),
                    new byte[] {1},
                    Qos.AT_LEAST_ONCE,
                    false
            )).get(5, SECONDS)).isEqualTo(PublishResult.of());
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

    @Test
    void a_client_whose_session_another_client_takes_over_stays_disconnected() throws Exception {
        var endings = new LinkedBlockingQueue<Disconnection>();
        try (
                MqttClient first = MqttClient.builder()
                        .server(endpoint())
                        .version(MqttVersion.V5)
                        .clientId("mqtt5-taken-over")
                        .connectionListener(endings::add)
                        .build();
                MqttClient second = client("mqtt5-taken-over", Duration.ZERO)
        ) {
            // Arrange: the first client reconnects after other endings of its connection.
            first.connect().get(5, SECONDS);

            // Act
            second.connect().get(5, SECONDS);

            // Assert: the broker ends the first connection, which leaves the session to the
            // second.
            Disconnection end = endings.poll(5, SECONDS);
            assertThat(end).isNotNull();
            assertThat(end.reconnectDelay()).isEmpty();
            assertThat(end.cause()).get().isInstanceOfSatisfying(
                    MqttBrokerDisconnectException.class,
                    ending -> assertThat(ending.reasonCode())
                            .isEqualTo(ReasonCode.SESSION_TAKEN_OVER)
            );
        }
    }

    MqttClient client(String clientId, Duration sessionExpiry) {
        return MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V5)
                .clientId(clientId)
                .sessionExpiry(sessionExpiry)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }

    abstract URI endpoint();
}
