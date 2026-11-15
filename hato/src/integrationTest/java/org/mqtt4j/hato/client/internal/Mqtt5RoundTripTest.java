package org.mqtt4j.hato.client.internal;

import java.io.EOFException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/// The client's MQTT 5.0 connections, shared by Mosquitto, EMQX, HiveMQ CE, NanoMQ, RabbitMQ,
/// and the embedded broker: publications at every QoS, message properties, repeated publications
/// on one topic, overlapping subscriptions, a persistent session that keeps messages while the
/// client is away, a session that a session file carries with its Subscription Identifiers to a
/// restarted client, and a session that another client takes over.
@Timeout(30)
abstract class Mqtt5RoundTripTest {

    @TempDir
    Path directory;

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
    void a_subscriber_receives_the_message_properties() throws Exception {
        var topic = new Topic("hato/mqtt5/properties");
        var lifetime = Duration.ofHours(1);
        var received = new LinkedBlockingQueue<MqttMessage>();
        try (
                MqttClient subscriber = client("mqtt5-properties-sub", Duration.ZERO);
                MqttClient publisher = client("mqtt5-properties-pub", Duration.ZERO)
        ) {
            // Arrange
            subscriber.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            subscriber.subscribe(
                    Subscribe.of(new TopicFilter(topic.name()), Qos.AT_LEAST_ONCE),
                    delivery -> received.add(delivery.message())
            ).established().get(5, SECONDS);
            MqttMessage message = withProperties(topic, lifetime);

            // Act
            publisher.publish(message).get(5, SECONDS);

            // Assert: the broker forwards the properties unaltered and the User Properties in
            // order, but sends what is left of the lifetime, which a passing second shortens.
            MqttMessage delivered = received.poll(5, SECONDS);
            assertThat(delivered).isNotNull();
            assertThat(delivered.messageExpiry()).hasValueSatisfying(
                    left -> assertThat(left).isBetween(lifetime.minusSeconds(1), lifetime)
            );
            assertThat(delivered.withMessageExpiry(lifetime)).isEqualTo(message);
        }
    }

    @Test
    void an_mqtt_3_1_1_subscriber_receives_the_message_without_its_properties() throws Exception {
        var topic = new Topic("hato/mqtt5/properties/to-3-1-1");
        var received = new LinkedBlockingQueue<MqttMessage>();
        try (
                MqttClient subscriber = MqttClient.builder()
                        .server(endpoint())
                        .version(MqttVersion.V3_1_1)
                        .clientId("mqtt5-properties-311-sub")
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .build();
                MqttClient publisher = client("mqtt5-properties-311-pub", Duration.ZERO)
        ) {
            // Arrange
            subscriber.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            subscriber.subscribe(
                    Subscribe.of(new TopicFilter(topic.name()), Qos.AT_LEAST_ONCE),
                    delivery -> received.add(delivery.message())
            ).established().get(5, SECONDS);
            MqttMessage message = withProperties(topic, Duration.ofHours(1));

            // Act
            publisher.publish(message).get(5, SECONDS);

            // Assert
            assertThat(received.poll(5, SECONDS)).isEqualTo(MqttMessage.of(
                    topic,
                    message.payload(),
                    Qos.AT_LEAST_ONCE,
                    false
            ));
        }
    }

    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_LEAST_ONCE", "EXACTLY_ONCE"})
    void a_publication_without_subscribers_reports_the_brokers_reason_code(Qos qos)
            throws Exception {
        try (MqttClient sut = client("mqtt5-unmatched-" + qos.level(), Duration.ZERO)) {
            // Arrange
            sut.connect().get(5, SECONDS);

            // Act
            PublishResult result = sut.publish(MqttMessage.of(
                    new Topic("hato/mqtt5/nobody"),
                    new byte[] {1},
                    qos,
                    false
            )).get(5, SECONDS);

            // Assert: a broker may answer No matching subscribers or success.
            assertThat(result).isEqualTo(PublishResult.of(unmatchedPublicationReason(qos)));
        }
    }

    @Test
    void repeated_publications_on_one_topic_arrive_with_their_topic() throws Exception {
        var topic = new Topic("hato/mqtt5/repeated");
        var received = new LinkedBlockingQueue<MqttMessage>();
        try (
                MqttClient subscriber = client("mqtt5-repeated-sub", Duration.ZERO);
                MqttClient publisher = client("mqtt5-repeated-pub", Duration.ZERO)
        ) {
            // Arrange: where the receiving side takes Topic Aliases, the publisher and the
            // broker send the topic with the first publication only.
            subscriber.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            subscriber.subscribe(
                    Subscribe.of(new TopicFilter(topic.name()), Qos.AT_LEAST_ONCE),
                    delivery -> received.add(delivery.message())
            ).established().get(5, SECONDS);
            List<MqttMessage> messages = List.of(
                    MqttMessage.of(topic, new byte[] {1}, Qos.AT_LEAST_ONCE, false),
                    MqttMessage.of(topic, new byte[] {2}, Qos.AT_LEAST_ONCE, false),
                    MqttMessage.of(topic, new byte[] {3}, Qos.AT_LEAST_ONCE, false)
            );

            // Act
            for (MqttMessage message : messages) {
                publisher.publish(message).get(5, SECONDS);
            }

            // Assert
            for (MqttMessage message : messages) {
                assertThat(received.poll(5, SECONDS)).isEqualTo(message);
            }
        }
    }

    @Test
    void overlapping_subscriptions_each_receive_one_copy_of_a_publication() throws Exception {
        var topic = new Topic("hato/mqtt5/overlap/a");
        var broad = new LinkedBlockingQueue<MqttMessage>();
        var narrow = new LinkedBlockingQueue<MqttMessage>();
        try (
                MqttClient sut = client("mqtt5-overlap", Duration.ZERO);
                MqttClient publisher = client("mqtt5-overlap-publisher", Duration.ZERO)
        ) {
            // Arrange
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter("hato/mqtt5/overlap/#"), Qos.AT_LEAST_ONCE),
                    delivery -> broad.add(delivery.message())
            ).established().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(topic.name()), Qos.AT_LEAST_ONCE),
                    delivery -> narrow.add(delivery.message())
            ).established().get(5, SECONDS);
            var first = MqttMessage.of(topic, new byte[] {1}, Qos.AT_LEAST_ONCE, false);
            var second = MqttMessage.of(topic, new byte[] {2}, Qos.AT_LEAST_ONCE, false);

            // Act
            publisher.publish(first).get(5, SECONDS);
            publisher.publish(second).get(5, SECONDS);

            // Assert: a second copy of the first publication would arrive before the second.
            assertThat(List.of(broad.poll(5, SECONDS), broad.poll(5, SECONDS)))
                    .containsExactly(first, second);
            assertThat(List.of(narrow.poll(5, SECONDS), narrow.poll(5, SECONDS)))
                    .containsExactly(first, second);
        }
    }

    @Test
    void a_persistent_session_keeps_messages_while_the_client_is_away_unless_the_broker_drops_it()
            throws Exception {
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

            // Assert: a broker that drops the session leaves the client to start a new one, and
            // the message is lost.
            if (keepsSessionsThatCleanStartBegan()) {
                assertThat(resumed.sessionPresent()).isTrue();
                assertThat(received.poll(5, SECONDS)).isEqualTo(message);
            } else {
                assertThat(resumed.sessionPresent()).isFalse();
                assertThat(received.poll(1, SECONDS)).isNull();
            }
        }
    }

    @Test
    void a_restarted_client_routes_stored_messages_by_the_identifiers_in_its_session_file()
            throws Exception {
        String unique = UUID.randomUUID().toString().replace("-", "");
        var first = new TopicFilter("hato/mqtt5/file/" + unique + "/first");
        var second = new TopicFilter("hato/mqtt5/file/" + unique + "/second");
        var toFirst = MqttMessage.of(
                new Topic(first.pattern()),
                new byte[] {1},
                Qos.AT_LEAST_ONCE,
                false
        );
        var toSecond = MqttMessage.of(
                new Topic(second.pattern()),
                new byte[] {2},
                Qos.AT_LEAST_ONCE,
                false
        );
        var receivedByFirst = new LinkedBlockingQueue<MqttMessage>();
        var receivedBySecond = new LinkedBlockingQueue<MqttMessage>();
        String clientId = "mqtt5-file-" + unique.substring(0, 12);
        try (MqttClient publisher = client("mqtt5-file-publisher", Duration.ZERO)) {
            // Arrange: an earlier client object subscribes to the first filter under identifier
            // 1 and to the second under 2, and saves the session when it disconnects. Then a
            // message arrives for each filter.
            publisher.connect().get(5, SECONDS);
            MqttClient earlier = sessionFileClient(clientId);
            try (earlier) {
                earlier.connect().get(5, SECONDS);
                earlier.subscribe(Subscribe.of(first, Qos.AT_LEAST_ONCE))
                        .established()
                        .get(5, SECONDS);
                earlier.subscribe(Subscribe.of(second, Qos.AT_LEAST_ONCE))
                        .established()
                        .get(5, SECONDS);
                earlier.disconnect().get(5, SECONDS);
            }
            // The earlier client object holds the file until its connection has ended.
            earlier.terminated().get(5, SECONDS);
            publisher.publish(toFirst).get(5, SECONDS);
            publisher.publish(toSecond).get(5, SECONDS);
            try (MqttClient sut = sessionFileClient(clientId)) {
                // The next client object registers the filters in the other order, so that new
                // identifiers would give the first one to the second filter.
                Subscription _ = sut.subscribe(
                        Subscribe.of(second, Qos.AT_LEAST_ONCE),
                        delivery -> receivedBySecond.add(delivery.message())
                );
                Subscription _ = sut.subscribe(
                        Subscribe.of(first, Qos.AT_LEAST_ONCE),
                        delivery -> receivedByFirst.add(delivery.message())
                );

                // Act
                ConnAck connAck = sut.connect().get(5, SECONDS);

                // Assert: each stored message reaches the subscription to its filter. A broker
                // that drops the session leaves the client to start a new one, and the messages
                // are lost.
                if (keepsSessionsThatCleanStartBegan()) {
                    assertThat(connAck.sessionPresent()).isTrue();
                    assertThat(receivedByFirst.poll(5, SECONDS)).isEqualTo(toFirst);
                    assertThat(receivedBySecond.poll(5, SECONDS)).isEqualTo(toSecond);
                } else {
                    assertThat(connAck.sessionPresent()).isFalse();
                    assertThat(receivedByFirst.poll(1, SECONDS)).isNull();
                    assertThat(receivedBySecond).isEmpty();
                }
                sut.disconnect().get(5, SECONDS);
            }
        }
    }

    @Test
    void a_taken_over_client_stays_disconnected_unless_the_broker_closes_without_a_reason()
            throws Exception {
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

            // Assert: a broker that reports the takeover leaves the session to the second
            // client. Closing the connection without a reason, which MQTT 5.0 allows, looks like
            // a lost connection, and the first client reconnects.
            Disconnection end = endings.poll(5, SECONDS);
            assertThat(end).isNotNull();
            if (reportsTakeover()) {
                assertThat(end.reconnectDelay()).isEmpty();
                assertThat(end.cause()).get().isInstanceOfSatisfying(
                        MqttBrokerDisconnectException.class,
                        ending -> assertThat(ending.reasonCode())
                                .isEqualTo(ReasonCode.SESSION_TAKEN_OVER)
                );
            } else {
                assertThat(end.reconnectDelay()).isPresent();
                assertThat(end.cause()).get().isInstanceOf(EOFException.class);
            }
        }
    }

    /// A QoS 1 message on `topic` with every message property set.
    private static MqttMessage withProperties(Topic topic, Duration lifetime) {
        return MqttMessage.of(
                topic,
                "{\"celsius\":21}".getBytes(StandardCharsets.UTF_8),
                Qos.AT_LEAST_ONCE,
                false
        )
                .withPayloadUtf8(true)
                .withMessageExpiry(lifetime)
                .withContentType("application/json")
                .withResponseTopic(new Topic(topic.name() + "/replies"))
                .withCorrelationData(new byte[] {1, 2, 3})
                .withUserProperties(List.of(
                        new UserProperty("source", "sensor"),
                        new UserProperty("hop", "1"),
                        new UserProperty("source", "relay")
                ));
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

    /// A client object that keeps the session of `clientId` in the test's session file, as each
    /// start of an application would build it.
    private MqttClient sessionFileClient(String clientId) {
        return MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V5)
                .clientId(clientId)
                .sessionExpiry(Duration.ofMinutes(1))
                .sessionFile(directory.resolve("session"))
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }

    /// The reason code with which the broker acknowledges a publication at `qos` that no
    /// subscription matches, as recorded for it.
    abstract ReasonCode unmatchedPublicationReason(Qos qos);

    /// Whether the broker sends a connection that another one takes over a DISCONNECT with
    /// Session taken over before closing it, as recorded for it.
    boolean reportsTakeover() {
        return true;
    }

    /// Whether the broker keeps a session that began with Clean Start and a Session Expiry
    /// Interval after the connection ends, as MQTT 5.0 requires and as recorded for it. The
    /// client starts its first connection with Clean Start.
    boolean keepsSessionsThatCleanStartBegan() {
        return true;
    }

    abstract URI endpoint();
}
