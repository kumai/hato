package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// A session file carries a persistent session from one client object to the next, as across
/// restarts of an application, against the embedded broker and Mosquitto in both protocol
/// versions.
@ParameterizedClass(name = "{0}")
@EnumSource(MqttVersion.class)
@Timeout(30)
abstract class SessionFileTest {

    private final MqttVersion version;

    private final String clientId = "hato" + UUID.randomUUID().toString().replace("-", "")
            .substring(0, 19);

    @TempDir
    Path directory;

    SessionFileTest(MqttVersion version) {
        this.version = version;
    }

    @Test
    void a_restarted_client_receives_what_the_broker_stored_while_it_was_stopped()
            throws Exception {
        // Arrange: an earlier client object subscribes and disconnects, and then messages
        // arrive for its session.
        String topic = uniqueTopic();
        var request = Subscribe.of(new TopicFilter(topic), Qos.EXACTLY_ONCE);
        MqttMessage atLeastOnce = message(topic, Qos.AT_LEAST_ONCE, 1);
        MqttMessage exactlyOnce = message(topic, Qos.EXACTLY_ONCE, 2);
        MqttMessage marker = message(topic, Qos.AT_LEAST_ONCE, 3);
        var connections = new AtomicInteger();
        try (MqttClient publisher = publisher()) {
            publisher.connect().get(5, SECONDS);
            MqttClient earlier = sessionClient(_ -> {});
            try (earlier) {
                PullSubscription subscription = earlier.subscribe(request);
                earlier.connect().get(5, SECONDS);
                subscription.established().get(5, SECONDS);
                earlier.disconnect().get(5, SECONDS);
            }
            // The earlier client object holds the file until its connection has ended.
            earlier.terminated().get(5, SECONDS);
            publisher.publish(atLeastOnce).get(5, SECONDS);
            publisher.publish(exactlyOnce).get(5, SECONDS);
            try (
                    MqttClient sut = sessionClientBuilder(_ -> {})
                            .transportFactory(id -> {
                                connections.incrementAndGet();
                                return Transport.tcp(id);
                            })
                            .build()
            ) {
                PullSubscription subscription = sut.subscribe(request);

                // Act
                ConnAck connAck = sut.connect().get(5, SECONDS);
                publisher.publish(marker).get(5, SECONDS);

                // Assert: one connection continues the session, and the stored messages arrive
                // once, ahead of the marker.
                assertFirstConnection(connAck, true);
                assertThat(connections).hasValue(1);
                for (MqttMessage expected : List.of(atLeastOnce, exactlyOnce, marker)) {
                    assertThat(received(subscription.receive(Duration.ofSeconds(5))).message())
                            .isEqualTo(expected);
                }
                assertThat(subscription.receive(Duration.ZERO))
                        .isEqualTo(new ReceiveResult.TimedOut());
                sut.disconnect().get(5, SECONDS);
            }
            publisher.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void stored_messages_reach_the_subscription_to_their_filter_whatever_the_order_of_registration()
            throws Exception {
        // Arrange: the earlier client object subscribes to two filters. The next one registers
        // a new filter first, then one of the earlier filters, and not the other.
        String base = uniqueTopic();
        var kept = new TopicFilter(base + "/kept");
        var dropped = new TopicFilter(base + "/dropped");
        var added = new TopicFilter(base + "/added");
        MqttMessage forKept = message(base + "/kept", Qos.AT_LEAST_ONCE, 1);
        MqttMessage forDropped = message(base + "/dropped", Qos.AT_LEAST_ONCE, 2);
        MqttMessage lateForDropped = message(base + "/dropped", Qos.AT_LEAST_ONCE, 3);
        MqttMessage marker = message(base + "/kept", Qos.AT_LEAST_ONCE, 4);
        try (MqttClient publisher = publisher()) {
            publisher.connect().get(5, SECONDS);
            MqttClient earlier = sessionClient(_ -> {});
            try (earlier) {
                PullSubscription first = earlier.subscribe(Subscribe.of(kept, Qos.AT_LEAST_ONCE));
                PullSubscription second = earlier.subscribe(
                        Subscribe.of(dropped, Qos.AT_LEAST_ONCE)
                );
                earlier.connect().get(5, SECONDS);
                first.established().get(5, SECONDS);
                second.established().get(5, SECONDS);
                earlier.disconnect().get(5, SECONDS);
            }
            // The earlier client object holds the file until its connection has ended.
            earlier.terminated().get(5, SECONDS);
            publisher.publish(forKept).get(5, SECONDS);
            publisher.publish(forDropped).get(5, SECONDS);
            try (MqttClient sut = sessionClient(_ -> {})) {
                PullSubscription newer = sut.subscribe(Subscribe.of(added, Qos.AT_LEAST_ONCE));
                PullSubscription older = sut.subscribe(Subscribe.of(kept, Qos.AT_LEAST_ONCE));

                // Act: once the client has removed the filter that it no longer registers,
                // another message for it and then a marker are published.
                sut.connect().get(5, SECONDS);
                newer.established().get(5, SECONDS);
                older.established().get(5, SECONDS);
                await().atMost(Duration.ofSeconds(5))
                        .until(() -> sut.stats().get(5, SECONDS).subscribing().removing() == 0);
                publisher.publish(lateForDropped).get(5, SECONDS);
                publisher.publish(marker).get(5, SECONDS);

                // Assert: the stored message for the kept filter reaches its subscription, not
                // the one registered first, and the one for the dropped filter reaches none. The
                // marker comes after the later message would have, which no longer arrives.
                assertThat(received(older.receive(Duration.ofSeconds(5))).message())
                        .isEqualTo(forKept);
                assertThat(received(older.receive(Duration.ofSeconds(5))).message())
                        .isEqualTo(marker);
                assertThat(newer.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
                assertThat(sut.stats().get(5, SECONDS).receiving().unmatched()).isEqualTo(1);
                sut.disconnect().get(5, SECONDS);
            }
            publisher.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void a_client_closed_without_disconnecting_leaves_the_next_one_a_new_session()
            throws Exception {
        // Arrange: the earlier client object is closed while connected, and then a message
        // arrives for its session.
        String topic = uniqueTopic();
        var request = Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE);
        MqttMessage lost = message(topic, Qos.AT_LEAST_ONCE, 1);
        MqttMessage marker = message(topic, Qos.AT_LEAST_ONCE, 2);
        try (MqttClient publisher = publisher()) {
            publisher.connect().get(5, SECONDS);
            MqttClient earlier = sessionClient(_ -> {});
            PullSubscription subscription = earlier.subscribe(request);
            earlier.connect().get(5, SECONDS);
            subscription.established().get(5, SECONDS);
            earlier.close();
            earlier.terminated().get(5, SECONDS);
            publisher.publish(lost).get(5, SECONDS);
            try (MqttClient sut = sessionClient(_ -> {})) {
                PullSubscription restarted = sut.subscribe(request);

                // Act
                ConnAck connAck = sut.connect().get(5, SECONDS);
                restarted.established().get(5, SECONDS);
                publisher.publish(marker).get(5, SECONDS);

                // Assert: the client starts a new session, without the stored message.
                assertFirstConnection(connAck, false);
                assertThat(received(restarted.receive(Duration.ofSeconds(5))).message())
                        .isEqualTo(marker);
                assertThat(restarted.receive(Duration.ZERO))
                        .isEqualTo(new ReceiveResult.TimedOut());
                sut.disconnect().get(5, SECONDS);
            }
            publisher.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void a_client_object_that_starts_while_another_one_holds_the_file_continues_its_session()
            throws Exception {
        // Arrange: the earlier client object of a rolling update holds the file while the later
        // one keeps trying to connect.
        String topic = uniqueTopic();
        var request = Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE);
        MqttMessage stored = message(topic, Qos.AT_LEAST_ONCE, 1);
        var earlierNotifications = new LinkedBlockingQueue<Object>();
        var laterNotifications = new LinkedBlockingQueue<Object>();
        try (
                MqttClient publisher = publisher();
                MqttClient earlier = sessionClient(recording(earlierNotifications));
                MqttClient later = sessionClientBuilder(recording(laterNotifications))
                        .reconnectPolicy(
                                ReconnectPolicy.exponentialBackoff(
                                        Duration.ofMillis(10),
                                        Duration.ofMillis(100)
                                ).withInitialRetry()
                        )
                        .build()
        ) {
            publisher.connect().get(5, SECONDS);
            PullSubscription subscription = earlier.subscribe(request);
            earlier.connect().get(5, SECONDS);
            subscription.established().get(5, SECONDS);
            PullSubscription restarted = later.subscribe(request);
            CompletableFuture<ConnAck> connecting = later.connect();
            Disconnection refused = assertInstanceOf(
                    Disconnection.class,
                    laterNotifications.poll(5, SECONDS)
            );

            // Act: the earlier client object disconnects, and then a message arrives for the
            // session.
            earlier.disconnect().get(5, SECONDS);
            publisher.publish(stored).get(5, SECONDS);

            // Assert: the later client object connects only once the earlier one has saved the
            // session, which it continues, and never took the earlier one's connection over.
            assertThat(refused.cause()).hasValueSatisfying(cause -> assertThat(cause)
                    .isInstanceOf(MqttConnectException.class)
                    .hasMessage("Another client object holds the session file"));
            assertThat(connecting.get(5, SECONDS).sessionPresent()).isTrue();
            assertThat(received(restarted.receive(Duration.ofSeconds(5))).message())
                    .isEqualTo(stored);
            assertFirstConnection(
                    assertInstanceOf(ConnAck.class, earlierNotifications.poll(5, SECONDS)),
                    false
            );
            assertThat(earlierNotifications.poll(5, SECONDS))
                    .isEqualTo(DisconnectionAccess.of(null, true, 1));
            assertThat(earlierNotifications).isEmpty();
            later.disconnect().get(5, SECONDS);
            publisher.disconnect().get(5, SECONDS);
        }
    }

    abstract URI endpoint();

    private MqttClient publisher() {
        return MqttClient.builder()
                .server(endpoint())
                .version(version)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }

    /// A client object that keeps its session in the test's file, as each start of an
    /// application would build it.
    private MqttClient sessionClient(ConnectionListener listener) {
        return sessionClientBuilder(listener).build();
    }

    private MqttClientBuilder sessionClientBuilder(ConnectionListener listener) {
        return MqttClient.builder()
                .server(endpoint())
                .version(version)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .clientId(clientId)
                .sessionExpiry(Duration.ofMinutes(1))
                .sessionFile(directory.resolve("session"))
                .connectionListener(listener);
    }

    /// Records each connection, failed attempt, and ending, in order.
    private static ConnectionListener recording(LinkedBlockingQueue<Object> notifications) {
        return new ConnectionListener() {
            @Override
            public void onConnected(ConnAck connAck) {
                notifications.add(connAck);
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
                notifications.add(disconnection);
            }

            @Override
            public void onConnectFailed(Disconnection failure) {
                notifications.add(failure);
            }
        };
    }

    /// Checks that `connAck` establishes a client object's first connection, and whether it
    /// continues a session. The rest of an MQTT 5.0 CONNACK depends on the broker.
    private static void assertFirstConnection(ConnAck connAck, boolean sessionPresent) {
        assertThat(connAck)
                .extracting(ConnAck::sessionPresent, ConnAck::connectionAttempt)
                .containsExactly(sessionPresent, 1L);
    }

    private static MqttMessage message(String topic, Qos qos, int value) {
        return MqttMessage.of(new Topic(topic), new byte[] {(byte) value}, qos, false);
    }

    private static MqttDelivery received(ReceiveResult result) {
        return assertInstanceOf(ReceiveResult.Received.class, result).delivery();
    }
}
