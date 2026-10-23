package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.testkit.FaultyTransport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The client's MQTT 3.1.1 connections to each broker product: connecting and a refused
/// connection, publications at every QoS, a persistent session that goes on or that the broker
/// dropped, automatic reconnection, the Will, and retained messages.
///
/// Each product's subclass starts its broker and states its capability profile: where the
/// recorded behavior of its pinned image differs from what these tests expect of a broker by
/// default. The records are the raw exchanges of hato-common's integration tests.
@Timeout(60)
@SuppressWarnings("try") // Explicit close is the abnormal MQTT termination under test.
abstract class AcceptanceTest {

    /// How many times [#replacing_an_unknown_broker_session_publishes_no_will] replaces a
    /// session. Before the client stopped resetting the connections it closed, RabbitMQ lost the
    /// DISCONNECT in about one replacement out of fifteen here, which this many replacements
    /// show almost always.
    private static final int REPLACEMENTS = 100;

    /// How long a test waits for a retained message that should not come.
    private static final Duration QUIET = Duration.ofMillis(500);

    /// How long a broker that may refuse an identifier while it releases it gets before the
    /// identifier is used again, and how often a connection with it is tried.
    private static final Duration RELEASE_PAUSE = Duration.ofMillis(500);

    private static final int RELEASE_TRIES = 10;

    @Test
    void a_client_connects_disconnects_and_closes() throws Exception {
        var ends = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = builder().connectionListener(ends::add).build()) {
            // Act
            ConnAck connAck = sut.connect().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);
            sut.close();

            // Assert
            assertThat(connAck).isEqualTo(ConnAck.of(false, 1));
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            assertThat(sut.terminated()).succeedsWithin(Duration.ofSeconds(5));
        }
    }

    @Test
    void unknown_credentials_are_refused_by_a_broker_that_checks_them() throws Exception {
        try (MqttClient sut = builder()
                .credentials(() -> MqttCredentials.of(
                        "hato-unknown",
                        "not-a-password".getBytes(UTF_8)
                ))
                .build()) {
            // Act
            CompletableFuture<ConnAck> connected = sut.connect();

            // Assert: a broker that lets anonymous clients in takes any credentials.
            if (refusesUnknownCredentials()) {
                assertThatThrownBy(() -> connected.get(5, SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .cause()
                        .isInstanceOfSatisfying(
                                MqttConnectException.class,
                                refusal -> assertThat(refusal.reasonCode())
                                        .contains(ReasonCode.BAD_USER_NAME_OR_PASSWORD)
                        );
            } else {
                assertThat(connected.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Qos.class)
    void publications_round_trip_at_every_qos(Qos qos) throws Exception {
        var topic = new Topic(uniqueTopic());
        try (MqttClient subscriber = client(); MqttClient sut = client()) {
            // Arrange: the subscription asks for QoS 2, so each publication keeps its own QoS
            // where the broker takes it.
            subscriber.connect().get(5, SECONDS);
            sut.connect().get(5, SECONDS);
            PullSubscription subscription = subscribe(subscriber, topic, Qos.EXACTLY_ONCE);
            var message = MqttMessage.of(topic, new byte[] {0, (byte) 0xFF, 42}, qos, false);

            // Act
            CompletableFuture<PublishResult> published = sut.publish(message);

            // Assert: a broker without QoS 2 takes a QoS 2 publication at QoS 1 and answers
            // PUBACK, so that publication fails while the message goes on at QoS 1, and so does
            // the connection.
            if (qos == Qos.EXACTLY_ONCE && !takesQos2()) {
                assertThatThrownBy(() -> published.get(5, SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .cause()
                        .isInstanceOf(MqttProtocolException.class);
                assertThat(receive(subscription)).isEqualTo(MqttMessage.of(
                        topic,
                        message.payload(),
                        Qos.AT_LEAST_ONCE,
                        false
                ));
                MqttMessage after = message(topic, (byte) 1);
                assertThat(sut.publish(after).get(5, SECONDS)).isEqualTo(PublishResult.of());
                assertThat(receive(subscription)).isEqualTo(after);
            } else {
                assertThat(published.get(5, SECONDS)).isEqualTo(PublishResult.of());
                assertThat(receive(subscription)).isEqualTo(message);
            }
        }
    }

    @Test
    void a_persistent_session_keeps_the_messages_published_while_the_client_is_away()
            throws Exception {
        var topic = new Topic(uniqueTopic());
        try (MqttClient sut = persistentClient(clientId()); MqttClient publisher = client()) {
            // Arrange
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            publisher.connect().get(5, SECONDS);
            PullSubscription subscription = subscribe(sut, topic, Qos.AT_LEAST_ONCE);
            sut.disconnect().get(5, SECONDS);
            MqttMessage away = message(topic, (byte) 1);
            publisher.publish(away).get(5, SECONDS);

            // Act
            letTheBrokerRelease();
            ConnAck resumed = sut.connect().get(5, SECONDS);
            MqttMessage after = message(topic, (byte) 2);
            publisher.publish(after).get(5, SECONDS);

            // Assert: a broker that drops what arrived for the absent client still keeps its
            // subscription.
            assertThat(resumed).isEqualTo(ConnAck.of(true, 2));
            if (keepsMessagesForAbsentClients()) {
                assertThat(receive(subscription)).isEqualTo(away);
            }
            assertThat(receive(subscription)).isEqualTo(after);
            assertThat(subscription.receive(Duration.ZERO))
                    .isEqualTo(new ReceiveResult.TimedOut());
        }
    }

    @Test
    void a_session_that_the_broker_dropped_starts_anew_with_the_same_subscription()
            throws Exception {
        var topic = new Topic(uniqueTopic());
        String clientId = clientId();
        try (MqttClient sut = persistentClient(clientId); MqttClient publisher = client()) {
            // Arrange: another client with the same identifier ends the broker's session.
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            PullSubscription subscription = subscribe(sut, topic, Qos.AT_LEAST_ONCE);
            sut.disconnect().get(5, SECONDS);
            try (MqttClient replacing = builder().clientId(clientId).build()) {
                connectReusingIdentifier(replacing);
                replacing.disconnect().get(5, SECONDS);
            }

            // Act
            letTheBrokerRelease();
            ConnAck resumed = sut.connect().get(5, SECONDS);

            // Assert: the client subscribes again for the new session.
            assertThat(resumed).isEqualTo(ConnAck.of(false, 2));
            subscription.established().get(5, SECONDS);
            MqttMessage after = message(topic, (byte) 3);
            publisher.publish(after).get(5, SECONDS);
            assertThat(receive(subscription)).isEqualTo(after);
        }
    }

    @Test
    void a_lost_connection_reconnects_and_the_session_goes_on() throws Exception {
        var topic = new Topic(uniqueTopic());
        var transport = new AtomicReference<FaultyTransport>();
        var connections = new LinkedBlockingQueue<Object>();
        try (MqttClient sut = builder()
                .clientId(clientId())
                .sessionExpiry(Duration.ofMinutes(1))
                .reconnectPolicy(ReconnectPolicy.exponentialBackoff(
                        Duration.ofMillis(100),
                        Duration.ofSeconds(1)
                ))
                .connectionListener(recording(connections))
                .transportFactory(id -> {
                    var faulty = new FaultyTransport(
                            Transport.tcp(id),
                            new ManualTimerScheduler()
                    );
                    transport.set(faulty);
                    return faulty;
                })
                .build();
                MqttClient publisher = client()) {
            // Arrange
            sut.connect().get(5, SECONDS);
            assertThat(connections.poll(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            publisher.connect().get(5, SECONDS);
            PullSubscription subscription = subscribe(sut, topic, Qos.AT_LEAST_ONCE);

            // Act
            var cause = new IOException("network lost");
            requireNonNull(transport.get()).disconnect(cause);

            // Assert: the client reconnects by itself and resumes the session.
            Object ended = connections.poll(5, SECONDS);
            assertThat(ended).isInstanceOfSatisfying(Disconnection.class, end -> {
                assertThat(end.cause()).contains(cause);
                assertThat(end.reconnectDelay()).isPresent();
            });
            assertThat(connections.poll(10, SECONDS)).isEqualTo(ConnAck.of(true, 2));
            MqttMessage after = message(topic, (byte) 4);
            publisher.publish(after).get(5, SECONDS);
            assertThat(receive(subscription)).isEqualTo(after);
        }
    }

    @ParameterizedTest
    @EnumSource(Qos.class)
    void closing_publishes_the_will_and_retains_it_for_later_subscribers(Qos qos)
            throws Exception {
        // Arrange
        var topic = new Topic(uniqueTopic());
        var will = MqttMessage.of(topic, new byte[] {0, (byte) 0xFF}, qos, true);
        try (MqttClient subscriber = client();
                MqttClient later = client();
                MqttClient sut = builder().will(will).build()) {
            subscriber.connect().get(5, SECONDS);
            PullSubscription subscription = subscribe(subscriber, topic, Qos.EXACTLY_ONCE);
            sut.connect().get(5, SECONDS);

            // Act
            sut.close();

            // Assert: a current subscriber receives the Will with RETAIN cleared, unless the
            // broker keeps it at QoS 0, and a later one receives it retained. A broker without
            // QoS 2 sends a QoS 2 Will at QoS 1.
            Qos delivered = qos == Qos.EXACTLY_ONCE && !takesQos2() ? Qos.AT_LEAST_ONCE : qos;
            boolean kept = qos == Qos.AT_MOST_ONCE && !clearsRetainOfQos0Forwards();
            assertThat(receive(subscription))
                    .isEqualTo(MqttMessage.of(topic, will.payload(), delivered, kept));
            later.connect().get(5, SECONDS);
            PullSubscription lateSubscription = subscribe(later, topic, Qos.EXACTLY_ONCE);
            assertThat(receive(lateSubscription))
                    .isEqualTo(MqttMessage.of(topic, will.payload(), delivered, true));
            clearRetained(later, topic);
        }
    }

    @Test
    void a_network_failure_publishes_the_will() throws Exception {
        // Arrange
        var topic = new Topic(uniqueTopic());
        MqttMessage will = message(topic, (byte) 1);
        var transport = new AtomicReference<FaultyTransport>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient subscriber = client();
                MqttClient sut = builder()
                        .will(will)
                        .connectionListener(ended::add)
                        .transportFactory(id -> {
                            var faulty = new FaultyTransport(
                                    Transport.tcp(id),
                                    new ManualTimerScheduler()
                            );
                            transport.set(faulty);
                            return faulty;
                        })
                        .build()) {
            subscriber.connect().get(5, SECONDS);
            PullSubscription subscription = subscribe(subscriber, topic, Qos.EXACTLY_ONCE);
            sut.connect().get(5, SECONDS);

            // Act
            var cause = new IOException("network lost");
            requireNonNull(transport.get()).disconnect(cause);

            // Assert
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(cause, false, 1));
            assertThat(receive(subscription)).isEqualTo(will);
        }
    }

    @Test
    void disconnecting_suppresses_the_will() throws Exception {
        // Arrange
        var topic = new Topic(uniqueTopic());
        String clientId = clientId();
        try (MqttClient subscriber = client();
                MqttClient sut = builder()
                        .clientId(clientId)
                        .will(message(topic, (byte) 1))
                        .build()) {
            subscriber.connect().get(5, SECONDS);
            PullSubscription subscription = subscribe(subscriber, topic, Qos.EXACTLY_ONCE);
            sut.connect().get(5, SECONDS);

            // Act
            sut.disconnect().get(5, SECONDS);

            // Assert
            assertNoWillAfterReplacement(sut, clientId, topic, subscription);
        }
    }

    @Test
    void replacing_an_unknown_broker_session_publishes_no_will() throws Exception {
        // The first connection receives what the broker kept for the old session while it
        // closes, which is when a reset connection lost its DISCONNECT. Repeat to make such a
        // loss show. A replacement that the broker refused does not count.
        int replaced = 0;
        int refused = 0;
        while (replaced < REPLACEMENTS) {
            if (replaceAnUnknownBrokerSession()) {
                replaced++;
            } else {
                refused++;
                assertThat(refused).as("refused replacements").isLessThan(REPLACEMENTS / 10);
            }
        }
    }

    @Test
    void a_new_subscription_receives_the_retained_message_until_an_empty_one_removes_it()
            throws Exception {
        var topic = new Topic(uniqueTopic());
        var retained = MqttMessage.of(topic, new byte[] {9}, Qos.AT_LEAST_ONCE, true);
        try (MqttClient publisher = client();
                MqttClient first = client();
                MqttClient second = client()) {
            // Arrange
            publisher.connect().get(5, SECONDS);
            first.connect().get(5, SECONDS);
            second.connect().get(5, SECONDS);
            publisher.publish(retained).get(5, SECONDS);

            // Act
            PullSubscription kept = subscribe(first, topic, Qos.AT_LEAST_ONCE);
            clearRetained(publisher, topic);
            PullSubscription removed = subscribe(second, topic, Qos.AT_LEAST_ONCE);

            // Assert: a broker can send a retained message after a later publication, so the
            // subscription after the removal waits a while past the marker for anything retained.
            assertThat(receive(kept)).isEqualTo(retained);
            MqttMessage marker = message(topic, (byte) 2);
            publisher.publish(marker).get(5, SECONDS);
            assertThat(receive(removed)).isEqualTo(marker);
            assertThat(removed.receive(QUIET)).isEqualTo(new ReceiveResult.TimedOut());
        }
    }

    @Test
    void a_new_wildcard_subscription_receives_the_retained_message_where_the_broker_sends_it()
            throws Exception {
        String base = uniqueTopic();
        var topic = new Topic(base + "/kept");
        var retained = MqttMessage.of(topic, new byte[] {9}, Qos.AT_LEAST_ONCE, true);
        try (MqttClient publisher = client(); MqttClient sut = client()) {
            // Arrange
            publisher.connect().get(5, SECONDS);
            sut.connect().get(5, SECONDS);
            publisher.publish(retained).get(5, SECONDS);

            // Act
            PullSubscription subscription = subscribe(
                    sut,
                    new TopicFilter(base + "/#"),
                    Qos.AT_LEAST_ONCE
            );
            MqttMessage marker = message(topic, (byte) 2);
            publisher.publish(marker).get(5, SECONDS);

            // Assert: a broker can send the retained message after the marker, as HiveMQ CE
            // does at times, so the two arrive in either order. Where no retained message comes,
            // nothing follows the marker for a while.
            var received = new ArrayList<MqttMessage>();
            received.add(receive(subscription));
            if (sendsRetainedToWildcards()) {
                received.add(receive(subscription));
                assertThat(received).containsExactlyInAnyOrder(retained, marker);
            } else {
                assertThat(received).containsExactly(marker);
                assertThat(subscription.receive(QUIET)).isEqualTo(new ReceiveResult.TimedOut());
            }
            clearRetained(publisher, topic);
        }
    }

    /// Whether the broker takes publications and subscriptions at QoS 2, as recorded for it.
    boolean takesQos2() {
        return true;
    }

    /// Whether the broker refuses a client whose credentials it does not know, as recorded for
    /// it. A broker that lets anonymous clients in takes any credentials.
    boolean refusesUnknownCredentials() {
        return false;
    }

    /// Whether the broker keeps the messages published while a client with a persistent session
    /// is away, as MQTT 3.1.1 requires for QoS 1 and 2 and as recorded for it.
    boolean keepsMessagesForAbsentClients() {
        return true;
    }

    /// Whether the broker sends a retained message to a new subscription with a wildcard, as
    /// recorded for it.
    boolean sendsRetainedToWildcards() {
        return true;
    }

    /// Whether the broker clears RETAIN when it forwards a retained QoS 0 message to an
    /// established subscription, as MQTT requires [MQTT-3.3.1-9] and as recorded for it.
    boolean clearsRetainOfQos0Forwards() {
        return true;
    }

    /// Whether the broker may refuse a connection with Server unavailable while it still releases
    /// the client identifier of the connection before it, as seen for it under load. Replacing a
    /// session that the client does not know opens three connections in a row.
    boolean mayRefuseWhileReleasingClientIdentifiers() {
        return false;
    }

    abstract URI endpoint();

    /// Leaves a broker session with a subscription and stored messages behind, then connects
    /// without its local state, so the client replaces the session, and checks that no Will
    /// follows. Returns false when the broker refused the replacement, as it may while it releases
    /// the client identifier of an earlier connection.
    private boolean replaceAnUnknownBrokerSession() throws Exception {
        var topic = new Topic(uniqueTopic());
        var storedTopic = new Topic(uniqueTopic());
        String clientId = clientId();
        try (MqttClient previous = persistentClient(clientId)) {
            previous.connect().get(5, SECONDS);
            subscribe(previous, storedTopic, Qos.AT_LEAST_ONCE);
            previous.disconnect().get(5, SECONDS);
        }
        var connections = new AtomicInteger();
        try (MqttClient subscriber = client();
                MqttClient sut = builder()
                        .clientId(clientId)
                        .sessionExpiry(Duration.ofMinutes(1))
                        .will(message(topic, (byte) 1))
                        .transportFactory(id -> {
                            connections.incrementAndGet();
                            return Transport.tcp(id);
                        })
                        .build()) {
            subscriber.connect().get(5, SECONDS);
            PullSubscription subscription = subscribe(subscriber, topic, Qos.EXACTLY_ONCE);
            // The broker sends these right after the first CONNACK, while the client closes that
            // connection.
            for (int value = 0; value < 25; value++) {
                subscriber.publish(message(storedTopic, (byte) value)).get(5, SECONDS);
            }

            // Act
            ConnAck connAck;
            try {
                connAck = sut.connect().get(5, SECONDS);
            } catch (ExecutionException failure) {
                if (!mayRefuseWhileReleasingClientIdentifiers() || !refusedAsUnavailable(failure)) {
                    throw failure;
                }
                // Checking for a Will would take the identifier over again, which the broker
                // may refuse just as well; the replacements that succeed check it.
                return false;
            }
            sut.disconnect().get(5, SECONDS);

            // Assert: the connections before the new session ended with DISCONNECT.
            assertThat(connAck).isEqualTo(ConnAck.of(false, 1));
            assertThat(connections).hasValue(3);
            assertNoWillAfterReplacement(sut, clientId, topic, subscription);
            return true;
        }
    }

    /// Gives a broker that may refuse an identifier while it releases it the time to do so,
    /// before the client whose connection held it connects with it again. A new connection of
    /// that client would count the refused attempt, so the test waits instead of trying again.
    private void letTheBrokerRelease() throws InterruptedException {
        if (mayRefuseWhileReleasingClientIdentifiers()) {
            Thread.sleep(RELEASE_PAUSE.toMillis());
        }
    }

    /// Connects `client` with an identifier that another connection held a moment ago. A broker
    /// that may refuse it while it releases it gets a few tries, a pause apart.
    private void connectReusingIdentifier(MqttClient client) throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                client.connect().get(5, SECONDS);
                return;
            } catch (ExecutionException failure) {
                if (!mayRefuseWhileReleasingClientIdentifiers()
                        || !refusedAsUnavailable(failure)
                        || attempt == RELEASE_TRIES) {
                    throw failure;
                }
                Thread.sleep(RELEASE_PAUSE.toMillis());
            }
        }
    }

    private static boolean refusedAsUnavailable(ExecutionException failure) {
        return failure.getCause() instanceof MqttConnectException refusal
                && refusal.reasonCode().equals(Optional.of(ReasonCode.SERVER_UNAVAILABLE));
    }

    /// Closes `ended`, which disconnected with `clientId`, then connects another client with that
    /// identifier, which ends any earlier connection of it, and publishes a marker on the Will
    /// topic. A broker that published the Will of an earlier connection before accepting this
    /// one delivers the Will first.
    ///
    /// The client finishes closing once the broker has closed its connection, which it does after
    /// reading the DISCONNECT. Taking the identifier over before then could end that connection
    /// as taken over and publish its Will.
    private void assertNoWillAfterReplacement(
            MqttClient ended,
            String clientId,
            Topic topic,
            PullSubscription subscription
    ) throws Exception {
        ended.close();
        ended.terminated().get(5, SECONDS);
        try (MqttClient replacement = builder().clientId(clientId).build()) {
            connectReusingIdentifier(replacement);
            MqttMessage marker = message(topic, (byte) 2);
            replacement.publish(marker).get(5, SECONDS);
            assertThat(receive(subscription)).isEqualTo(marker);
            assertThat(subscription.receive(Duration.ZERO))
                    .isEqualTo(new ReceiveResult.TimedOut());
            replacement.disconnect().get(5, SECONDS);
        }
    }

    private MqttClientBuilder builder() {
        return MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled());
    }

    private MqttClient client() {
        return builder().build();
    }

    private MqttClient persistentClient(String clientId) {
        return builder().clientId(clientId).sessionExpiry(Duration.ofMinutes(1)).build();
    }

    private static String clientId() {
        return "hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19);
    }

    private static MqttMessage message(Topic topic, byte value) {
        return MqttMessage.of(topic, new byte[] {value}, Qos.AT_LEAST_ONCE, false);
    }

    private static PullSubscription subscribe(MqttClient client, Topic topic, Qos qos)
            throws Exception {
        return subscribe(client, new TopicFilter(topic.name()), qos);
    }

    private static PullSubscription subscribe(MqttClient client, TopicFilter filter, Qos qos)
            throws Exception {
        PullSubscription subscription = client.subscribe(Subscribe.of(filter, qos));
        subscription.established().get(5, SECONDS);
        return subscription;
    }

    private static MqttMessage receive(PullSubscription subscription) throws Exception {
        ReceiveResult.Received received = assertInstanceOf(
                ReceiveResult.Received.class,
                subscription.receive(Duration.ofSeconds(5))
        );
        return received.delivery().message();
    }

    /// Removes the retained message of `topic`, so that later runs against the same container
    /// start without it.
    private static void clearRetained(MqttClient publisher, Topic topic) throws Exception {
        publisher.publish(MqttMessage.of(topic, new byte[0], Qos.AT_LEAST_ONCE, true))
                .get(5, SECONDS);
    }

    private static ConnectionListener recording(LinkedBlockingQueue<Object> events) {
        return new ConnectionListener() {
            @Override
            public void onConnected(ConnAck connAck) {
                events.add(connAck);
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
                events.add(disconnection);
            }
        };
    }
}
