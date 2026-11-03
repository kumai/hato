package org.mqtt4j.hato.client.internal;

import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.DisconnectionAccess;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.RecordingPort;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.DISCONNECTED;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class SessionClaimTest {

    private static final SessionState SAVED = new SessionState(
            List.of(new SessionState.Registration(new TopicFilter("a/#"), 7, false)),
            8,
            List.of()
    );

    private final RecordingPort port = new RecordingPort();
    private final SessionCore sut = core(ReconnectBackoff.disabled());
    private long nextOperation;
    private long generation;

    @Test
    void an_attempt_claims_the_stored_session_once_its_transport_is_open() {
        // Arrange
        beginConnect();
        assertThat(port.claims).isZero();

        // Act
        sut.opened(generation);

        // Assert: CONNECT waits for the claim.
        assertThat(port.claims).isEqualTo(1);
        assertThat(sent(Packet.Connect.class)).isEmpty();
    }

    @Test
    void a_claimed_state_is_continued_on_the_connection() {
        // Arrange
        long subscription = subscribe("a/#");
        long connection = beginConnect();
        sut.opened(generation);

        // Act
        sut.claimed(SAVED, false, null, 0);
        connAck(true);

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(result(connection).failure()).isNull();
        assertThat(sent(Packet.Connect.class))
                .singleElement()
                .satisfies(request -> assertThat(request.cleanStart()).isFalse());
        assertThat(sent(Packet.Subscribe.class))
                .singleElement()
                .extracting(SessionClaimTest::identifier)
                .isEqualTo(7);
        assertThat(port.removed).doesNotContain(subscription);
    }

    @Test
    void a_claim_without_a_saved_state_starts_a_new_session() {
        // Arrange
        beginConnect();
        sut.opened(generation);

        // Act
        sut.claimed(null, false, null, 0);
        connAck(false);

        // Assert
        assertThat(sent(Packet.Connect.class))
                .singleElement()
                .satisfies(request -> assertThat(request.cleanStart()).isTrue());
    }

    @Test
    void a_failed_claim_fails_the_attempt_without_a_connect() {
        // Arrange
        long connection = beginConnect();
        sut.opened(generation);
        var failure = new IOException("disk failure");

        // Act
        sut.claimed(null, false, failure, 0);

        // Assert
        assertThat(result(connection).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasCause(failure);
        assertThat(sent(Packet.Connect.class)).isEmpty();
    }

    @Test
    void a_claim_refused_while_another_client_object_holds_the_session_fails_the_attempt() {
        // Arrange
        long connection = beginConnect();
        sut.opened(generation);

        // Act
        sut.claimRefused(0);

        // Assert: the attempt sends no CONNECT, and the next one claims the session again.
        assertThat(result(connection).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasMessage("Another client object holds the session file");
        assertThat(sent(Packet.Connect.class)).isEmpty();
        beginConnect();
        sut.opened(generation);
        assertThat(port.claims).isEqualTo(2);
    }

    @Test
    void a_claim_answered_after_the_connection_deadline_fails_the_attempt_without_a_connect() {
        // Arrange: the connection has 30 seconds.
        long connection = beginConnect();
        sut.opened(generation);

        // Act: the claim's answer arrives before the timer that would end the attempt.
        sut.claimed(null, false, null, seconds(31));

        // Assert: the claim still makes the client own the session, which the next connection
        // checks.
        assertThat(sent(Packet.Connect.class)).isEmpty();
        assertThat(result(connection).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasCauseInstanceOf(TimeoutException.class)
                .hasRootCauseMessage("Session claim deadline expired");
        beginConnect();
        sut.opened(generation);
        assertThat(port.checks).containsExactly(generation);
    }

    @Test
    void a_check_answered_after_the_connection_deadline_fails_the_attempt_without_a_connect() {
        // Arrange: the client owns the session, loses its connection, and connects again 100
        // seconds in, with 30 seconds for the connection.
        connect();
        sut.failed(generation, new IOException("connection lost"));
        long reconnection = operation();
        sut.connect(new ConnectionAttempt(reconnection), seconds(100));
        generation = port.generation;
        sut.opened(generation);

        // Act
        sut.checked(generation, true, null, seconds(131));

        // Assert
        assertThat(sent(Packet.Connect.class)).hasSize(1);
        assertThat(result(reconnection).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasRootCauseMessage("Session claim deadline expired");
    }

    @Test
    void each_connection_that_replaces_a_broker_session_has_its_own_deadline_for_the_check() {
        // Arrange: an MQTT 3.1.1 client without a saved state finds a session on the broker. Its
        // attempt therefore writes DISCONNECT 20 seconds in and opens a second connection to
        // discard the session, which checks the claim like every connection.
        SessionCore replacing = core(MqttVersion.V3_1_1, ReconnectBackoff.disabled());
        replacing.connect(new ConnectionAttempt(operation()), 0);
        replacing.opened(port.generation);
        replacing.claimed(null, false, null, 0);
        replacing.received(
                port.generation,
                new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY),
                seconds(1),
                0
        );
        long replaced = port.generation;
        replacing.sent(replaced, port.ticket, seconds(20));
        assertThat(port.finished).contains(replaced);
        replacing.failed(replaced, new EOFException("Peer closed the byte stream"));
        replacing.opened(port.generation);

        // Act: the check's answer arrives 25 seconds into the second connection's 30.
        replacing.checked(port.generation, true, null, seconds(45));

        // Assert
        assertThat(sent(Packet.Connect.class)).hasSize(2);
    }

    @Test
    void a_disconnection_completes_once_the_session_is_saved() {
        // Arrange
        connect();

        // Act
        long disconnection = disconnect();

        // Assert
        assertThat(port.saves).hasSize(1);
        assertThat(port.results).doesNotContainKey(disconnection);
        sut.saved(port.savedDisconnections.getFirst());
        assertThat(result(disconnection).failure()).isNull();
    }

    @Test
    void a_disconnection_during_a_save_completes_with_it() {
        // Arrange: the first disconnect() waits for the save of the session.
        connect();
        long first = disconnect();

        // Act
        long second = operation();
        sut.disconnect(second);

        // Assert
        assertThat(port.results).doesNotContainKeys(first, second);
        sut.saved(port.savedDisconnections.getFirst());
        assertThat(result(first).failure()).isNull();
        assertThat(result(second).failure()).isNull();
        assertThat(port.saves).hasSize(1);
    }

    @Test
    void a_disconnection_during_a_save_waits_for_the_claim_of_a_connection_that_follows_it() {
        // Arrange: a second disconnect() waits for the save, and the next connection claims the
        // session, which the store does after the save.
        connect();
        long first = disconnect();
        long second = operation();
        sut.disconnect(second);
        beginConnect();
        sut.opened(generation);
        assertThat(port.claims).isEqualTo(2);

        // Act
        sut.saved(port.savedDisconnections.getFirst());

        // Assert
        assertThat(result(first).failure()).isNull();
        assertThat(port.results).doesNotContainKey(second);
        sut.claimed(null, true, null, 0);
        assertThat(result(second).failure()).isNull();
    }

    @Test
    void an_unfinished_publication_completes_the_disconnection_without_a_save() {
        // Arrange
        connect();
        sut.publish(
                operation(),
                MqttMessage.of(new Topic("t"), new byte[] {1}, Qos.AT_LEAST_ONCE, false)
        );

        // Act
        long disconnection = disconnect();

        // Assert
        assertThat(port.saves).isEmpty();
        assertThat(port.results).containsKey(disconnection);
    }

    @Test
    void the_next_connection_claims_the_session_again_after_a_save() {
        // Arrange
        connect();
        disconnect();
        sut.saved(port.savedDisconnections.getFirst());
        beginConnect();

        // Act
        sut.opened(generation);

        // Assert
        assertThat(port.claims).isEqualTo(2);
        assertThat(port.checks).isEmpty();
    }

    @Test
    void a_reconnection_checks_the_claim_before_it_sends_connect() {
        // Arrange
        connect();
        sut.failed(generation, new IOException("connection lost"));
        beginConnect();

        // Act
        sut.opened(generation);

        // Assert: the reconnection asks whether the claim still holds, and sends CONNECT once the
        // store says that it does.
        assertThat(port.claims).isEqualTo(1);
        assertThat(port.checks).containsExactly(generation);
        assertThat(sent(Packet.Connect.class)).hasSize(1);
        sut.checked(generation, true, null, 0);
        assertThat(sent(Packet.Connect.class)).hasSize(2);
    }

    @Test
    void a_client_whose_claim_another_client_object_took_stops_without_connect() {
        // Arrange: a client that reconnects on its own owns the session and loses its
        // connection; its reconnection opens a transport.
        SessionCore reconnecting = core(ReconnectBackoff.exponential(
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)
        ));
        reconnecting.connect(new ConnectionAttempt(operation()), 0);
        reconnecting.opened(port.generation);
        reconnecting.claimed(null, false, null, 0);
        reconnecting.received(
                port.generation,
                new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY),
                0,
                0
        );
        reconnecting.failed(port.generation, new IOException("connection lost"));
        reconnecting.timer(0, SessionCore.Timer.RECONNECT, 0);
        reconnecting.opened(port.generation);

        // Act: the store no longer marks the session as this client object's.
        reconnecting.checked(port.generation, false, null, 0);

        // Assert: the reconnection sends no CONNECT, and the client does not try again.
        assertThat(sent(Packet.Connect.class)).hasSize(1);
        assertThat(reconnecting.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).last().satisfies(end -> {
            assertThat(end.cause().orElseThrow())
                    .isInstanceOf(MqttConnectException.class)
                    .hasMessage("Another client object took over the session file");
            assertThat(end.reconnectDelay()).isEmpty();
        });
    }

    @Test
    void a_connection_after_losing_the_claim_claims_the_session_anew() {
        // Arrange: another client object claimed the session while this one reconnected.
        connect();
        sut.failed(generation, new IOException("connection lost"));
        beginConnect();
        sut.opened(generation);
        sut.checked(generation, false, null, 0);
        beginConnect();

        // Act
        sut.opened(generation);

        // Assert
        assertThat(port.claims).isEqualTo(2);
    }

    @Test
    void a_failed_check_fails_the_attempt_and_keeps_the_claim() {
        // Arrange
        connect();
        sut.failed(generation, new IOException("connection lost"));
        long reconnection = beginConnect();
        sut.opened(generation);
        var failure = new IOException("disk failure");

        // Act
        sut.checked(generation, false, failure, 0);

        // Assert: the next connection checks the claim again.
        assertThat(result(reconnection).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasCause(failure);
        beginConnect();
        sut.opened(generation);
        assertThat(port.claims).isEqualTo(1);
        assertThat(port.checks).hasSize(2);
    }

    @Test
    void a_claim_that_outlives_its_connection_still_takes_the_session_over() {
        // Arrange: the attempt runs out of time while the claim is in flight.
        long connection = beginConnect();
        sut.opened(generation);
        sut.timer(generation, SessionCore.Timer.CONNECT, Duration.ofSeconds(30).toNanos());
        assertThat(result(connection).failure()).isNotNull();

        // Act
        sut.claimed(SAVED, false, null, 0);
        beginConnect();
        sut.opened(generation);
        sut.checked(generation, true, null, 0);
        connAck(true);

        // Assert
        assertThat(port.claims).isEqualTo(1);
        assertThat(sent(Packet.Connect.class))
                .singleElement()
                .satisfies(request -> assertThat(request.cleanStart()).isFalse());
    }

    @Test
    void a_disconnection_during_a_claim_waits_for_it_and_saves_the_claimed_session() {
        // Arrange: the attempt ran out of time while the claim is in flight.
        beginConnect();
        sut.opened(generation);
        sut.timer(generation, SessionCore.Timer.CONNECT, Duration.ofSeconds(30).toNanos());
        long disconnection = operation();
        sut.disconnect(disconnection);
        assertThat(port.results).doesNotContainKey(disconnection);

        // Act
        sut.claimed(SAVED, false, null, 0);

        // Assert
        assertThat(port.saves).singleElement().satisfies(saved -> {
            assertThat(saved.registrations()).isEqualTo(SAVED.registrations());
            assertThat(saved.nextIdentifier()).isEqualTo(SAVED.nextIdentifier());
        });
        sut.saved(port.savedDisconnections.getFirst());
        assertThat(result(disconnection).failure()).isNull();
    }

    @Test
    void a_session_that_another_client_object_saved_replaces_the_local_one() {
        // Arrange: this client object saved its session with a/# under identifier 1, and another
        // one continued the session and saved it with a/# under 7.
        subscribe("a/#");
        connect();
        Packet.Subscribe first = sent(Packet.Subscribe.class).getFirst();
        receive(new Packet.SubAck(first.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
        disconnect();
        sut.saved(port.savedDisconnections.getFirst());
        port.sent.clear();

        // Act
        beginConnect();
        sut.opened(generation);
        sut.claimed(SAVED, false, null, 0);
        connAck(true);

        // Assert
        assertThat(sent(Packet.Subscribe.class))
                .singleElement()
                .extracting(SessionClaimTest::identifier)
                .isEqualTo(7);
    }

    @Test
    void a_claim_of_its_own_save_keeps_the_local_session() {
        // Arrange: this client object saved its session and claims its own save.
        subscribe("a/#");
        connect();
        Packet.Subscribe first = sent(Packet.Subscribe.class).getFirst();
        receive(new Packet.SubAck(first.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
        disconnect();
        sut.saved(port.savedDisconnections.getFirst());
        port.sent.clear();

        // Act
        beginConnect();
        sut.opened(generation);
        sut.claimed(null, true, null, 0);
        connAck(true);

        // Assert: the continued session keeps the established subscription.
        assertThat(sent(Packet.Connect.class))
                .singleElement()
                .satisfies(request -> assertThat(request.cleanStart()).isFalse());
        assertThat(sent(Packet.Subscribe.class)).isEmpty();
    }

    @Test
    void a_closed_client_saves_nothing() {
        // Arrange
        connect();

        // Act
        sut.close(null);

        // Assert
        assertThat(port.saves).isEmpty();
    }

    private SessionCore core(ReconnectBackoff backoff) {
        return core(MqttVersion.V5, backoff);
    }

    private SessionCore core(MqttVersion version, ReconnectBackoff backoff) {
        return new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ZERO,
                version,
                Duration.ofHours(1),
                16 * 1024 * 1024,
                null,
                false,
                backoff,
                RetryRules.DEFAULT,
                Duration.ofSeconds(60),
                DisconnectionAccess::of,
                ClientLimits.DEFAULT,
                true
        );
    }

    private long operation() {
        return ++nextOperation;
    }

    private static long seconds(long value) {
        return Duration.ofSeconds(value).toNanos();
    }

    private SessionPersistenceTest.Result result(long operation) {
        return requireNonNull(port.results.get(operation));
    }

    private long beginConnect() {
        long operation = operation();
        sut.connect(new ConnectionAttempt(operation), 0);
        generation = port.generation;
        return operation;
    }

    /// Establishes a first connection, whose claim finds no saved state.
    private void connect() {
        beginConnect();
        sut.opened(generation);
        sut.claimed(null, false, null, 0);
        connAck(false);
    }

    private void connAck(boolean sessionPresent) {
        receive(new Packet.ConnAck(sessionPresent, ReasonCode.SUCCESS, EMPTY));
    }

    /// Requests a disconnection and completes the write of its DISCONNECT.
    private long disconnect() {
        long operation = operation();
        sut.disconnect(operation);
        sut.sent(generation, port.ticket, 0);
        return operation;
    }

    private void receive(Packet packet) {
        sut.received(generation, packet, 0, 0);
    }

    private long subscribe(String filter) {
        long operation = operation();
        sut.subscribe(operation, Subscribe.of(new TopicFilter(filter), Qos.AT_LEAST_ONCE));
        return operation;
    }

    private <T extends Packet> List<T> sent(Class<T> type) {
        return port.sent.stream()
                .filter(type::isInstance)
                .map(type::cast)
                .toList();
    }

    private static int identifier(Packet.Subscribe request) {
        for (Property property : request.properties().entries()) {
            if (property instanceof Property.VariableByteValue(var id, var value)
                    && id == PropertyId.SUBSCRIPTION_IDENTIFIER) {
                return value;
            }
        }
        throw new AssertionError("No Subscription Identifier in " + request);
    }
}
