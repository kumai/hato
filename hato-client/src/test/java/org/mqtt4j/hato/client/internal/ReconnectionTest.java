package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.SessionCore.State.*;
import static org.mqtt4j.hato.client.internal.SessionCore.Timer.*;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class ReconnectionTest {

    private final SessionPersistenceTest.RecordingPort port =
            new SessionPersistenceTest.RecordingPort();

    @ParameterizedTest
    @CsvSource({
            "UNSUPPORTED_PROTOCOL_VERSION,false",
            "CLIENT_IDENTIFIER_NOT_VALID,false",
            "SERVER_UNAVAILABLE,true",
            "BAD_USER_NAME_OR_PASSWORD,true",
            "NOT_AUTHORIZED,true"
    })
    void broker_refusals_follow_the_retry_classification(String reason, boolean retry) {
        var failure = new MqttConnectException("Refused", ReasonCode.valueOf(reason));
        assertThat(SessionCore.reconnectable(failure)).isEqualTo(retry);
    }

    @ParameterizedTest
    @CsvSource({
            "UNSUPPORTED_PROTOCOL_VERSION,false",
            "CLIENT_IDENTIFIER_NOT_VALID,false",
            "SERVER_UNAVAILABLE,true",
            "BAD_USER_NAME_OR_PASSWORD,true",
            "NOT_AUTHORIZED,true"
    })
    void an_initial_retry_request_obeys_the_brokers_refusal(String reason, boolean retry) {
        // Arrange
        SessionCore sut = core(true);
        sut.connect(attempt(1), 0);
        sut.opened(1);

        // Act
        sut.received(1, new Packet.ConnAck(false, ReasonCode.valueOf(reason), EMPTY), 0, 4);

        // Assert
        assertThat(sut.state()).isEqualTo(retry ? RECONNECT_WAIT : DISCONNECTED);
        assertThat(port.timers.containsKey(RECONNECT)).isEqualTo(retry);
        assertThat(port.results.containsKey(1L)).isEqualTo(!retry);
    }

    @ParameterizedTest
    @MethodSource("retryableFailures")
    void local_connection_failures_allow_retry(Throwable failure) {
        assertThat(SessionCore.reconnectable(failure)).isTrue();
    }

    private static Stream<Throwable> retryableFailures() {
        return Stream.of(
                new IOException("EOF"),
                new UnknownHostException("host"),
                new SSLHandshakeException("certificate"),
                new TimeoutException("response"),
                new MqttCapacityException("overload"),
                new MqttProtocolException("packet", ReasonCode.PROTOCOL_ERROR),
                new MqttConnectException("Supplier", new IllegalArgumentException("token")),
                new IllegalStateException("transport factory")
        );
    }

    @Test
    void the_default_first_failure_abandons_the_request() {
        // Arrange
        SessionCore sut = core(false);
        sut.connect(attempt(1), 0);

        // Act
        sut.failed(1, new IOException("offline"));

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.timers).isEmpty();
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class);
        assertThat(port.requests).containsExactly(true, false);
    }

    @Test
    void joined_requests_wait_across_initial_retries_until_one_connection_succeeds() {
        // Arrange
        SessionCore sut = core(true);
        sut.connect(attempt(1), 0);
        sut.connect(attempt(2), 0);

        // Act & Assert
        sut.failed(1, new IOException("offline"));
        assertThat(port.results).isEmpty();
        assertThat(port.timers).containsEntry(RECONNECT, Duration.ofNanos(3));
        assertThat(port.events).endsWith("schedule RECONNECT", "end");
        sut.timer(0, RECONNECT, 3);
        connected(sut, 2, 3);

        assertThat(port.attempts).containsExactly(1L, 2L);
        assertThat(requireNonNull(port.results.get(1L)).failure()).isNull();
        assertThat(requireNonNull(port.results.get(2L)).value())
                .isSameAs(requireNonNull(port.results.get(1L)).value());
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void success_and_manual_connect_do_not_restart_the_delay_sequence() {
        // Arrange
        SessionCore sut = core(false);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);

        // Act
        sut.failed(1, new IOException("lost"));
        sut.connect(attempt(2), 1);
        connected(sut, 2, 1);
        sut.failed(2, new IOException("lost again"));

        // Assert
        assertThat(port.reconnectDelays).containsExactly(Duration.ofNanos(3), Duration.ofNanos(6));
        assertThat(port.attempts).containsExactly(1L, 2L);
        assertThat(port.timers).containsOnlyKeys(RECONNECT);
    }

    @Test
    void only_thirty_seconds_of_stability_reset_the_delay_sequence() {
        // Arrange
        SessionCore sut = core(false);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);
        sut.failed(1, new IOException("lost"));
        sut.timer(0, RECONNECT, 3);
        connected(sut, 2, 3);

        // Act & Assert
        sut.timer(2, STABLE, 3 + Duration.ofSeconds(29).toNanos());
        assertThat(port.timers).containsEntry(STABLE, Duration.ofSeconds(1));
        sut.failed(2, new IOException("too short"));
        assertThat(port.reconnectDelays.getLast()).isEqualTo(Duration.ofNanos(6));
        sut.timer(0, RECONNECT, 9);
        connected(sut, 3, 9);
        sut.timer(3, STABLE, 9 + Duration.ofSeconds(30).toNanos());
        sut.failed(3, new IOException("stable"));

        assertThat(port.reconnectDelays.getLast()).isEqualTo(Duration.ofNanos(3));
    }

    @Test
    void connecting_again_on_an_established_connection_returns_the_same_ack() {
        // Arrange
        SessionCore sut = core(false);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);

        // Act
        sut.connect(attempt(2), 1);

        // Assert
        assertThat(port.attempts).containsExactly(1L);
        assertThat(requireNonNull(port.results.get(2L)).value())
                .isSameAs(requireNonNull(port.results.get(1L)).value());
    }

    @Test
    void disconnect_during_an_attempt_waits_for_connack_and_the_disconnect_write() {
        // Arrange
        SessionCore sut = core(true);
        sut.connect(attempt(1), 0);
        // Act
        sut.disconnect(2);
        sut.disconnect(3);
        sut.connect(attempt(4), 0);

        // Assert
        assertThat(port.disconnecting).isEqualTo(1);
        assertThat(port.requests).containsExactly(true);
        assertThat(requireNonNull(port.results.get(4L)).failure())
                .isInstanceOf(IllegalStateException.class);
        connected(sut, 1, 0);
        assertThat(port.sent).hasSize(2);
        assertThat(port.sent.getLast()).isInstanceOf(Packet.Disconnect.class);
        assertThat(port.results).doesNotContainKeys(1L, 2L, 3L);
        sut.sent(1, port.ticket, 1);

        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class);
        assertThat(requireNonNull(port.results.get(2L)).failure()).isNull();
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.userRequested()).isTrue()
        );
        assertThat(port.timers).isEmpty();
        assertThat(port.requests.getLast()).isFalse();
    }

    @Test
    void disconnect_cancels_a_retry_and_is_successful_when_nothing_remains() {
        // Arrange
        SessionCore sut = core(true);
        sut.connect(attempt(1), 0);
        sut.failed(1, new IOException("offline"));

        // Act
        sut.disconnect(2);
        sut.disconnect(3);
        sut.timer(0, RECONNECT, 3);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.attempts).containsExactly(1L);
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class);
        assertThat(requireNonNull(port.results.get(2L)).failure()).isNull();
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        assertThat(port.ends).hasSize(1);
        assertThat(port.timers).isEmpty();
    }

    @Test
    void withdrawing_a_wait_closes_stop_admission_before_releasing_paused_lanes() {
        // Arrange
        SessionCore sut = core(false);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);
        sut.failed(1, new IOException("lost"));

        // Act
        sut.disconnect(2);

        // Assert: a stop admitted while waiting is discarded with the request.
        assertThat(port.stopsDiscarded).isEqualTo(1);
        assertThat(port.events).containsSubsequence("request false", "stop discarded");
        assertThat(requireNonNull(port.results.get(2L)).failure()).isNull();
    }

    @Test
    void disconnect_during_a_refused_attempt_completes_normally() {
        // Arrange
        SessionCore sut = core(false);
        sut.connect(attempt(1), 0);
        sut.opened(1);
        sut.disconnect(2);

        // Act
        sut.received(1, new Packet.ConnAck(false, ReasonCode.NOT_AUTHORIZED, EMPTY), 0, 4);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class);
        assertThat(requireNonNull(port.results.get(2L)).failure()).isNull();
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.userRequested()).isTrue()
        );
    }

    @Test
    void disconnect_during_a_stale_session_refusal_completes_when_its_disconnect_is_written() {
        // Arrange: the broker keeps a session for which this client has no local state.
        SessionCore sut = core(false);
        sut.connect(attempt(1), 0);
        sut.opened(1);
        sut.received(1, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 0, 4);
        sut.disconnect(2);

        // Act
        sut.sent(1, port.ticket, 1);

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .hasMessage("Broker session has no local state");
        assertThat(requireNonNull(port.results.get(2L)).failure()).isNull();
    }

    @Test
    void an_initial_retry_request_does_not_retry_a_stale_broker_session() {
        // Arrange
        SessionCore sut = core(true);
        sut.connect(attempt(1), 0);
        sut.opened(1);
        sut.received(1, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 0, 4);

        // Act: the connection fails before the refusing DISCONNECT is written.
        sut.failed(1, new IOException("lost during DISCONNECT"));

        // Assert: another attempt would meet the same broker session.
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.timers).doesNotContainKey(RECONNECT);
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .hasMessage("Broker session has no local state");
    }

    @Test
    void a_failed_retry_reservation_stops_within_the_same_ending() {
        // Arrange: a nonpersistent session, which is discarded when the connection ends.
        var sut = new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ZERO,
                true,
                null,
                false,
                new ReconnectBackoff(
                        Duration.ofNanos(3),
                        Duration.ofNanos(20),
                        new ReconnectBackoffTest.MaximumRandom()
                ),
                false
        );
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);
        sut.publish(2, MqttMessage.of(new Topic("t"), new byte[] {1}, Qos.AT_LEAST_ONCE, false));
        port.failingTimer = RECONNECT;
        var failure = new IOException("lost");

        // Act
        sut.failed(1, failure);

        // Assert: the pending publication fails before its session is discarded.
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause()).containsSame(failure)
        );
        assertThat(failure.getSuppressed()).containsExactly(port.timerFailure);
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(failure);
        assertThat(port.latestReconnectDelay).isNull();
        assertThat(port.requests.getLast()).isFalse();
    }

    @Test
    void a_handler_failure_cancels_waiting_and_active_automatic_attempts() {
        // Arrange
        SessionCore sut = core(false);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);
        sut.failed(1, new IOException("lost"));
        var failure = new IllegalArgumentException("handler");

        // Act
        sut.deliveryFailed(new DeliveryToken(sut.sessionNumber(), 1, 0), failure);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends.getLast().cause()).containsSame(failure);
        assertThat(port.endedAttempt).isEqualTo(2);
        assertThat(port.latestReconnectDelay).isNull();
        assertThat(port.timers).isEmpty();
        sut.connect(attempt(2), 0);
        connected(sut, 2, 0);
        sut.failed(2, new IOException("lost again"));
        sut.timer(0, RECONNECT, 0);
        sut.deliveryFailed(new DeliveryToken(sut.sessionNumber(), 2, 0), failure);

        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends.getLast().cause()).containsSame(failure);
        assertThat(port.endedAttempt).isEqualTo(4);
        assertThat(port.attempts).containsExactly(1L, 3L, 4L);
        assertThat(port.latestReconnectDelay).isNull();
        assertThat(port.timers).isEmpty();
    }

    @Test
    void a_scheduler_failure_while_waiting_reports_the_ending_without_another_retry() {
        // Arrange
        SessionCore sut = core(true);
        sut.connect(attempt(1), 0);
        sut.failed(1, new IOException("offline"));
        var failure = new IllegalStateException("clock failed");

        // Act
        sut.schedulerFailed(0, failure);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).hasSize(2);
        assertThat(port.ends.getLast().cause()).containsSame(failure);
        assertThat(port.latestReconnectDelay).isNull();
        assertThat(port.timers).isEmpty();
        assertThat(requireNonNull(port.results.get(1L)).failure()).hasCause(failure);
    }

    @Test
    void each_automatic_attempt_evaluates_credentials_and_stopping_cancels_the_evaluation() {
        // Arrange
        var sut = new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ZERO,
                false,
                null,
                true,
                new ReconnectBackoff(
                        Duration.ofNanos(3),
                        Duration.ofNanos(20),
                        new ReconnectBackoffTest.MaximumRandom()
                ),
                true
        );
        ConnectionAttempt first = attempt(1);
        sut.connect(first, 0);
        sut.credentials(first, null, new NullPointerException("credentials"), 0);

        // Act
        sut.timer(0, RECONNECT, 3);
        sut.deliveryFailed(
                new DeliveryToken(sut.sessionNumber(), 1, 0),
                new IllegalArgumentException("handler")
        );

        // Assert
        assertThat(port.cancellations).hasSize(2);
        assertThat(port.evaluations).containsExactlyElementsOf(port.cancellations);
        assertThat(port.cancellations.getFirst()).isSameAs(first);
        assertThat(port.cancellations.getLast()).isNotSameAs(first);
        assertThat(port.cancellations.getLast().operation).isZero();
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.timers).isEmpty();
    }

    @Test
    void scheduler_failure_stops_even_an_initial_retry_request() {
        // Arrange
        SessionCore sut = core(true);
        sut.connect(attempt(1), 0);
        var failure = new IllegalStateException("scheduler closed");

        // Act
        sut.schedulerFailed(1, failure);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends.getLast().cause()).containsSame(failure);
        assertThat(port.timers).isEmpty();
        assertThat(requireNonNull(port.results.get(1L)).failure()).hasCause(failure);
    }

    @ParameterizedTest
    @MethodSource("seeds")
    void withdrawal_and_close_cannot_leave_a_late_retry_running(int seed) {
        // Arrange
        SessionCore sut = core(true);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);
        sut.failed(1, new IOException("lost"));
        sut.timer(0, RECONNECT, 3);
        connected(sut, 2, 3);
        sut.failed(2, new IOException("lost again"));
        var actions = new ArrayList<Runnable>(List.of(
                () -> sut.timer(0, RECONNECT, 9),
                () -> sut.disconnect(2),
                () -> sut.close(null),
                () -> connected(sut, 2, 3),
                () -> sut.failed(2, new IOException("late"))
        ));
        Collections.shuffle(actions, new Random(seed));

        // Act
        actions.forEach(Runnable::run);
        sut.timer(0, RECONNECT, Long.MAX_VALUE);

        // Assert
        assertThat(sut.state()).isEqualTo(CLOSED);
        assertThat(port.attempts).hasSizeLessThanOrEqualTo(3);
        assertThat(port.timers).isEmpty();
        assertThat(port.requests.getLast()).isFalse();
        assertThat(port.results).containsKeys(1L, 2L);
    }

    private static IntStream seeds() {
        return IntStream.range(0, 100);
    }

    private SessionCore core(boolean initialRetry) {
        return new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ZERO,
                false,
                null,
                false,
                new ReconnectBackoff(
                        Duration.ofNanos(3),
                        Duration.ofNanos(20),
                        new ReconnectBackoffTest.MaximumRandom()
                ),
                initialRetry
        );
    }

    private static ConnectionAttempt attempt(long operation) {
        return new ConnectionAttempt(operation, null);
    }

    private static void connected(SessionCore sut, long generation, long now) {
        sut.opened(generation);
        sut.received(generation, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), now, 4);
    }
}
