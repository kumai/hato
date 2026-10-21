package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttBrokerDisconnectException;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.Result;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.SessionCore.State.*;
import static org.mqtt4j.hato.client.internal.SessionCore.Timer.*;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class ReconnectionTest {

    private final SessionPersistenceTest.RecordingPort port =
            new SessionPersistenceTest.RecordingPort();

    @ParameterizedTest(name = "{0} reconnects: {1}")
    @CsvSource({
            "MALFORMED_PACKET,false",
            "PROTOCOL_ERROR,false",
            "UNSUPPORTED_PROTOCOL_VERSION,false",
            "CLIENT_IDENTIFIER_NOT_VALID,false",
            "BANNED,false",
            "BAD_AUTHENTICATION_METHOD,false",
            "SESSION_TAKEN_OVER,false",
            "TOPIC_FILTER_INVALID,false",
            "TOPIC_NAME_INVALID,false",
            "RECEIVE_MAXIMUM_EXCEEDED,false",
            "TOPIC_ALIAS_INVALID,false",
            "PACKET_TOO_LARGE,false",
            "ADMINISTRATIVE_ACTION,false",
            "PAYLOAD_FORMAT_INVALID,false",
            "RETAIN_NOT_SUPPORTED,false",
            "QOS_NOT_SUPPORTED,false",
            "USE_ANOTHER_SERVER,false",
            "SERVER_MOVED,false",
            "SHARED_SUBSCRIPTIONS_NOT_SUPPORTED,false",
            "SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED,false",
            "WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED,false",
            "SUCCESS,true",
            "UNSPECIFIED_ERROR,true",
            "IMPLEMENTATION_SPECIFIC_ERROR,true",
            "BAD_USER_NAME_OR_PASSWORD,true",
            "NOT_AUTHORIZED,true",
            "SERVER_UNAVAILABLE,true",
            "SERVER_BUSY,true",
            "SERVER_SHUTTING_DOWN,true",
            "KEEP_ALIVE_TIMEOUT,true",
            "MESSAGE_RATE_TOO_HIGH,true",
            "QUOTA_EXCEEDED,true",
            "CONNECTION_RATE_EXCEEDED,true",
            "MAXIMUM_CONNECT_TIME,true"
    })
    void broker_reason_codes_follow_one_reconnection_table(String name, boolean reconnects) {
        // Arrange: a CONNACK refusal and an MQTT 5.0 DISCONNECT with the same reason code.
        var reason = ReasonCode.valueOf(name);
        var refusal = new MqttConnectException("Refused", reason);
        var ending = new MqttBrokerDisconnectException(reason, null, null);

        // Act & Assert
        assertThat(ConnectionRequest.reconnectable(refusal, RetryRules.DEFAULT))
                .isEqualTo(reconnects);
        assertThat(ConnectionRequest.reconnectable(ending, RetryRules.DEFAULT))
                .isEqualTo(reconnects);
    }

    @ParameterizedTest
    @EnumSource(ReasonCode.class)
    void retrying_administrative_actions_changes_no_other_reason_code(ReasonCode reason) {
        // Arrange: MQTT 5.0 brokers send 0x98 only in DISCONNECT.
        var ending = new MqttBrokerDisconnectException(reason, null, null);
        boolean byDefault = ConnectionRequest.reconnectable(ending, RetryRules.DEFAULT);

        // Act
        boolean reconnects = ConnectionRequest.reconnectable(
                ending,
                RetryRules.DEFAULT.withAdministrativeActionRetry()
        );

        // Assert
        assertThat(reconnects).isEqualTo(reason == ReasonCode.ADMINISTRATIVE_ACTION || byDefault);
    }

    @ParameterizedTest
    @EnumSource(ReasonCode.class)
    void stopping_at_unspecified_errors_changes_no_other_reason_code(ReasonCode reason) {
        // Arrange: MQTT 5.0 brokers send 0x80 in CONNACK and in DISCONNECT.
        var refusal = new MqttConnectException("Refused", reason);
        var ending = new MqttBrokerDisconnectException(reason, null, null);
        boolean expected = reason != ReasonCode.UNSPECIFIED_ERROR
                && ConnectionRequest.reconnectable(ending, RetryRules.DEFAULT);
        RetryRules rules = RetryRules.DEFAULT.withoutUnspecifiedErrorRetry();

        // Act & Assert
        assertThat(ConnectionRequest.reconnectable(refusal, rules)).isEqualTo(expected);
        assertThat(ConnectionRequest.reconnectable(ending, rules)).isEqualTo(expected);
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

    @Test
    void a_policy_that_stops_at_unspecified_errors_ends_even_an_initial_retry_request() {
        // Arrange
        SessionCore sut = core(
                MqttVersion.V5,
                RetryRules.DEFAULT.withInitialRetry().withoutUnspecifiedErrorRetry()
        );
        sut.connect(attempt(1), 0);
        sut.opened(1);

        // Act
        sut.received(1, new Packet.ConnAck(false, ReasonCode.UNSPECIFIED_ERROR, EMPTY), 0, 4);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.timers).doesNotContainKey(RECONNECT);
        assertThat(requireNonNull(port.results.get(1L)).failure()).isInstanceOfSatisfying(
                MqttConnectException.class,
                refusal -> assertThat(refusal.reasonCode()).contains(ReasonCode.UNSPECIFIED_ERROR)
        );
    }

    @Test
    void a_broker_that_hands_the_session_to_another_client_stops_reconnection() {
        // Arrange: an MQTT 5.0 connection with a QoS 1 publication waiting for PUBACK.
        SessionCore sut = core(MqttVersion.V5, false);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);
        sut.publish(2, message(Qos.AT_LEAST_ONCE));

        // Act
        sut.received(1, new Packet.Disconnect(
                ReasonCode.SESSION_TAKEN_OVER,
                properties(new Property.StringValue(PropertyId.REASON_STRING, "taken over"))
        ), 0, 4);

        // Assert: the publication fails with the broker's reason as the cause of the ending.
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        Disconnection end = port.ends.getLast();
        assertThat(end.reconnectDelay()).isEmpty();
        Throwable cause = end.cause().orElseThrow();
        assertThat(cause).isInstanceOfSatisfying(MqttBrokerDisconnectException.class, ending -> {
            assertThat(ending.reasonCode()).isEqualTo(ReasonCode.SESSION_TAKEN_OVER);
            assertThat(ending.reasonString()).contains("taken over");
        });
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(cause);
    }

    @Test
    void a_broker_shutting_down_is_reconnected_with_the_session_kept() {
        // Arrange: a persistent MQTT 5.0 session with a QoS 1 publication waiting for PUBACK.
        SessionCore sut = core(MqttVersion.V5, false);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);
        sut.publish(2, message(Qos.AT_LEAST_ONCE));

        // Act
        sut.received(1, new Packet.Disconnect(ReasonCode.SERVER_SHUTTING_DOWN, EMPTY), 0, 4);

        // Assert: the publication waits for the next connection of the session.
        assertThat(sut.state()).isEqualTo(RECONNECT_WAIT);
        assertThat(port.ends.getLast().reconnectDelay()).isPresent();
        assertThat(port.results).doesNotContainKey(2L);
    }

    @Test
    void a_policy_that_stops_at_unspecified_errors_fails_the_waiting_publication() {
        // Arrange: a persistent MQTT 5.0 session with a QoS 1 publication waiting for PUBACK.
        SessionCore sut = core(MqttVersion.V5, RetryRules.DEFAULT.withoutUnspecifiedErrorRetry());
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);
        sut.publish(2, message(Qos.AT_LEAST_ONCE));

        // Act
        sut.received(1, new Packet.Disconnect(ReasonCode.UNSPECIFIED_ERROR, EMPTY), 0, 4);

        // Assert: no attempt follows, so the publication's outcome stays unknown.
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.timers).doesNotContainKey(RECONNECT);
        Disconnection end = port.ends.getLast();
        assertThat(end.reconnectDelay()).isEmpty();
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(end.cause().orElseThrow());
    }

    @Test
    void a_redirecting_broker_names_another_server_that_the_client_does_not_follow() {
        // Arrange
        SessionCore sut = core(MqttVersion.V5, false);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);

        // Act
        sut.received(1, new Packet.Disconnect(
                ReasonCode.SERVER_MOVED,
                properties(new Property.StringValue(PropertyId.SERVER_REFERENCE, "other:1883"))
        ), 0, 4);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.timers).doesNotContainKey(RECONNECT);
        assertThat(port.ends.getLast().cause()).get().isInstanceOfSatisfying(
                MqttBrokerDisconnectException.class,
                ending -> assertThat(ending.serverReference()).contains("other:1883")
        );
    }

    @Test
    void an_mqtt_3_1_1_broker_that_sends_disconnect_breaks_the_protocol() {
        // Arrange
        SessionCore sut = core(false);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);

        // Act
        sut.received(1, new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY), 0, 4);

        // Assert: MQTT 3.1.1 has no broker DISCONNECT, so it ends the connection as before.
        assertThat(sut.state()).isEqualTo(RECONNECT_WAIT);
        assertThat(port.ends.getLast().cause()).get().isInstanceOf(MqttProtocolException.class);
    }

    @Test
    void an_mqtt_5_connack_refusal_carries_its_explanation_and_server_reference() {
        // Arrange: the request retries initial failures that the broker does not rule out.
        SessionCore sut = core(MqttVersion.V5, true);
        sut.connect(attempt(1), 0);
        sut.opened(1);

        // Act
        sut.received(1, new Packet.ConnAck(false, ReasonCode.USE_ANOTHER_SERVER, properties(
                new Property.StringValue(PropertyId.REASON_STRING, "draining"),
                new Property.StringValue(PropertyId.SERVER_REFERENCE, "other:1883")
        )), 0, 4);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(requireNonNull(port.results.get(1L)).failure()).isInstanceOfSatisfying(
                MqttConnectException.class,
                refusal -> {
                    assertThat(refusal.reasonCode()).contains(ReasonCode.USE_ANOTHER_SERVER);
                    assertThat(refusal.reasonString()).contains("draining");
                    assertThat(refusal.serverReference()).contains("other:1883");
                }
        );
    }

    @ParameterizedTest
    @MethodSource("retryableFailures")
    void local_connection_failures_allow_retry(Throwable failure) {
        assertThat(ConnectionRequest.reconnectable(failure, RetryRules.DEFAULT)).isTrue();
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
    void a_failed_retry_reservation_stops_within_the_same_ending() {
        // Arrange: a nonpersistent session, which is discarded when the connection ends.
        var sut = new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ZERO,
                MqttVersion.V3_1_1,
                Duration.ZERO,
                16 * 1024 * 1024,
                null,
                false,
                new ReconnectBackoff(
                        Duration.ofNanos(3),
                        Duration.ofNanos(20),
                        new ReconnectBackoffTest.MaximumRandom()
                ),
                RetryRules.DEFAULT,
                Duration.ofSeconds(60),
                ClientLimits.DEFAULT
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
        sut.deliveryFailed(
                new DeliveryToken(sut.sessionNumber(), 1, 1, 0, DeliveryToken.UNTIMED),
                failure
        );

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
        sut.deliveryFailed(
                new DeliveryToken(sut.sessionNumber(), 2, 2, 0, DeliveryToken.UNTIMED),
                failure
        );

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
        Throwable connectFailure = requireNonNull(port.results.get(1L)).failure();
        assertThat(connectFailure).hasCause(failure);
        assertThat(port.ends.getLast().cause()).containsSame(connectFailure);
        assertThat(port.latestReconnectDelay).isNull();
        assertThat(port.timers).isEmpty();
    }

    @Test
    void each_automatic_attempt_evaluates_credentials_and_stopping_cancels_the_evaluation() {
        // Arrange
        var sut = new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ZERO,
                MqttVersion.V3_1_1,
                Duration.ofHours(1),
                16 * 1024 * 1024,
                null,
                true,
                new ReconnectBackoff(
                        Duration.ofNanos(3),
                        Duration.ofNanos(20),
                        new ReconnectBackoffTest.MaximumRandom()
                ),
                RetryRules.DEFAULT.withInitialRetry(),
                Duration.ofSeconds(60),
                ClientLimits.DEFAULT
        );
        ConnectionAttempt first = attempt(1);
        sut.connect(first, 0);
        sut.credentials(first, null, new NullPointerException("credentials"), 0);

        // Act
        sut.timer(0, RECONNECT, 3);
        sut.deliveryFailed(
                new DeliveryToken(sut.sessionNumber(), 1, 1, 0, DeliveryToken.UNTIMED),
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
        Throwable connectFailure = requireNonNull(port.results.get(1L)).failure();
        assertThat(connectFailure).hasCause(failure);
        assertThat(port.ends.getLast().cause()).containsSame(connectFailure);
        assertThat(port.timers).isEmpty();
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

    @Test
    void a_scheduled_reconnection_fails_qos_zero_and_continues_qos_one_on_session_present() {
        // Arrange: a QoS 0 write and a QoS 1 acknowledgement are outstanding.
        SessionCore sut = core(false);
        int id = holdBothAcrossReconnection(sut);

        // Act
        sut.timer(0, RECONNECT, 3);
        sut.opened(2);
        sut.received(2, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 3, 4);

        // Assert: QoS 0 belonged to the ended connection; QoS 1 waits for its PUBACK.
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class);
        assertThat(port.accepted.getLast().packetId()).isEqualTo(id);
        assertThat(port.results).doesNotContainKey(3L);
        sut.received(2, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 3, 4);
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
    }

    @Test
    void a_reconnection_without_session_present_fails_the_held_qos_one_publication() {
        // Arrange
        SessionCore sut = core(false);
        holdBothAcrossReconnection(sut);

        // Act
        sut.timer(0, RECONNECT, 3);
        sut.opened(2);
        sut.received(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 3, 4);

        // Assert
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(MqttConnectionLostException.class);
    }

    @Test
    void a_held_publication_survives_a_failed_automatic_attempt() {
        // Arrange: the first automatic attempt fails before CONNACK and is retried.
        SessionCore sut = core(false);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);
        sut.publish(2, message(Qos.AT_LEAST_ONCE));
        int id = port.accepted.getLast().packetId();
        sut.failed(1, new IOException("lost"));
        sut.timer(0, RECONNECT, 3);
        sut.opened(2);
        sut.failed(2, new IOException("refused at once"));
        assertThat(port.results).doesNotContainKey(2L);

        // Act
        sut.timer(0, RECONNECT, 10);
        sut.opened(3);
        sut.received(3, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 10, 4);
        sut.received(3, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 10, 4);

        // Assert: the recording port rejects a second completion.
        assertThat(port.accepted.getLast().packetId()).isEqualTo(id);
        assertThat(requireNonNull(port.results.get(2L)).failure()).isNull();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("waysToGiveUp")
    void every_way_of_giving_up_a_reconnection_fails_the_held_publication(GiveUp ending) {
        // Arrange: a persistent session holds a QoS 1 publication while waiting to reconnect.
        SessionCore sut = core(false);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);
        sut.publish(2, message(Qos.AT_LEAST_ONCE));
        sut.failed(1, new IOException("lost"));
        assertThat(port.results).doesNotContainKey(2L);

        // Act
        ending.action().accept(sut, port);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class);
        assertThat(port.timers).doesNotContainKey(RECONNECT);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void a_held_publication_completes_once_across_reconnection_withdrawal_and_late_events(
            int seed
    ) {
        // Arrange: a held QoS 1 publication meets a reconnection, a withdrawal, and late events
        // of its original connection in a seed-chosen order.
        var random = new Random(seed);
        SessionCore sut = core(false);
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);
        sut.publish(10, message(Qos.AT_LEAST_ONCE));
        int id = port.accepted.getLast().packetId();
        sut.sent(1, port.ticket, 0);
        sut.failed(1, new IOException("lost"));
        var acknowledgedOnNext = new boolean[1];
        var chains = new ArrayList<ArrayDeque<Runnable>>();
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.timer(0, RECONNECT, 3),
                () -> sut.opened(2),
                () -> sut.received(2, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 3, 4),
                () -> {
                    boolean before = port.results.containsKey(10L);
                    sut.received(2, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 3, 4);
                    acknowledgedOnNext[0] = !before && port.results.containsKey(10L);
                }
        )));
        chains.add(new ArrayDeque<>(List.of(
                random.nextBoolean() ? () -> sut.disconnect(20) : () -> sut.close(null)
        )));
        chains.add(new ArrayDeque<>(List.of(
                () -> {
                    boolean before = port.results.containsKey(10L);
                    sut.received(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0, 4);
                    assertThat(port.results.containsKey(10L)).isEqualTo(before);
                },
                () -> sut.failed(1, new IOException("late"))
        )));

        // Act
        while (!chains.isEmpty()) {
            ArrayDeque<Runnable> chain = chains.get(random.nextInt(chains.size()));
            chain.removeFirst().run();
            if (chain.isEmpty()) {
                chains.remove(chain);
            }
        }
        sut.close(null);

        // Assert: the recording port rejects a second completion, and only the next connection's
        // PUBACK can complete the publication.
        Result outcome = requireNonNull(port.results.get(10L));
        if (outcome.failure() == null) {
            assertThat(acknowledgedOnNext[0]).isTrue();
        } else {
            assertThat(outcome.failure()).isInstanceOf(MqttConnectionLostException.class);
        }
        assertThat(sut.state()).isEqualTo(CLOSED);
        assertThat(port.timers).isEmpty();
    }

    // Leaves a QoS 0 write (operation 2) and a QoS 1 acknowledgement (operation 3) outstanding
    // when the connection is lost, and returns the QoS 1 packet identifier.
    private int holdBothAcrossReconnection(SessionCore sut) {
        sut.connect(attempt(1), 0);
        connected(sut, 1, 0);
        sut.publish(2, message(Qos.AT_MOST_ONCE));
        sut.publish(3, message(Qos.AT_LEAST_ONCE));
        int id = port.accepted.getLast().packetId();
        sut.failed(1, new IOException("lost"));
        return id;
    }

    private static MqttMessage message(Qos qos) {
        return MqttMessage.of(new Topic("held"), new byte[] {1}, qos, false);
    }

    private static Stream<GiveUp> waysToGiveUp() {
        return Stream.of(
                new GiveUp("disconnect while waiting", (sut, _) -> sut.disconnect(3)),
                new GiveUp("close while waiting", (sut, _) -> sut.close(null)),
                new GiveUp("handler failure while waiting", (sut, _) -> failHandler(sut)),
                new GiveUp(
                        "scheduler failure while waiting",
                        (sut, _) -> sut.schedulerFailed(0, new IllegalStateException("clock"))
                ),
                new GiveUp("refusal of the automatic attempt", (sut, _) -> {
                    var refusal = new Packet.ConnAck(
                            false,
                            ReasonCode.UNSUPPORTED_PROTOCOL_VERSION,
                            EMPTY
                    );
                    sut.timer(0, RECONNECT, 3);
                    sut.opened(2);
                    sut.received(2, refusal, 3, 4);
                }),
                new GiveUp("disconnect during the automatic attempt", (sut, port) -> {
                    sut.timer(0, RECONNECT, 3);
                    sut.opened(2);
                    sut.disconnect(3);
                    sut.received(2, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 3, 4);
                    sut.sent(2, port.ticket, 4);
                }),
                new GiveUp("handler failure during the automatic attempt", (sut, _) -> {
                    sut.timer(0, RECONNECT, 3);
                    sut.opened(2);
                    failHandler(sut);
                })
        );
    }

    private static void failHandler(SessionCore sut) {
        sut.deliveryFailed(
                new DeliveryToken(sut.sessionNumber(), 1, 1, 0, DeliveryToken.UNTIMED),
                new IllegalStateException("handler")
        );
    }

    private static IntStream seeds() {
        return IntStream.range(0, 100);
    }

    private SessionCore core(boolean initialRetry) {
        return core(MqttVersion.V3_1_1, initialRetry);
    }

    /// A core of a persistent session that reconnects after a nanosecond-scale backoff.
    private SessionCore core(MqttVersion version, boolean initialRetry) {
        return core(
                version,
                initialRetry ? RetryRules.DEFAULT.withInitialRetry() : RetryRules.DEFAULT
        );
    }

    private SessionCore core(MqttVersion version, RetryRules rules) {
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
                new ReconnectBackoff(
                        Duration.ofNanos(3),
                        Duration.ofNanos(20),
                        new ReconnectBackoffTest.MaximumRandom()
                ),
                rules,
                Duration.ofSeconds(60),
                ClientLimits.DEFAULT
        );
    }

    private static ConnectionAttempt attempt(long operation) {
        return new ConnectionAttempt(operation);
    }

    private static Properties properties(Property... properties) {
        return new Properties(List.of(properties));
    }

    private static void connected(SessionCore sut, long generation, long now) {
        sut.opened(generation);
        sut.received(generation, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), now, 4);
    }

    /// One way to give up a waiting reconnection, named for the test display.
    record GiveUp(
            String name,
            BiConsumer<SessionCore, SessionPersistenceTest.RecordingPort> action
    ) {

        @Override
        public String toString() {
            return name;
        }
    }
}
