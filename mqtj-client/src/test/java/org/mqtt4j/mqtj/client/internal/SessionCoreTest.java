package org.mqtt4j.mqtj.client.internal;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.mqtt4j.mqtj.ConnAck;
import org.mqtt4j.mqtj.Disconnection;
import org.mqtt4j.mqtj.MqttCapacityException;
import org.mqtt4j.mqtj.MqttConnectException;
import org.mqtt4j.mqtj.MqttConnectionLostException;
import org.mqtt4j.mqtj.MqttNotConnectedException;
import org.mqtt4j.mqtj.MqttProtocolException;
import org.mqtt4j.mqtj.ReasonCode;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketDecodeException;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.mqtj.client.internal.SessionCore.State.CLOSED;
import static org.mqtt4j.mqtj.client.internal.SessionCore.State.CONNECTED;
import static org.mqtt4j.mqtj.client.internal.SessionCore.State.CONNECTING;
import static org.mqtt4j.mqtj.client.internal.SessionCore.State.DISCONNECTED;
import static org.mqtt4j.mqtj.client.internal.SessionCore.Timer.CONNECT;
import static org.mqtt4j.mqtj.client.internal.SessionCore.Timer.KEEP_ALIVE;
import static org.mqtt4j.mqtj.client.internal.SessionCore.Timer.PING;
import static org.mqtt4j.mqtj.client.internal.common.protocol.Properties.EMPTY;

class SessionCoreTest {

    private final RecordingPort port = new RecordingPort();
    private final SessionCore sut = new SessionCore(
            port,
            "client",
            Duration.ofSeconds(30),
            Duration.ofSeconds(10)
    );

    @Test
    void connect_waits_for_connack_and_rejects_a_second_attempt() {
        // Arrange
        sut.connect(1, 0);

        // Act
        sut.opened(1);
        sut.connect(2, 0);

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTING);
        assertThat(port.results).doesNotContainKey(1L);
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(IllegalStateException.class);
        assertThat(port.sent).singleElement().isInstanceOf(Packet.Connect.class);
        assertThat(port.opened).containsExactly(1L);

        accept();
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(requireNonNull(port.results.get(1L)).value()).isEqualTo(new ConnAck(false));
        assertThat(port.timers).doesNotContainKey(CONNECT);
    }

    @Test
    void refusal_preserves_the_broker_reason_and_does_not_notify_the_listener() {
        // Arrange
        sut.connect(1, 0);

        // Act
        sut.received(1, new Packet.ConnAck(false, ReasonCode.NOT_AUTHORIZED, EMPTY), 0);

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .hasNoCause()
                .isInstanceOfSatisfying(
                        MqttConnectException.class,
                        failure -> assertThat(failure.reasonCode())
                                .contains(ReasonCode.NOT_AUTHORIZED)
                );
        assertThat(port.established).containsExactly(false);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void refusing_connect_for_capacity_fails_only_the_connection_attempt() {
        // Arrange
        sut.connect(1, 0);
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act
        sut.opened(1);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasCauseInstanceOf(MqttCapacityException.class);
        assertThat(port.established).containsExactly(false);
        assertThat(port.timers).isEmpty();
        assertThat(port.sent).isEmpty();
    }

    @Test
    void refusing_ping_for_capacity_ends_the_connection_without_registering_another_timer() {
        // Arrange
        connected();
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act
        sut.timer(1, KEEP_ALIVE, seconds(10));

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause()).isInstanceOf(MqttCapacityException.class);
            assertThat(end.userRequested()).isFalse();
        });
        assertThat(port.timers).isEmpty();
        assertThat(port.sent).isEmpty();
    }

    @Test
    void refusing_disconnect_for_capacity_fails_the_operation_and_ends_the_connection() {
        // Arrange
        connected();
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act
        sut.disconnect(2);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        Throwable cause = port.ends.getFirst().cause();
        assertThat(cause).isInstanceOf(MqttCapacityException.class);
        assertThat(port.ends).containsExactly(new Disconnection(cause, true));
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(cause);
        assertThat(port.timers).isEmpty();
        assertThat(port.sent).isEmpty();
    }

    @Test
    void a_closed_queue_leaves_the_pending_operation_for_the_original_termination_cause() {
        // Arrange
        connected();
        port.sendResult = SendQueue.OfferResult.CLOSED;
        var failure = new IOException("Peer reset the connection");

        // Act
        sut.disconnect(2);

        // Assert: refusal alone neither fails the operation nor reports a capacity failure.
        assertThat(port.results).doesNotContainKey(2L);
        assertThat(port.ends).isEmpty();
        assertThat(port.sent).isEmpty();

        // Act
        sut.failed(1, failure);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(failure);
        assertThat(port.ends).containsExactly(new Disconnection(failure, true));
    }

    @Test
    void a_packet_before_connack_and_a_present_clean_session_are_protocol_errors() {
        // Arrange
        sut.connect(1, 0);

        // Act
        sut.received(1, new Packet.PingResp(), 0);

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class);
        sut.connect(2, 0);
        sut.received(2, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 0);
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectException.class);
        assertThat(port.established).containsExactly(false, false);
    }

    @Test
    void the_connect_deadline_and_old_connection_events_cannot_affect_a_new_attempt() {
        // Arrange
        sut.connect(1, 0);
        sut.opened(1);

        // Act
        sut.timer(1, CONNECT, seconds(30));

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .hasCauseInstanceOf(TimeoutException.class)
                .hasRootCauseMessage("CONNACK deadline expired")
                .isInstanceOfSatisfying(
                        MqttConnectException.class,
                        failure -> assertThat(failure.reasonCode()).isEmpty()
                );

        // Act: deliver stale events after starting another connection attempt.
        sut.connect(2, seconds(30));
        sut.opened(1);
        sut.timer(1, CONNECT, seconds(60));
        sut.failed(1, new IllegalStateException("old reader"));
        sut.received(1, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), seconds(60));

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTING);
        assertThat(port.results).doesNotContainKey(2L);

        // Act: the new transport has not connected, regardless of the old one's phase.
        sut.timer(2, CONNECT, seconds(60));

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasCauseInstanceOf(TimeoutException.class)
                .hasRootCauseMessage("Transport connection deadline expired");
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void sends_move_the_idle_deadline_without_rescheduling_on_every_send() {
        // Arrange: an established connection with a ten-second keep-alive.
        connected();

        // Act
        sut.sent(1, 1, seconds(6));

        // Assert
        assertThat(port.timers.get(KEEP_ALIVE)).isEqualTo(Duration.ofSeconds(10));
        sut.timer(1, KEEP_ALIVE, seconds(10));
        assertThat(port.sent).isEmpty();
        assertThat(port.timers.get(KEEP_ALIVE)).isEqualTo(Duration.ofSeconds(6));
        sut.timer(1, KEEP_ALIVE, seconds(16));
        assertThat(port.sent).singleElement().isInstanceOf(Packet.PingReq.class);
        assertThat(port.timers.get(PING)).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void pingresp_can_arrive_before_the_write_completion() {
        // Arrange: an established connection with a ten-second keep-alive.
        connected();

        // Act
        sut.timer(1, KEEP_ALIVE, seconds(10));
        sut.received(1, new Packet.PingResp(), seconds(10));
        sut.sent(1, port.ticket, seconds(10));
        sut.timer(1, PING, seconds(20));

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.timers).doesNotContainKey(PING);
    }

    @Test
    void unsolicited_and_duplicate_pingresp_do_not_end_the_connection_or_delay_the_next_ping() {
        // Arrange
        connected();

        // Act: an unsolicited response does not move the send-based keep-alive deadline.
        sut.received(1, new Packet.PingResp(), seconds(9));
        sut.timer(1, KEEP_ALIVE, seconds(10));

        // Assert
        assertThat(port.sent).singleElement().isInstanceOf(Packet.PingReq.class);
        assertThat(port.timers).containsKey(PING);

        // Act: consume the response once, then tolerate a duplicate.
        sut.received(1, new Packet.PingResp(), seconds(10));
        sut.received(1, new Packet.PingResp(), seconds(11));
        sut.timer(1, PING, seconds(20));

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.ends).isEmpty();
        assertThat(port.timers).doesNotContainKey(PING);
        assertThat(port.ignoredResponses).isEqualTo(2);
    }

    @Test
    void missing_pingresp_ends_an_established_connection_once() {
        // Arrange: an established connection with a ten-second keep-alive.
        connected();

        // Act
        sut.timer(1, KEEP_ALIVE, seconds(10));
        sut.timer(1, PING, seconds(20));
        sut.failed(1, new IllegalStateException("reader also ended"));

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause()).isInstanceOf(TimeoutException.class);
            assertThat(end.userRequested()).isFalse();
        });
        assertThat(port.timers).isEmpty();
    }

    @Test
    void disconnect_waits_for_the_write_then_closes_without_more_sends() {
        // Arrange: an established connection with a ten-second keep-alive.
        connected();

        // Act
        sut.disconnect(2);
        long ticket = port.ticket;

        // Assert
        assertThat(port.results).doesNotContainKey(2L);
        sut.disconnect(3);
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(MqttNotConnectedException.class);
        sut.timer(1, KEEP_ALIVE, seconds(10));
        assertThat(port.sent).singleElement().isInstanceOf(Packet.Disconnect.class);
        sut.sent(1, ticket, seconds(1));
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(requireNonNull(port.results.get(2L)).failure()).isNull();
        assertThat(port.ends).containsExactly(new Disconnection(null, true));
    }

    @Test
    void a_packet_failure_is_shared_by_the_notification_and_the_pending_operation() {
        // Arrange
        connected();
        sut.disconnect(2);
        var original = new PacketDecodeException(ReasonCode.MALFORMED_PACKET, "Invalid packet");

        // Act
        sut.failed(1, original);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        Throwable cause = port.ends.getFirst().cause();
        assertThat(cause).isInstanceOfSatisfying(MqttProtocolException.class, failure -> {
            assertThat(failure.reasonCode()).isEqualTo(ReasonCode.MALFORMED_PACKET);
            assertThat(failure).hasMessage("Invalid packet").hasCause(original);
        });
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(cause);
    }

    @Test
    void close_is_terminal_idempotent_and_fails_pending_operations() {
        // Arrange
        sut.connect(1, 0);

        // Act
        sut.close(null);
        sut.close(null);
        sut.connect(2, 0);

        // Assert
        assertThat(sut.state()).isEqualTo(CLOSED);
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .hasNoCause()
                .isInstanceOfSatisfying(
                        MqttConnectException.class,
                        failure -> assertThat(failure.reasonCode()).isEmpty()
                );
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(IllegalStateException.class);
        assertThat(port.ends).hasSize(1);
    }

    @Test
    void close_fails_a_pending_disconnect_without_inventing_a_cause() {
        // Arrange
        connected();
        sut.disconnect(2);

        // Act
        sut.close(null);

        // Assert
        assertThat(sut.state()).isEqualTo(CLOSED);
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasNoCause();
        assertThat(port.ends).containsExactly(new Disconnection(null, true));
    }

    private void connected() {
        sut.connect(1, 0);
        accept();
    }

    private void accept() {
        sut.received(1, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
    }

    private static long seconds(long value) {
        return Duration.ofSeconds(value).toNanos();
    }

    private record Result(@Nullable ConnAck value, @Nullable Throwable failure) {
    }

    private static final class RecordingPort implements SessionCore.Port {

        final List<Packet> sent = new ArrayList<>();
        final List<Disconnection> ends = new ArrayList<>();
        final List<Boolean> established = new ArrayList<>();
        final Map<Long, Result> results = new HashMap<>();
        final Map<SessionCore.Timer, Duration> timers = new EnumMap<>(SessionCore.Timer.class);
        final List<Long> opened = new ArrayList<>();
        SendQueue.OfferResult sendResult = SendQueue.OfferResult.ACCEPTED;
        long ticket;
        int ignoredResponses;

        @Override
        public void open(long generation, long operation) {
            opened.add(generation);
        }

        @Override
        public SendQueue.OfferResult send(long generation, long id, Packet packet) {
            ticket = id;
            if (sendResult == SendQueue.OfferResult.ACCEPTED) {
                sent.add(packet);
            }
            return sendResult;
        }

        @Override
        public void schedule(long generation, SessionCore.Timer timer, Duration delay) {
            assertThat(opened).contains(generation);
            timers.put(timer, delay);
        }

        @Override
        public void cancel(SessionCore.Timer timer) {
            timers.remove(timer);
        }

        @Override
        public void ignoredPingResponse() {
            ignoredResponses++;
        }

        @Override
        public void complete(long operation, @Nullable ConnAck value, @Nullable Throwable failure) {
            assertThat(results.put(operation, new Result(value, failure))).isNull();
        }

        @Override
        public void end(long generation, boolean connected, Disconnection end) {
            established.add(connected);
            ends.add(end);
        }
    }
}
