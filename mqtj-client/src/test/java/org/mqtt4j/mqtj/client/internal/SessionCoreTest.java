package org.mqtt4j.mqtj.client.internal;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import com.sun.management.ThreadMXBean;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.mqtj.ConnAck;
import org.mqtt4j.mqtj.Disconnection;
import org.mqtt4j.mqtj.MqttCapacityException;
import org.mqtt4j.mqtj.MqttConnectException;
import org.mqtt4j.mqtj.MqttConnectionLostException;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.MqttNotConnectedException;
import org.mqtt4j.mqtj.MqttProtocolException;
import org.mqtt4j.mqtj.MqttSubscribeException;
import org.mqtt4j.mqtj.PublishResult;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.ReasonCode;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.Subscription;
import org.mqtt4j.mqtj.Topic;
import org.mqtt4j.mqtj.TopicFilter;
import org.mqtt4j.mqtj.client.internal.common.io.SendQueue;
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
        receive(1, new Packet.ConnAck(false, ReasonCode.NOT_AUTHORIZED, EMPTY), 0);

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
        receive(1, new Packet.PingResp(), 0);

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class);
        sut.connect(2, 0);
        receive(2, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 0);
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
        receive(1, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), seconds(60));

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
        receive(1, new Packet.PingResp(), seconds(10));
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
        receive(1, new Packet.PingResp(), seconds(9));
        sut.timer(1, KEEP_ALIVE, seconds(10));

        // Assert
        assertThat(port.sent).singleElement().isInstanceOf(Packet.PingReq.class);
        assertThat(port.timers).containsKey(PING);

        // Act: consume the response once, then tolerate a duplicate.
        receive(1, new Packet.PingResp(), seconds(10));
        receive(1, new Packet.PingResp(), seconds(11));
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

    @Test
    void publish_passes_the_message_and_completes_only_after_its_write() {
        // Arrange
        connected();
        var payload = new byte[] {1, 2};
        var message = new MqttMessage(new Topic("a/b"), payload, Qos.AT_MOST_ONCE, true);

        // Act
        sut.publish(2, message);
        long ticket = port.ticket;
        payload[0] = 9;
        message.payload()[1] = 9;

        // Assert: queue admission does not complete a QoS 0 publish or expose its payload.
        assertThat(port.results).doesNotContainKey(2L);
        assertThat(port.publications).singleElement().isSameAs(message);
        assertThat(port.publications.getFirst().payload()).containsExactly(1, 2);
        assertThat(port.sent).isEmpty();

        // Act: an unrelated write completion cannot complete the publication.
        sut.sent(1, ticket + 1, 0);

        // Assert
        assertThat(port.results).doesNotContainKey(2L);

        // Act: the matching write completes once, even when its notification is repeated.
        sut.sent(1, ticket, 0);
        sut.sent(1, ticket, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(new PublishResult());
    }

    @Test
    void publish_and_subscribe_fail_without_a_connection_or_after_disconnect_starts() {
        // Act: the client has not established a connection.
        sut.publish(2, message());
        sut.subscribe(3, subscription("a/#"));

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttNotConnectedException.class);
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(MqttNotConnectedException.class);

        // Arrange: establish a connection, then start its graceful shutdown.
        connected();
        sut.disconnect(4);

        // Act
        sut.publish(5, message());
        sut.subscribe(6, subscription("a/#"));

        // Assert
        assertThat(requireNonNull(port.results.get(5L)).failure())
                .isInstanceOf(MqttNotConnectedException.class);
        assertThat(requireNonNull(port.results.get(6L)).failure())
                .isInstanceOf(MqttNotConnectedException.class);
        assertThat(port.registered).isEmpty();
        assertThat(port.publications).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_LEAST_ONCE", "EXACTLY_ONCE"})
    void unsupported_qos_is_rejected_without_sending_or_registering(Qos qos) {
        // Arrange
        connected();

        // Act
        sut.publish(2, new MqttMessage(new Topic("a"), new byte[0], qos, false));
        sut.subscribe(3, new Subscribe(new TopicFilter("a"), qos, 1));

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(port.sent).isEmpty();
        assertThat(port.registered).isEmpty();
        assertThat(port.publications).isEmpty();
    }

    @Test
    void matching_deliveries_reach_all_pending_and_active_subscriptions() {
        // Arrange: overlapping ordinary filters match the same topic.
        connected();
        sut.subscribe(2, subscription("a/#"));
        int firstId = ((Packet.Subscribe) port.sent.getLast()).packetId();
        sut.subscribe(3, subscription("a/+"));
        sut.subscribe(4, subscription("a/b"));
        sut.subscribe(5, subscription("other/#"));

        // Act: MQTT 3.1.1 section 3.8.4 lets the broker deliver a PUBLISH before SUBACK.
        sut.received(1, publication(), 0, 19);

        // Assert
        assertThat(port.registered).containsExactly(2L, 3L, 4L, 5L);
        assertThat(port.deliveries).singleElement().satisfies(delivery -> {
            assertThat(delivery.ids()).containsExactly(2L, 3L, 4L);
            assertThat(delivery.bytes()).isEqualTo(19);
            assertThat(delivery.message()).isEqualTo(message());
        });
        assertThat(port.results).doesNotContainKeys(2L, 3L, 4L, 5L);

        // Act: SUBACK arrives before the writer's completion notification.
        receive(1, new Packet.SubAck(firstId, List.of(ReasonCode.SUCCESS), EMPTY), 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).value())
                .isInstanceOfSatisfying(Subscription.class, value -> {
                    assertThat(value.filter()).isEqualTo(new TopicFilter("a/#"));
                    assertThat(value.grantedQos()).isEqualTo(Qos.AT_MOST_ONCE);
                });

        // Act: later deliveries still reach the established subscription.
        sut.sent(1, 1, 0);
        receive(1, publication(), 0);

        // Assert
        assertThat(port.deliveries).hasSize(2);
    }

    @Test
    void shared_deliveries_match_the_inner_filter_before_and_after_suback() {
        // Arrange: unrelated ordinary and shared subscriptions remain independent.
        connected();
        sut.subscribe(2, subscription("$share/group/a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        sut.subscribe(3, subscription("$share/other/other/#"));
        sut.subscribe(4, subscription("third/#"));

        // Act
        receive(1, publication(), 0);
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        receive(1, publication(), 0);

        // Assert
        assertThat(port.registered).containsExactly(2L, 3L, 4L);
        assertThat(port.deliveries).hasSize(2).allSatisfy(delivery -> {
            assertThat(delivery.ids()).containsExactly(2L);
            assertThat(delivery.message()).isEqualTo(message());
        });
        assertThat(requireNonNull(port.results.get(2L)).value())
                .isInstanceOfSatisfying(Subscription.class, value -> {
                    assertThat(value.filter()).isEqualTo(new TopicFilter("$share/group/a/#"));
                });
    }

    @ParameterizedTest(name = "rejects overlap between {0} and {1}")
    @CsvSource(
            value = {
                    "$share/group/a/# | a/+",
                    "a/+ | $share/group/a/#",
                    "$share/group/a/# | $share/other/a/#",
                    "$share/group/a/# | $share/other/a/+",
                    "$share/other/a/+ | $share/group/a/#",
                    "$share/group/a/# | $share/group/a/+",
                    "$share/group/a/# | a",
                    "$share/group/a/+ | a/",
                    "$share/group/# | /#",
                    "$share/group/+/# | /#",
                    "$share/group/+/+ | /",
                    "$share/group/$SYS/# | $SYS/+",
                    "$share/group/a//+ | a//#",
                    "$share/group/+ | +",
                    "+ | $share/group/#"
            },
            delimiter = '|'
    )
    void shared_overlap_is_rejected_before_reserving_a_lane_or_packet_identifier(
            String first,
            String second
    ) {
        // Arrange
        connected();
        sut.subscribe(2, subscription(first));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act
        sut.subscribe(3, subscription(second));

        // Assert
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(IllegalStateException.class);
        assertThat(port.registered).containsExactly(2L);
        assertThat(port.sent).hasSize(1);
        assertThat(port.removed).isEmpty();
        assertThat(sut.state()).isEqualTo(CONNECTED);

        // Act: the existing subscription still succeeds and gets the next identifier.
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        sut.unsubscribe(4, 2);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure()).isNull();
        assertThat(((Packet.Unsubscribe) port.sent.getLast()).packetId()).isEqualTo(id + 1);
    }

    @ParameterizedTest(name = "allows disjoint filters {0} and {1}")
    @CsvSource(
            value = {
                    "$share/group//# | +",
                    "+ | $share/group//#",
                    "$share/group/a/+ | a",
                    "$share/group/a/+ | b/+",
                    "$share/group/# | $SYS/#",
                    "$share/group/+/x | $SYS/x",
                    "$share/group/$SYS/# | #",
                    "$share/group/$SYS/x | $SYS/y",
                    "$share/group/a//# | a/b/#",
                    "$share/group/a/# | aB/#",
                    "$share/group/a/# | $share/group/b/#",
                    "$share/group/a/# | $share/other/b/#"
            },
            delimiter = '|'
    )
    void shared_filters_with_no_common_valid_topic_are_allowed(String first, String second) {
        // Arrange
        connected();
        sut.subscribe(2, subscription(first));

        // Act
        sut.subscribe(3, subscription(second));

        // Assert
        assertThat(port.registered).containsExactly(2L, 3L);
        assertThat(port.sent).hasSize(2);
        assertThat(port.results).doesNotContainKeys(2L, 3L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "overlap within UTF-8 limit: {0} and {1} repetitions of {2}")
    @CsvSource({"32767, 32767, a", "16383, 16384, é", "8191, 8192, 😀"})
    void shared_overlap_includes_topics_at_the_utf8_length_limit(
            int firstLength,
            int secondLength,
            String character
    ) {
        // Arrange: each filter supplies a different literal level of the common topic.
        connected();
        sut.subscribe(2, subscription("$share/group/" + character.repeat(firstLength) + "/+"));

        // Act
        sut.subscribe(3, subscription("+/" + character.repeat(secondLength)));

        // Assert
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(IllegalStateException.class);
        assertThat(port.registered).containsExactly(2L);
        assertThat(port.sent).hasSize(1);
    }

    @ParameterizedTest(name = "overlap beyond UTF-8 limit: {0} and {1} repetitions of {2}")
    @CsvSource({"32767, 32768, a", "16384, 16384, é", "8192, 8192, 😀"})
    void shared_filters_are_allowed_when_every_common_topic_exceeds_the_utf8_length_limit(
            int firstLength,
            int secondLength,
            String character
    ) {
        // Arrange: both filters are valid, but their shortest common topic is too long.
        connected();
        sut.subscribe(2, subscription("$share/group/" + character.repeat(firstLength) + "/+"));

        // Act
        sut.subscribe(3, subscription("+/" + character.repeat(secondLength)));

        // Assert
        assertThat(port.registered).containsExactly(2L, 3L);
        assertThat(port.sent).hasSize(2);
        assertThat(port.results).doesNotContainKeys(2L, 3L);
    }

    @ParameterizedTest(name = "reserves overlap between {0} and {1} until UNSUBACK")
    @CsvSource(
            value = {
                    "a/# | $share/group/a/+",
                    "$share/group/a/+ | a/#",
                    "$share/group/a/# | $share/other/a/#",
                    "$share/group/a/# | $share/group/a/+"
            },
            delimiter = '|'
    )
    void shared_overlap_remains_reserved_until_unsubscribe_is_acknowledged(
            String first,
            String second
    ) {
        // Arrange
        connected();
        sut.subscribe(2, subscription(first));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        Subscribe candidate = subscription(second);

        // Act: the original filter remains reserved while pending, active, and being removed.
        sut.subscribe(3, candidate);
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        sut.subscribe(4, candidate);
        sut.unsubscribe(5, 2);
        int unsubscribeId = ((Packet.Unsubscribe) port.sent.getLast()).packetId();
        sut.subscribe(6, candidate);
        receive(1, new Packet.UnsubAck(unsubscribeId, List.of(), EMPTY), 0);
        sut.subscribe(7, candidate);

        // Assert
        for (long operation : List.of(3L, 4L, 6L)) {
            assertThat(requireNonNull(port.results.get(operation)).failure())
                    .isInstanceOf(IllegalStateException.class);
        }
        assertThat(requireNonNull(port.results.get(5L)).failure()).isNull();
        assertThat(port.registered).containsExactly(2L, 7L);
        assertThat(port.removed).containsExactly(2L);
        assertThat(port.results).doesNotContainKey(7L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void suback_refusal_removes_the_registration_and_allows_a_new_attempt() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.UNSPECIFIED_ERROR), EMPTY), 0);
        receive(1, publication(), 0);
        sut.subscribe(3, subscription("a/#"));

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOfSatisfying(
                        MqttSubscribeException.class,
                        failure -> assertThat(failure.reasonCode())
                                .isEqualTo(ReasonCode.UNSPECIFIED_ERROR)
                );
        assertThat(port.removed).containsExactly(2L);
        assertThat(port.deliveries).isEmpty();
        assertThat(port.sent).hasSize(2);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void duplicate_filters_are_reserved_until_unsubscribe_is_acknowledged() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("$share/group/a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act: the original filter remains reserved while pending, active, and being removed.
        sut.subscribe(3, subscription("$share/group/a/#"));
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        sut.subscribe(4, subscription("$share/group/a/#"));
        sut.unsubscribe(5, 2);
        int unsubscribeId = ((Packet.Unsubscribe) port.sent.getLast()).packetId();
        sut.subscribe(6, subscription("$share/group/a/#"));

        // A disjoint shared subscription remains independent while the original is removed.
        sut.subscribe(7, subscription("$share/other/other/#"));
        receive(1, publication(), 0);

        // Assert
        assertThat(port.deliveries).isEmpty();

        // Act: the same filter can receive messages again after removal completes.
        receive(1, new Packet.UnsubAck(unsubscribeId, List.of(), EMPTY), 0);
        sut.subscribe(8, subscription("$share/group/a/#"));
        receive(1, publication(), 0);

        // Assert
        for (long operation : List.of(3L, 4L, 6L)) {
            assertThat(requireNonNull(port.results.get(operation)).failure())
                    .isInstanceOf(IllegalStateException.class);
        }
        assertThat(port.deliveries).singleElement()
                .satisfies(delivery -> assertThat(delivery.ids()).containsExactly(8L));
        assertThat(requireNonNull(port.results.get(5L)).failure()).isNull();
        assertThat(port.removed).containsExactly(2L);
        assertThat(port.registered).containsExactly(2L, 7L, 8L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void full_normal_send_capacity_rolls_back_only_the_refused_operation() {
        // Arrange: an established subscription and a full normal send queue.
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act
        sut.publish(3, message());
        long publicationTicket = port.ticket;
        sut.subscribe(4, subscription("new/#"));
        sut.unsubscribe(5, 2);

        // Assert
        for (long operation : List.of(3L, 4L, 5L)) {
            assertThat(requireNonNull(port.results.get(operation)).failure())
                    .isInstanceOf(MqttCapacityException.class);
        }

        // Act: a completion for the refused publication is ignored, delivery resumes after
        // refusal, and both subscription operations can retry.
        sut.sent(1, publicationTicket, 0);
        receive(1, publication(), 0);
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.subscribe(6, subscription("new/#"));
        sut.unsubscribe(7, 2);
        int unsubscribeId = ((Packet.Unsubscribe) port.sent.getLast()).packetId();
        receive(1, new Packet.UnsubAck(unsubscribeId, List.of(), EMPTY), 0);

        // Assert
        assertThat(port.removed).containsExactly(4L, 2L);
        assertThat(port.deliveries).singleElement()
                .satisfies(delivery -> assertThat(delivery.ids()).containsExactly(2L));
        assertThat(requireNonNull(port.results.get(7L)).failure()).isNull();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest
    @EnumSource(SendQueue.OfferResult.class)
    void publish_passes_the_message_without_copying_the_payload(SendQueue.OfferResult result) {
        // Arrange
        connected();
        port.sendResult = result;
        var message = new MqttMessage(
                new Topic("a/b"),
                new byte[16 * 1024 * 1024 + 1],
                Qos.AT_MOST_ONCE,
                false
        );
        var memory = (ThreadMXBean) ManagementFactory.getThreadMXBean();

        // Act: MqttMessage.payload() would copy the whole payload.
        long before = memory.getCurrentThreadAllocatedBytes();
        sut.publish(2, message);
        long allocated = memory.getCurrentThreadAllocatedBytes() - before;

        // Assert: permit class-loading overhead, but not a copy of the payload.
        assertThat(before).as("per-thread allocation measurement").isNotNegative();
        assertThat(allocated).isLessThan(1024 * 1024);
        assertThat(port.publications).singleElement().isSameAs(message);
        assertThat(port.sent).isEmpty();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void a_full_subscription_capacity_never_sends_a_subscribe() {
        // Arrange
        connected();
        port.acceptRegistration = false;

        // Act
        sut.subscribe(2, subscription("a/#"));

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttCapacityException.class);
        assertThat(port.sent).isEmpty();

        // Act: releasing capacity allows the same filter to be registered.
        port.acceptRegistration = true;
        sut.subscribe(3, subscription("a/#"));

        // Assert
        assertThat(port.sent).singleElement().isInstanceOf(Packet.Subscribe.class);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void a_suback_for_a_refused_registration_is_a_protocol_error() {
        // Arrange: the refusal returned the first packet identifier without sending it.
        connected();
        port.acceptRegistration = false;
        sut.subscribe(2, subscription("a/#"));

        // Act
        receive(1, new Packet.SubAck(1, List.of(ReasonCode.SUCCESS), EMPTY), 0);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause()).isInstanceOf(MqttProtocolException.class));
    }

    @Test
    void normal_operations_on_a_closed_send_queue_wait_for_the_terminal_cause() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        port.sendResult = SendQueue.OfferResult.CLOSED;
        var failure = new IOException("Peer reset");

        // Act
        sut.publish(3, message());
        sut.subscribe(4, subscription("new/#"));
        sut.unsubscribe(5, 2);

        // Assert: refusal alone does not fail the operations or end the connection.
        assertThat(port.results).doesNotContainKeys(3L, 4L, 5L);
        assertThat(port.ends).isEmpty();

        // Act
        sut.failed(1, failure);

        // Assert
        for (long operation : List.of(3L, 4L, 5L)) {
            assertThat(requireNonNull(port.results.get(operation)).failure())
                    .isInstanceOf(MqttConnectionLostException.class)
                    .hasCause(failure);
        }
        assertThat(port.removed).containsExactly(2L, 4L);
    }

    @ParameterizedTest(name = "invalid SUBACK reasons: {0}")
    @MethodSource("invalidSubackReasons")
    void invalid_subacks_end_the_connection_and_fail_the_subscribe(List<ReasonCode> reasons) {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act
        receive(1, new Packet.SubAck(id, reasons, EMPTY), 0);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCauseInstanceOf(MqttProtocolException.class);
    }

    @ParameterizedTest(name = "unknown acknowledgement: {0}")
    @MethodSource("unknownSubscriptionAcknowledgements")
    void unknown_suback_and_unsuback_identifiers_are_protocol_errors(Packet acknowledgement) {
        // Arrange
        connected();

        // Act
        receive(1, acknowledgement, 0);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause()).isInstanceOf(MqttProtocolException.class);
        });
    }

    @Test
    void duplicate_suback_cannot_complete_a_subscription_twice() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        var ack = new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY);

        // Act
        receive(1, ack, 0);
        receive(1, ack, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure()).isNull();
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause()).isInstanceOf(MqttProtocolException.class);
        });
        assertThat(port.removed).containsExactly(2L);
    }

    @Test
    void duplicate_unsuback_cannot_complete_an_unsubscription_twice() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        sut.unsubscribe(3, 2);
        int unsubscribeId = ((Packet.Unsubscribe) port.sent.getLast()).packetId();
        var ack = new Packet.UnsubAck(unsubscribeId, List.of(), EMPTY);

        // Act
        receive(1, ack, 0);
        receive(1, ack, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause()).isInstanceOf(MqttProtocolException.class);
        });
        assertThat(port.removed).containsExactly(2L);
    }

    @Test
    void an_unsuback_with_reason_codes_is_invalid_in_mqtt_three() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        sut.unsubscribe(3, 2);
        int unsubscribeId = ((Packet.Unsubscribe) port.sent.getLast()).packetId();

        // Act
        receive(1, new Packet.UnsubAck(unsubscribeId, List.of(ReasonCode.SUCCESS), EMPTY), 0);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCauseInstanceOf(MqttProtocolException.class);
        assertThat(port.removed).containsExactly(2L);
    }

    @Test
    void refused_subscribe_and_unsubscribe_release_identifiers_across_allocator_wrap() {
        // Arrange: both operations share one packet-identifier pool [MQTT-2.3.1-2].
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        port.sent.clear();
        port.sendResult = SendQueue.OfferResult.FULL;
        Subscribe request = subscription("new/#");

        // Act: repeated refusal must return every identifier before the allocator wraps.
        for (int i = 0; i < 65535; i++) {
            sut.subscribe(3, request);
            sut.unsubscribe(4, 2);
            port.acceptRegistration = false;
            sut.subscribe(5, request);
            port.acceptRegistration = true;
            port.results.clear();
            port.registered.clear();
            port.removed.clear();
        }

        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.subscribe(3, request);
        sut.unsubscribe(4, 2);

        // Assert
        assertThat(port.sent).hasSize(2);
        var subscribed = (Packet.Subscribe) port.sent.getFirst();
        var unsubscribed = (Packet.Unsubscribe) port.sent.getLast();
        assertThat(subscribed.packetId()).isNotEqualTo(unsubscribed.packetId());

        // Act: both exchanges can still complete after capacity returns.
        receive(
                1,
                new Packet.SubAck(subscribed.packetId(), List.of(ReasonCode.SUCCESS), EMPTY),
                0
        );
        receive(1, new Packet.UnsubAck(unsubscribed.packetId(), List.of(), EMPTY), 0);

        // Assert
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        assertThat(requireNonNull(port.results.get(4L)).failure()).isNull();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void delivery_capacity_failure_ends_the_connection_and_removes_all_subscriptions() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        port.acceptDelivery = false;

        // Act: incoming delivery never waits for an application handler to free capacity.
        receive(1, publication(), 0);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause()).isInstanceOf(MqttCapacityException.class);
        });
        assertThat(port.removed).containsExactly(2L);
    }

    @Test
    void old_generation_writes_and_acks_cannot_complete_new_operations() {
        // Arrange: lose the first connection with pending operations, then reconnect.
        connected();
        sut.publish(2, message());
        long oldTicket = port.ticket;
        sut.subscribe(3, subscription("a/#"));
        int oldId = ((Packet.Subscribe) port.sent.getLast()).packetId();
        sut.failed(1, new IOException("Peer reset"));
        sut.connect(4, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        sut.publish(5, message());
        sut.subscribe(6, subscription("a/#"));

        // Act: callbacks from the old connection arrive after the new operations start.
        sut.sent(1, oldTicket, 0);
        receive(1, new Packet.SubAck(oldId, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        receive(1, publication(), 0);

        // Assert
        assertThat(port.results).doesNotContainKeys(5L, 6L);
        assertThat(port.deliveries).isEmpty();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    private static Stream<List<ReasonCode>> invalidSubackReasons() {
        return Stream.of(
                List.of(ReasonCode.SUCCESS, ReasonCode.SUCCESS),
                List.of(ReasonCode.GRANTED_QOS_1),
                List.of(ReasonCode.GRANTED_QOS_2),
                List.of(ReasonCode.NOT_AUTHORIZED)
        );
    }

    private static Stream<Packet> unknownSubscriptionAcknowledgements() {
        return Stream.of(
                new Packet.SubAck(1, List.of(ReasonCode.SUCCESS), EMPTY),
                new Packet.UnsubAck(1, List.of(), EMPTY)
        );
    }

    private static MqttMessage message() {
        return new MqttMessage(new Topic("a/b"), new byte[] {1, 2}, Qos.AT_MOST_ONCE, false);
    }

    private static Packet.Publish publication() {
        return new Packet.Publish(
                new Topic("a/b"),
                new byte[] {1, 2},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        );
    }

    private static Subscribe subscription(String filter) {
        return new Subscribe(new TopicFilter(filter), Qos.AT_MOST_ONCE, 4);
    }

    private void connected() {
        sut.connect(1, 0);
        accept();
    }

    private void accept() {
        receive(1, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
    }

    /// Production passes the frame size; these tests count only a PUBLISH payload.
    private void receive(long connection, Packet packet, long now) {
        sut.received(connection, packet, now,
                packet instanceof Packet.Publish publish ? publish.payload().length : 0);
    }

    private static long seconds(long value) {
        return Duration.ofSeconds(value).toNanos();
    }

    private record Result(@Nullable Object value, @Nullable Throwable failure) {
    }

    private record Received(List<Long> ids, MqttMessage message, int bytes) {
    }

    private static final class RecordingPort implements SessionCore.Port {

        final List<Packet> sent = new ArrayList<>();
        final List<MqttMessage> publications = new ArrayList<>();
        final List<Disconnection> ends = new ArrayList<>();
        final List<Boolean> established = new ArrayList<>();
        final Map<Long, Result> results = new HashMap<>();
        final Map<SessionCore.Timer, Duration> timers = new EnumMap<>(SessionCore.Timer.class);
        final List<Long> opened = new ArrayList<>();
        final List<Long> registered = new ArrayList<>();
        final List<Long> removed = new ArrayList<>();
        final List<Received> deliveries = new ArrayList<>();
        boolean acceptRegistration = true;
        boolean acceptDelivery = true;
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
        public SendQueue.OfferResult publish(long generation, long id, MqttMessage message) {
            ticket = id;
            publications.add(message);
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
        public void complete(long operation, @Nullable Object value, @Nullable Throwable failure) {
            assertThat(results.put(operation, new Result(value, failure))).isNull();
        }

        @Override
        public boolean registerSubscription(long generation, long id) {
            if (acceptRegistration) {
                registered.add(id);
            }

            return acceptRegistration;
        }

        @Override
        public Subscription subscribed(long id, Qos granted) {
            TopicFilter filter = sent.stream()
                    .filter(Packet.Subscribe.class::isInstance)
                    .map(Packet.Subscribe.class::cast)
                    .toList()
                    .get(registered.indexOf(id))
                    .subscriptions()
                    .getFirst()
                    .filter();
            return new Subscription() {
                @Override
                public TopicFilter filter() {
                    return filter;
                }

                @Override
                public Qos grantedQos() {
                    return granted;
                }

                @Override
                public CompletableFuture<Void> unsubscribe() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void close() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public boolean deliver(
                long generation,
                List<Long> ids,
                MqttMessage message,
                boolean dup,
                int bytes
        ) {
            if (acceptDelivery) {
                deliveries.add(new Received(List.copyOf(ids), message, bytes));
            }

            return acceptDelivery;
        }

        @Override
        public void removeSubscription(long id) {
            removed.add(id);
        }

        @Override
        public void end(long generation, boolean connected, Disconnection end) {
            established.add(connected);
            ends.add(end);
        }
    }
}
