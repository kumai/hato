package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.sun.management.ThreadMXBean;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttNotConnectedException;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttSubscribeException;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CLOSED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CONNECTING;
import static org.mqtt4j.hato.client.internal.SessionCore.State.DISCONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.Timer.CONNECT;
import static org.mqtt4j.hato.client.internal.SessionCore.Timer.KEEP_ALIVE;
import static org.mqtt4j.hato.client.internal.SessionCore.Timer.PING;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class SessionCoreTest {

    private final RecordingPort port = new RecordingPort();
    private final SessionCore sut = new SessionCore(
            port,
            "client",
            Duration.ofSeconds(30),
            Duration.ofSeconds(10),
            true
    );

    @Test
    void deferred_removal_waits_for_send_capacity_and_completes_only_after_unsuback() {
        // Arrange
        connected();
        establishSubscriptions(1);
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act
        sut.closeSubscription(1000, 2);
        sut.sendCapacityAvailable(1);
        sut.sendCapacityAvailable(1);

        // Assert
        assertThat(port.results).doesNotContainKey(1000L);
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.sendCapacityAvailable(1);
        var packet = (Packet.Unsubscribe) port.sent.getLast();
        assertThat(port.results).doesNotContainKey(1000L);
        receive(1, new Packet.UnsubAck(packet.packetId(), List.of(), EMPTY), 0);
        assertThat(requireNonNull(port.results.get(1000L)).failure()).isNull();
        assertThat(port.removed).containsExactly(2L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void deferred_removals_bound_inflight_work_and_resume_as_each_unsuback_arrives() {
        // Arrange
        connected();
        establishSubscriptions(100);
        for (int i = 0; i < 100; i++) {
            sut.closeSubscription(1000 + i, 2 + i);
        }

        // Act
        sut.sendCapacityAvailable(1);

        // Assert: only a bounded window is sent even though regular send capacity remains.
        assertThat(port.sent).filteredOn(Packet.Unsubscribe.class::isInstance).hasSize(16);
        for (int i = 0; i < 100; i++) {
            List<Packet.Unsubscribe> removals = port.sent.stream()
                    .filter(Packet.Unsubscribe.class::isInstance)
                    .map(Packet.Unsubscribe.class::cast).toList();
            assertThat(removals).hasSize(Math.min(100, i + 16));
            Packet.Unsubscribe removal = removals.get(i);
            assertThat(removal.filters()).containsExactly(new TopicFilter("slot/" + i));
            receive(1, new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY), 0);
            assertThat(requireNonNull(port.results.get(1000L + i)).failure()).isNull();
        }
        assertThat(port.removed).hasSize(100).doesNotHaveDuplicates();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest
    @EnumSource(SendQueue.OfferResult.class)
    void termination_preserves_sent_and_queued_closures_until_clean_session_reconciliation(
            SendQueue.OfferResult result
    ) {
        // Arrange
        connected();
        establishSubscriptions(20);
        port.sendResult = result;
        for (int i = 0; i < 20; i++) {
            sut.closeSubscription(1000 + i, 2 + i);
        }
        sut.sendCapacityAvailable(1);
        var failure = new IOException("connection lost during removal");

        // Act
        sut.failed(1, failure);

        // Assert: a background close remains pending until the broker state is known.
        for (int i = 0; i < 20; i++) {
            assertThat(port.results).doesNotContainKey(1000L + i);
        }
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        port.sent.clear();
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.connect(2000, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        sut.sendCapacityAvailable(1);
        sut.sendCapacityAvailable(2);
        assertThat(port.sent).isEmpty();
        for (int i = 0; i < 20; i++) {
            assertThat(requireNonNull(port.results.get(1000L + i)).failure()).isNull();
        }
        assertThat(port.removed).hasSize(20).doesNotHaveDuplicates();
        // Exchanges abandoned with the old connection no longer occupy the closure window.
        sut.subscribe(3000, subscription("slot/0"));
        var subscribe = (Packet.Subscribe) port.sent.getLast();
        receive(2, new Packet.SubAck(subscribe.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        sut.closeSubscription(3001, 3000);
        sut.sendCapacityAvailable(2);
        assertThat(port.sent.getLast()).isInstanceOf(Packet.Unsubscribe.class);
    }

    @Test
    void a_pending_disconnect_prevents_deferred_removals_from_being_sent() {
        // Arrange
        connected();
        establishSubscriptions(1);
        sut.publish(3, qosOneMessage());
        int id = port.publicationIds.getLast();
        port.sendResult = SendQueue.OfferResult.FULL;
        sut.closeSubscription(1000, 2);

        // Act: returned send capacity and a PUBACK can both arrive before DISCONNECT is written.
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.disconnect(2000);
        sut.sendCapacityAvailable(1);
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert
        assertThat(port.sent.getLast()).isInstanceOf(Packet.Disconnect.class);
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
        sut.sent(1, port.ticket, 0);
        assertThat(port.results).doesNotContainKey(1000L);
        sut.connect(2001, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        assertThat(requireNonNull(port.results.get(1000L)).failure()).isNull();
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
    }

    @Test
    void confirmed_subscriptions_keep_their_handles_and_refresh_the_grant_after_reconnect() {
        // Arrange: one confirmed subscription and one whose first SUBACK never arrives.
        connected();
        sut.subscribe(2, Subscribe.of(new TopicFilter("a/#"), Qos.AT_LEAST_ONCE));
        var initial = (Packet.Subscribe) port.sent.getLast();
        receive(1, new Packet.SubAck(initial.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        Subscription handle = (Subscription) requireNonNull(port.results.get(2L)).value();
        sut.subscribe(3, subscription("other/#"));
        var failure = new IOException("lost before initial SUBACK");

        // Act
        sut.failed(1, failure);
        port.sent.clear();
        sut.connect(4, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert: CONNECT completes before recreation, and the pending initial attempt ends.
        assertThat(requireNonNull(port.results.get(4L)).value()).isEqualTo(ConnAck.of(false));
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(MqttConnectionLostException.class).hasCause(failure);
        assertThat(port.removed).containsExactly(3L);
        assertThat(port.registered).containsExactly(2L, 3L);
        assertThat(port.sent).singleElement().isInstanceOfSatisfying(Packet.Subscribe.class,
                packet -> assertThat(packet.subscriptions()).containsExactly(
                        new Packet.Subscription(new TopicFilter("a/#"), Qos.AT_LEAST_ONCE,
                                false, false, 0)));
        assertThat(requireNonNull(handle).grantedQos()).isEqualTo(Qos.AT_MOST_ONCE);

        // Act: recreation updates the existing handle without completing its future again.
        var recreated = (Packet.Subscribe) port.sent.getLast();
        receive(2, new Packet.SubAck(recreated.packetId(),
                List.of(ReasonCode.GRANTED_QOS_1), EMPTY), 0);

        // Assert: RecordingPort rejects duplicate operation completion.
        assertThat(requireNonNull(port.results.get(2L)).value()).isSameAs(handle);
        assertThat(handle.grantedQos()).isEqualTo(Qos.AT_LEAST_ONCE);
        assertThat(port.grants).containsExactly(handle, handle);
        sut.failed(2, new IOException("lost again"));
        sut.close(null);
        assertThat(port.removed).containsExactly(3L, 2L);
    }

    @Test
    void recreating_the_subscription_limit_waits_for_capacity_and_bounds_the_suback_window() {
        // Arrange: a full registration table is larger than the ordinary send queue.
        connected();
        establishSubscriptions(256);
        sut.failed(1, new IOException("lost connection"));
        port.sent.clear();
        port.sendResult = SendQueue.OfferResult.FULL;
        sut.connect(1000, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        assertThat(requireNonNull(port.results.get(1000L)).failure()).isNull();
        assertThat(port.sent).isEmpty();

        // Act: old capacity cannot resume work; current capacity opens only the bounded window.
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.sendCapacityAvailable(1);
        assertThat(port.sent).isEmpty();
        sut.sendCapacityAvailable(2);

        // Assert: each SUBACK admits one replacement without registering or completing again.
        for (int i = 0; i < 256; i++) {
            assertThat(port.sent).hasSize(Math.min(256, i + 16));
            var packet = (Packet.Subscribe) port.sent.get(i);
            assertThat(packet.subscriptions().getFirst().filter())
                    .isEqualTo(new TopicFilter("slot/" + i));
            receive(2, new Packet.SubAck(packet.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        }
        assertThat(port.registered).hasSize(256).doesNotHaveDuplicates();
        assertThat(port.grants).hasSize(512);
        assertThat(port.removed).isEmpty();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "logging failure: {0}")
    @ValueSource(booleans = {false, true})
    void a_refused_recreation_ends_only_that_subscription_with_its_original_handle(
            boolean loggingFailure
    ) {
        // Arrange: unsubscribe waits for recreation while another subscription stays active.
        connected();
        establishSubscriptions(2);
        Object refused = requireNonNull(port.results.get(2L)).value();
        sut.failed(1, new IOException("lost connection"));
        port.sent.clear();
        sut.connect(4, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        var first = (Packet.Subscribe) port.sent.getFirst();
        var second = (Packet.Subscribe) port.sent.getLast();
        sut.unsubscribe(5, 2);
        var logCalls = new AtomicInteger();
        var jul = Logger.getLogger(SessionCore.class.getName());
        Level previousLevel = jul.getLevel();
        boolean previousParents = jul.getUseParentHandlers();
        var handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                logCalls.incrementAndGet();
                if (loggingFailure) {
                    throw new AssertionError("logging backend failure");
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        jul.addHandler(handler);
        jul.setLevel(Level.WARNING);
        jul.setUseParentHandlers(false);
        try {
            // Act
            receive(2, new Packet.SubAck(first.packetId(),
                    List.of(ReasonCode.UNSPECIFIED_ERROR), EMPTY), 0);
            receive(2, new Packet.SubAck(second.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
            receive(2, qosOnePublication(7, "slot/1", false), 0);

            // Assert: even failed logging cannot interrupt removal or terminate the connection.
            assertThat(port.removed).containsExactly(2L);
            assertThat(port.removals).singleElement().satisfies(removal -> {
                assertThat(removal.handle()).isSameAs(refused);
                assertThat(removal.cause()).isInstanceOf(MqttSubscribeException.class);
            });
            assertThat(requireNonNull(port.results.get(5L)).failure()).isNull();
            assertThat(port.deliveries).singleElement()
                    .satisfies(delivery -> assertThat(delivery.ids()).containsExactly(3L));
            assertThat(port.ends).hasSize(1);
            assertThat(sut.state()).isEqualTo(CONNECTED);
            assertThat(logCalls.get()).isEqualTo(1);
        } finally {
            jul.removeHandler(handler);
            jul.setLevel(previousLevel);
            jul.setUseParentHandlers(previousParents);
        }
    }

    @Test
    void an_interrupted_unsubscribe_restores_the_confirmed_subscription_for_recreation() {
        // Arrange
        connected();
        establishSubscriptions(1);
        sut.unsubscribe(3, 2);
        var failure = new IOException("lost before UNSUBACK");

        // Act
        sut.failed(1, failure);
        port.sent.clear();
        sut.connect(4, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        var recreated = (Packet.Subscribe) port.sent.getLast();
        receive(2, new Packet.SubAck(recreated.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        receive(2, qosOnePublication(7, "slot/0", false), 0);

        // Assert: the failed operation stays failed while the original intent resumes delivery.
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(MqttConnectionLostException.class).hasCause(failure);
        assertThat(port.registered).containsExactly(2L);
        assertThat(port.removed).isEmpty();
        assertThat(port.deliveries).singleElement()
                .satisfies(delivery -> assertThat(delivery.ids()).containsExactly(2L));
        sut.unsubscribe(5, 2);
        var removal = (Packet.Unsubscribe) port.sent.getLast();
        receive(2, new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY), 0);
        assertThat(requireNonNull(port.results.get(5L)).failure()).isNull();
        assertThat(port.removed).containsExactly(2L);
    }

    @ParameterizedTest(name = "close while connecting: {0}")
    @ValueSource(booleans = {false, true})
    void closing_without_an_established_connection_finishes_on_clean_session_connack(
            boolean connecting
    ) {
        // Arrange
        connected();
        establishSubscriptions(1);
        sut.failed(1, new IOException("lost connection"));
        port.sent.clear();
        if (connecting) {
            sut.connect(4, 0);
        }

        // Act
        sut.closeSubscription(3, 2);
        assertThat(port.results).doesNotContainKey(3L);
        if (!connecting) {
            sut.connect(4, 0);
        }
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        sut.sendCapacityAvailable(2);

        // Assert: neither SUBSCRIBE nor UNSUBSCRIBE is needed for the closed intention.
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        assertThat(port.sent).isEmpty();
        assertThat(port.removed).containsExactly(2L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "recreation refused: {0}, handle closed: {1}")
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void unsubscribe_during_recreation_waits_for_suback_and_accepts_its_refusal(
            boolean refused,
            boolean closed
    ) {
        // Arrange
        connected();
        establishSubscriptions(1);
        Object handle = requireNonNull(port.results.get(2L)).value();
        sut.failed(1, new IOException("lost connection"));
        port.sent.clear();
        sut.connect(3, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        var recreated = (Packet.Subscribe) port.sent.getLast();

        // Act
        sut.unsubscribe(4, 2);
        if (closed) {
            sut.closeSubscription(5, 2);
            assertThat(port.results).doesNotContainKey(5L);
        }
        assertThat(port.results).doesNotContainKey(4L);
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
        receive(2, new Packet.SubAck(recreated.packetId(), List.of(refused
                ? ReasonCode.UNSPECIFIED_ERROR : ReasonCode.SUCCESS), EMPTY), 0);

        // Assert: refusal already proves that the broker has no matching subscription.
        if (refused) {
            assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
            assertThat(port.removals).singleElement().satisfies(removal ->
                    assertThat(removal.cause()).isInstanceOf(MqttSubscribeException.class));
        } else {
            var removal = (Packet.Unsubscribe) port.sent.getLast();
            assertThat(port.results).doesNotContainKey(4L);
            receive(2, new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY), 0);
        }
        assertThat(requireNonNull(port.results.get(4L)).failure()).isNull();
        if (closed) {
            assertThat(requireNonNull(port.results.get(5L)).failure()).isNull();
        }
        assertThat(requireNonNull(port.results.get(2L)).value()).isSameAs(handle);
        assertThat(port.removed).containsExactly(2L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "recreation sent: {0}")
    @ValueSource(booleans = {false, true})
    void close_during_recreation_waits_only_for_an_already_sent_subscribe(boolean sent) {
        // Arrange
        connected();
        establishSubscriptions(1);
        sut.failed(1, new IOException("lost connection"));
        port.sent.clear();
        port.sendResult = sent ? SendQueue.OfferResult.ACCEPTED : SendQueue.OfferResult.FULL;
        sut.connect(3, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);

        // Act
        sut.closeSubscription(4, 2);
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.sendCapacityAvailable(2);

        // Assert
        if (sent) {
            assertThat(port.results).doesNotContainKey(4L);
            assertThat(port.sent).singleElement().isInstanceOf(Packet.Subscribe.class);
            var recreated = (Packet.Subscribe) port.sent.getLast();
            receive(2, new Packet.SubAck(recreated.packetId(),
                    List.of(ReasonCode.SUCCESS), EMPTY), 0);
            var removal = (Packet.Unsubscribe) port.sent.getLast();
            receive(2, new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY), 0);
        } else {
            assertThat(port.sent).isEmpty();
        }
        assertThat(requireNonNull(port.results.get(4L)).failure()).isNull();
        assertThat(port.removed).containsExactly(2L);
    }

    private void establishSubscriptions(int count) {
        for (int i = 0; i < count; i++) {
            sut.subscribe(2 + i, subscription("slot/" + i));
            var packet = (Packet.Subscribe) port.sent.getLast();
            receive(1, new Packet.SubAck(packet.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        }
    }

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
        assertThat(requireNonNull(port.results.get(1L)).value()).isEqualTo(ConnAck.of(false));
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
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class);
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
        Throwable cause = port.ends.getFirst().cause().orElseThrow();
        assertThat(cause).isInstanceOf(MqttCapacityException.class);
        assertThat(port.ends).containsExactly(Disconnection.of(cause, true));
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
        assertThat(port.ends).containsExactly(Disconnection.of(failure, true));
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
            assertThat(end.cause().orElseThrow()).isInstanceOf(TimeoutException.class);
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
        assertThat(port.ends).containsExactly(Disconnection.of(null, true));
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
        Throwable cause = port.ends.getFirst().cause().orElseThrow();
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
        assertThat(port.ends).containsExactly(Disconnection.of(null, true));
    }

    @Test
    void publish_passes_the_message_and_completes_only_after_its_write() {
        // Arrange
        connected();
        var payload = new byte[] {1, 2};
        var message = MqttMessage.of(new Topic("a/b"), payload, Qos.AT_MOST_ONCE, true);

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
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(PublishResult.of());
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
    @EnumSource(value = Qos.class, names = "EXACTLY_ONCE")
    void unsupported_qos_is_rejected_without_sending_or_registering(Qos qos) {
        // Arrange
        connected();

        // Act
        sut.publish(2, MqttMessage.of(new Topic("a"), new byte[0], qos, false));
        sut.subscribe(3, Subscribe.of(new TopicFilter("a"), qos).withBufferCapacity(1));

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
        var message = MqttMessage.of(
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
                assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class));
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
        assertThat(port.removed).containsExactly(4L);
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
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class);
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
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class);
        });
        assertThat(port.removed).isEmpty();
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
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class);
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
        assertThat(port.removed).isEmpty();
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
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class);
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
        return MqttMessage.of(new Topic("a/b"), new byte[] {1, 2}, Qos.AT_MOST_ONCE, false);
    }

    @Test
    void qos_one_publish_waits_for_puback_and_ignores_repeated_acknowledgement() {
        // Arrange
        connected();
        sut.publish(2, qosOneMessage());
        int id = port.publicationIds.getLast();

        // Act: a successful write is insufficient for QoS 1.
        sut.sent(1, port.ticket, 0);

        // Assert
        assertThat(id).isPositive();
        assertThat(port.results).doesNotContainKey(2L);

        // Act
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert: RecordingPort also rejects completing an operation a second time.
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(PublishResult.of());
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void puback_before_write_completion_completes_only_its_publication() {
        // Arrange
        connected();
        sut.publish(2, qosOneMessage());
        int firstId = port.publicationIds.getLast();
        long ticket = port.ticket;

        // Act: inbound and writer notifications may arrive in this order.
        receive(1, new Packet.PubAck(firstId, ReasonCode.SUCCESS, EMPTY), 0);
        sut.publish(3, qosOneMessage());
        sut.sent(1, ticket, 0);
        receive(1, new Packet.PubAck(firstId, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(PublishResult.of());
        assertThat(port.results).doesNotContainKey(3L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void puback_does_not_release_a_subscription_packet_identifier() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act: an unknown PUBACK is tolerated even if another exchange uses its id.
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).value()).isInstanceOf(Subscription.class);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void puback_before_close_completes_a_publication_exactly_once() {
        // Arrange
        connected();
        sut.publish(2, qosOneMessage());
        int id = port.publicationIds.getLast();

        // Act
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);
        sut.close(null);
        sut.sent(1, port.ticket, 0);

        // Assert: RecordingPort also rejects completing an operation a second time.
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(PublishResult.of());
        assertThat(sut.state()).isEqualTo(CLOSED);
    }

    @Test
    void close_before_puback_fails_a_publication_exactly_once() {
        // Arrange
        connected();
        sut.publish(2, qosOneMessage());
        int id = port.publicationIds.getLast();

        // Act
        sut.close(null);
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);
        sut.sent(1, port.ticket, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class);
        assertThat(sut.state()).isEqualTo(CLOSED);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void reordered_qos_one_events_complete_once_and_acknowledge_in_receive_order(int seed) {
        // Arrange: each chain keeps its own causal order, and the seed interleaves the chains.
        // Some runs end the connection at a random point; later events then arrive stale.
        var random = new Random(seed);
        connected();
        sut.subscribe(2, subscription("a/#"));
        var chains = new ArrayList<ArrayDeque<Runnable>>();
        List<Long> operations = List.of(10L, 11L, 12L);
        for (long operation : operations) {
            chains.add(publicationChain(operation, random));
        }
        var admitted = new ArrayList<Integer>();
        for (int id = 100; id < 104; id++) {
            chains.add(receptionChain(id, random.nextBoolean() ? "a/b" : "other", admitted));
        }
        boolean terminates = random.nextBoolean();
        if (terminates) {
            Runnable end = random.nextBoolean()
                    ? () -> sut.close(null)
                    : () -> sut.failed(1, new IOException("Connection reset"));
            chains.add(new ArrayDeque<>(List.of(end)));
        }

        // Act
        while (!chains.isEmpty()) {
            ArrayDeque<Runnable> chain = chains.get(random.nextInt(chains.size()));
            chain.removeFirst().run();
            if (chain.isEmpty()) {
                chains.remove(chain);
            }
        }

        // Assert: RecordingPort rejects a second completion and PacketIds a second release, so
        // reaching here also shows that each happened at most once.
        assertThat(operations).allSatisfy(operation -> {
            Result result = requireNonNull(port.results.get(operation));
            if (!terminates) {
                assertThat(result.value()).isEqualTo(PublishResult.of());
            }
        });
        List<Integer> acknowledged = pubAckIds();
        assertThat(acknowledged).hasSizeLessThanOrEqualTo(admitted.size());
        assertThat(acknowledged).isEqualTo(admitted.subList(0, acknowledged.size()));
        if (!terminates) {
            assertThat(acknowledged).isEqualTo(admitted);
        }
        assertThat(port.released).doesNotHaveDuplicates().hasSize(acknowledged.size());
    }

    static IntStream seeds() {
        return IntStream.range(0, 100);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void reordered_recreation_removals_and_old_events_preserve_subscription_intent(int seed) {
        // Arrange: one continuing subscription, one close, and one explicit unsubscribe.
        var random = new Random(seed);
        connected();
        establishSubscriptions(3);
        Object continuing = requireNonNull(port.results.get(2L)).value();
        long oldTicket = port.ticket;
        receive(1, qosOnePublication(7, "slot/0", false), 0);
        DeliveryToken old = port.deliveries.getLast().delivery();
        sut.failed(1, new IOException("lost connection"));
        List<Disconnection> expectedEnds = List.copyOf(port.ends);
        port.sent.clear();
        sut.connect(100, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        List<Packet.Subscribe> recreated = port.sent.stream()
                .map(Packet.Subscribe.class::cast).toList();
        boolean refused = seed % 2 == 0;
        var chains = new ArrayList<ArrayDeque<Runnable>>();
        chains.add(new ArrayDeque<>(List.of(() -> receive(2, new Packet.SubAck(
                recreated.getFirst().packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0))));
        chains.add(new ArrayDeque<>(List.of(() -> {
            sut.closeSubscription(200, 3);
            sut.sendCapacityAvailable(2);
        })));
        chains.add(new ArrayDeque<>(List.of(() -> receive(2, new Packet.SubAck(
                recreated.get(1).packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0))));
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.unsubscribe(300, 4),
                () -> receive(2, new Packet.SubAck(recreated.getLast().packetId(), List.of(refused
                        ? ReasonCode.UNSPECIFIED_ERROR : ReasonCode.SUCCESS), EMPTY), 0)
        )));
        chains.add(new ArrayDeque<>(List.of(
                () -> receive(1, new Packet.SubAck(recreated.getFirst().packetId(),
                        List.of(ReasonCode.SUCCESS), EMPTY), 0),
                () -> receive(1, new Packet.UnsubAck(recreated.getLast().packetId(),
                        List.of(), EMPTY), 0),
                () -> sut.deliveryReady(old.acknowledgement()),
                () -> sut.deliveryFailed(old, new IllegalStateException("old handler failed")),
                () -> sut.sent(1, oldTicket, 0),
                () -> sut.sendCapacityAvailable(1)
        )));

        // Act & Assert: stale events never complete or terminate current work.
        while (!chains.isEmpty()) {
            ArrayDeque<Runnable> chain = chains.get(random.nextInt(chains.size()));
            chain.removeFirst().run();
            if (chain.isEmpty()) {
                chains.remove(chain);
            }
            assertThat(port.ends).isEqualTo(expectedEnds);
            assertThat(sut.state()).isEqualTo(CONNECTED);
            assertThat(requireNonNull(port.results.get(2L)).value()).isSameAs(continuing);
            assertThat(port.registered).containsExactly(2L, 3L, 4L);
        }
        List<Packet.Unsubscribe> removals = port.sent.stream()
                .filter(Packet.Unsubscribe.class::isInstance)
                .map(Packet.Unsubscribe.class::cast).toList();
        assertThat(removals).hasSize(refused ? 1 : 2);
        for (Packet.Unsubscribe removal : removals) {
            receive(2, new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY), 0);
        }
        assertThat(requireNonNull(port.results.get(200L)).failure()).isNull();
        assertThat(requireNonNull(port.results.get(300L)).failure()).isNull();
        assertThat(port.removed).containsExactlyInAnyOrder(3L, 4L);
        assertThat(port.removals).filteredOn(removal -> removal.id() == 4)
                .singleElement().satisfies(removal -> {
                    if (refused) {
                        assertThat(removal.cause()).isInstanceOf(MqttSubscribeException.class);
                    } else {
                        assertThat(removal.cause()).isNull();
                    }
                });
        assertThat(pubAckIds()).isEmpty();
        port.deliveries.clear();
        receive(2, qosOnePublication(7, "slot/0", false), 0);
        assertThat(port.deliveries).singleElement()
                .satisfies(delivery -> assertThat(delivery.ids()).containsExactly(2L));
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void reordered_delivery_failures_stop_only_the_receiving_connection(int seed) {
        // Arrange: an old failure crosses connect and CONNACK, while the current failure may
        // come from QoS 0 or a QoS 1 delivery whose PUBACK has already been written.
        var random = new Random(seed);
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        DeliveryToken old = port.deliveries.getLast().delivery();
        sut.failed(1, new IOException("Lost connection"));
        var expected = new ArrayList<>(port.ends);
        var failure = new IllegalStateException("current handler failed");
        var oldFailure = new IllegalStateException("old handler failed");
        var chains = new ArrayList<ArrayDeque<Runnable>>();
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.connect(3, 0),
                () -> receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0),
                () -> {
                    sut.subscribe(4, subscription("a/#"));
                    receive(2, seed % 2 == 0 ? publication()
                            : qosOnePublication(7, "a/b", false), 0);
                },
                () -> {
                    DeliveryToken current = port.deliveries.getLast().delivery();
                    sut.deliveryReady(current.acknowledgement());
                    if (current.acknowledgement() != 0) {
                        sut.written(2, port.ackTickets.getLast());
                    }
                },
                () -> {
                    expected.add(Disconnection.of(failure, false));
                    sut.deliveryFailed(port.deliveries.getLast().delivery(), failure);
                }
        )));
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.deliveryFailed(old, oldFailure),
                () -> sut.deliveryReady(old.acknowledgement()),
                () -> sut.deliveryFailed(old, oldFailure)
        )));

        // Act & Assert: old failures never add a termination, including during establishment.
        while (!chains.isEmpty()) {
            ArrayDeque<Runnable> chain = chains.get(random.nextInt(chains.size()));
            chain.removeFirst().run();
            if (chain.isEmpty()) {
                chains.remove(chain);
            }
            assertThat(port.ends).isEqualTo(expected);
        }
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(pubAckIds()).isEqualTo(seed % 2 == 0 ? List.of() : List.of(7));
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void reordered_old_readiness_cannot_acknowledge_a_reused_id_after_reconnect(int seed) {
        // Arrange: an old snapshot may reach the core before or after the next CONNACK.
        var random = new Random(seed);
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        long oldToken = port.deliveries.getLast().token();
        List<Long> snapshot = List.of(oldToken);
        var current = new Exchange();
        var expected = new ArrayList<Integer>();
        var chains = new ArrayList<ArrayDeque<Runnable>>();
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.failed(1, new IOException("Lost connection")),
                () -> sut.connect(3, 0),
                () -> receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0),
                () -> {
                    sut.subscribe(4, subscription("a/#"));
                    receive(2, qosOnePublication(7, "a/b", false), 0);
                    current.token = port.deliveries.getLast().token();
                    assertThat(current.token).isGreaterThan(oldToken);
                },
                () -> {
                    expected.add(7);
                    sut.deliveryReady(current.token);
                }
        )));
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.deliveriesReady(snapshot),
                () -> sut.deliveriesReady(snapshot)
        )));
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.sendCapacityAvailable(1),
                () -> sut.written(1, port.ticket)
        )));

        // Act & Assert: readiness before connection loss may finish the original receipt,
        // but only the new delivery's readiness can acknowledge the reused identifier.
        while (!chains.isEmpty()) {
            ArrayDeque<Runnable> chain = chains.get(random.nextInt(chains.size()));
            chain.removeFirst().run();
            if (chain.isEmpty()) {
                chains.remove(chain);
            }
            var acknowledged = new ArrayList<Integer>();
            if (port.released.contains(oldToken)) {
                acknowledged.add(7);
            }
            acknowledged.addAll(expected);
            assertThat(pubAckIds()).isEqualTo(acknowledged);
        }
        assertThat(port.released).contains(current.token).doesNotHaveDuplicates();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void disconnect_completes_publications_acknowledged_before_it_ends_and_not_the_rest() {
        // Arrange
        connected();
        sut.publish(2, qosOneMessage());
        int acknowledgedId = port.publicationIds.getLast();
        sut.publish(3, qosOneMessage());

        // Act: shutdown does not wait for PUBACK, but uses one that arrives before it ends.
        sut.disconnect(4);
        receive(1, new Packet.PubAck(acknowledgedId, ReasonCode.SUCCESS, EMPTY), 0);
        sut.sent(1, port.ticket, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(PublishResult.of());
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(MqttConnectionLostException.class);
        assertThat(requireNonNull(port.results.get(4L)).failure()).isNull();
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.sent).singleElement().isInstanceOf(Packet.Disconnect.class);
    }

    @Test
    void refused_qos_one_publications_return_identifiers_across_allocator_wrap() {
        // Arrange
        connected();
        MqttMessage message = qosOneMessage();
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act: all identifiers are offered and refused once.
        for (int i = 0; i < 65535; i++) {
            sut.publish(i + 2L, message);
        }
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.publish(65537, message);
        int id = port.publicationIds.getLast();
        sut.subscribe(65538, subscription("other"));
        int subscriptionId = ((Packet.Subscribe) port.sent.getLast()).packetId();
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert: the pool is shared with SUBSCRIBE and no refused operation retained an id.
        assertThat(id).isEqualTo(1);
        assertThat(subscriptionId).isEqualTo(2);
        assertThat(requireNonNull(port.results.get(65536L)).failure())
                .isInstanceOf(MqttCapacityException.class);
        assertThat(requireNonNull(port.results.get(65537L)).value()).isEqualTo(PublishResult.of());
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void acknowledged_qos_one_publications_return_identifiers_across_allocator_wrap() {
        // Arrange
        connected();
        MqttMessage message = qosOneMessage();

        // Act: one connection publishes more QoS 1 messages than there are identifiers.
        for (int i = 0; i < 65536; i++) {
            sut.publish(i + 2L, message);
            int id = port.publicationIds.getLast();
            receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);
        }

        // Assert: the last publication wrapped to an identifier that PUBACK had returned.
        assertThat(port.publicationIds).hasSize(65536);
        assertThat(port.publicationIds.getLast()).isEqualTo(1);
        assertThat(requireNonNull(port.results.get(65537L)).value())
                .isEqualTo(PublishResult.of());
    }

    @Test
    void a_puback_for_a_refused_publication_is_unknown() {
        // Arrange: send admission refuses the publication after it allocated an identifier.
        connected();
        port.sendResult = SendQueue.OfferResult.FULL;
        sut.publish(2, qosOneMessage());
        int id = port.publicationIds.getLast();
        port.sendResult = SendQueue.OfferResult.ACCEPTED;

        // Act
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert: RecordingPort also rejects completing the refused operation a second time.
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttCapacityException.class);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void closed_send_queue_preserves_the_terminal_cause_for_qos_one_publish() {
        // Arrange
        connected();
        port.sendResult = SendQueue.OfferResult.CLOSED;
        var failure = new IOException("Peer reset before PUBACK");

        // Act
        sut.publish(2, qosOneMessage());

        // Assert
        assertThat(port.results).doesNotContainKey(2L);

        // Act
        sut.failed(1, failure);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(failure);
    }

    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_MOST_ONCE", "AT_LEAST_ONCE"})
    void qos_one_subscriptions_accept_the_requested_or_lower_grant(Qos granted) {
        // Arrange
        connected();
        sut.subscribe(2, Subscribe.of(new TopicFilter("a/#"), Qos.AT_LEAST_ONCE));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.of(granted.level())), EMPTY), 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).value())
                .isInstanceOfSatisfying(Subscription.class, value ->
                        assertThat(value.grantedQos()).isEqualTo(granted));
    }

    @Test
    void qos_one_deliveries_wait_for_all_recipients_and_preserve_receive_order() {
        // Arrange: the lane aggregates each publication's per-recipient completion conditions.
        connected();
        sut.subscribe(2, subscription("a/#"));
        sut.subscribe(3, subscription("a/+"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        receive(1, qosOnePublication(8, "a/b", false), 0);
        long first = port.deliveries.getFirst().token();
        long second = port.deliveries.getLast().token();

        // Act: later recipients are all ready while the first publication is still pending.
        sut.deliveryReady(second);
        sut.deliveryReady(second);

        // Assert
        assertThat(pubAckIds()).isEmpty();
        assertThat(port.released).isEmpty();
        assertThat(port.deliveries).allSatisfy(delivery ->
                assertThat(delivery.ids()).containsExactly(2L, 3L));

        // Act
        sut.deliveryReady(first);
        sut.deliveryReady(first);

        // Assert
        assertThat(pubAckIds()).containsExactly(7, 8);
        assertThat(port.released).containsExactly(first, second);
    }

    @Test
    void unmatched_qos_one_delivery_waits_behind_an_earlier_matching_delivery() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        long first = port.deliveries.getFirst().token();

        // Act
        receive(1, qosOnePublication(8, "other", false), 0);

        // Assert
        assertThat(pubAckIds()).isEmpty();
        assertThat(port.deliveries).hasSize(1);

        // Act
        sut.deliveryReady(first);

        // Assert
        assertThat(pubAckIds()).containsExactly(7, 8);
        assertThat(port.released).hasSize(2);
    }

    @Test
    void qos_zero_delivery_bypasses_a_pending_qos_one_acknowledgement() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);

        // Act
        receive(1, publication(), 0);

        // Assert
        assertThat(port.deliveries).hasSize(2);
        assertThat(port.deliveries.getLast().token()).isZero();
        assertThat(port.deliveries.getLast().message().qos()).isEqualTo(Qos.AT_MOST_ONCE);
        assertThat(pubAckIds()).isEmpty();
    }

    @Test
    void retransmissions_share_delivery_and_ack_until_the_puback_is_written() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        for (int id = 1; id <= 32; id++) {
            receive(1, qosOnePublication(id, "a/b", false), 0);
        }

        // Act: more retransmissions than the acknowledgement limit do not consume new slots.
        for (int round = 0; round < 130; round++) {
            for (int id = 1; id <= 32; id++) {
                receive(1, qosOnePublication(id, "a/b", true), 0);
            }
        }
        sut.deliveriesReady(port.deliveries.stream().map(Received::token).toList());
        receive(1, qosOnePublication(1, "a/b", true), 0);

        // Assert: even an admitted PUBACK has not yet ended the exchange.
        assertThat(port.deliveries).hasSize(32);
        assertThat(pubAckIds()).hasSize(16).doesNotHaveDuplicates();
        assertThat(sut.state()).isEqualTo(CONNECTED);

        // Act
        for (int i = 0; i < 32; i++) {
            sut.written(1, port.ackTickets.get(i));
        }

        // Assert
        assertThat(pubAckIds()).isEqualTo(IntStream.rangeClosed(1, 32).boxed().toList());
        assertThat(port.released).hasSize(32).doesNotHaveDuplicates();
    }

    @ParameterizedTest(name = "reused id has DUP={0}")
    @ValueSource(booleans = {false, true})
    void a_written_puback_allows_id_reuse_regardless_of_dup_and_ignores_the_old_ack(boolean dup) {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", true), 0);
        long first = port.deliveries.getFirst().token();
        sut.deliveryReady(first);
        long ackTicket = port.ackTickets.getFirst();
        sut.written(1, ackTicket);

        // Act: the next message may even have identical topic and payload.
        receive(1, qosOnePublication(7, "a/b", dup), 0);
        sut.deliveryReady(first);
        sut.written(1, ackTicket);

        // Assert: the new exchange has its own readiness condition.
        assertThat(port.deliveries).hasSize(2);
        assertThat(pubAckIds()).containsExactly(7);

        // Act
        sut.deliveryReady(port.deliveries.getLast().token());

        // Assert
        assertThat(pubAckIds()).containsExactly(7, 7);
    }

    @Test
    void reuse_without_dup_before_its_puback_is_queued_withdraws_the_earlier_acknowledgement() {
        // Arrange: the earlier reception is not ready, so its PUBACK is not queued yet.
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        receive(1, qosOnePublication(8, "a/b", false), 0);
        long earlier = port.deliveries.get(0).token();
        long following = port.deliveries.get(1).token();
        sut.deliveryReady(following);

        // Act: a sender reuses an identifier only for an exchange it considers acknowledged.
        receive(1, qosOnePublication(7, "a/b", false), 0);
        long later = port.deliveries.getLast().token();

        // Assert: the earlier reception no longer holds back the ready follower.
        assertThat(port.deliveries).hasSize(3);
        assertThat(port.released).containsExactly(earlier, following);
        assertThat(pubAckIds()).containsExactly(8);
        assertThat(sut.state()).isEqualTo(CONNECTED);

        // Act
        sut.deliveriesReady(List.of(earlier, later));

        // Assert: one PUBACK covers the reused identifier.
        assertThat(pubAckIds()).containsExactly(8, 7);
        assertThat(port.released).containsExactly(earlier, following, later);
    }

    @Test
    void reuse_without_dup_after_its_puback_is_queued_starts_a_new_reception() {
        // Arrange: the earlier PUBACK is queued, but its write has not been reported.
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        sut.deliveryReady(port.deliveries.getFirst().token());
        long earlierAck = port.ackTickets.getFirst();

        // Act: the reported write must not free the identifier of the new reception.
        receive(1, qosOnePublication(7, "a/b", false), 0);
        sut.written(1, earlierAck);
        receive(1, qosOnePublication(7, "a/b", true), 0);

        // Assert: the retransmission joins the new reception.
        assertThat(port.deliveries).hasSize(2);
        assertThat(pubAckIds()).containsExactly(7);

        // Act
        sut.deliveryReady(port.deliveries.getLast().token());

        // Assert
        assertThat(pubAckIds()).containsExactly(7, 7);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void old_generation_readiness_and_puback_do_not_finish_new_connection_work() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        long oldToken = port.deliveries.getLast().token();
        sut.failed(1, new IOException("Lost connection"));
        sut.connect(3, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        sut.subscribe(4, subscription("a/#"));
        receive(2, qosOnePublication(7, "a/b", false), 0);
        long newToken = port.deliveries.getLast().token();
        sut.publish(5, qosOneMessage());
        int id = port.publicationIds.getLast();

        // Act
        sut.deliveryReady(oldToken);
        sut.deliveryReady(oldToken);
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert
        assertThat(newToken).isGreaterThan(oldToken);
        assertThat(pubAckIds()).isEmpty();
        assertThat(port.results).doesNotContainKey(5L);

        // Act
        sut.deliveryReady(newToken);
        receive(2, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert
        assertThat(pubAckIds()).containsExactly(7);
        assertThat(requireNonNull(port.results.get(5L)).value()).isEqualTo(PublishResult.of());
    }

    @Test
    void pending_unmatched_acknowledgements_are_bounded_and_cleared_on_connection_end() {
        // Arrange: one unfinished delivery holds the head, including unmatched followers.
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(1, "a/b", false), 0);
        for (int id = 2; id <= 4096; id++) {
            receive(1, qosOnePublication(id, "other", false), 0);
        }
        assertThat(sut.state()).isEqualTo(CONNECTED);

        // Act
        receive(1, qosOnePublication(4097, "other", false), 0);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class));
        assertThat(pubAckIds()).isEmpty();

        // Act
        sut.connect(3, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        receive(2, qosOnePublication(1, "other", false), 0);

        // Assert
        assertThat(pubAckIds()).containsExactly(1);
    }

    @Test
    void full_puback_reservation_keeps_the_head_and_resumes_only_as_capacity_returns() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        for (int id = 1; id <= 100; id++) {
            receive(1, qosOnePublication(id, "a/b", false), 0);
        }
        port.ackSlots = 2;

        // Act
        sut.deliveriesReady(port.deliveries.stream().map(Received::token).toList());

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(pubAckIds()).containsExactly(1, 2);
        assertThat(port.released).hasSize(2);
        assertThat(port.ends).isEmpty();

        // Act: write observation precedes budget release; neither event loses the waiting head.
        for (int i = 0; i < 100; i++) {
            sut.written(1, port.ackTickets.get(i));
            port.ackSlots++;
            sut.sendCapacityAvailable(1);
        }

        // Assert: draining requires no new input or delivery notification.
        assertThat(pubAckIds()).isEqualTo(IntStream.rangeClosed(1, 100).boxed().toList());
        assertThat(port.released).hasSize(100).doesNotHaveDuplicates();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void disconnect_prevents_deferred_pubacks_from_refilling_the_queue() {
        // Arrange
        connected();
        receive(1, qosOnePublication(7, "other", false), 0);
        port.ackSlots = 0;
        receive(1, qosOnePublication(8, "other", false), 0);
        sut.disconnect(2);

        // Act
        port.ackSlots = 14;
        sut.written(1, port.ackTickets.getFirst());
        sut.sendCapacityAvailable(1);

        // Assert
        assertThat(pubAckIds()).containsExactly(7);
        assertThat(port.sent.getLast()).isInstanceOf(Packet.Disconnect.class);
    }

    @Test
    void closed_puback_queue_preserves_the_original_terminal_cause() {
        // Arrange
        connected();
        port.sendResult = SendQueue.OfferResult.CLOSED;

        // Act
        receive(1, qosOnePublication(7, "other", false), 0);

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.released).isEmpty();
        assertThat(port.ends).isEmpty();

        // Act
        var failure = new IOException("Transport closed during PUBACK");
        sut.failed(1, failure);

        // Assert
        assertThat(port.ends).containsExactly(Disconnection.of(failure, false));
    }

    /// Publishes, then reports the write completion and PUBACK in a seed-chosen order.
    private ArrayDeque<Runnable> publicationChain(long operation, Random random) {
        var exchange = new Exchange();
        Runnable written = () -> {
            if (exchange.packetId != 0) {
                sut.sent(1, exchange.ticket, 0);
            }
        };
        Runnable acknowledged = () -> {
            if (exchange.packetId != 0) {
                receive(1, new Packet.PubAck(exchange.packetId, ReasonCode.SUCCESS, EMPTY), 0);
            }
        };
        var chain = new ArrayDeque<Runnable>();
        chain.add(() -> {
            int published = port.publicationIds.size();
            sut.publish(operation, qosOneMessage());
            if (port.publicationIds.size() > published) {
                exchange.packetId = port.publicationIds.getLast();
                exchange.ticket = port.ticket;
            }
        });
        chain.addAll(random.nextBoolean()
                ? List.of(written, acknowledged)
                : List.of(acknowledged, written));
        return chain;
    }

    /// Receives a QoS 1 publication, then reports its delivery readiness if it was routed.
    /// Receptions while connected are admitted in this order, which PUBACKs must follow.
    private ArrayDeque<Runnable> receptionChain(
            int packetId,
            String topic,
            List<Integer> admitted
    ) {
        var exchange = new Exchange();
        var chain = new ArrayDeque<Runnable>();
        chain.add(() -> {
            if (sut.state() == CONNECTED) {
                admitted.add(packetId);
            }
            int routed = port.deliveries.size();
            receive(1, qosOnePublication(packetId, topic, false), 0);
            if (port.deliveries.size() > routed) {
                exchange.token = port.deliveries.getLast().token();
            }
        });
        chain.add(() -> {
            if (exchange.token != 0) {
                sut.deliveryReady(exchange.token);
            }
        });
        return chain;
    }

    private List<Integer> pubAckIds() {
        return port.sent.stream()
                .filter(Packet.PubAck.class::isInstance)
                .map(Packet.PubAck.class::cast)
                .map(Packet.PubAck::packetId)
                .toList();
    }

    private static MqttMessage qosOneMessage() {
        return MqttMessage.of(new Topic("a/b"), new byte[] {1, 2}, Qos.AT_LEAST_ONCE, false);
    }

    private static Packet.Publish qosOnePublication(int packetId, String topic, boolean dup) {
        return new Packet.Publish(
                new Topic(topic), new byte[] {1, 2}, Qos.AT_LEAST_ONCE, false, dup, packetId, EMPTY
        );
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
        return Subscribe.of(new TopicFilter(filter), Qos.AT_MOST_ONCE).withBufferCapacity(4);
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

    /// State one exchange's later steps read after its first step; all steps run on one thread.
    private static final class Exchange {

        int packetId;
        long ticket;
        long token;
    }

    private record Received(
            DeliveryToken delivery,
            List<Long> ids,
            MqttMessage message,
            boolean dup,
            int bytes
    ) {
        long token() {
            return delivery.acknowledgement();
        }
    }

    private record Removal(long id, Subscription handle, @Nullable Throwable cause) {
    }

    private static final class TestSubscription implements Subscription {

        final long id;
        @Nullable TopicFilter filter;
        Qos granted = Qos.AT_MOST_ONCE;

        TestSubscription(long id) {
            this.id = id;
        }

        @Override
        public TopicFilter filter() {
            return requireNonNull(filter);
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
    }

    private static final class RecordingPort implements SessionCore.Port {

        final List<Packet> sent = new ArrayList<>();
        final List<MqttMessage> publications = new ArrayList<>();
        final List<Integer> publicationIds = new ArrayList<>();
        final List<Disconnection> ends = new ArrayList<>();
        final List<Boolean> established = new ArrayList<>();
        final Map<Long, Result> results = new HashMap<>();
        final Map<SessionCore.Timer, Duration> timers = new EnumMap<>(SessionCore.Timer.class);
        final List<Long> opened = new ArrayList<>();
        final List<Long> registered = new ArrayList<>();
        final List<Long> removed = new ArrayList<>();
        final List<Removal> removals = new ArrayList<>();
        final List<Subscription> grants = new ArrayList<>();
        @Nullable TestSubscription registering;
        final List<Received> deliveries = new ArrayList<>();
        final List<Long> released = new ArrayList<>();
        final List<Long> ackTickets = new ArrayList<>();
        int ackSlots = Integer.MAX_VALUE;
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
        public SendQueue.OfferResult send(
                long generation,
                long id,
                Packet packet,
                @Nullable SendReceipt receipt
        ) {
            ticket = id;
            if (packet instanceof Packet.Subscribe subscribe && registering != null) {
                registering.filter = subscribe.subscriptions().getFirst().filter();
                registering = null;
            }
            if (packet instanceof Packet.PubAck && ackSlots == 0) {
                return SendQueue.OfferResult.FULL;
            }
            if (sendResult == SendQueue.OfferResult.ACCEPTED) {
                if (packet instanceof Packet.PubAck) {
                    ackSlots--;
                    ackTickets.add(id);
                }
                sent.add(packet);
            }
            return sendResult;
        }

        @Override
        public SendQueue.OfferResult publish(
                long generation,
                long id,
                MqttMessage message,
                int packetId,
                @Nullable SendReceipt receipt
        ) {
            ticket = id;
            publications.add(message);
            publicationIds.add(packetId);
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
        public Optional<Subscription> registerSubscription(long id) {
            if (!acceptRegistration) {
                return Optional.empty();
            }
            registered.add(id);
            var handle = new TestSubscription(id);
            registering = handle;
            return Optional.of(handle);
        }

        @Override
        public void subscribed(Subscription handle, Qos granted) {
            ((TestSubscription) handle).granted = granted;
            grants.add(handle);
        }

        @Override
        public boolean deliver(
                DeliveryToken token,
                List<Long> ids,
                MqttMessage message,
                boolean dup,
                int bytes
        ) {
            if (acceptDelivery) {
                deliveries.add(new Received(token, List.copyOf(ids), message, dup, bytes));
            }

            return acceptDelivery;
        }

        @Override
        public void releaseDelivery(long token) {
            released.add(token);
        }

        @Override
        public void forgetDelivery(long token) {
        }

        @Override
        public Map<Long, Boolean> receiveState() {
            return Map.of();
        }

        @Override
        public boolean readyDelivery(long token) {
            return false;
        }

        @Override
        public void sessionChanged(long session) {
        }

        @Override
        public void removeSubscription(long id, Subscription handle, @Nullable Throwable cause) {
            assertThat(((TestSubscription) handle).id).isEqualTo(id);
            removed.add(id);
            removals.add(new Removal(id, handle, cause));
        }

        @Override
        public Map<Long, Boolean> end(
                long generation,
                boolean connected,
                Disconnection end,
                boolean preserveSession,
                boolean stopped
        ) {
            established.add(connected);
            ends.add(end);
            return Map.of();
        }
    }
}
