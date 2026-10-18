package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttSubscribeException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CLOSED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CONNECTING;
import static org.mqtt4j.hato.client.internal.SessionCore.State.DISCONNECTED;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class SessionPersistenceTest {

    private final RecordingPort port = new RecordingPort();
    private final SessionCore sut = core(false);
    private long nextOperation;
    private long generation;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void connect_uses_the_configured_clean_session_flag(boolean cleanStart) {
        SessionCore configured = core(cleanStart);
        configured.connect(new ConnectionAttempt(1), 0);
        configured.opened(1);

        assertThat(port.sent).singleElement().satisfies(packet -> {
            var request = (Packet.Connect) packet;
            assertThat(request.cleanStart()).isEqualTo(cleanStart);
            assertThat(request.clientId()).isEqualTo("client");
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void retained_publications_replay_in_order_with_the_original_ids_and_handoff_flags(
            boolean completedWrite
    ) {
        connect(false);
        long firstOperation = publish(message("first"));
        Publication first = port.accepted.getLast();
        long secondOperation = publish(message("second"));
        Publication second = port.accepted.getLast();
        requireNonNull(first.receipt()).markAccepted();
        if (completedWrite) {
            sut.sent(generation, first.ticket(), 0);
        }
        var failure = new IOException("connection lost");
        sut.failed(generation, failure);
        assertFailure(firstOperation, MqttConnectionLostException.class);
        assertFailure(secondOperation, MqttConnectionLostException.class);
        assertThat(result(firstOperation).failure()).hasCause(failure);
        Result firstFailure = result(firstOperation);
        Result secondFailure = result(secondOperation);
        int start = port.accepted.size();

        connect(true);

        assertThat(port.accepted.subList(start, port.accepted.size()))
                .extracting(Publication::packetId)
                .containsExactly(first.packetId(), second.packetId());
        // Each replay carries its original receipt, from which the I/O derives DUP.
        assertThat(port.accepted.get(start).receipt()).isSameAs(first.receipt());
        assertThat(port.accepted.get(start + 1).receipt()).isSameAs(second.receipt());
        assertThat(port.accepted.get(start).message()).isSameAs(first.message());
        assertThat(port.accepted.get(start + 1).message()).isSameAs(second.message());
        acknowledge(first.packetId());
        acknowledge(second.packetId());
        assertThat(result(firstOperation)).isSameAs(firstFailure);
        assertThat(result(secondOperation)).isSameAs(secondFailure);
    }

    @Test
    void session_loss_discards_old_publications_and_old_connection_events() {
        connect(false);
        long original = publish(message("old"));
        Publication old = port.accepted.getLast();
        requireNonNull(old.receipt()).markAccepted();
        sut.failed(generation, new IOException("lost"));
        int start = port.accepted.size();

        connect(false);

        assertThat(port.accepted).hasSize(start);
        long current = publish(message("new"));
        Publication fresh = port.accepted.getLast();
        assertThat(requireNonNull(fresh.receipt()).accepted()).isFalse();
        sut.received(
                old.generation(),
                new Packet.PubAck(old.packetId(), ReasonCode.SUCCESS, EMPTY),
                0,
                0
        );
        sut.sent(old.generation(), old.ticket(), 0);
        assertThat(port.results).doesNotContainKey(current);
        assertFailure(original, MqttConnectionLostException.class);
        acknowledge(fresh.packetId());
        assertThat(result(current).value()).isEqualTo(PublishResult.of());
    }

    @ParameterizedTest
    @EnumSource(Qos.class)
    void full_replay_capacity_keeps_the_head_and_rejects_new_publish_until_drained(Qos qos) {
        connect(false);
        publish(message("first"));
        publish(message("second"));
        List<Publication> old = List.copyOf(port.accepted);
        sut.failed(generation, new IOException("lost"));
        port.publishAllowance = 0;
        connect(true);
        int offers = port.offers.size();

        long refused = publish(message("must wait").withQos(qos));

        assertFailure(refused, MqttCapacityException.class);
        assertThat(port.offers).hasSize(offers);
        assertThat(sut.state()).isEqualTo(CONNECTED);
        port.publishAllowance = 1;
        sut.sendCapacityAvailable(generation);
        assertThat(port.accepted.getLast().packetId()).isEqualTo(old.getFirst().packetId());
        long stillRefused = publish(message("still waiting").withQos(qos));
        assertFailure(stillRefused, MqttCapacityException.class);
        port.publishAllowance = 2;
        sut.sendCapacityAvailable(generation);
        assertThat(port.accepted.getLast().packetId()).isEqualTo(old.getLast().packetId());
        long admitted = publish(message("after replay").withQos(qos));
        assertThat(port.results).doesNotContainKey(admitted);
        assertThat(port.accepted.getLast().packetId())
                .isNotIn(old.stream().map(Publication::packetId).toList());
    }

    @ParameterizedTest(name = "clean={0}, present={1}, subscriptions={2}")
    @CsvSource({"true, false, 17", "false, false, 256", "false, true, 17"})
    void subscription_restoration_does_not_block_new_publications(
            boolean cleanStart,
            boolean present,
            int subscriptions
    ) {
        // Arrange: leave more registrations to restore than the pending SUBACK window permits.
        SessionCore configured = core(cleanStart);
        configured.connect(new ConnectionAttempt(operation()), 0);
        configured.received(1, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0, 0);
        for (int i = 0; i < subscriptions; i++) {
            long subscription = operation();
            configured.subscribe(
                    subscription,
                    Subscribe.of(new TopicFilter("restored/" + i), Qos.AT_LEAST_ONCE)
            );
            if (!present) {
                var request = (Packet.Subscribe) port.sent.getLast();
                configured.received(
                        1,
                        new Packet.SubAck(
                                request.packetId(),
                                List.of(ReasonCode.GRANTED_QOS_1),
                                EMPTY
                        ),
                        0,
                        0
                );
            }
        }
        configured.failed(1, new IOException("connection lost before restoration"));
        port.sent.clear();
        configured.connect(new ConnectionAttempt(operation()), 0);
        configured.received(2, new Packet.ConnAck(present, ReasonCode.SUCCESS, EMPTY), 0, 0);
        assertThat(port.sent).hasSize(16).allMatch(Packet.Subscribe.class::isInstance);

        // Act: no restoration SUBACK arrives, but ordinary send capacity remains available.
        for (Qos qos : List.of(Qos.AT_MOST_ONCE, Qos.AT_LEAST_ONCE)) {
            long publication = operation();
            configured.publish(publication, message("during-restoration").withQos(qos));

            // Assert: both acknowledgement modes still use their normal completion path.
            assertThat(port.results).doesNotContainKey(publication);
            Publication admitted = port.accepted.getLast();
            assertThat(admitted.message().qos()).isEqualTo(qos);
            if (qos == Qos.AT_MOST_ONCE) {
                configured.sent(2, admitted.ticket(), 0);
            } else {
                configured.received(
                        2,
                        new Packet.PubAck(admitted.packetId(), ReasonCode.SUCCESS, EMPTY),
                        0,
                        0
                );
            }
            assertThat(result(publication).value()).isEqualTo(PublishResult.of());
        }
        assertThat(port.accepted).hasSize(2);
        assertThat(port.sent).hasSize(16);
        assertThat(configured.state()).isEqualTo(CONNECTED);
    }

    @Test
    void an_acknowledged_backlog_entry_is_not_replayed_or_allowed_to_hold_the_head() {
        connect(false);
        publish(message("old"));
        Publication old = port.accepted.getLast();
        requireNonNull(old.receipt()).markAccepted();
        sut.failed(generation, new IOException("lost"));
        port.publishAllowance = 0;
        connect(true);
        acknowledge(old.packetId());
        int start = port.accepted.size();
        port.publishAllowance = 1;

        sut.sendCapacityAvailable(generation);
        long operation = publish(message("new"));

        assertThat(port.accepted).hasSize(start + 1);
        assertThat(port.accepted.getLast().message().topic()).isEqualTo(new Topic("new"));
        assertThat(port.results).doesNotContainKey(operation);
    }

    @Test
    void closed_replay_capacity_waits_for_the_original_terminal_cause() {
        connect(false);
        long original = publish(message("old"));
        sut.failed(generation, new IOException("first loss"));
        Result originalFailure = result(original);
        port.publishResult = SendQueue.OfferResult.CLOSED;
        connect(true);
        int endings = port.ends.size();
        var failure = new IOException("original terminal cause");

        sut.failed(generation, failure);

        assertThat(port.ends).hasSize(endings + 1);
        assertThat(port.ends.getLast().cause()).containsSame(failure);
        assertThat(result(original)).isSameAs(originalFailure);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void stopped_futures_keep_all_sixty_four_wire_slots_until_ack_or_session_loss(boolean present) {
        connect(false);
        for (int i = 0; i < 64; i++) {
            publish(message("slot/" + i));
        }
        List<Publication> original = List.copyOf(port.accepted);
        long full = publish(message("full"));
        assertFailure(full, MqttCapacityException.class);
        sut.failed(generation, new IOException("lost"));
        connect(present);

        long afterReconnect = publish(message("after reconnect"));

        if (present) {
            assertFailure(afterReconnect, MqttCapacityException.class);
            acknowledge(original.getFirst().packetId());
            long admitted = publish(message("released slot"));
            assertThat(port.results).doesNotContainKey(admitted);
            assertThat(port.accepted.getLast().packetId()).isNotIn(
                    original.subList(1, 64).stream()
                            .map(Publication::packetId).toList()
            );
            assertFailure(publish(message("full again")), MqttCapacityException.class);
        } else {
            assertThat(port.results).doesNotContainKey(afterReconnect);
            assertThat(port.accepted).hasSize(65);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void exact_thirty_two_mebibyte_wire_budget_survives_failure_until_ack_or_loss(boolean present) {
        connect(false);
        MqttMessage half = messageWithWireBytes(16 * 1024 * 1024);
        publish(half);
        Publication first = port.accepted.getLast();
        requireNonNull(first.receipt()).markAccepted();
        sut.sent(generation, first.ticket(), 0);
        publish(half);
        assertThat(port.accepted).hasSize(2);
        assertFailure(publish(message("too much")), MqttCapacityException.class);
        sut.failed(generation, new IOException("lost"));
        connect(present);

        long afterReconnect = publish(message("after reconnect"));

        if (present) {
            assertFailure(afterReconnect, MqttCapacityException.class);
            acknowledge(first.packetId());
            long admitted = publish(half);
            assertThat(port.results).doesNotContainKey(admitted);
            assertFailure(publish(message("full again")), MqttCapacityException.class);
        } else {
            assertThat(port.results).doesNotContainKey(afterReconnect);
            assertThat(port.accepted).hasSize(3);
        }
    }

    @Test
    void full_admission_rolls_back_the_session_budget_and_identifier() {
        connect(false);
        MqttMessage half = messageWithWireBytes(16 * 1024 * 1024);
        port.publishResult = SendQueue.OfferResult.FULL;
        for (int i = 0; i < 65; i++) {
            assertFailure(publish(half), MqttCapacityException.class);
        }
        port.publishResult = SendQueue.OfferResult.ACCEPTED;

        long first = publish(half);
        long second = publish(half);

        assertThat(port.results).doesNotContainKeys(first, second);
        assertThat(port.accepted).hasSize(2);
        assertThat(port.accepted).extracting(Publication::packetId).doesNotHaveDuplicates();
    }

    @Test
    void clean_sessions_do_not_retain_the_new_session_byte_budget() {
        SessionCore clean = core(true);
        clean.connect(new ConnectionAttempt(1), 0);
        clean.received(1, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0, 0);
        MqttMessage half = messageWithWireBytes(16 * 1024 * 1024);

        for (int i = 0; i < 3; i++) {
            clean.publish(2 + i, half);
            clean.sent(1, port.ticket, 0);
        }

        assertThat(port.accepted).hasSize(3);
        assertThat(port.results).doesNotContainKeys(2L, 3L, 4L);
    }

    @Test
    void qos_zero_is_not_replayed_after_a_persistent_connection_ends() {
        connect(false);
        long operation = publish(message("qos-zero").withQos(Qos.AT_MOST_ONCE));
        sut.failed(generation, new IOException("lost"));
        int sent = port.accepted.size();

        connect(true);

        assertFailure(operation, MqttConnectionLostException.class);
        assertThat(port.accepted).hasSize(sent);
    }

    @Test
    void an_empty_established_session_can_resume_with_session_present() {
        connect(false);
        sut.failed(generation, new IOException("lost without traffic"));

        connect(true);

        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.accepted).isEmpty();
    }

    @Test
    void first_session_present_is_disconnected_without_delivering_or_acknowledging() {
        long operation = beginConnect();
        receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
        Packet packet = port.sent.getLast();
        assertThat(packet).isInstanceOf(Packet.Disconnect.class);
        long disconnectTicket = port.ticket;
        int sent = port.sent.size();
        assertThat(sut.state()).isEqualTo(CONNECTING);
        assertThat(port.results).doesNotContainKey(operation);
        assertThat(port.timers).containsOnlyKeys(SessionCore.Timer.DISCONNECT);

        receive(
                new Packet.Publish(
                        new Topic("unowned"),
                        new byte[] {1},
                        Qos.AT_LEAST_ONCE,
                        false,
                        true,
                        7,
                        EMPTY
                )
        );
        sut.sent(generation, disconnectTicket, 0);

        assertThat(port.sent).hasSize(sent);
        assertThat(port.deliveries).isEmpty();
        Throwable failure = assertSessionRefusal(operation, DISCONNECTED);
        assertThat(failure.getSuppressed()).isEmpty();
        // The refusing DISCONNECT is the client's own; the application requested none.
        assertThat(port.ends.getLast().userRequested()).isFalse();
    }

    @Test
    void a_disconnect_deadline_preserves_the_session_refusal() {
        // Arrange
        long operation = beginConnect();
        receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));

        // Act
        sut.timer(generation, SessionCore.Timer.DISCONNECT, Duration.ofSeconds(30).toNanos());

        // Assert
        Throwable failure = assertSessionRefusal(operation, DISCONNECTED);
        assertThat(failure.getSuppressed()).singleElement()
                .satisfies(cause -> assertThat(cause).isInstanceOf(TimeoutException.class)
                        .hasMessage("DISCONNECT write deadline expired"));
    }

    @Test
    void a_full_disconnect_queue_preserves_the_session_refusal() {
        // Arrange
        long operation = beginConnect();
        port.disconnectResult = SendQueue.OfferResult.FULL;

        // Act
        receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));

        // Assert
        Throwable failure = assertSessionRefusal(operation, DISCONNECTED);
        assertThat(failure.getSuppressed()).singleElement()
                .satisfies(cause -> assertThat(cause).isInstanceOf(MqttCapacityException.class)
                        .hasMessage("Reserved send queue capacity"));
    }

    @Test
    void a_transport_failure_preserves_the_session_refusal_and_ignores_late_events() {
        // Arrange
        long operation = beginConnect();
        receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
        long disconnectTicket = port.ticket;
        var transportFailure = new IOException("closed before DISCONNECT completed");

        // Act
        sut.failed(generation, transportFailure);

        // Assert
        Throwable failure = assertSessionRefusal(operation, DISCONNECTED);
        assertThat(failure.getSuppressed()).containsExactly(transportFailure);

        // Late transport events cannot complete the attempt again or alter its diagnosis.
        sut.sent(generation, disconnectTicket, 0);
        sut.failed(generation, new IOException("late termination"));
        assertThat(result(operation).failure()).isSameAs(failure);
        assertThat(failure.getSuppressed()).containsExactly(transportFailure);
        assertThat(port.ends).hasSize(1);
    }

    @ParameterizedTest
    @NullSource
    @MethodSource("closeFailures")
    void explicit_close_preserves_the_session_refusal(@Nullable Throwable cause) {
        // Arrange
        long operation = beginConnect();
        receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));

        // Act
        sut.close(cause);
        sut.close(null);

        // Assert
        Throwable failure = assertSessionRefusal(operation, CLOSED);
        assertThat(failure.getSuppressed())
                .containsExactlyElementsOf(Optional.ofNullable(cause).stream().toList());
        assertThat(port.ends).hasSize(1);
    }

    @Test
    void a_session_refusal_does_not_replace_a_later_connection_attempts_failure() {
        // Arrange
        long refusedOperation = beginConnect();
        receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
        var cleanupFailure = new IOException("refused connection closed");
        sut.failed(generation, cleanupFailure);
        Throwable refusal = requireNonNull(result(refusedOperation).failure());
        long nextAttempt = beginConnect();
        var connectionFailure = new IOException("next connection failed");

        // Act: the new attempt fails before receiving any CONNACK.
        sut.failed(generation, connectionFailure);

        // Assert
        Throwable failure = requireNonNull(result(nextAttempt).failure());
        assertThat(failure).isInstanceOf(MqttConnectException.class)
                .hasMessage("Connection failed").hasCause(connectionFailure).isNotSameAs(refusal);
        assertThat(failure.getSuppressed()).isEmpty();
        assertThat(refusal).hasMessage("Broker session has no local state");
        assertThat(refusal.getSuppressed()).containsExactly(cleanupFailure);
        // The failed attempt reports the same failure as its connect().
        assertThat(port.ends.getLast().cause()).containsSame(failure);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.timers).isEmpty();
    }

    @Test
    void clean_session_present_remains_a_protocol_error() {
        SessionCore clean = core(true);
        clean.connect(new ConnectionAttempt(1), 0);
        clean.received(1, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 0, 0);

        assertThat(result(1L).failure()).isInstanceOf(MqttConnectException.class)
                .hasCauseInstanceOf(MqttProtocolException.class);
        assertThat(port.sent).noneMatch(Packet.Disconnect.class::isInstance);
    }

    @Test
    void confirmed_subscriptions_are_not_recreated_when_the_session_survives() {
        connect(false);
        long subscription = subscribe("kept/#");
        establishLastSubscription();
        var handle = (Subscription) outcome(subscription).value();
        sut.failed(generation, new IOException("lost"));
        port.sent.clear();

        connect(true);
        receive(
                new Packet.Publish(
                        new Topic("kept/topic"),
                        new byte[] {1},
                        Qos.AT_MOST_ONCE,
                        false,
                        false,
                        0,
                        EMPTY
                )
        );

        assertThat(port.sent).noneMatch(Packet.Subscribe.class::isInstance);
        assertThat(port.handles.get(subscription)).isSameAs(handle);
        assertThat(port.grants).containsExactly(Qos.AT_LEAST_ONCE, Qos.AT_LEAST_ONCE);
        assertThat(port.deliveries).containsExactly(List.of(subscription));
        assertThat(port.removed).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void closed_subscriptions_are_reconciled_after_reconnect(boolean present) {
        connect(false);
        long subscription = subscribe("closed/#");
        establishLastSubscription();
        long closing = operation();
        sut.unsubscribe(closing, subscription);
        Packet.Unsubscribe original = (Packet.Unsubscribe) port.sent.getLast();
        sut.failed(generation, new IOException("lost UNSUBACK"));
        port.sent.clear();

        connect(present);

        assertThat(port.sent).noneMatch(Packet.Subscribe.class::isInstance);
        if (present) {
            Packet.Unsubscribe retried = (Packet.Unsubscribe) port.sent.getLast();
            assertThat(retried.filters()).isEqualTo(original.filters());
            assertThat(port.results).doesNotContainKey(closing);
            receive(new Packet.UnsubAck(retried.packetId(), List.of(), EMPTY));
        } else {
            assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
        }
        assertThat(result(closing).failure()).isNull();
        assertThat(port.removed).containsExactly(subscription);
    }

    @Test
    void an_unconfirmed_removal_is_retried_when_the_broker_session_survives() {
        // Arrange
        connect(false);
        long subscription = subscribe("removed/#");
        establishLastSubscription();
        long removal = operation();
        sut.unsubscribe(removal, subscription);
        var original = (Packet.Unsubscribe) port.sent.getLast();
        sut.failed(generation, new IOException("lost UNSUBACK"));
        port.sent.clear();

        // Act
        connect(true);

        // Assert: the request keeps waiting and the registration never resumes delivery.
        assertThat(port.results).doesNotContainKey(removal);
        assertThat(port.sent).filteredOn(Packet.Unsubscribe.class::isInstance).hasSize(1);
        var retried = (Packet.Unsubscribe) port.sent.getLast();
        assertThat(retried.filters()).isEqualTo(original.filters());
        receive(new Packet.UnsubAck(retried.packetId(), List.of(), EMPTY));
        assertThat(result(removal).failure()).isNull();
        assertThat(port.removed).containsExactly(subscription);
    }

    @Test
    void an_invalid_suback_ends_a_persistent_registration_and_removes_it_on_reconnection() {
        // Arrange
        connect(false);
        long subscription = subscribe("invalid/#");
        var request = (Packet.Subscribe) port.sent.getLast();

        // Act: a QoS 2 grant exceeds the QoS 1 request.
        receive(new Packet.SubAck(request.packetId(), List.of(ReasonCode.GRANTED_QOS_2), EMPTY));

        // Assert: the handle ends at once, while the filter stays reserved because the broker
        // may hold the subscription; the next connection removes it instead of subscribing.
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(outcome(subscription).failure()).isInstanceOf(MqttProtocolException.class);
        assertFailure(subscribe("invalid/#"), IllegalStateException.class);
        port.sent.clear();
        connect(true);
        assertThat(port.sent).noneMatch(Packet.Subscribe.class::isInstance);
        var removal = (Packet.Unsubscribe) port.sent.getLast();
        assertThat(removal.filters()).containsExactly(new TopicFilter("invalid/#"));
        receive(new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY));
        long again = subscribe("invalid/#");
        assertThat(port.results).doesNotContainKey(again);
        assertThat(port.sent).last().isInstanceOf(Packet.Subscribe.class);
    }

    @Test
    void removing_a_deferred_restore_cleans_up_the_surviving_broker_subscription() {
        // Arrange: the first SUBSCRIBE may have reached the persistent broker without a SUBACK.
        connect(false);
        long subscription = subscribe("still-registered/#");
        sut.failed(generation, new IOException("SUBSCRIBE outcome unknown"));
        port.subscribeResult = SendQueue.OfferResult.FULL;
        connect(true);
        port.sent.clear();
        port.subscribeResult = SendQueue.OfferResult.ACCEPTED;

        // Act: remove the handle before its queued SUBSCRIBE can be admitted.
        long removal = operation();
        sut.unsubscribe(removal, subscription);

        // Assert: Session Present means the broker registration may still need removal.
        assertThat(port.results).doesNotContainKey(removal);
        assertThat(port.removed).isEmpty();
        assertThat(port.sent).filteredOn(Packet.Unsubscribe.class::isInstance).hasSize(1);
        var request = (Packet.Unsubscribe) port.sent.getLast();
        assertThat(request.filters()).containsExactly(new TopicFilter("still-registered/#"));
        receive(new Packet.UnsubAck(request.packetId(), List.of(), EMPTY));
        sut.sendCapacityAvailable(generation);
        assertThat(result(removal).failure()).isNull();
        assertThat(port.removed).containsExactly(subscription);
        assertThat(port.sent).hasSize(1);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void an_unconfirmed_registration_is_retried_with_or_without_the_broker_session(
            boolean present
    ) {
        // Arrange
        connect(false);
        long subscription = subscribe("pending/#");
        Subscription handle = port.handles.get(subscription);
        sut.failed(generation, new IOException("lost SUBACK"));
        port.sent.clear();

        // Act
        connect(present);

        // Assert: the original handle and registration wait for the new acknowledgement.
        assertThat(port.outcomes).doesNotContainKey(subscription);
        assertThat(port.removed).isEmpty();
        assertThat(port.sent).filteredOn(Packet.Subscribe.class::isInstance).hasSize(1);
        establishLastSubscription();
        assertThat(outcome(subscription).value()).isSameAs(handle);
        assertThat(outcome(subscription).failure()).isNull();
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
    }

    @Test
    void an_unconfirmed_registration_keeps_its_filter_reserved_across_connections() {
        // Arrange
        connect(false);
        long original = subscribe("reserved/#");
        sut.failed(generation, new IOException("lost SUBACK"));
        connect(true);

        // Act
        long duplicate = subscribe("reserved/#");
        establishLastSubscription();
        receive(
                new Packet.Publish(
                        new Topic("reserved/topic"),
                        new byte[] {1},
                        Qos.AT_MOST_ONCE,
                        false,
                        false,
                        0,
                        EMPTY
                )
        );

        // Assert
        assertFailure(duplicate, IllegalStateException.class);
        assertThat(port.removed).isEmpty();
        assertThat(port.deliveries).containsExactly(List.of(original));
        assertThat(outcome(original).failure()).isNull();
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
    }

    @Test
    void old_publish_ids_remain_reserved_for_subscription_and_unsubscription() {
        connect(false);
        publish(message("old"));
        int oldId = port.accepted.getLast().packetId();
        long subscription = subscribe("kept/#");
        establishLastSubscription();
        sut.failed(generation, new IOException("lost"));
        connect(true);

        long newSubscription = subscribe("new/#");
        var subscribe = (Packet.Subscribe) port.sent.getLast();
        assertThat(subscribe.packetId()).isNotEqualTo(oldId);
        sut.unsubscribe(operation(), subscription);
        var unsubscribe = (Packet.Unsubscribe) port.sent.getLast();
        assertThat(unsubscribe.packetId()).isNotIn(oldId, subscribe.packetId());
        receive(new Packet.SubAck(subscribe.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
        assertThat(outcome(newSubscription).failure()).isNull();
    }

    @Test
    void disconnect_and_close_do_not_complete_an_already_failed_publication_again() {
        connect(false);
        long publication = publish(message("pending"));
        long disconnect = operation();
        sut.disconnect(disconnect);
        sut.sent(generation, port.ticket, 0);
        Result failure = result(publication);
        assertFailure(publication, MqttConnectionLostException.class);
        connect(true);
        int id = port.accepted.getLast().packetId();
        acknowledge(id);

        sut.close(null);
        sut.close(null);
        acknowledge(id);

        assertThat(result(publication)).isSameAs(failure);
        assertThat(result(disconnect).failure()).isNull();
        assertThat(sut.state()).isEqualTo(CLOSED);
        assertThat(port.timers).isEmpty();
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void reordered_acknowledgements_endings_and_resumption_complete_each_operation_once(int seed) {
        var random = new SplittableRandom(seed);
        connect(false);
        for (int round = 0; round < 2; round++) {
            var pending = new ArrayList<Publication>();
            var events = new ArrayList<>(List.of(-1));
            for (int i = 0; i < 3; i++) {
                publish(message("round/" + round + "/" + i));
                Publication publication = port.accepted.getLast();
                if (random.nextBoolean()) {
                    requireNonNull(publication.receipt()).markAccepted();
                    events.add(i);
                }
                pending.add(publication);
            }
            List<Publication> original = List.copyOf(pending);
            Collections.shuffle(events, random);
            long oldGeneration = generation;
            for (int event : events) {
                if (event == -1) {
                    sut.failed(oldGeneration, new IOException("seed " + seed));
                } else {
                    Publication publication = original.get(event);
                    if (sut.state() == CONNECTED) {
                        pending.remove(publication);
                    }
                    sut.received(
                            oldGeneration,
                            new Packet.PubAck(
                                    publication.packetId(),
                                    ReasonCode.SUCCESS,
                                    EMPTY
                            ),
                            0,
                            0
                    );
                    sut.sent(oldGeneration, publication.ticket(), 0);
                }
            }
            boolean present = ((seed + round) & 1) == 0;
            if (!present) {
                pending.clear();
            }
            int start = port.accepted.size();
            port.publishAllowance = random.nextInt(pending.size() + 1);
            connect(present);
            int admitted = port.accepted.size() - start;
            while (admitted < pending.size()) {
                assertFailure(publish(message("blocked")), MqttCapacityException.class);
                port.publishAllowance = 1;
                sut.sendCapacityAvailable(generation);
                assertThat(port.accepted.size() - start).isEqualTo(++admitted);
            }
            List<Publication> resumed = List.copyOf(
                    port.accepted.subList(
                            start,
                            port.accepted.size()
                    )
            );
            assertThat(resumed).extracting(Publication::packetId)
                    .containsExactlyElementsOf(
                            pending.stream()
                                    .map(Publication::packetId).toList()
                    );
            for (int i = 0; i < resumed.size(); i++) {
                assertThat(resumed.get(i).receipt()).isSameAs(pending.get(i).receipt());
            }
            var completions = new ArrayList<>(resumed);
            Collections.shuffle(completions, random);
            for (Publication publication : completions) {
                requireNonNull(publication.receipt()).markAccepted();
                if (random.nextBoolean()) {
                    sut.sent(generation, publication.ticket(), 0);
                    acknowledge(publication.packetId());
                } else {
                    acknowledge(publication.packetId());
                    sut.sent(generation, publication.ticket(), 0);
                }
                sut.sent(oldGeneration, original.getFirst().ticket(), 0);
            }
            port.publishAllowance = Integer.MAX_VALUE;
            assertThat(sut.state()).isEqualTo(CONNECTED);
        }
        sut.close(null);
        assertThat(sut.state()).isEqualTo(CLOSED);
        assertThat(port.timers).isEmpty();
    }

    @Test
    void offline_removal_is_confirmed_by_a_new_broker_session() {
        // Arrange
        connect(false);
        long subscription = subscribe("offline/#");
        establishLastSubscription();
        long removal = removeAfterConnectionLoss(subscription);

        // Act
        connect(false);

        // Assert: Session Present false proves that the broker holds nothing to remove.
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
        assertThat(result(removal).failure()).isNull();
        assertThat(port.removed).containsExactly(subscription);
        assertThat(port.sent).noneMatch(Packet.Subscribe.class::isInstance);
    }

    @Test
    void offline_removal_waits_for_unsuback_when_the_broker_session_continues() {
        // Arrange
        connect(false);
        long subscription = subscribe("offline/#");
        establishLastSubscription();
        long removal = removeAfterConnectionLoss(subscription);

        // Act
        connect(true);

        // Assert
        assertThat(port.results).doesNotContainKey(removal);
        var packet = (Packet.Unsubscribe) port.sent.getLast();
        receive(new Packet.UnsubAck(packet.packetId(), List.of(), EMPTY));
        assertThat(result(removal).failure()).isNull();
        assertThat(port.removed).containsExactly(subscription);
        assertThat(port.sent).noneMatch(Packet.Subscribe.class::isInstance);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void reordered_registrations_removals_and_reconnection_preserve_each_intent(int seed) {
        // Arrange: registration and removal can cross both sides of the connection boundary.
        var random = new SplittableRandom(seed);
        connect(false);
        long oldGeneration = generation;
        long continuing = operation();
        long removing = operation();
        long removal = operation();
        boolean present = (seed & 1) == 0;
        var connectionStart = new int[1];
        var chains = new ArrayList<ArrayDeque<Runnable>>();
        chains.add(new ArrayDeque<>(List.of(
                () -> {
                    sut.failed(oldGeneration, new IOException("connection lost"));
                    connectionStart[0] = port.sent.size();
                },
                () -> beginConnect(),
                () -> receive(new Packet.ConnAck(present, ReasonCode.SUCCESS, EMPTY))
        )));
        chains.add(new ArrayDeque<>(List.of(() -> sut.subscribe(
                continuing,
                Subscribe.of(new TopicFilter("continuing/#"), Qos.AT_LEAST_ONCE)
        ))));
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.subscribe(
                        removing,
                        Subscribe.of(new TopicFilter("removing/#"), Qos.AT_LEAST_ONCE)
                ),
                () -> sut.unsubscribe(removal, removing)
        )));

        // Act & Assert: old acknowledgements never change the resumed connection's work.
        while (!chains.isEmpty()) {
            ArrayDeque<Runnable> chain = chains.get(random.nextInt(chains.size()));
            chain.removeFirst().run();
            if (chain.isEmpty()) {
                chains.remove(chain);
            }
            assertThat(port.outcomes).doesNotContainKey(continuing);
            assertThat(port.removed).doesNotHaveDuplicates();
            assertThat(port.handles).hasSizeLessThanOrEqualTo(2);
        }
        sut.received(oldGeneration, new Packet.SubAck(1, List.of(ReasonCode.SUCCESS), EMPTY), 0, 0);
        sut.received(oldGeneration, new Packet.UnsubAck(1, List.of(), EMPTY), 0, 0);
        assertThat(sut.state()).isEqualTo(CONNECTED);
        for (int i = connectionStart[0]; i < port.sent.size(); i++) {
            switch (port.sent.get(i)) {
                case Packet.Subscribe packet -> receive(
                        new Packet.SubAck(
                                packet.packetId(),
                                List.of(ReasonCode.GRANTED_QOS_1),
                                EMPTY
                        )
                );
                case Packet.Unsubscribe packet -> receive(
                        new Packet.UnsubAck(packet.packetId(), List.of(), EMPTY)
                );
                default -> {}
            }
        }
        assertThat(outcome(continuing).failure()).isNull();
        assertThat(result(removal).failure()).isNull();
        assertThat(port.removed).containsExactly(removing);
        assertThat(outcome(continuing).value()).isSameAs(port.handles.get(continuing));
        assertThat(port.ends).hasSize(1);
    }

    private SessionCore core(boolean cleanStart) {
        return new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ZERO,
                cleanStart,
                null,
                false,
                ReconnectBackoff.disabled(),
                false,
                Duration.ofSeconds(60)
        );
    }

    private long operation() {
        return ++nextOperation;
    }

    private long beginConnect() {
        long operation = operation();
        sut.connect(new ConnectionAttempt(operation), 0);
        generation = port.generation;
        sut.opened(generation);
        return operation;
    }

    private void connect(boolean present) {
        long operation = beginConnect();
        receive(new Packet.ConnAck(present, ReasonCode.SUCCESS, EMPTY));
        assertThat(result(operation).value())
                .isEqualTo(ConnAck.of(present, port.attempts.getLast()));
    }

    private void receive(Packet packet) {
        sut.received(
                generation,
                packet,
                0,
                packet instanceof Packet.Publish publish ? publish.payload().length : 0
        );
    }

    private long publish(MqttMessage message) {
        long operation = operation();
        sut.publish(operation, message);
        return operation;
    }

    private long subscribe(String filter) {
        long operation = operation();
        sut.subscribe(operation, Subscribe.of(new TopicFilter(filter), Qos.AT_LEAST_ONCE));
        return operation;
    }

    /// Requests removal after the connection ends. The request waits, and the filter stays
    /// reserved until a later connection reconciles it.
    private long removeAfterConnectionLoss(long subscription) {
        sut.failed(generation, new IOException("connection lost"));
        port.sent.clear();
        long removal = operation();
        sut.unsubscribe(removal, subscription);
        assertThat(port.results).doesNotContainKey(removal);
        assertThat(port.sent).isEmpty();
        assertFailure(subscribe("offline/#"), IllegalStateException.class);
        return removal;
    }

    private void establishLastSubscription() {
        var packet = (Packet.Subscribe) port.sent.getLast();
        receive(new Packet.SubAck(packet.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
    }

    private void acknowledge(int id) {
        receive(new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY));
    }

    private Result result(long operation) {
        return requireNonNull(port.results.get(operation));
    }

    private Result outcome(long subscription) {
        return requireNonNull(port.outcomes.get(subscription));
    }

    private void assertFailure(long operation, Class<? extends Throwable> type) {
        assertThat(result(operation).failure()).isInstanceOf(type);
    }

    private Throwable assertSessionRefusal(long operation, SessionCore.State state) {
        Throwable failure = requireNonNull(result(operation).failure());
        assertThat(failure).isInstanceOf(MqttConnectException.class)
                .hasMessage("Broker session has no local state").hasNoCause();
        assertThat(port.ends.getLast().cause()).containsSame(failure);
        assertThat(port.established).containsExactly(false);
        assertThat(sut.state()).isEqualTo(state);
        assertThat(port.timers).isEmpty();
        return failure;
    }

    private static MqttMessage message(String topic) {
        return MqttMessage.of(new Topic(topic), new byte[] {1}, Qos.AT_LEAST_ONCE, false);
    }

    static MqttMessage messageWithWireBytes(int bytes) {
        var codec = PacketCodec.of(MqttVersion.V3_1_1);
        var topic = new Topic("budget");
        int header = codec.encodePublishHeader(
                topic,
                Qos.AT_LEAST_ONCE,
                false,
                false,
                1,
                EMPTY,
                bytes
        )
                .length;
        MqttMessage message = MqttMessage.of(
                topic,
                new byte[bytes - header],
                Qos.AT_LEAST_ONCE,
                false
        );
        assertThat(
                codec.encodePublishHeader(
                        topic,
                        Qos.AT_LEAST_ONCE,
                        false,
                        false,
                        1,
                        EMPTY,
                        message.payloadSize()
                )
                        .length + message.payloadSize()
        )
                .isEqualTo(bytes);
        return message;
    }

    private static IntStream seeds() {
        return IntStream.range(0, 100);
    }

    private static List<Throwable> closeFailures() {
        return List.of(new IOException("client shutdown failed"));
    }

    record Publication(
            long generation,
            long ticket,
            MqttMessage message,
            int packetId,
            @Nullable SendReceipt receipt
    ) {
    }

    record Result(@Nullable Object value, @Nullable Throwable failure) {
    }

    private static final class TestSubscription implements Subscription {

        final long id;
        @Nullable TopicFilter filter;
        Qos granted = Qos.AT_LEAST_ONCE;

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
        public CompletableFuture<Qos> established() {
            return CompletableFuture.completedFuture(grantedQos());
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

    static final class RecordingPort implements SessionCore.Port {

        final List<Packet> sent = new ArrayList<>();
        final List<Publication> offers = new ArrayList<>();
        final List<Publication> accepted = new ArrayList<>();
        final List<Packet.PubRel> releases = new ArrayList<>();
        final List<Long> releaseTickets = new ArrayList<>();
        final List<Packet.PubAck> acknowledgements = new ArrayList<>();
        final List<Integer> outgoingIds = new ArrayList<>();
        final Map<Long, Result> results = new HashMap<>();
        // The first outcome each registration reports: its grant or the cause that ended it.
        final Map<Long, Result> outcomes = new HashMap<>();
        final Map<Long, TestSubscription> handles = new HashMap<>();
        final List<Long> removed = new ArrayList<>();
        final List<Qos> grants = new ArrayList<>();
        final List<List<Long>> deliveries = new ArrayList<>();
        final List<Disconnection> ends = new ArrayList<>();
        final List<Boolean> established = new ArrayList<>();
        final Map<SessionCore.Timer, Duration> timers = new EnumMap<>(SessionCore.Timer.class);
        final List<Long> attempts = new ArrayList<>();
        final List<Duration> reconnectDelays = new ArrayList<>();
        final List<Boolean> requests = new ArrayList<>();
        final List<ConnectionAttempt> cancellations = new ArrayList<>();
        final List<ConnectionAttempt> evaluations = new ArrayList<>();
        final List<String> events = new ArrayList<>();
        int disconnecting;
        int stopsDiscarded;
        SessionCore.@Nullable Timer failingTimer;
        final IllegalStateException timerFailure = new IllegalStateException("Timer unavailable");
        long endedAttempt;
        @Nullable Duration latestReconnectDelay;
        SendQueue.OfferResult publishResult = SendQueue.OfferResult.ACCEPTED;
        SendQueue.OfferResult subscribeResult = SendQueue.OfferResult.ACCEPTED;
        SendQueue.OfferResult unsubscribeResult = SendQueue.OfferResult.ACCEPTED;
        SendQueue.OfferResult disconnectResult = SendQueue.OfferResult.ACCEPTED;
        SendQueue.OfferResult releaseResult = SendQueue.OfferResult.ACCEPTED;
        int publishAllowance = Integer.MAX_VALUE;
        int releaseAllowance = Integer.MAX_VALUE;
        // PUBACK and PUBREL share this room, as they share the send queue's reserved budget.
        int acknowledgementAllowance = Integer.MAX_VALUE;
        long generation;
        long ticket;
        @Nullable TestSubscription registering;

        @Override
        public void beginAttempt(
                long connection,
                ConnectionAttempt attempt,
                long attemptNumber
        ) {
            generation = connection;
            attempts.add(attemptNumber);
        }

        @Override
        public void connectionRequested(boolean requested) {
            requests.add(requested);
            events.add("request " + requested);
        }

        @Override
        public void evaluateCredentials(ConnectionAttempt attempt) {
            evaluations.add(attempt);
        }

        @Override
        public void cancelCredentials(ConnectionAttempt attempt) {
            cancellations.add(attempt);
        }

        @Override
        public void openTransport(long generation, Duration timeout) {
        }

        @Override
        public void admit(long connection) {
        }

        @Override
        public void connected(long connection, ConnAck ack) {
        }

        @Override
        public void disconnecting() {
            disconnecting++;
        }

        @Override
        public void stopDiscarded() {
            stopsDiscarded++;
            events.add("stop discarded");
        }

        @Override
        public void resubscribeFailed(Subscription handle, MqttSubscribeException failure) {
            long id = handles.entrySet().stream()
                    .filter(entry -> entry.getValue() == handle)
                    .mapToLong(Map.Entry::getKey)
                    .findFirst()
                    .orElseThrow();
            removeSubscription(id, handle, failure);
        }

        @Override
        public SendQueue.OfferResult send(
                long connection,
                long id,
                Packet packet,
                @Nullable SendReceipt receipt
        ) {
            ticket = id;
            sent.add(packet);
            if (packet instanceof Packet.Subscribe subscribe && registering != null) {
                registering.filter = subscribe.subscriptions().getFirst().filter();
                registering = null;
            }
            SendQueue.OfferResult result = switch (packet) {
                case Packet.Subscribe _ -> subscribeResult;
                case Packet.Unsubscribe _ -> unsubscribeResult;
                case Packet.Disconnect _ -> disconnectResult;
                case Packet.PubRel _ -> releaseAllowance == 0
                        ? SendQueue.OfferResult.FULL : releaseResult;
                default -> SendQueue.OfferResult.ACCEPTED;
            };
            if ((packet instanceof Packet.PubAck || packet instanceof Packet.PubRel)
                    && result == SendQueue.OfferResult.ACCEPTED) {
                if (acknowledgementAllowance == 0) {
                    result = SendQueue.OfferResult.FULL;
                } else {
                    acknowledgementAllowance--;
                }
            }
            if (packet instanceof Packet.PubAck acknowledgement
                    && result == SendQueue.OfferResult.ACCEPTED) {
                acknowledgements.add(acknowledgement);
            }
            if (packet instanceof Packet.PubRel release
                    && result == SendQueue.OfferResult.ACCEPTED) {
                releaseAllowance--;
                releases.add(release);
                releaseTickets.add(id);
                outgoingIds.add(release.packetId());
            }
            return result;
        }

        @Override
        public SendQueue.OfferResult publish(
                long connection,
                long id,
                MqttMessage message,
                int packetId,
                @Nullable SendReceipt receipt
        ) {
            ticket = id;
            var publication = new Publication(connection, id, message, packetId, receipt);
            offers.add(publication);
            SendQueue.OfferResult result = publishResult;
            if (result == SendQueue.OfferResult.ACCEPTED && publishAllowance == 0) {
                result = SendQueue.OfferResult.FULL;
            }
            if (result == SendQueue.OfferResult.ACCEPTED) {
                publishAllowance--;
                accepted.add(publication);
                outgoingIds.add(packetId);
            }
            return result;
        }

        @Override
        public void schedule(long connection, SessionCore.Timer timer, Duration delay) {
            if (timer == failingTimer) {
                throw SpiException.scheduler(connection, timerFailure);
            }
            timers.put(timer, delay);
            events.add("schedule " + timer);
        }

        @Override
        public void cancel(SessionCore.Timer timer) {
            timers.remove(timer);
            events.add("cancel " + timer);
        }

        @Override
        public void ignoredPingResponse() {
        }

        @Override
        public void complete(long operation, @Nullable Object value, @Nullable Throwable failure) {
            assertThat(operation).isPositive();
            assertThat(results.put(operation, new Result(value, failure))).isNull();
            events.add("complete " + operation);
        }

        @Override
        public Optional<Subscription> registerSubscription(long id) {
            var subscription = new TestSubscription(id);
            assertThat(handles.put(id, subscription)).isNull();
            registering = subscription;
            return Optional.of(subscription);
        }

        @Override
        public void subscribed(Subscription handle, Qos granted) {
            var subscription = (TestSubscription) handle;
            subscription.granted = granted;
            grants.add(granted);
            outcomes.putIfAbsent(subscription.id, new Result(handle, null));
        }

        @Override
        public boolean deliver(
                DeliveryToken token,
                List<Long> ids,
                MqttMessage message,
                boolean dup,
                int bytes
        ) {
            deliveries.add(List.copyOf(ids));
            return true;
        }

        @Override
        public void releaseDelivery(long token) {
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
            assertThat(handles.get(id)).isSameAs(handle);
            removed.add(id);
            if (cause != null) {
                outcomes.putIfAbsent(id, new Result(null, cause));
            }
        }

        @Override
        public Map<Long, Boolean> end(
                long connection,
                boolean connected,
                Disconnection disconnection,
                boolean preserveSession,
                boolean stopped
        ) {
            established.add(connected);
            ends.add(disconnection);
            endedAttempt = disconnection.attempt();
            latestReconnectDelay = disconnection.reconnectDelay().orElse(null);
            disconnection.reconnectDelay().ifPresent(reconnectDelays::add);
            events.add("end");
            return Map.of();
        }
    }
}
