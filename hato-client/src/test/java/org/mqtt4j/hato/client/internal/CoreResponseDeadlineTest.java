package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.ResponseDeadlines.Kind;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.Publication;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.RecordingPort;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.Result;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.DISCONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.RECONNECT_WAIT;
import static org.mqtt4j.hato.client.internal.SessionCore.Timer.RECONNECT;
import static org.mqtt4j.hato.client.internal.SessionCore.Timer.RESPONSE;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class CoreResponseDeadlineTest {

    private static final Duration RESPONSE_TIMEOUT = Duration.ofNanos(100);

    private final RecordingPort port = new RecordingPort();

    @ParameterizedTest
    @EnumSource(Kind.class)
    void every_response_expires_from_write_completion_even_with_keep_alive_disabled(Kind kind) {
        // Arrange
        SessionCore sut = connected(true, false);
        long ticket = exchange(sut, kind);

        // Act & Assert: waiting for a write is outside the broker's response deadline.
        assertThat(port.timers).doesNotContainKey(RESPONSE);
        sut.timer(1, RESPONSE, 1000);
        assertThat(sut.state()).isEqualTo(CONNECTED);
        sut.sent(1, ticket, 1000);
        assertThat(port.timers).containsEntry(RESPONSE, RESPONSE_TIMEOUT);
        sut.timer(1, RESPONSE, 1099);
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.timers).containsEntry(RESPONSE, Duration.ofNanos(1));
        sut.timer(1, RESPONSE, 1100);

        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause()).hasValueSatisfying(cause ->
                        assertThat(cause).isInstanceOf(TimeoutException.class)
                                .hasMessageContaining(kind.name())));
        assertThat(port.timers).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void an_answer_before_the_write_notification_cannot_leave_a_stale_deadline(Kind kind) {
        // Arrange
        SessionCore sut = connected(true, false);
        long ticket = exchange(sut, kind);
        int packetId = switch (kind) {
            case PUBACK, PUBREC, PUBCOMP -> port.accepted.getLast().packetId();
            case SUBACK -> ((Packet.Subscribe) port.sent.getLast()).packetId();
            case UNSUBACK -> ((Packet.Unsubscribe) port.sent.getLast()).packetId();
        };

        // Act
        receive(sut, answer(kind, packetId), 20);
        sut.sent(1, ticket, 10, 1000);
        sut.timer(1, RESPONSE, 1000);

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.timers).doesNotContainKey(RESPONSE);
        assertThat(port.ends).isEmpty();
    }

    @Test
    void answering_the_head_does_not_rearm_or_extend_the_next_exchange() {
        // Arrange
        SessionCore sut = connected(true, false);
        sut.publish(2, message(Qos.AT_LEAST_ONCE));
        Publication first = port.accepted.getLast();
        sut.sent(1, first.ticket(), 10);
        sut.publish(3, message(Qos.AT_LEAST_ONCE));
        Publication second = port.accepted.getLast();
        sut.sent(1, second.ticket(), 30);
        long scheduled = responseReservations();

        // Act
        receive(sut, new Packet.PubAck(first.packetId(), ReasonCode.SUCCESS, EMPTY), 90);
        receive(sut, new Packet.PingResp(), 100);

        // Assert
        assertThat(responseReservations()).isEqualTo(scheduled);
        sut.timer(1, RESPONSE, 110);
        assertThat(port.timers).containsEntry(RESPONSE, Duration.ofNanos(20));
        sut.timer(1, RESPONSE, 130);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(result(2).failure()).isNull();
        assertThat(result(3).failure()).isInstanceOf(MqttConnectionLostException.class)
                .hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void qos_two_starts_a_new_deadline_after_pubrel_and_duplicate_pubrec_does_not_extend_it() {
        // Arrange
        SessionCore sut = connected(true, false);
        sut.publish(2, message(Qos.EXACTLY_ONCE));
        Publication publication = port.accepted.getLast();
        sut.sent(1, publication.ticket(), 10);
        var received = new Packet.PubRec(publication.packetId(), ReasonCode.SUCCESS, EMPTY);
        receive(sut, received, 80);
        long firstRelease = port.releaseTickets.getLast();
        sut.sent(1, firstRelease, 90);

        // Act & Assert
        receive(sut, received, 100);
        sut.timer(1, RESPONSE, 110);
        assertThat(port.timers).containsEntry(RESPONSE, Duration.ofNanos(80));
        sut.sent(1, port.releaseTickets.getLast(), 120);
        sut.timer(1, RESPONSE, 190);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(result(2).failure()).hasCauseInstanceOf(TimeoutException.class)
                .cause().hasMessageContaining("PUBCOMP");
    }

    @Test
    void an_unknown_exchange_waits_for_every_admitted_pubcomp_without_extending_the_deadline() {
        // Arrange
        SessionCore sut = connected(true, false);
        var received = new Packet.PubRec(7, ReasonCode.SUCCESS, EMPTY);
        receive(sut, received, 0);
        sut.sent(1, port.releaseTickets.getLast(), 10);
        receive(sut, received, 20);
        sut.sent(1, port.releaseTickets.getLast(), 30);

        // Act
        receive(sut, new Packet.PubComp(7, ReasonCode.SUCCESS, EMPTY), 50);
        sut.timer(1, RESPONSE, 110);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends.getLast().cause()).hasValueSatisfying(cause ->
                assertThat(cause).isInstanceOf(TimeoutException.class)
                        .hasMessageContaining("PUBCOMP"));
    }

    @ParameterizedTest
    @ValueSource(longs = {70, 200})
    void actor_delay_does_not_extend_the_first_response_deadline(long now) {
        // Arrange
        SessionCore sut = connected(true, false);
        sut.publish(2, message(Qos.AT_LEAST_ONCE));

        // Act
        sut.sent(1, port.accepted.getLast().ticket(), 10, now);

        // Assert
        assertThat(port.timers).containsEntry(
                RESPONSE,
                Duration.ofNanos(Math.max(0, 110 - now))
        );
        assertThat(sut.state()).isEqualTo(CONNECTED);
        sut.timer(1, RESPONSE, Math.max(110, now));
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void an_ack_already_waiting_in_the_actor_can_precede_a_zero_delay_response_timer() {
        // Arrange
        SessionCore sut = connected(true, false);
        sut.publish(2, message(Qos.AT_LEAST_ONCE));
        Publication publication = port.accepted.getLast();

        // Act
        sut.sent(1, publication.ticket(), 10, 200);
        receive(sut, new Packet.PubAck(publication.packetId(), ReasonCode.SUCCESS, EMPTY), 200);
        sut.timer(1, RESPONSE, 200);

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(result(2).failure()).isNull();
        assertThat(port.ends).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void an_expired_exchange_retries_with_fresh_deadlines_and_only_a_persistent_session_replays(
            boolean cleanStart
    ) {
        // Arrange
        SessionCore sut = connected(cleanStart, true);
        sut.publish(2, message(Qos.AT_LEAST_ONCE));
        Publication original = port.accepted.getLast();
        sut.sent(1, original.ticket(), 10);

        // Act
        sut.timer(1, RESPONSE, 110);

        // Assert
        assertThat(sut.state()).isEqualTo(RECONNECT_WAIT);
        assertThat(port.timers).containsOnlyKeys(RECONNECT);
        assertThat(result(2).failure()).hasCauseInstanceOf(TimeoutException.class);
        sut.connect(new ConnectionAttempt(3, null), 200);
        sut.opened(2);
        sut.received(2, new Packet.ConnAck(!cleanStart, ReasonCode.SUCCESS, EMPTY), 200, 0);
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.timers).doesNotContainKey(RESPONSE);
        sut.timer(1, RESPONSE, 1000);
        sut.sent(1, original.ticket(), 1000);
        assertThat(sut.state()).isEqualTo(CONNECTED);
        if (cleanStart) {
            assertThat(port.accepted).hasSize(1);
        } else {
            assertThat(port.accepted).hasSize(2);
            Publication replay = port.accepted.getLast();
            assertThat(replay.packetId()).isEqualTo(original.packetId());
            sut.sent(2, replay.ticket(), 1000);
            assertThat(port.timers).containsEntry(RESPONSE, RESPONSE_TIMEOUT);
            sut.timer(2, RESPONSE, 1100);
            assertThat(sut.state()).isEqualTo(RECONNECT_WAIT);
        }
    }

    @Test
    void a_pending_disconnect_uses_its_write_deadline_and_releases_response_waits() {
        // Arrange
        SessionCore sut = connected(true, false);
        sut.publish(2, message(Qos.AT_LEAST_ONCE));
        sut.sent(1, port.accepted.getLast().ticket(), 10);

        // Act
        sut.disconnect(3);
        long disconnectTicket = port.ticket;
        sut.timer(1, RESPONSE, 110);

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.timers).doesNotContainKey(RESPONSE);
        sut.sent(1, disconnectTicket, 120);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(result(3).failure()).isNull();
    }

    private SessionCore connected(boolean cleanStart, boolean reconnect) {
        ReconnectBackoff backoff = reconnect
                ? new ReconnectBackoff(
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(120),
                        new Random(1)
                )
                : ReconnectBackoff.disabled(new Random(1));
        var core = new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ZERO,
                cleanStart,
                null,
                false,
                backoff,
                false,
                RESPONSE_TIMEOUT
        );
        core.connect(new ConnectionAttempt(1, null), 0);
        core.opened(1);
        receive(core, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        return core;
    }

    private long exchange(SessionCore core, Kind kind) {
        switch (kind) {
            case PUBACK -> core.publish(2, message(Qos.AT_LEAST_ONCE));
            case PUBREC -> core.publish(2, message(Qos.EXACTLY_ONCE));
            case PUBCOMP -> {
                core.publish(2, message(Qos.EXACTLY_ONCE));
                receive(
                        core,
                        new Packet.PubRec(
                                port.accepted.getLast().packetId(),
                                ReasonCode.SUCCESS,
                                EMPTY
                        ),
                        0
                );
            }
            case SUBACK, UNSUBACK -> {
                core.subscribe(2, Subscribe.of(new TopicFilter("a/#"), Qos.AT_MOST_ONCE));
                if (kind == Kind.UNSUBACK) {
                    var packet = (Packet.Subscribe) port.sent.getLast();
                    receive(
                            core,
                            new Packet.SubAck(
                                    packet.packetId(),
                                    List.of(ReasonCode.SUCCESS),
                                    EMPTY
                            ),
                            0
                    );
                    core.unsubscribe(3, 2);
                }
            }
        }
        return port.ticket;
    }

    private static Packet answer(Kind kind, int packetId) {
        return switch (kind) {
            case PUBACK -> new Packet.PubAck(packetId, ReasonCode.SUCCESS, EMPTY);
            case PUBREC -> new Packet.PubRec(packetId, ReasonCode.SUCCESS, EMPTY);
            case PUBCOMP -> new Packet.PubComp(packetId, ReasonCode.SUCCESS, EMPTY);
            case SUBACK -> new Packet.SubAck(
                    packetId,
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            );
            case UNSUBACK -> new Packet.UnsubAck(packetId, List.of(), EMPTY);
        };
    }

    private static MqttMessage message(Qos qos) {
        return MqttMessage.of(new Topic("a/b"), new byte[] {1}, qos, false);
    }

    private static void receive(SessionCore core, Packet packet, long now) {
        core.received(1, packet, now, 0);
    }

    private long responseReservations() {
        return port.events.stream().filter(event -> event.equals("schedule RESPONSE")).count();
    }

    private Result result(long operation) {
        return requireNonNull(port.results.get(operation));
    }
}
