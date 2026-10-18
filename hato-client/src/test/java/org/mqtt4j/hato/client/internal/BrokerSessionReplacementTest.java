package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.ObjLongConsumer;
import java.util.stream.IntStream;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttCredentials;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.RecordingPort;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.Result;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mqtt4j.hato.client.internal.SessionCore.State.*;
import static org.mqtt4j.hato.client.internal.SessionCore.Timer.*;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A persistent client without local state replaces the broker's session within one attempt.
class BrokerSessionReplacementTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final Packet.Will WILL = new Packet.Will(
            new Topic("will"),
            new byte[] {1},
            Qos.AT_LEAST_ONCE,
            false,
            EMPTY
    );

    private static final ConnectFlags PERSISTENT = new ConnectFlags(false, WILL);

    private static final ConnectFlags DISCARDING = new ConnectFlags(true, null);

    private static final String REFUSAL = "Broker session present after discarding it";

    private final RecordingPort port = new RecordingPort();
    private long nextOperation;

    @Test
    void a_broker_session_without_local_state_is_replaced_by_a_new_one_in_the_same_attempt() {
        // Arrange
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);

        // Act
        receive(sut, connAck(true));
        writeDisconnect(sut);
        receive(sut, connAck(false));
        writeDisconnect(sut);
        receive(sut, connAck(false));

        // Assert: one Clean Session 1 connection without a Will sits between two persistent ones.
        assertThat(port.sent).map(Packet::getClass).containsExactly(
                Packet.Connect.class,
                Packet.Disconnect.class,
                Packet.Connect.class,
                Packet.Disconnect.class,
                Packet.Connect.class
        );
        assertThat(connectFlags()).containsExactly(PERSISTENT, DISCARDING, PERSISTENT);
        assertThat(port.continuations).containsExactly(2L, 3L);
        assertThat(result(operation).value()).isEqualTo(ConnAck.of(false, 1));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void the_replaced_connections_are_neither_announced_nor_numbered() {
        // Arrange
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);

        // Act
        discardBrokerSession(sut);
        receive(sut, connAck(false));

        // Assert
        assertThat(port.attempts).containsExactly(1L);
        assertThat(port.ends).isEmpty();
        assertThat(port.announcements).containsExactly(ConnAck.of(false, 1));
        assertThat(port.disconnecting).isZero();
        assertThat(result(operation).value()).isEqualTo(ConnAck.of(false, 1));
    }

    @Test
    void nothing_received_on_a_replaced_connection_is_delivered_or_acknowledged() {
        // Arrange: a registration made before connecting matches what the broker stored.
        SessionCore sut = core(false, false);
        sut.subscribe(operation(), Subscribe.of(new TopicFilter("stored"), Qos.AT_LEAST_ONCE));
        beginConnect(sut);
        receive(sut, connAck(true));

        // Act: the broker sends its stored state before reading each DISCONNECT.
        receive(sut, publication(7));
        receive(sut, new Packet.PubRel(8, ReasonCode.SUCCESS, EMPTY));
        writeDisconnect(sut);
        receive(sut, connAck(false));
        receive(sut, publication(9));
        writeDisconnect(sut);
        receive(sut, connAck(false));

        // Assert: only the last connection subscribes, after its CONNACK.
        assertThat(port.deliveries).isEmpty();
        assertThat(port.sent).map(Packet::getClass).containsExactly(
                Packet.Connect.class,
                Packet.Disconnect.class,
                Packet.Connect.class,
                Packet.Disconnect.class,
                Packet.Connect.class,
                Packet.Subscribe.class
        );
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Abandoned.class)
    void an_abandoned_connection_waits_only_for_its_disconnect_deadline(Abandoned abandoned) {
        // Arrange
        SessionCore sut = core(false, false);
        beginConnect(sut);
        reach(sut, abandoned);

        // Act
        receive(sut, connAck(abandoned.sessionPresent));

        // Assert: the attempt's connection deadline no longer applies to this connection.
        assertThat(port.sent.getLast()).isInstanceOf(Packet.Disconnect.class);
        assertThat(port.timers).containsOnlyKeys(DISCONNECT);
        assertThat(port.timers.get(DISCONNECT)).isEqualTo(TIMEOUT);
        assertThat(port.timerGenerations.get(DISCONNECT)).isEqualTo(port.generation);
    }

    @Test
    void each_connection_of_the_attempt_has_the_whole_connection_timeout() {
        // Arrange: credentials take 20 of the first connection's 30 seconds.
        SessionCore sut = core(true, false);
        var attempt = new ConnectionAttempt(operation());
        sut.connect(attempt, 0);
        sut.credentials(attempt, MqttCredentials.of("user"), null, seconds(20));
        sut.opened(port.generation);
        receive(sut, connAck(true), seconds(21));

        // Act: each DISCONNECT completes late in its own deadline.
        writeDisconnect(sut, seconds(25));
        receive(sut, connAck(false), seconds(40));
        writeDisconnect(sut, seconds(54));

        // Assert
        assertThat(port.transportTimeouts)
                .containsExactly(Duration.ofSeconds(10), TIMEOUT, TIMEOUT);
        assertThat(port.timers).containsOnlyKeys(CONNECT);
        assertThat(port.timers.get(CONNECT)).isEqualTo(TIMEOUT);
        assertThat(port.timerGenerations.get(CONNECT)).isEqualTo(port.generation);
    }

    @Test
    void a_connection_deadline_on_the_discarding_connection_fails_the_attempt() {
        // Arrange
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);
        receive(sut, connAck(true));
        writeDisconnect(sut);

        // Act
        sut.timer(port.generation, CONNECT, TIMEOUT.toNanos());

        // Assert
        Throwable failure = requireNonNull(result(operation).failure());
        assertThat(failure).isInstanceOf(MqttConnectException.class)
                .hasMessage("Connection failed");
        assertThat(failure.getCause()).isInstanceOf(TimeoutException.class)
                .hasMessage("CONNACK deadline expired");
        assertThat(port.continuations).containsExactly(2L);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void credentials_are_evaluated_once_and_sent_on_every_connection_of_the_attempt() {
        // Arrange
        SessionCore sut = core(true, false);
        var attempt = new ConnectionAttempt(operation());
        sut.connect(attempt, 0);
        sut.credentials(attempt, MqttCredentials.of("user"), null, 0);
        sut.opened(port.generation);

        // Act
        discardBrokerSession(sut);
        receive(sut, connAck(false));

        // Assert
        assertThat(port.evaluations).containsExactly(attempt);
        assertThat(port.sent).filteredOn(Packet.Connect.class::isInstance)
                .map(packet -> ((Packet.Connect) packet).username())
                .containsExactly("user", "user", "user");
        assertThat(result(attempt.operation).value()).isEqualTo(ConnAck.of(false, 1));
    }

    @Test
    void a_session_present_answer_to_the_discarding_connection_is_a_protocol_error() {
        // Arrange
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);
        receive(sut, connAck(true));
        writeDisconnect(sut);

        // Act
        receive(sut, connAck(true));

        // Assert
        assertThat(result(operation).failure()).isInstanceOf(MqttConnectException.class)
                .hasCauseInstanceOf(MqttProtocolException.class);
        assertThat(port.sent.getLast()).isInstanceOf(Packet.Connect.class);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @ParameterizedTest(name = "{1} of {0}")
    @MethodSource("disconnectFailures")
    void a_failed_disconnect_of_a_replaced_connection_fails_the_attempt(
            Abandoned abandoned,
            DisconnectFailure failure
    ) {
        // Arrange
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);
        reach(sut, abandoned);
        failure.prepare().accept(port);
        receive(sut, connAck(abandoned.sessionPresent));

        // Act
        failure.inject().accept(sut, port.generation);

        // Assert: the attempt reports the failure and opens no further connection.
        Throwable connectFailure = requireNonNull(result(operation).failure());
        assertThat(connectFailure).isInstanceOf(MqttConnectException.class)
                .hasMessage("Connection failed");
        assertThat(connectFailure.getCause()).isInstanceOf(failure.cause());
        assertThat(port.ends).singleElement()
                .satisfies(end -> assertThat(end.cause()).containsSame(connectFailure));
        assertThat(port.continuations).hasSize(abandoned.steps / 2);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.timers).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({
            "the first connection awaits CONNACK,0,1",
            "the first connection writes DISCONNECT,1,1",
            "the discarding connection awaits CONNACK,2,2",
            "the discarding connection writes DISCONNECT,3,2"
    })
    void a_disconnect_during_the_replacement_stops_at_the_current_connection(
            String stage,
            int completedSteps,
            int connections
    ) {
        // Arrange
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);
        List<Runnable> steps = replacementSteps(sut);
        steps.subList(0, completedSteps).forEach(Runnable::run);
        long disconnect = operation();

        // Act: the remaining steps find that the attempt has stopped.
        sut.disconnect(disconnect);
        steps.subList(completedSteps, steps.size()).forEach(Runnable::run);

        // Assert
        assertThat(result(operation).failure()).isInstanceOf(MqttConnectException.class)
                .hasMessage("Connection request withdrawn");
        assertThat(result(disconnect).failure()).isNull();
        assertThat(port.ends).singleElement()
                .satisfies(end -> assertThat(end.userRequested()).isTrue());
        assertThat(port.established).containsExactly(false);
        assertThat(port.sent.getLast()).isInstanceOf(Packet.Disconnect.class);
        assertThat(connectFlags()).hasSize(connections);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void a_pending_disconnect_fails_when_an_accepted_connection_ends_before_its_disconnect() {
        // Arrange: the first connection carries the Will, which only DISCONNECT discards.
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);
        receive(sut, connAck(true));
        long disconnect = operation();
        sut.disconnect(disconnect);
        var lost = new IOException("lost before DISCONNECT was written");

        // Act
        sut.failed(port.generation, lost);

        // Assert
        assertThat(result(disconnect).failure()).isInstanceOf(MqttConnectionLostException.class)
                .hasCause(lost);
        assertThat(result(operation).failure()).isInstanceOf(MqttConnectException.class)
                .hasCause(lost);
        assertThat(port.continuations).isEmpty();
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void a_pending_disconnect_completes_and_stops_retrying_when_the_refusal_is_written() {
        // Arrange: the policy would retry the refusal without the disconnect.
        SessionCore sut = core(false, true);
        long operation = beginConnect(sut);
        discardBrokerSession(sut);
        long disconnect = operation();
        sut.disconnect(disconnect);
        receive(sut, connAck(true));

        // Act
        writeDisconnect(sut);

        // Assert
        assertThat(result(operation).failure()).hasMessage(REFUSAL);
        assertThat(result(disconnect).failure()).isNull();
        assertThat(port.ends).singleElement()
                .satisfies(end -> assertThat(end.userRequested()).isTrue());
        assertThat(port.timers).doesNotContainKey(RECONNECT);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void a_disconnect_while_the_new_session_starts_keeps_it_as_local_state() {
        // Arrange: the disconnect waits for the last CONNACK, which starts the session.
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);
        discardBrokerSession(sut);
        long disconnect = operation();
        sut.disconnect(disconnect);
        receive(sut, connAck(false));
        writeDisconnect(sut);

        // Act
        long next = beginConnect(sut);
        receive(sut, connAck(true));

        // Assert: the later Session Present continues that session.
        assertThat(result(operation).failure()).hasMessage("Connection request withdrawn");
        assertThat(result(disconnect).failure()).isNull();
        assertThat(port.established).containsExactly(true);
        assertThat(result(next).value()).isEqualTo(ConnAck.of(true, 2));
    }

    @Test
    void a_session_found_again_after_discarding_fails_the_attempt_after_disconnect() {
        // Arrange
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);
        discardBrokerSession(sut);
        receive(sut, connAck(true));
        assertThat(port.results).doesNotContainKey(operation);

        // Act
        writeDisconnect(sut);

        // Assert
        Throwable failure = assertRefusal(sut, operation, DISCONNECTED);
        assertThat(failure.getSuppressed()).isEmpty();
        // The DISCONNECT is the client's own; the application requested none.
        assertThat(port.ends.getLast().userRequested()).isFalse();
        assertThat(port.continuations).containsExactly(2L, 3L);
    }

    @Test
    void a_disconnect_deadline_preserves_the_session_refusal() {
        // Arrange
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);
        discardBrokerSession(sut);
        receive(sut, connAck(true));

        // Act
        sut.timer(port.generation, DISCONNECT, TIMEOUT.toNanos());

        // Assert
        Throwable failure = assertRefusal(sut, operation, DISCONNECTED);
        assertThat(failure.getSuppressed()).singleElement()
                .satisfies(cause -> assertThat(cause).isInstanceOf(TimeoutException.class)
                        .hasMessage("DISCONNECT write deadline expired"));
    }

    @Test
    void a_full_disconnect_queue_preserves_the_session_refusal() {
        // Arrange
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);
        discardBrokerSession(sut);
        port.disconnectResult = SendQueue.OfferResult.FULL;

        // Act
        receive(sut, connAck(true));

        // Assert
        Throwable failure = assertRefusal(sut, operation, DISCONNECTED);
        assertThat(failure.getSuppressed()).singleElement()
                .satisfies(cause -> assertThat(cause).isInstanceOf(MqttCapacityException.class)
                        .hasMessage("Reserved send queue capacity"));
    }

    @Test
    void a_transport_failure_preserves_the_session_refusal_and_ignores_late_events() {
        // Arrange
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);
        discardBrokerSession(sut);
        receive(sut, connAck(true));
        long generation = port.generation;
        long disconnectTicket = port.ticket;
        var transportFailure = new IOException("closed before DISCONNECT completed");

        // Act
        sut.failed(generation, transportFailure);

        // Assert
        Throwable failure = assertRefusal(sut, operation, DISCONNECTED);
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
        SessionCore sut = core(false, false);
        long operation = beginConnect(sut);
        discardBrokerSession(sut);
        receive(sut, connAck(true));

        // Act
        sut.close(cause);
        sut.close(null);

        // Assert
        Throwable failure = assertRefusal(sut, operation, CLOSED);
        assertThat(failure.getSuppressed())
                .containsExactlyElementsOf(Optional.ofNullable(cause).stream().toList());
        assertThat(port.ends).hasSize(1);
    }

    @Test
    void a_session_refusal_does_not_replace_a_later_connection_attempts_failure() {
        // Arrange
        SessionCore sut = core(false, false);
        long refusedOperation = beginConnect(sut);
        discardBrokerSession(sut);
        receive(sut, connAck(true));
        var cleanupFailure = new IOException("refused connection closed");
        sut.failed(port.generation, cleanupFailure);
        Throwable refusal = requireNonNull(result(refusedOperation).failure());
        long nextAttempt = beginConnect(sut);
        var connectionFailure = new IOException("next connection failed");

        // Act: the new attempt fails before receiving any CONNACK.
        sut.failed(port.generation, connectionFailure);

        // Assert
        Throwable failure = requireNonNull(result(nextAttempt).failure());
        assertThat(failure).isInstanceOf(MqttConnectException.class)
                .hasMessage("Connection failed").hasCause(connectionFailure).isNotSameAs(refusal);
        assertThat(failure.getSuppressed()).isEmpty();
        assertThat(refusal).hasMessage(REFUSAL);
        assertThat(refusal.getSuppressed()).containsExactly(cleanupFailure);
        // The failed attempt reports the same failure as its connect().
        assertThat(port.ends.getLast().cause()).containsSame(failure);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.timers).isEmpty();
    }

    @Test
    void a_clock_failure_while_closing_a_refused_connection_stops_with_its_own_cause() {
        // Arrange: the policy would retry the refusal.
        SessionCore sut = core(false, true);
        long operation = beginConnect(sut);
        discardBrokerSession(sut);
        port.failingTimer = DISCONNECT;

        // Act: the shell reports the scheduler's failure for this connection.
        SpiException thrown = catchThrowableOfType(
                SpiException.class,
                () -> receive(sut, connAck(true))
        );
        sut.schedulerFailed(thrown.generation(), requireNonNull(thrown.getCause()));

        // Assert
        Throwable failure = requireNonNull(result(operation).failure());
        assertThat(failure).isInstanceOf(MqttConnectException.class)
                .hasMessage("Connection failed").hasCause(port.timerFailure);
        assertThat(failure.getSuppressed()).singleElement()
                .satisfies(refusal -> assertThat(refusal).hasMessage(REFUSAL));
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause()).containsSame(failure);
            assertThat(end.reconnectDelay()).isEmpty();
        });
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void a_clock_failure_opening_the_next_connection_ends_the_attempt() {
        // Arrange
        SessionCore sut = core(false, true);
        long operation = beginConnect(sut);
        receive(sut, connAck(true));
        port.failingTimer = CONNECT;

        // Act: the shell reports the scheduler's failure for the next connection.
        SpiException thrown = catchThrowableOfType(
                SpiException.class,
                () -> sut.sent(port.generation, port.ticket, 0)
        );
        sut.schedulerFailed(thrown.generation(), requireNonNull(thrown.getCause()));

        // Assert: no transport opened for that connection.
        assertThat(result(operation).failure()).isInstanceOf(MqttConnectException.class)
                .hasCause(port.timerFailure);
        assertThat(port.continuations).containsExactly(2L);
        assertThat(port.transportTimeouts).hasSize(1);
        assertThat(port.ends).hasSize(1);
        assertThat(port.timers).isEmpty();
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void an_initial_retry_reports_and_retries_a_session_found_again() {
        // Arrange
        SessionCore sut = core(false, true);
        long operation = beginConnect(sut);
        discardBrokerSession(sut);
        receive(sut, connAck(true));

        // Act
        writeDisconnect(sut);

        // Assert
        assertThat(sut.state()).isEqualTo(RECONNECT_WAIT);
        assertThat(port.timers).containsOnlyKeys(RECONNECT);
        assertThat(port.ends.getLast().cause()).hasValueSatisfying(cause ->
                assertThat(cause).hasMessage(REFUSAL)
        );
        assertThat(port.results).doesNotContainKey(operation);
    }

    @ParameterizedTest(name = "after {0}")
    @EnumSource(EndedReplacement.class)
    void an_initial_retry_replaces_the_session_again_from_the_first_connection(
            EndedReplacement ended
    ) {
        // Arrange: the first attempt ends during its replacement.
        SessionCore sut = core(false, true);
        long operation = beginConnect(sut);
        endFirstAttempt(sut, ended);

        // Act
        sut.timer(0, RECONNECT, 0);
        sut.opened(port.generation);
        discardBrokerSession(sut);
        receive(sut, connAck(false));

        // Assert: the next attempt starts over with a persistent first connection.
        assertThat(result(operation).value()).isEqualTo(ConnAck.of(false, 2));
        List<ConnectFlags> flags = connectFlags();
        assertThat(flags.subList(ended.connections, flags.size()))
                .containsExactly(PERSISTENT, DISCARDING, PERSISTENT);
        assertThat(port.attempts).containsExactly(1L, 2L);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void interruptions_late_events_and_joining_connects_settle_the_attempt_once(int seed) {
        // Arrange: a registration matches what the broker stored, and at most one interruption
        // falls at a random point of the replacement.
        var random = new SplittableRandom(seed);
        SessionCore sut = core(false, false);
        sut.subscribe(operation(), Subscribe.of(new TopicFilter("stored"), Qos.AT_LEAST_ONCE));
        var connects = new ArrayList<>(List.of(beginConnect(sut)));
        long disconnect = operation();
        Interruption[] interruptions = Interruption.values();
        Interruption interruption = interruptions[random.nextInt(interruptions.length)];
        List<Runnable> steps = List.of(
                () -> receive(sut, connAck(true)),
                () -> sut.sent(port.generation, port.ticket, 0),
                () -> sut.opened(port.generation),
                () -> receive(sut, connAck(false)),
                () -> sut.sent(port.generation, port.ticket, 0),
                () -> sut.opened(port.generation),
                () -> receive(sut, connAck(false))
        );
        int position = random.nextInt(steps.size() + 1);
        var late = new ArrayList<Runnable>(List.of(() -> join(sut, connects)));
        int continuationsBefore = 0;

        // Act: each replaced connection leaves events that can still arrive in any order.
        for (int step = 0; step <= steps.size(); step++) {
            if (step == position) {
                continuationsBefore = port.continuations.size();
                interrupt(sut, interruption, disconnect);
            }
            if (step < steps.size()) {
                long generation = port.generation;
                long ticket = port.ticket;
                steps.get(step).run();
                if (port.generation != generation) {
                    late.addAll(lateEvents(sut, generation, ticket, seed));
                    late.add(() -> join(sut, connects));
                }
                runSome(late, random);
            }
        }
        // A disconnect() after the last CONNACK waits for this write.
        if (sut.state() == DISCONNECTING) {
            sut.sent(port.generation, port.ticket, 0);
        }
        late.forEach(Runnable::run);

        // Assert: every connect() shares one outcome, and no replaced connection did anything.
        Result outcome = result(connects.getFirst());
        for (long operation : connects) {
            assertThat(result(operation).value()).isEqualTo(outcome.value());
            assertThat(result(operation).failure()).isSameAs(outcome.failure());
        }
        List<ConnectFlags> flags = connectFlags();
        assertThat(List.of(PERSISTENT, DISCARDING, PERSISTENT)).startsWith(
                flags.toArray(new ConnectFlags[0])
        );
        assertThat(port.continuations).hasSizeLessThanOrEqualTo(2);
        assertThat(port.sent).noneMatch(BrokerSessionReplacementTest::acknowledges);
        assertThat(port.deliveries).isEmpty();
        assertThat(port.attempts).containsExactly(1L);
        if (interruption == Interruption.DISCONNECT || interruption == Interruption.CLOSE) {
            assertThat(port.continuations).hasSize(continuationsBefore);
        }
        if (interruption == Interruption.DISCONNECT) {
            assertThat(port.results).containsKey(disconnect);
        }
        if (interruption == Interruption.NONE) {
            assertThat(sut.state()).isEqualTo(CONNECTED);
        }
        if (sut.state() == CONNECTED) {
            assertThat(outcome.value()).isEqualTo(ConnAck.of(false, 1));
            assertThat(port.ends).isEmpty();
        } else {
            assertThat(port.ends).hasSize(1);
            assertThat(port.timers).isEmpty();
        }
    }

    private static boolean acknowledges(Packet packet) {
        return packet instanceof Packet.PubAck
                || packet instanceof Packet.PubRec
                || packet instanceof Packet.PubComp;
    }

    /// The events that a connection replaced at `generation` can still deliver.
    private List<Runnable> lateEvents(SessionCore sut, long generation, long ticket, int seed) {
        return List.of(
                () -> sut.received(generation, connAck(false), 0, 0),
                () -> sut.received(generation, publication(1), 0, 1),
                () -> sut.sent(generation, ticket, 0),
                () -> sut.opened(generation),
                () -> sut.timer(generation, CONNECT, TIMEOUT.toNanos()),
                () -> sut.timer(generation, DISCONNECT, TIMEOUT.toNanos()),
                () -> sut.failed(generation, new IOException("seed " + seed))
        );
    }

    /// Joins the attempt with another connect() while it is still connecting.
    private void join(SessionCore sut, List<Long> connects) {
        if (sut.state() == CONNECTING) {
            long operation = operation();
            connects.add(operation);
            sut.connect(new ConnectionAttempt(operation), 0);
        }
    }

    private void interrupt(SessionCore sut, Interruption interruption, long disconnect) {
        switch (interruption) {
            case NONE -> {
                // The replacement runs to its end.
            }
            case DISCONNECT -> sut.disconnect(disconnect);
            case CLOSE -> sut.close(null);
            case TRANSPORT_FAILURE -> sut.failed(port.generation, new IOException("interrupted"));
            case CONNECT_DEADLINE -> fire(sut, CONNECT);
            case DISCONNECT_DEADLINE -> fire(sut, DISCONNECT);
        }
    }

    /// Fires `timer` only while it is scheduled, as the shell does.
    private void fire(SessionCore sut, SessionCore.Timer timer) {
        Long generation = port.timerGenerations.get(timer);
        if (generation != null) {
            sut.timer(generation, timer, TIMEOUT.toNanos());
        }
    }

    /// Runs and removes a random selection of the pending events, in random order.
    private static void runSome(List<Runnable> pending, SplittableRandom random) {
        for (int i = pending.size(); i > 0; i--) {
            if (random.nextBoolean()) {
                pending.remove(random.nextInt(pending.size())).run();
            }
        }
    }

    /// Ends the first attempt during its replacement.
    private void endFirstAttempt(SessionCore sut, EndedReplacement ended) {
        switch (ended) {
            case LOST_DISCARDING_CONNECTION -> {
                receive(sut, connAck(true));
                writeDisconnect(sut);
                sut.failed(port.generation, new IOException("lost"));
            }
            case SESSION_FOUND_AGAIN -> {
                discardBrokerSession(sut);
                receive(sut, connAck(true));
                writeDisconnect(sut);
            }
        }
    }

    /// The steps from a first CONNACK with Session Present to the last connection's CONNECT.
    private List<Runnable> replacementSteps(SessionCore sut) {
        return List.of(
                () -> receive(sut, connAck(true)),
                () -> writeDisconnect(sut),
                () -> receive(sut, connAck(false)),
                () -> writeDisconnect(sut)
        );
    }

    /// Answers the first connection with Session Present and lets the attempt discard the
    /// broker's session, up to the CONNECT of the connection that starts a new session.
    private void discardBrokerSession(SessionCore sut) {
        replacementSteps(sut).forEach(Runnable::run);
    }

    /// Runs the replacement up to the CONNACK that makes the attempt abandon a connection.
    private void reach(SessionCore sut, Abandoned abandoned) {
        replacementSteps(sut).subList(0, abandoned.steps).forEach(Runnable::run);
    }

    private long beginConnect(SessionCore sut) {
        long operation = operation();
        sut.connect(new ConnectionAttempt(operation), 0);
        sut.opened(port.generation);
        return operation;
    }

    private void receive(SessionCore sut, Packet packet) {
        receive(sut, packet, 0);
    }

    private void receive(SessionCore sut, Packet packet, long now) {
        sut.received(
                port.generation,
                packet,
                now,
                packet instanceof Packet.Publish publish ? publish.payload().length : 0
        );
    }

    private void writeDisconnect(SessionCore sut) {
        writeDisconnect(sut, 0);
    }

    /// Completes the write of the latest DISCONNECT. When the attempt continues, its next
    /// connection opens at once.
    private void writeDisconnect(SessionCore sut, long now) {
        long replaced = port.generation;
        sut.sent(replaced, port.ticket, now);
        if (port.generation != replaced) {
            sut.opened(port.generation);
        }
    }

    private List<ConnectFlags> connectFlags() {
        return port.sent.stream()
                .filter(Packet.Connect.class::isInstance)
                .map(packet -> (Packet.Connect) packet)
                .map(connect -> new ConnectFlags(connect.cleanStart(), connect.will()))
                .toList();
    }

    private Throwable assertRefusal(SessionCore sut, long operation, SessionCore.State state) {
        Throwable failure = requireNonNull(result(operation).failure());
        assertThat(failure).isInstanceOf(MqttConnectException.class)
                .hasMessage(REFUSAL).hasNoCause();
        assertThat(port.ends.getLast().cause()).containsSame(failure);
        assertThat(port.established).containsExactly(false);
        assertThat(sut.state()).isEqualTo(state);
        assertThat(port.timers).isEmpty();
        return failure;
    }

    private Result result(long operation) {
        return requireNonNull(port.results.get(operation));
    }

    private long operation() {
        return ++nextOperation;
    }

    private SessionCore core(boolean credentialsRequired, boolean initialRetry) {
        ReconnectBackoff backoff = initialRetry
                ? new ReconnectBackoff(
                        Duration.ofNanos(3),
                        Duration.ofNanos(20),
                        new ReconnectBackoffTest.MaximumRandom()
                )
                : ReconnectBackoff.disabled();
        return new SessionCore(
                port,
                "client",
                TIMEOUT,
                Duration.ZERO,
                false,
                WILL,
                credentialsRequired,
                backoff,
                initialRetry,
                Duration.ofSeconds(60)
        );
    }

    private static long seconds(long value) {
        return Duration.ofSeconds(value).toNanos();
    }

    private static Packet.ConnAck connAck(boolean sessionPresent) {
        return new Packet.ConnAck(sessionPresent, ReasonCode.SUCCESS, EMPTY);
    }

    private static Packet.Publish publication(int packetId) {
        return new Packet.Publish(
                new Topic("stored"),
                new byte[] {1},
                Qos.AT_LEAST_ONCE,
                false,
                true,
                packetId,
                EMPTY
        );
    }

    private static IntStream seeds() {
        return IntStream.range(0, 100);
    }

    private static List<Throwable> closeFailures() {
        return List.of(new IOException("client shutdown failed"));
    }

    private static List<Arguments> disconnectFailures() {
        List<DisconnectFailure> failures = List.of(
                new DisconnectFailure(
                        "write deadline",
                        TimeoutException.class,
                        _ -> { },
                        (sut, generation) -> sut.timer(generation, DISCONNECT, TIMEOUT.toNanos())
                ),
                new DisconnectFailure(
                        "full send queue",
                        MqttCapacityException.class,
                        port -> port.disconnectResult = SendQueue.OfferResult.FULL,
                        (_, _) -> { }
                ),
                new DisconnectFailure(
                        "transport failure",
                        IOException.class,
                        _ -> { },
                        (sut, generation) -> sut.failed(generation, new IOException("lost"))
                )
        );
        var arguments = new ArrayList<Arguments>();
        for (Abandoned abandoned : List.of(Abandoned.FIRST, Abandoned.DISCARDING)) {
            for (DisconnectFailure failure : failures) {
                arguments.add(Arguments.of(abandoned, failure));
            }
        }
        return arguments;
    }

    /// A connection that the attempt abandons after its CONNACK: the replacement steps that
    /// reach that CONNACK, and whether it reports Session Present.
    private enum Abandoned {
        FIRST("the first connection", 0, true),
        DISCARDING("the discarding connection", 2, false),
        FOUND_AGAIN("a connection that finds the session again", 4, true);

        final String description;
        final int steps;
        final boolean sessionPresent;

        Abandoned(String description, int steps, boolean sessionPresent) {
            this.description = description;
            this.steps = steps;
            this.sessionPresent = sessionPresent;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    /// How a first attempt ends during its replacement, and how many CONNECTs it sent.
    private enum EndedReplacement {
        LOST_DISCARDING_CONNECTION(2),
        SESSION_FOUND_AGAIN(3);

        final int connections;

        EndedReplacement(int connections) {
            this.connections = connections;
        }
    }

    /// At most one event that ends or interrupts the attempt's current connection.
    private enum Interruption {
        NONE,
        DISCONNECT,
        CLOSE,
        TRANSPORT_FAILURE,
        CONNECT_DEADLINE,
        DISCONNECT_DEADLINE
    }

    /// What CONNECT asked for. Packet arrays compare by identity, so only the configured Will
    /// instance equals [#WILL].
    private record ConnectFlags(boolean cleanSession, Packet.@Nullable Will will) {
    }

    /// One way for an abandoned connection's DISCONNECT to fail, named for the test display.
    /// `prepare` runs before CONNACK, and `inject` after it with the connection's generation.
    private record DisconnectFailure(
            String name,
            Class<? extends Throwable> cause,
            Consumer<RecordingPort> prepare,
            ObjLongConsumer<SessionCore> inject
    ) {

        @Override
        public String toString() {
            return name;
        }
    }
}
