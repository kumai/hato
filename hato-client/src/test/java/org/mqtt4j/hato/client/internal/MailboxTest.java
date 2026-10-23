package org.mqtt4j.hato.client.internal;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.Mailbox.OfferResult.ACCEPTED;
import static org.mqtt4j.hato.client.internal.Mailbox.OfferResult.FULL;
import static org.mqtt4j.hato.client.internal.Mailbox.OfferResult.STALE;

@Timeout(10)
class MailboxTest {

    @Test
    void registration_and_operation_capacity_are_independent_in_the_same_fifo() throws Exception {
        // Arrange: operation admission is already full before the registration burst.
        var sut = new Mailbox(1, 10, 256, 0, 0, 0);
        var command = new Mailbox.Event(0, () -> { }, Mailbox.Kind.COMMAND, 10);
        var registration = new Mailbox.Event(0, () -> { }, Mailbox.Kind.REGISTRATION, 0);
        var next = new Mailbox.Event(0, () -> { }, Mailbox.Kind.REGISTRATION, 0);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);

        // Act & Assert: all subscription slots remain available, and neither class can grow.
        for (int i = 0; i < 256; i++) {
            assertThat(sut.offer(registration)).isEqualTo(ACCEPTED);
        }
        assertThat(sut.offer(next)).isEqualTo(FULL);
        assertThat(sut.offer(command)).isEqualTo(FULL);
        assertThat(sut.take(() -> false)).containsSame(command);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(registration);
        assertThat(sut.offer(next)).isEqualTo(ACCEPTED);
        for (int i = 0; i < 255; i++) {
            assertThat(sut.take(() -> false)).containsSame(registration);
        }
        assertThat(sut.take(() -> false)).containsSame(command);
        assertThat(sut.take(() -> false)).containsSame(next);
        assertThat(sut.size()).isZero();
        assertThat(sut.bytes()).isZero();
    }

    @Test
    void queued_registrations_keep_their_slots_across_connections_and_release_them_on_shutdown() {
        // Arrange
        var sut = new Mailbox(0, 0, 256, 0, 0, 0);
        var registration = new Mailbox.Event(0, () -> { }, Mailbox.Kind.REGISTRATION, 0);
        sut.activate(1);
        for (int i = 0; i < 256; i++) {
            assertThat(sut.offer(registration)).isEqualTo(ACCEPTED);
        }

        // Act & Assert: a connection ending cannot lose accepted registrations or their budget.
        sut.discard(1);
        sut.activate(2);
        assertThat(sut.offer(registration)).isEqualTo(FULL);
        sut.discard(0);
        assertThat(sut.size()).isZero();
        for (int i = 0; i < 256; i++) {
            assertThat(sut.offer(registration)).isEqualTo(ACCEPTED);
        }
        assertThat(sut.offer(registration)).isEqualTo(FULL);
    }

    @Test
    void commands_incoming_packets_and_control_have_independent_capacity_in_one_fifo()
            throws Exception {
        // Arrange
        var sut = new Mailbox(1, 10, 256, 1, 10, 1);
        var command = new Mailbox.Event(0, () -> { }, Mailbox.Kind.COMMAND, 10);
        var incoming = new Mailbox.Event(1, () -> { }, Mailbox.Kind.INCOMING, 10);
        var control = new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0);
        sut.activate(1);

        // Act & Assert: filling one admission class leaves the other two available.
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(command)).isEqualTo(FULL);
        assertThat(sut.offer(incoming)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(incoming)).isEqualTo(FULL);
        assertThat(sut.offer(control)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(control)).isEqualTo(FULL);

        // Act & Assert: taking a command frees its slot without disturbing the shared FIFO.
        assertThat(sut.take(() -> false)).containsSame(command);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(incoming);
        assertThat(sut.take(() -> false)).containsSame(control);
        assertThat(sut.take(() -> false)).containsSame(command);
    }

    @Test
    void keep_alive_events_go_ahead_of_queued_input_but_not_of_termination() throws Exception {
        // Arrange
        var sut = new Mailbox(1, 10, 256, 1, 10, 1);
        var incoming = new Mailbox.Event(1, () -> { }, Mailbox.Kind.INCOMING, 10);
        var timer = new Mailbox.Event(1, () -> { }, Mailbox.Kind.KEEP_ALIVE, 0);
        var response = new Mailbox.Event(1, () -> { }, Mailbox.Kind.KEEP_ALIVE, 0);
        var end = new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0);
        sut.activate(1);
        assertThat(sut.offer(incoming)).isEqualTo(ACCEPTED);

        // Act
        assertThat(sut.offer(timer)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(response)).isEqualTo(ACCEPTED);
        sut.terminate(end);

        // Assert: keep-alive events keep their own order between termination and the FIFO.
        assertThat(sut.take(() -> false)).containsSame(end);
        assertThat(sut.take(() -> false)).containsSame(timer);
        assertThat(sut.take(() -> false)).containsSame(response);
        assertThat(sut.take(() -> false)).containsSame(incoming);
    }

    @Test
    void keep_alive_events_have_a_bounded_class_that_ends_with_its_connection() {
        // Arrange
        var sut = new Mailbox(1, 10, 256, 1, 10, 1);
        var current = new Mailbox.Event(1, () -> { }, Mailbox.Kind.KEEP_ALIVE, 0);
        var next = new Mailbox.Event(2, () -> { }, Mailbox.Kind.KEEP_ALIVE, 0);
        sut.activate(1);
        for (int i = 0; i < 16; i++) {
            assertThat(sut.offer(current)).isEqualTo(ACCEPTED);
        }

        // Act & Assert: the class is full without touching the others, and old events go.
        assertThat(sut.offer(current)).isEqualTo(FULL);
        assertThat(sut.offer(new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0)))
                .isEqualTo(ACCEPTED);
        assertThat(sut.offer(next)).isEqualTo(STALE);
        sut.discard(1);
        assertThat(sut.size()).isZero();
        sut.activate(2);
        assertThat(sut.offer(next)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(current)).isEqualTo(STALE);
    }

    @Test
    void coalesced_wakes_keep_independent_slots_when_control_capacity_is_full() throws Exception {
        // Arrange
        var sut = new Mailbox(1, 0, 256, 0, 0, 1);
        var control = new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0);
        var ready = new Mailbox.Event(1, () -> { }, Mailbox.Kind.DELIVERY_READY, 0);
        var capacity = new Mailbox.Event(1, () -> { }, Mailbox.Kind.SEND_CAPACITY, 0);
        var command = new Mailbox.Event(0, () -> { }, Mailbox.Kind.COMMAND, 0);
        var closing = new Mailbox.Event(1, () -> { }, Mailbox.Kind.SUBSCRIPTION_CLOSE, 0);
        var credentials = new Mailbox.Event(0, () -> { }, Mailbox.Kind.CREDENTIALS_READY, 0);
        var reconnect = new Mailbox.Event(0, () -> { }, Mailbox.Kind.RECONNECT, 0);
        sut.activate(1);
        assertThat(sut.offer(control)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(control)).isEqualTo(FULL);

        // Act: all wake classes remain available and repeated notices occupy no extra slots.
        assertThat(sut.offer(ready)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(capacity)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(closing)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(credentials)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(reconnect)).isEqualTo(ACCEPTED);
        for (int i = 0; i < 300; i++) {
            assertThat(sut.offer(ready)).isEqualTo(ACCEPTED);
            assertThat(sut.offer(capacity)).isEqualTo(ACCEPTED);
            assertThat(sut.offer(closing)).isEqualTo(ACCEPTED);
            assertThat(sut.offer(credentials)).isEqualTo(ACCEPTED);
            assertThat(sut.offer(reconnect)).isEqualTo(ACCEPTED);
        }
        assertThat(sut.offer(control)).isEqualTo(FULL);

        // Assert: the first marker keeps its FIFO position and each wake appears exactly once.
        assertThat(sut.take(() -> false)).containsSame(control);
        assertThat(sut.take(() -> false)).containsSame(ready);
        assertThat(sut.take(() -> false)).containsSame(command);
        assertThat(sut.take(() -> false)).containsSame(capacity);
        assertThat(sut.take(() -> false)).containsSame(closing);
        assertThat(sut.take(() -> false)).containsSame(credentials);
        assertThat(sut.take(() -> false)).containsSame(reconnect);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(command);
    }

    static Stream<Mailbox.Kind> wakes() {
        return Arrays.stream(Mailbox.Kind.values()).filter(Mailbox.Kind::wake);
    }

    @ParameterizedTest
    @MethodSource("wakes")
    void taking_a_wake_rearms_it_before_its_action_runs(Mailbox.Kind kind) throws Exception {
        // Arrange
        var sut = new Mailbox(1, 0, 256, 0, 0, 0);
        var first = new Mailbox.Event(1, () -> { }, kind, 0);
        var next = new Mailbox.Event(1, () -> { }, kind, 0);
        var duplicate = new Mailbox.Event(1, () -> { }, kind, 0);
        var command = new Mailbox.Event(0, () -> { }, Mailbox.Kind.COMMAND, 0);
        sut.activate(1);
        assertThat(sut.offer(first)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(duplicate)).isEqualTo(ACCEPTED);

        // Act: a producer can request another pass while the taken marker is being handled.
        assertThat(sut.take(() -> false)).containsSame(first);
        assertThat(sut.offer(next)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(duplicate)).isEqualTo(ACCEPTED);

        // Assert: the new marker joins the tail, and duplicates do not replace either action.
        assertThat(sut.take(() -> false)).containsSame(command);
        assertThat(sut.take(() -> false)).containsSame(next);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(command);
    }

    @ParameterizedTest
    @MethodSource("wakes")
    void discarding_an_old_wake_does_not_clear_the_current_generations_marker(Mailbox.Kind kind)
            throws Exception {
        // Arrange: a new marker is admitted before the old generation is discarded.
        var sut = new Mailbox(1, 0, 256, 0, 0, 0);
        var old = new Mailbox.Event(1, () -> { }, kind, 0);
        var current = new Mailbox.Event(2, () -> { }, kind, 0);
        var duplicate = new Mailbox.Event(2, () -> { }, kind, 0);
        var command = new Mailbox.Event(0, () -> { }, Mailbox.Kind.COMMAND, 0);
        sut.activate(1);
        assertThat(sut.offer(old)).isEqualTo(ACCEPTED);
        sut.activate(2);
        assertThat(sut.offer(current)).isEqualTo(ACCEPTED);

        // Act
        sut.discard(1);
        assertThat(sut.offer(old)).isEqualTo(STALE);
        assertThat(sut.offer(duplicate)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);

        // Assert: removing the old marker neither admits a duplicate nor deactivates generation 2.
        assertThat(sut.take(() -> false)).containsSame(current);
        assertThat(sut.take(() -> false)).containsSame(command);
    }

    @ParameterizedTest
    @MethodSource("wakes")
    void wakes_require_an_active_matching_generation(Mailbox.Kind kind) throws Exception {
        // Arrange
        var sut = new Mailbox(1, 0, 256, 0, 0, 0);
        var old = new Mailbox.Event(1, () -> { }, kind, 0);
        var current = new Mailbox.Event(2, () -> { }, kind, 0);

        // Act & Assert: a connection-scoped wake still rejects late callbacks.
        assertThat(sut.offer(old)).isEqualTo(STALE);
        sut.activate(1);
        assertThat(sut.offer(old)).isEqualTo(ACCEPTED);
        sut.discard(1);
        assertThat(sut.offer(old)).isEqualTo(STALE);
        sut.activate(2);
        assertThat(sut.offer(old)).isEqualTo(STALE);
        assertThat(sut.offer(current)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(current);
    }

    @ParameterizedTest
    @MethodSource("wakes")
    void client_wakes_survive_connection_end_and_rearm_before_their_action(Mailbox.Kind kind)
            throws Exception {
        // Arrange: every ordinary admission class is full, even while disconnected.
        var sut = new Mailbox(0, 0, 256, 0, 0, 0);
        var first = new Mailbox.Event(0, () -> { }, kind, 0);
        var next = new Mailbox.Event(0, () -> { }, kind, 0);
        assertThat(sut.offer(first)).isEqualTo(ACCEPTED);
        sut.activate(1);
        for (int i = 0; i < 300; i++) {
            assertThat(sut.offer(next)).isEqualTo(ACCEPTED);
        }

        // Act: ending one connection and starting another must not lose the pending wake.
        sut.discard(1);
        assertThat(sut.offer(next)).isEqualTo(ACCEPTED);
        sut.activate(2);

        // Assert: duplicates kept the first action and taking it releases its independent slot.
        assertThat(sut.size()).isEqualTo(1);
        assertThat(sut.take(() -> false)).containsSame(first);
        assertThat(sut.offer(next)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(next);
        assertThat(sut.size()).isZero();

        // Act & Assert: closing the client clears the wake and its marker as well.
        assertThat(sut.offer(first)).isEqualTo(ACCEPTED);
        sut.discard(0);
        assertThat(sut.size()).isZero();
        assertThat(sut.offer(next)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(next);
    }

    @Test
    void reconnect_keeps_queued_command_capacity_until_the_command_is_taken() throws Exception {
        // Arrange
        var sut = new Mailbox(1, 10, 256, 1, 10, 1);
        var command = new Mailbox.Event(0, () -> { }, Mailbox.Kind.COMMAND, 10);
        var incoming = new Mailbox.Event(1, () -> { }, Mailbox.Kind.INCOMING, 10);
        sut.activate(1);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(incoming)).isEqualTo(ACCEPTED);

        // Act & Assert: ending the connection preserves the command and its reservation.
        sut.discard(1);
        assertThat(sut.offer(command)).isEqualTo(FULL);
        assertThat(sut.take(() -> false)).containsSame(command);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);

        // Act & Assert: final shutdown releases the queued command's reservation.
        sut.discard(0);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);
    }

    @Test
    void a_larger_incoming_packet_limit_does_not_expand_the_command_byte_budget() {
        // Arrange
        Mailbox sut = ClientActor.createMailbox(64 * 1024 * 1024, ClientLimits.DEFAULT);
        var command = new Mailbox.Event(
                0,
                () -> { },
                Mailbox.Kind.COMMAND,
                32 * 1024 * 1024 + 1
        );
        var incoming = new Mailbox.Event(
                1,
                () -> { },
                Mailbox.Kind.INCOMING,
                64 * 1024 * 1024
        );
        sut.activate(1);

        // Act & Assert
        assertThat(sut.offer(command)).isEqualTo(FULL);
        assertThat(sut.offer(incoming)).isEqualTo(ACCEPTED);
    }

    @Test
    void transport_termination_follows_preceding_input_when_control_capacity_remains()
            throws Exception {
        // Arrange
        var sut = new Mailbox(2, 10, 256, 1, 10, 2);
        var incoming = new Mailbox.Event(1, () -> { }, Mailbox.Kind.INCOMING, 10);
        var control = new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0);
        var end = new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0);
        sut.activate(1);
        assertThat(sut.offer(incoming)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(control)).isEqualTo(ACCEPTED);

        // Act
        sut.offerTermination(end);

        // Assert
        assertThat(sut.take(() -> false)).containsSame(incoming);
        assertThat(sut.take(() -> false)).containsSame(control);
        assertThat(sut.take(() -> false)).containsSame(end);
    }

    @Test
    void transport_termination_keeps_its_original_event_when_the_control_queue_is_full()
            throws Exception {
        // Arrange
        var sut = new Mailbox(2, 10, 256, 1, 10, 1);
        var incoming = new Mailbox.Event(1, () -> { }, Mailbox.Kind.INCOMING, 10);
        var control = new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0);
        var end = new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0);
        sut.activate(1);
        assertThat(sut.offer(incoming)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(control)).isEqualTo(ACCEPTED);

        // Act
        sut.offerTermination(end);

        // Assert
        assertThat(sut.take(() -> false)).containsSame(end);
        assertThat(sut.take(() -> false)).containsSame(incoming);
        assertThat(sut.take(() -> false)).containsSame(control);
    }

    @ParameterizedTest(name = "control capacity {0}")
    @ValueSource(ints = {1, 2})
    void a_late_transport_end_cannot_hide_the_current_end(int controlCapacity) throws Exception {
        // Arrange: leave either zero or one control slot available in the new connection.
        var sut = new Mailbox(2, 10, 256, 1, 10, controlCapacity);
        var old = new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0);
        var control = new Mailbox.Event(2, () -> { }, Mailbox.Kind.CONTROL, 0);
        var current = new Mailbox.Event(2, () -> { }, Mailbox.Kind.CONTROL, 0);
        sut.activate(1);
        sut.discard(1);
        sut.activate(2);
        assertThat(sut.offer(control)).isEqualTo(ACCEPTED);

        // Act
        sut.offerTermination(old);
        sut.offerTermination(current);

        // Assert: an old end cannot occupy either the FIFO or the termination slot.
        assertThat(List.of(
                sut.take(() -> false).orElseThrow(),
                sut.take(() -> false).orElseThrow()
        )).containsExactlyInAnyOrder(control, current);
    }

    @Test
    void a_late_failure_from_an_old_connection_cannot_hide_the_current_failure() throws Exception {
        // Arrange
        var sut = new Mailbox(2, 10, 256, 1, 10, 1);
        var old = new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0);
        var current = new Mailbox.Event(2, () -> { }, Mailbox.Kind.CONTROL, 0);
        var later = new Mailbox.Event(2, () -> { }, Mailbox.Kind.CONTROL, 0);
        sut.activate(1);
        sut.discard(1);
        sut.activate(2);

        // Act: the old callback resumes after the connection has changed.
        assertThat(sut.terminate(old)).isFalse();
        assertThat(sut.terminate(current)).isTrue();
        assertThat(sut.terminate(old)).isFalse();
        assertThat(sut.terminate(later)).isTrue();

        // Assert: preserve the first failure of the current connection.
        assertThat(sut.take(() -> false)).containsSame(current);
    }

    @ParameterizedTest(name = "handler failure first: {0}")
    @ValueSource(booleans = {false, true})
    void a_retained_sessions_stop_precedes_transport_failure_regardless_of_arrival_order(
            boolean handlerFirst
    ) throws Exception {
        // Arrange: every ordinary slot is full after the same session reconnects.
        var sut = new Mailbox(1, 0, 256, 1, 0, 0);
        long session = 1;
        var input = new Mailbox.Event(2, () -> { }, Mailbox.Kind.INCOMING, 0);
        var command = new Mailbox.Event(0, () -> { }, Mailbox.Kind.COMMAND, 0);
        Runnable handler = () -> { };
        var transport = new Mailbox.Event(2, () -> { }, Mailbox.Kind.CONTROL, 0);
        sut.activateSession(session);
        sut.activate(1);
        sut.discard(1);
        sut.activate(2);
        sut.connectionRequested(true);
        assertThat(sut.offer(input)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);

        // Act: a stop and a connection ending have independent priority slots.
        if (!handlerFirst) {
            assertThat(sut.terminate(transport)).isTrue();
        }
        assertThat(sut.stopSession(session, handler)).isTrue();
        if (handlerFirst) {
            assertThat(sut.terminate(transport)).isTrue();
        }
        sut.activateSession(session);

        // Assert: reasserting the same session cannot erase its accepted notification.
        Mailbox.Event first = sut.take(() -> false).orElseThrow();
        assertThat(first.generation()).isZero();
        assertThat(first.action()).isSameAs(handler);
        assertThat(sut.take(() -> false)).containsSame(input);
        assertThat(sut.take(() -> false)).containsSame(command);
        assertThat(sut.size()).isZero();
    }

    @ParameterizedTest(name = "pending transport failure: {0}")
    @ValueSource(booleans = {false, true})
    void replacing_a_session_removes_only_its_handler_notification(boolean transportFirst)
            throws Exception {
        // Arrange: CONNACK can replace the session within the same connection generation.
        var sut = new Mailbox(0, 0, 256, 0, 0, 0);
        long old = 1;
        long current = 2;
        var transport = new Mailbox.Event(2, () -> { }, Mailbox.Kind.CONTROL, 0);
        sut.activateSession(old);
        sut.activate(2);
        sut.connectionRequested(true);
        if (transportFirst) {
            assertThat(sut.terminate(transport)).isTrue();
        }
        assertThat(sut.stopSession(old, () -> { })).isTrue();
        if (!transportFirst) {
            assertThat(sut.terminate(transport)).isTrue();
        }
        var later = new Mailbox.Event(2, () -> { }, Mailbox.Kind.CONTROL, 0);
        assertThat(sut.terminate(later)).isTrue();

        // Act: both causes arrived before replacement; neither transport callback is repeated.
        sut.activateSession(current);
        assertThat(sut.stopSession(old, () -> { })).isFalse();

        // Assert: a discarded session cannot hide the current transport's cause.
        assertThat(sut.take(() -> false)).containsSame(transport);
        assertThat(sut.size()).isZero();
    }

    @Test
    void a_session_stop_requires_a_connection_request_and_the_current_session() throws Exception {
        // Arrange
        var sut = new Mailbox(0, 0, 256, 0, 0, 0);
        long session = 1;
        long other = 2;
        sut.activateSession(session);

        // Act & Assert: a retained session alone has no connection request to stop.
        assertThat(sut.stopSession(session, () -> { })).isFalse();
        sut.connectionRequested(true);
        sut.activate(1);
        assertThat(sut.stopSession(other, () -> { })).isFalse();
        Runnable first = () -> { };
        assertThat(sut.stopSession(session, first)).isTrue();
        sut.discard(1);
        assertThat(sut.stopSession(session, () -> { })).isTrue();
        assertThat(sut.take(() -> false).orElseThrow().action()).isSameAs(first);
        sut.connectionRequested(false);
        assertThat(sut.stopSession(session, () -> { })).isFalse();
        assertThat(sut.size()).isZero();
    }

    @Test
    void closing_stop_admission_removes_a_pending_stop_before_lanes_resume() throws Exception {
        // Arrange: no connection generation exists while waiting to reconnect.
        var sut = new Mailbox(1, 0, 256, 0, 0, 0);
        sut.activateSession(1);
        sut.connectionRequested(true);
        assertThat(sut.stopSession(1, () -> { })).isTrue();
        var command = new Mailbox.Event(0, () -> { }, Mailbox.Kind.COMMAND, 0);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);

        // Act
        sut.connectionRequested(false);

        // Assert
        assertThat(sut.stopSession(1, () -> { })).isFalse();
        assertThat(sut.take(() -> false)).containsSame(command);
    }

    @ParameterizedTest(name = "control event: {0}")
    @ValueSource(booleans = {false, true})
    void old_callbacks_cannot_consume_capacity_after_reconnecting(boolean control)
            throws Exception {
        // Arrange
        var sut = new Mailbox(2, 10, 256, 1, 10, 1);
        Mailbox.Kind kind = control ? Mailbox.Kind.CONTROL : Mailbox.Kind.INCOMING;
        var old = new Mailbox.Event(1, () -> { }, kind, 10);
        var current = new Mailbox.Event(2, () -> { }, kind, 10);
        sut.activate(1);
        assertThat(sut.offer(old)).isEqualTo(ACCEPTED);
        sut.discard(1);
        sut.activate(2);

        // Act & Assert
        assertThat(sut.offer(old)).isEqualTo(STALE);
        assertThat(sut.offer(current)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(current)).isEqualTo(FULL);
        assertThat(sut.offer(old)).isEqualTo(STALE);
        assertThat(sut.take(() -> false)).containsSame(current);
    }

    @ParameterizedTest(name = "discard generation {0}")
    @ValueSource(longs = {0, 1})
    void ending_a_connection_clears_queued_work_and_rejects_late_callbacks(long generation)
            throws Exception {
        // Arrange
        var sut = new Mailbox(2, 10, 256, 1, 10, 1);
        var incoming = new Mailbox.Event(1, () -> { }, Mailbox.Kind.INCOMING, 10);
        var end = new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0);
        var command = new Mailbox.Event(0, () -> { }, Mailbox.Kind.COMMAND, 0);
        sut.activate(1);
        assertThat(sut.offer(incoming)).isEqualTo(ACCEPTED);
        assertThat(sut.terminate(end)).isTrue();

        // Act
        sut.discard(generation);
        assertThat(sut.terminate(end)).isFalse();

        // Assert: neither the pending failure nor a late callback survives disconnection.
        assertThat(sut.offer(incoming)).isEqualTo(STALE);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(command);
        sut.activate(2);
        var current = new Mailbox.Event(2, () -> { }, Mailbox.Kind.INCOMING, 10);
        assertThat(sut.offer(current)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(current);
    }

    @Test
    void api_commands_survive_connection_changes_in_fifo_order() throws Exception {
        // Arrange
        var sut = new Mailbox(2, 10, 256, 1, 10, 2);
        var first = new Mailbox.Event(0, () -> { }, Mailbox.Kind.COMMAND, 0);
        var second = new Mailbox.Event(0, () -> { }, Mailbox.Kind.COMMAND, 0);
        assertThat(sut.offer(first)).isEqualTo(ACCEPTED);
        sut.activate(1);
        assertThat(sut.offer(second)).isEqualTo(ACCEPTED);

        // Act
        sut.discard(1);
        sut.activate(2);

        // Assert
        assertThat(sut.take(() -> false)).containsSame(first);
        assertThat(sut.take(() -> false)).containsSame(second);
    }

    @ParameterizedTest(name = "maximum packet size {0}")
    @ValueSource(ints = {
            16 * 1024 * 1024,
            32 * 1024 * 1024,
            32 * 1024 * 1024 + 1,
            64 * 1024 * 1024,
            268435460
    })
    void an_empty_client_mailbox_accepts_the_largest_packet_that_decoding_admits(int maxPacketSize)
            throws Exception {
        // Arrange: a packet of the configured maximum size that also carries the most properties.
        Mailbox sut = ClientActor.createMailbox(maxPacketSize, ClientLimits.DEFAULT);
        sut.activate(1);
        int largest = maxPacketSize + PacketCodec.MAX_PROPERTIES * ClientIo.PROPERTY_COST;
        var packet = new Mailbox.Event(1, () -> { }, Mailbox.Kind.INCOMING, largest);

        // Act & Assert: account for the full size without allocating a large payload.
        assertThat(sut.offer(packet)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(packet);
    }

    @ParameterizedTest
    @EnumSource(value = Mailbox.Kind.class, names = {"COMMAND", "INCOMING"})
    void byte_capacity_is_enforced_even_when_event_slots_remain(Mailbox.Kind kind)
            throws Exception {
        // Arrange
        var sut = new Mailbox(2, 10, 256, 2, 10, 1);
        sut.activate(1);
        long generation = kind == Mailbox.Kind.COMMAND ? 0 : 1;
        var full = new Mailbox.Event(generation, () -> { }, kind, 10);
        var extra = new Mailbox.Event(generation, () -> { }, kind, 1);

        // Act & Assert
        assertThat(sut.offer(full)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(extra)).isEqualTo(FULL);
        assertThat(sut.take(() -> false)).containsSame(full);
        assertThat(sut.offer(extra)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(full)).isEqualTo(FULL);
    }

    @Test
    void full_incoming_capacity_preserves_control_capacity_fifo_and_out_of_band_termination()
            throws Exception {
        // Arrange
        var sut = new Mailbox(2, 10, 256, 1, 10, 1);
        sut.activate(1);
        var incoming = new Mailbox.Event(1, () -> { }, Mailbox.Kind.INCOMING, 10);
        var control = new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0);
        var end = new Mailbox.Event(1, () -> { }, Mailbox.Kind.CONTROL, 0);

        // Act & Assert: fill each admission class independently.
        assertThat(sut.offer(incoming)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(incoming)).isEqualTo(FULL);
        assertThat(sut.offer(control)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(control)).isEqualTo(FULL);

        // Act: termination bypasses the full FIFO.
        sut.terminate(end);

        // Assert
        assertThat(sut.take(() -> false)).containsSame(end);
        assertThat(sut.take(() -> false)).containsSame(incoming);
        assertThat(sut.take(() -> false)).containsSame(control);
        assertThat(sut.offer(incoming)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> true)).isEmpty();
    }
}
