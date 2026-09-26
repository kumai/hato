package org.mqtt4j.mqtj.client.internal;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.mqtj.client.internal.Mailbox.OfferResult.ACCEPTED;
import static org.mqtt4j.mqtj.client.internal.Mailbox.OfferResult.FULL;
import static org.mqtt4j.mqtj.client.internal.Mailbox.OfferResult.STALE;

@Timeout(10)
class MailboxTest {

    @Test
    void transport_termination_follows_preceding_input_when_control_capacity_remains()
            throws Exception {
        // Arrange
        var sut = new Mailbox(1, 2, 10);
        var data = new Mailbox.Event(1, () -> { }, false, 10);
        var control = new Mailbox.Event(1, () -> { }, true, 0);
        var end = new Mailbox.Event(1, () -> { }, true, 0);
        sut.activate(1);
        assertThat(sut.offer(data)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(control)).isEqualTo(ACCEPTED);

        // Act
        sut.offerTermination(end);

        // Assert
        assertThat(sut.take(() -> false)).containsSame(data);
        assertThat(sut.take(() -> false)).containsSame(control);
        assertThat(sut.take(() -> false)).containsSame(end);
    }

    @Test
    void transport_termination_keeps_its_original_event_when_the_control_queue_is_full()
            throws Exception {
        // Arrange
        var sut = new Mailbox(1, 1, 10);
        var data = new Mailbox.Event(1, () -> { }, false, 10);
        var control = new Mailbox.Event(1, () -> { }, true, 0);
        var end = new Mailbox.Event(1, () -> { }, true, 0);
        sut.activate(1);
        assertThat(sut.offer(data)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(control)).isEqualTo(ACCEPTED);

        // Act
        sut.offerTermination(end);

        // Assert
        assertThat(sut.take(() -> false)).containsSame(end);
        assertThat(sut.take(() -> false)).containsSame(data);
        assertThat(sut.take(() -> false)).containsSame(control);
    }

    @ParameterizedTest(name = "control capacity {0}")
    @ValueSource(ints = {1, 2})
    void a_late_transport_end_cannot_hide_the_current_end(int controlCapacity) throws Exception {
        // Arrange: leave either zero or one control slot available in the new connection.
        var sut = new Mailbox(1, controlCapacity, 10);
        var old = new Mailbox.Event(1, () -> { }, true, 0);
        var control = new Mailbox.Event(2, () -> { }, true, 0);
        var current = new Mailbox.Event(2, () -> { }, true, 0);
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
        var sut = new Mailbox(1, 1, 10);
        var old = new Mailbox.Event(1, () -> { }, true, 0);
        var current = new Mailbox.Event(2, () -> { }, true, 0);
        var later = new Mailbox.Event(2, () -> { }, true, 0);
        sut.activate(1);
        sut.discard(1);
        sut.activate(2);

        // Act: the old callback resumes after the connection has changed.
        sut.terminate(old);
        sut.terminate(current);
        sut.terminate(old);
        sut.terminate(later);

        // Assert: preserve the first failure of the current connection.
        assertThat(sut.take(() -> false)).containsSame(current);
    }

    @ParameterizedTest(name = "control event: {0}")
    @ValueSource(booleans = {false, true})
    void old_callbacks_cannot_consume_capacity_after_reconnecting(boolean control)
            throws Exception {
        // Arrange
        var sut = new Mailbox(1, 1, 10);
        var old = new Mailbox.Event(1, () -> { }, control, 10);
        var current = new Mailbox.Event(2, () -> { }, control, 10);
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
        var sut = new Mailbox(1, 1, 10);
        var data = new Mailbox.Event(1, () -> { }, false, 10);
        var end = new Mailbox.Event(1, () -> { }, true, 0);
        var command = new Mailbox.Event(0, () -> { }, true, 0);
        sut.activate(1);
        assertThat(sut.offer(data)).isEqualTo(ACCEPTED);
        sut.terminate(end);

        // Act
        sut.discard(generation);
        sut.terminate(end);

        // Assert: neither the pending failure nor a late callback survives disconnection.
        assertThat(sut.offer(data)).isEqualTo(STALE);
        assertThat(sut.offer(command)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(command);
        sut.activate(2);
        var current = new Mailbox.Event(2, () -> { }, false, 10);
        assertThat(sut.offer(current)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(current);
    }

    @Test
    void api_commands_survive_connection_changes_in_fifo_order() throws Exception {
        // Arrange
        var sut = new Mailbox(1, 2, 10);
        var first = new Mailbox.Event(0, () -> { }, true, 0);
        var second = new Mailbox.Event(0, () -> { }, true, 0);
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
    void an_empty_client_mailbox_accepts_the_configured_maximum_packet_size(int maxPacketSize)
            throws Exception {
        // Arrange
        Mailbox sut = ClientActor.createMailbox(maxPacketSize);
        sut.activate(1);
        var packet = new Mailbox.Event(1, () -> { }, false, maxPacketSize);

        // Act & Assert: account for the full size without allocating a large payload.
        assertThat(sut.offer(packet)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> false)).containsSame(packet);
    }

    @Test
    void byte_capacity_is_enforced_even_when_data_slots_remain() throws Exception {
        // Arrange
        var sut = new Mailbox(2, 1, 10);
        sut.activate(1);
        var full = new Mailbox.Event(1, () -> { }, false, 10);
        var extra = new Mailbox.Event(1, () -> { }, false, 1);

        // Act & Assert
        assertThat(sut.offer(full)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(extra)).isEqualTo(FULL);
        assertThat(sut.take(() -> false)).containsSame(full);
        assertThat(sut.offer(extra)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(full)).isEqualTo(FULL);
    }

    @Test
    void full_data_mailbox_preserves_control_capacity_fifo_and_out_of_band_termination()
            throws Exception {
        // Arrange
        var sut = new Mailbox(1, 1, 10);
        sut.activate(1);
        var data = new Mailbox.Event(1, () -> { }, false, 10);
        var control = new Mailbox.Event(1, () -> { }, true, 0);
        var end = new Mailbox.Event(1, () -> { }, true, 0);

        // Act & Assert: fill each admission class independently.
        assertThat(sut.offer(data)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(data)).isEqualTo(FULL);
        assertThat(sut.offer(control)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(control)).isEqualTo(FULL);

        // Act: termination bypasses the full FIFO.
        sut.terminate(end);

        // Assert
        assertThat(sut.take(() -> false)).containsSame(end);
        assertThat(sut.take(() -> false)).containsSame(data);
        assertThat(sut.take(() -> false)).containsSame(control);
        assertThat(sut.offer(data)).isEqualTo(ACCEPTED);
        assertThat(sut.take(() -> true)).isEmpty();
    }
}
