package org.mqtt4j.hato.comparison;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

class ReceptionTest {

    private static final Duration IDLE = Duration.ofMillis(50);

    private static final Duration LIMIT = Duration.ofSeconds(10);

    @Test
    void a_client_that_drops_messages_is_measured_over_the_messages_that_came_off_the_wire()
            throws Exception {
        // Arrange: ten messages sent, the first three not measured.
        var sut = new Reception(10, 3, false);
        // Act: the client drops 2, 3, 6, 7, and 9.
        for (long sequence : new long[] {0, 1, 4, 5, 8}) {
            sut.handle(sequence, 0);
        }
        Reception.Result result = sut.await(IDLE, LIMIT);
        // Assert: the stretch runs from 4 to 8, so 4 messages arrived and 2 were handled.
        assertThat(result.handled()).isEqualTo(5);
        assertThat(result.arrived()).isEqualTo(4);
        assertThat(result.measured()).isEqualTo(2);
    }

    @Test
    void a_client_that_handles_every_message_is_measured_over_all_after_the_warm_up()
            throws Exception {
        // Arrange
        var sut = new Reception(10, 3, false);
        // Act
        for (long sequence = 0; sequence < 10; sequence++) {
            sut.handle(sequence, 0);
        }
        Reception.Result result = sut.await(IDLE, LIMIT);
        // Assert: the stretch runs from 3 to 9.
        assertThat(result.arrived()).isEqualTo(6);
        assertThat(result.measured()).isEqualTo(6);
    }

    @Test
    void latency_runs_from_the_send_time_in_the_payload_and_leaves_out_the_warm_up()
            throws Exception {
        // Arrange
        var sut = new Reception(10, 3, false);
        long now = Payloads.nowMicros();
        // Act: the warm-up messages were sent a minute ago, the others two milliseconds ago.
        for (long sequence = 0; sequence < 10; sequence++) {
            sut.handle(sequence, now - (sequence < 3 ? 60_000_000 : 2_000));
        }
        Reception.Result result = sut.await(IDLE, LIMIT);
        // Assert
        assertThat(result.latency().p50()).isGreaterThanOrEqualTo(2_000);
        assertThat(result.latency().max()).isLessThan(60_000_000);
    }

    @Test
    void messages_handled_out_of_order_count_up_to_the_highest_number() throws Exception {
        // Arrange
        var sut = new Reception(10, 3, false);
        // Act: a second subscription's thread handles 7 after 8 and 9.
        for (long sequence : new long[] {0, 1, 2, 3, 4, 5, 6, 8, 9, 7}) {
            sut.handle(sequence, 0);
        }
        Reception.Result result = sut.await(IDLE, LIMIT);
        // Assert: the stretch runs from 3 to 9.
        assertThat(result.arrived()).isEqualTo(6);
        assertThat(result.measured()).isEqualTo(6);
    }

    @Test
    void the_last_message_ends_the_run_only_once_the_messages_it_overtook_are_handled()
            throws Exception {
        // Arrange: 8 and 9 overtake 7, which arrives a moment later on another thread.
        var sut = new Reception(10, 3, false);
        for (long sequence : new long[] {0, 1, 2, 3, 4, 5, 6, 8, 9}) {
            sut.handle(sequence, 0);
        }
        Thread.ofVirtual().start(() -> {
            LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
            sut.handle(7, 0);
        });
        // Act
        Reception.Result result = sut.await(Duration.ofSeconds(5), LIMIT);
        // Assert
        assertThat(result.handled()).isEqualTo(10);
    }

    @Test
    void the_backlog_counts_the_messages_due_and_not_yet_handled_at_its_worst() {
        // Arrange: due every 10 µs; the first handled at once, the rest after a stall, and one
        // never handled.
        long[] sent = {0, 10, 20, 30, 40};
        long[] handled = {5, 100, 101, 102, Reception.NOT_HANDLED};
        // Act
        long peak = Reception.peakBacklog(sent, handled);
        // Assert: at 30 µs, four were due and one handled; the unhandled one does not count.
        assertThat(peak).isEqualTo(3);
    }

    @Test
    void the_longest_gap_is_the_longest_time_without_a_handled_message() {
        // Arrange: every 10 µs, then nothing until 1,000 µs; one message never handled.
        long[] handled = {0, 10, 20, 1_000, 1_010, Reception.NOT_HANDLED};
        // Act & Assert
        assertThat(Reception.longestGap(handled)).isEqualTo(980);
    }

    @Test
    void a_run_whose_connection_ends_during_the_warm_up_measures_nothing() throws Exception {
        // Arrange
        var sut = new Reception(10, 3, false);
        // Act
        sut.handle(0, 0);
        sut.handle(1, 0);
        Reception.Result result = sut.await(IDLE, LIMIT);
        // Assert
        assertThat(result.handled()).isEqualTo(2);
        assertThat(result.measured()).isZero();
    }
}
