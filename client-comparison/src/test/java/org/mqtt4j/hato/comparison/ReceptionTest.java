package org.mqtt4j.hato.comparison;

import java.time.Duration;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

class ReceptionTest {

    private static final Duration IDLE = Duration.ofMillis(50);

    private static final Duration LIMIT = Duration.ofSeconds(10);

    @Test
    void a_client_that_drops_messages_is_measured_over_the_messages_that_came_off_the_wire()
            throws Exception {
        // Arrange: ten messages sent, the first three not measured.
        var sut = new Reception(10, 3);
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
        var sut = new Reception(10, 3);
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
        var sut = new Reception(10, 3);
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
    void a_run_whose_connection_ends_during_the_warm_up_measures_nothing() throws Exception {
        // Arrange
        var sut = new Reception(10, 3);
        // Act
        sut.handle(0, 0);
        sut.handle(1, 0);
        Reception.Result result = sut.await(IDLE, LIMIT);
        // Assert
        assertThat(result.handled()).isEqualTo(2);
        assertThat(result.measured()).isZero();
    }
}
