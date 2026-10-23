package org.mqtt4j.hato.comparison;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

class ScheduleTest {

    @Test
    void a_burst_sends_its_extra_messages_together_and_then_goes_on_at_the_steady_rate() {
        // Arrange: 1,000 messages a second, with 3 extra messages after the first 5.
        var sut = new Schedule.Burst(1_000, 5, 3);
        // Act & Assert: messages 5 to 7 are the burst, and message 8 is the sixth steady one.
        assertThat(sut.dueMicros(4)).isEqualTo(4_000);
        assertThat(sut.dueMicros(5)).isEqualTo(5_000);
        assertThat(sut.dueMicros(7)).isEqualTo(5_000);
        assertThat(sut.dueMicros(8)).isEqualTo(5_000);
        assertThat(sut.dueMicros(9)).isEqualTo(6_000);
    }

    @Test
    void every_message_is_due_at_its_time_and_not_a_microsecond_before() {
        for (Schedule sut : new Schedule[] {
                new Schedule.Steady(3_000),
                new Schedule.Burst(3_000, 7, 11)
        }) {
            for (long sequence = 0; sequence < 100; sequence++) {
                long due = sut.dueMicros(sequence);
                assertThat(sut.dueBy(due)).as("%s at %d", sut, sequence).isGreaterThan(sequence);
                if (due > 0) {
                    assertThat(sut.dueBy(due - 1)).as("%s before %d", sut, sequence)
                            .isLessThanOrEqualTo(sequence);
                }
            }
        }
    }
}
