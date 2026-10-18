package org.mqtt4j.hato.client.internal;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.client.internal.ResponseDeadlines.Kind;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResponseDeadlinesTest {

    private static final Duration TIMEOUT = Duration.ofNanos(100);

    @ParameterizedTest
    @EnumSource(Kind.class)
    void each_response_deadline_starts_after_its_write_completes(Kind kind) {
        // Arrange
        var sut = new ResponseDeadlines(TIMEOUT);
        sut.add(1, kind, 7);

        // Act & Assert
        assertThat(sut.expired(1000)).isEmpty();
        assertThat(sut.written(1, 1000)).contains(TIMEOUT);
        assertThat(sut.expired(1099)).contains(Duration.ofNanos(1));
        assertThat(sut.expired(1100)).contains(Duration.ZERO);
        assertThat(sut.headKind()).isEqualTo(kind);
    }

    @Test
    void responses_do_not_rearm_the_timer_and_the_next_firing_follows_the_remaining_head() {
        // Arrange
        var sut = new ResponseDeadlines(TIMEOUT);
        sut.add(1, Kind.PUBACK, 7);
        sut.add(2, Kind.SUBACK, 8);
        sut.add(3, Kind.UNSUBACK, 9);
        assertThat(sut.written(1, 10)).contains(TIMEOUT);
        assertThat(sut.written(2, 30)).isEmpty();
        assertThat(sut.written(3, 50)).isEmpty();

        // Act
        sut.answered(Kind.PUBACK, 7);
        sut.answered(Kind.SUBACK, 8);

        // Assert
        assertThat(sut.expired(110)).contains(Duration.ofNanos(40));
        assertThat(sut.expired(150)).contains(Duration.ZERO);
    }

    @Test
    void write_completion_order_determines_the_earliest_deadline() {
        // Arrange
        var sut = new ResponseDeadlines(TIMEOUT);
        sut.add(1, Kind.PUBACK, 7);
        sut.add(2, Kind.SUBACK, 8);
        sut.written(2, 10);
        sut.written(1, 30);

        // Act
        sut.answered(Kind.PUBACK, 7);

        // Assert
        assertThat(sut.expired(110)).contains(Duration.ZERO);
    }

    @Test
    void duplicate_writes_do_not_extend_the_response_deadline() {
        // Arrange
        var sut = new ResponseDeadlines(TIMEOUT);
        sut.add(1, Kind.PUBCOMP, 7);
        sut.add(2, Kind.PUBCOMP, 7);
        sut.written(1, 10);

        // Act
        assertThat(sut.written(2, 90)).isEmpty();
        sut.add(3, Kind.PUBCOMP, 7);
        assertThat(sut.written(3, 100)).isEmpty();

        // Assert
        assertThat(sut.expired(110)).contains(Duration.ZERO);
    }

    @Test
    void an_answer_before_write_completion_prevents_a_stale_deadline_after_id_reuse() {
        // Arrange
        var sut = new ResponseDeadlines(TIMEOUT);
        sut.add(1, Kind.PUBACK, 7);
        sut.add(2, Kind.PUBACK, 7);

        // Act
        sut.answered(Kind.PUBACK, 7);
        sut.add(3, Kind.PUBACK, 7);

        // Assert
        assertThat(sut.written(1, 10)).isEmpty();
        assertThat(sut.written(2, 20)).isEmpty();
        assertThat(sut.written(3, 30)).contains(TIMEOUT);
        assertThat(sut.expired(110)).contains(Duration.ofNanos(20));
        assertThat(sut.expired(130)).contains(Duration.ZERO);
    }

    @Test
    void the_two_qos_two_responses_have_independent_deadlines_for_the_same_id() {
        // Arrange
        var sut = new ResponseDeadlines(TIMEOUT);
        sut.add(1, Kind.PUBREC, 7);
        sut.written(1, 10);

        // Act
        sut.answered(Kind.PUBREC, 7);
        sut.add(2, Kind.PUBCOMP, 7);
        assertThat(sut.written(2, 90)).isEmpty();

        // Assert
        assertThat(sut.expired(110)).contains(Duration.ofNanos(80));
        assertThat(sut.expired(190)).contains(Duration.ZERO);
    }

    @Test
    void an_empty_timer_can_adopt_new_waits_without_an_extra_reservation() {
        // Arrange
        var sut = new ResponseDeadlines(TIMEOUT);
        sut.add(1, Kind.UNSUBACK, 7);
        sut.written(1, 10);
        sut.answered(Kind.UNSUBACK, 7);

        // Act
        sut.add(2, Kind.PUBACK, 8);

        // Assert
        assertThat(sut.written(2, 90)).isEmpty();
        assertThat(sut.expired(110)).contains(Duration.ofNanos(80));
        sut.answered(Kind.PUBACK, 8);
        assertThat(sut.expired(190)).isEmpty();
        sut.add(3, Kind.SUBACK, 9);
        assertThat(sut.written(3, 200)).contains(TIMEOUT);
    }

    @Test
    void monotonic_counter_wraparound_preserves_the_deadline() {
        // Arrange
        var sut = new ResponseDeadlines(TIMEOUT);
        sut.add(1, Kind.PUBACK, 7);
        sut.written(1, Long.MAX_VALUE - 50);

        // Act & Assert
        assertThat(sut.expired(Long.MIN_VALUE + 48)).contains(Duration.ofNanos(1));
        assertThat(sut.expired(Long.MIN_VALUE + 49)).contains(Duration.ZERO);
    }

    @Test
    void response_timeouts_must_be_positive_and_fit_the_monotonic_counter() {
        assertThatThrownBy(() -> new ResponseDeadlines(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ResponseDeadlines(Duration.ofNanos(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ResponseDeadlines(Duration.ofSeconds(Long.MAX_VALUE)))
                .isInstanceOf(ArithmeticException.class);
    }
}
