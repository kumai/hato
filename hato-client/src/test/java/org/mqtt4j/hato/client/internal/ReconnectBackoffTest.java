package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.Random;
import java.util.random.RandomGenerator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class ReconnectBackoffTest {

    @Test
    void jitter_includes_zero_and_the_ceiling_and_doubling_saturates() {
        // Arrange
        var random = new MaximumRandom();
        var sut = new ReconnectBackoff(Duration.ofNanos(3), Duration.ofNanos(20), random);

        // Act & Assert
        assertThat(sut.next()).isEqualTo(Duration.ofNanos(3));
        assertThat(sut.next()).isEqualTo(Duration.ofNanos(6));
        assertThat(sut.next()).isEqualTo(Duration.ofNanos(12));
        assertThat(sut.next()).isEqualTo(Duration.ofNanos(20));
        for (int i = 0; i < 1000; i++) {
            assertThat(sut.next()).isEqualTo(Duration.ofNanos(20));
        }
        random.zero = true;
        assertThat(sut.next()).isZero();
        sut.reset();
        random.zero = false;
        assertThat(sut.next()).isEqualTo(Duration.ofNanos(3));
    }

    @Test
    void the_largest_supported_delay_never_overflows_when_doubled() {
        // Arrange
        var maximum = Duration.ofMillis(Integer.MAX_VALUE);
        var sut = new ReconnectBackoff(maximum.minusNanos(1), maximum, new MaximumRandom());

        // Act & Assert
        assertThat(sut.next()).isEqualTo(maximum.minusNanos(1));
        for (int i = 0; i < 1000; i++) {
            assertThat(sut.next()).isEqualTo(maximum);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 7, 19, 31, 103, Long.MAX_VALUE})
    void seeded_jitter_stays_inside_each_exponential_ceiling(long seed) {
        // Arrange
        var sut = new ReconnectBackoff(
                Duration.ofSeconds(1),
                Duration.ofSeconds(120),
                new Random(seed)
        );
        long ceiling = Duration.ofSeconds(1).toNanos();

        // Act & Assert
        for (int i = 0; i < 1000; i++) {
            assertThat(sut.next().toNanos()).isBetween(0L, ceiling);
            ceiling = Math.min(Duration.ofSeconds(120).toNanos(), ceiling * 2);
        }
    }

    @Test
    void a_delay_range_is_validated_before_the_sequence_is_created() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ReconnectBackoff(
                Duration.ZERO,
                Duration.ofSeconds(1),
                new Random(0)
        ));
        assertThatIllegalArgumentException().isThrownBy(() -> new ReconnectBackoff(
                Duration.ofSeconds(2),
                Duration.ofSeconds(1),
                new Random(0)
        ));
        assertThatIllegalArgumentException().isThrownBy(() -> new ReconnectBackoff(
                Duration.ofSeconds(1),
                Duration.ofMillis(Integer.MAX_VALUE).plusNanos(1),
                new Random(0)
        ));
    }

    static final class MaximumRandom implements RandomGenerator {

        boolean zero;

        @Override
        public long nextLong() {
            return 0;
        }

        @Override
        public long nextLong(long bound) {
            return zero ? 0 : bound - 1;
        }
    }
}
