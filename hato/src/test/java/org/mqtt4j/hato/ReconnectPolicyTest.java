package org.mqtt4j.hato;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class ReconnectPolicyTest {

    @Test
    void backoff_policies_compare_their_delays_and_initial_retry() {
        // Act
        var sut = ReconnectPolicy.exponentialBackoff(Duration.ofSeconds(1), Duration.ofMinutes(2));
        var same = ReconnectPolicy.exponentialBackoff(Duration.ofSeconds(1), Duration.ofMinutes(2));

        // Assert
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same)
                .isNotEqualTo(ReconnectPolicy.exponentialBackoff(
                        Duration.ofSeconds(2),
                        Duration.ofMinutes(2)
                ))
                .isNotEqualTo(ReconnectPolicy.exponentialBackoff(
                        Duration.ofSeconds(1),
                        Duration.ofMinutes(3)
                ))
                .isNotEqualTo(sut.withInitialRetry())
                .isNotEqualTo(ReconnectPolicy.disabled());
        assertThat(sut.withInitialRetry()).isEqualTo(same.withInitialRetry())
                .isNotEqualTo(ReconnectPolicy.exponentialBackoff(
                        Duration.ofSeconds(2),
                        Duration.ofMinutes(2)
                ).withInitialRetry());
        assertThat(sut.withInitialRetry().withInitialRetry()).isEqualTo(sut.withInitialRetry());
        assertThat(sut.toString()).contains("PT1S", "PT2M", "initialRetry=false");
        // Retrying initial failures keeps the delays.
        assertThat(sut.withInitialRetry().toString()).contains("PT1S", "PT2M", "initialRetry=true");
        assertThat(ReconnectPolicy.disabled()).isEqualTo(ReconnectPolicy.disabled());
        assertThat(ReconnectPolicy.disabled().toString()).contains("disabled");
    }

    @Test
    void classification_settings_take_part_in_equality_and_combine_in_any_order() {
        // Arrange
        var sut = ReconnectPolicy.exponentialBackoff(Duration.ofSeconds(1), Duration.ofMinutes(2));
        var same = ReconnectPolicy.exponentialBackoff(Duration.ofSeconds(1), Duration.ofMinutes(2));

        // Act
        ReconnectPolicy administrative = sut.withAdministrativeActionRetry();
        ReconnectPolicy unspecified = sut.withoutUnspecifiedErrorRetry();
        ReconnectPolicy all = sut.withInitialRetry()
                .withAdministrativeActionRetry()
                .withoutUnspecifiedErrorRetry();
        ReconnectPolicy reordered = same.withoutUnspecifiedErrorRetry()
                .withAdministrativeActionRetry()
                .withInitialRetry()
                .withAdministrativeActionRetry();

        // Assert
        assertThat(administrative).isEqualTo(same.withAdministrativeActionRetry())
                .hasSameHashCodeAs(same.withAdministrativeActionRetry())
                .isNotEqualTo(sut)
                .isNotEqualTo(unspecified)
                .isNotEqualTo(administrative.withoutUnspecifiedErrorRetry());
        assertThat(unspecified).isEqualTo(same.withoutUnspecifiedErrorRetry())
                .hasSameHashCodeAs(same.withoutUnspecifiedErrorRetry())
                .isNotEqualTo(sut)
                .isNotEqualTo(unspecified.withInitialRetry());
        // The settings combine in any order, and repeating one changes nothing.
        assertThat(all).isEqualTo(reordered).hasSameHashCodeAs(reordered);
        assertThat(sut.toString())
                .contains("administrativeActionRetry=false", "unspecifiedErrorRetry=true");
        // The settings keep the delays and each other.
        assertThat(all.toString()).contains(
                "PT1S",
                "PT2M",
                "initialRetry=true",
                "administrativeActionRetry=true",
                "unspecifiedErrorRetry=false"
        );
    }

    @Test
    void equal_delays_and_the_largest_delay_are_accepted() {
        var largest = Duration.ofMillis(Integer.MAX_VALUE);
        assertThatCode(() -> ReconnectPolicy.exponentialBackoff(largest, largest))
                .doesNotThrowAnyException();
        assertThatCode(() -> ReconnectPolicy.exponentialBackoff(
                Duration.ofNanos(1),
                Duration.ofNanos(1)
        )).doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "initial {0}, max {1}")
    @CsvSource({
            "PT0S, PT1S",
            "PT-1S, PT1S",
            "PT2S, PT1S",
            "PT1S, PT596H31M23.648S"
    })
    void out_of_range_delays_are_rejected(Duration initial, Duration max) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ReconnectPolicy.exponentialBackoff(initial, max));
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the factory's non-null contract.
    void null_delays_are_rejected() {
        assertThatNullPointerException()
                .isThrownBy(() -> ReconnectPolicy.exponentialBackoff(null, Duration.ofSeconds(1)));
        assertThatNullPointerException()
                .isThrownBy(() -> ReconnectPolicy.exponentialBackoff(Duration.ofSeconds(1), null));
    }

    @Test
    void a_disabled_policy_rejects_every_retry_setting() {
        assertThatIllegalStateException()
                .isThrownBy(() -> ReconnectPolicy.disabled().withInitialRetry());
        assertThatIllegalStateException()
                .isThrownBy(() -> ReconnectPolicy.disabled().withAdministrativeActionRetry());
        assertThatIllegalStateException()
                .isThrownBy(() -> ReconnectPolicy.disabled().withoutUnspecifiedErrorRetry());
    }
}
