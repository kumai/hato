package org.mqtt4j.hato;

import java.io.IOException;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class DisconnectionTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void absent_causes_preserve_the_request_flag_and_compare_by_value(boolean userRequested) {
        // Act
        var sut = Disconnection.of(null, userRequested, 1);
        var same = Disconnection.of(null, userRequested, 1);

        // Assert
        assertThat(sut.cause()).isEmpty();
        assertThat(sut.userRequested()).isEqualTo(userRequested);
        assertThat(sut.connectionAttempt()).isEqualTo(1);
        assertThat(sut.reconnectDelay()).isEmpty();
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same)
                .isNotEqualTo(Disconnection.of(null, !userRequested, 1));
        assertThat(same).isEqualTo(sut);
        assertThat(sut.toString()).contains(
                "cause=null",
                "userRequested=" + userRequested,
                "connectionAttempt=1",
                "reconnectDelay=null"
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failures_preserve_the_original_cause_and_compare_every_field(boolean userRequested) {
        // Arrange
        var cause = new IOException("connection failed");

        // Act
        var sut = Disconnection.of(cause, userRequested, 7);
        var same = Disconnection.of(cause, userRequested, 7);

        // Assert
        assertThat(sut.cause()).containsSame(cause);
        assertThat(sut.userRequested()).isEqualTo(userRequested);
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same)
                .isNotEqualTo(Disconnection.of(cause, !userRequested, 7))
                .isNotEqualTo(Disconnection.of(
                        new IOException("connection failed"),
                        userRequested,
                        7
                ))
                .isNotEqualTo(Disconnection.of(null, userRequested, 7))
                .isNotEqualTo(Disconnection.of(cause, userRequested, 8))
                .isNotEqualTo(Disconnection.of(cause, userRequested, 7, Duration.ZERO));
        assertThat(same).isEqualTo(sut);
        assertThat(sut.toString()).contains("cause=" + cause, "userRequested=" + userRequested);
    }

    @Test
    void a_scheduled_reconnection_reports_its_delay_and_compares_it() {
        // Act
        var sut = Disconnection.of(null, false, 2, Duration.ofMillis(1500));

        // Assert
        assertThat(sut.reconnectDelay()).contains(Duration.ofMillis(1500));
        assertThat(sut.connectionAttempt()).isEqualTo(2);
        assertThat(sut).isEqualTo(Disconnection.of(null, false, 2, Duration.ofMillis(1500)))
                .isNotEqualTo(Disconnection.of(null, false, 2, Duration.ofMillis(1499)))
                .isNotEqualTo(Disconnection.of(null, false, 2));
        assertThat(sut.toString()).contains("reconnectDelay=PT1.5S");
    }

    @Test
    void invalid_attempt_numbers_and_negative_delays_are_rejected() {
        assertThatIllegalArgumentException().isThrownBy(() -> Disconnection.of(null, false, 0));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Disconnection.of(null, false, 0, Duration.ZERO));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Disconnection.of(null, false, 1, Duration.ofNanos(-1)));
    }
}
