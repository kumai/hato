package org.mqtt4j.mqtj;

import java.io.IOException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class DisconnectionTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void absent_causes_preserve_the_request_flag_and_compare_by_value(boolean userRequested) {
        // Act
        var sut = Disconnection.of(null, userRequested);
        var same = Disconnection.of(null, userRequested);

        // Assert
        assertThat(sut.cause()).isEmpty();
        assertThat(sut.userRequested()).isEqualTo(userRequested);
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same)
                .isNotEqualTo(Disconnection.of(null, !userRequested));
        assertThat(same).isEqualTo(sut);
        assertThat(sut.toString()).contains("cause=null", "userRequested=" + userRequested);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failures_preserve_the_original_cause_and_compare_both_fields(boolean userRequested) {
        // Arrange
        var cause = new IOException("connection failed");

        // Act
        var sut = Disconnection.of(cause, userRequested);
        var same = Disconnection.of(cause, userRequested);

        // Assert
        assertThat(sut.cause()).containsSame(cause);
        assertThat(sut.userRequested()).isEqualTo(userRequested);
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same)
                .isNotEqualTo(Disconnection.of(cause, !userRequested))
                .isNotEqualTo(Disconnection.of(new IOException("connection failed"), userRequested))
                .isNotEqualTo(Disconnection.of(null, userRequested));
        assertThat(same).isEqualTo(sut);
        assertThat(sut.toString()).contains("cause=" + cause, "userRequested=" + userRequested);
    }
}
