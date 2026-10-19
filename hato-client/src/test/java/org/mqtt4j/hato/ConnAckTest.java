package org.mqtt4j.hato;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class ConnAckTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void preserves_session_presence_and_attempt_and_compares_by_value(boolean sessionPresent) {
        // Act
        var sut = ConnAck.of(sessionPresent, 3);
        var same = ConnAck.of(sessionPresent, 3);

        // Assert
        assertThat(sut.sessionPresent()).isEqualTo(sessionPresent);
        assertThat(sut.connectionAttempt()).isEqualTo(3);
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same)
                .isNotEqualTo(ConnAck.of(!sessionPresent, 3))
                .isNotEqualTo(ConnAck.of(sessionPresent, 4));
        assertThat(same).isEqualTo(sut);
        assertThat(sut.toString())
                .contains("sessionPresent=" + sessionPresent, "connectionAttempt=3");
    }

    @Test
    void attempt_numbers_start_at_one() {
        assertThatIllegalArgumentException().isThrownBy(() -> ConnAck.of(false, 0));
    }
}
