package org.mqtt4j.hato;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class ConnAckTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void preserves_session_presence_and_compares_by_value(boolean sessionPresent) {
        // Act
        var sut = ConnAck.of(sessionPresent);
        var same = ConnAck.of(sessionPresent);

        // Assert
        assertThat(sut.sessionPresent()).isEqualTo(sessionPresent);
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same)
                .isNotEqualTo(ConnAck.of(!sessionPresent));
        assertThat(same).isEqualTo(sut);
        assertThat(sut.toString()).contains("sessionPresent=" + sessionPresent);
    }
}
