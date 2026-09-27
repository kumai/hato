package org.mqtt4j.mqtj;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttSubscribeExceptionTest {

    @Test
    void exposes_broker_refusal_reason() {
        // Act
        var sut = new MqttSubscribeException(ReasonCode.UNSPECIFIED_ERROR);

        // Assert
        assertThat(sut.reasonCode()).isEqualTo(ReasonCode.UNSPECIFIED_ERROR);
        assertThat(sut).hasMessageContaining("UNSPECIFIED_ERROR");
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the constructor's non-null contract.
    void a_broker_refusal_requires_a_reason() {
        // Act & Assert
        assertThatThrownBy(() -> new MqttSubscribeException(null))
                .isInstanceOf(NullPointerException.class);
    }
}
