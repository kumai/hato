package org.mqtt4j.hato;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttSubscriptionRefusedExceptionTest {

    @Test
    void exposes_broker_refusal_reason() {
        // Act
        var sut = new MqttSubscriptionRefusedException(ReasonCode.UNSPECIFIED_ERROR);

        // Assert
        assertThat(sut.reasonCode()).isEqualTo(ReasonCode.UNSPECIFIED_ERROR);
        assertThat(sut).hasMessageContaining("UNSPECIFIED_ERROR");
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the constructor's non-null contract.
    void a_broker_refusal_requires_a_reason() {
        // Act & Assert
        assertThatThrownBy(() -> new MqttSubscriptionRefusedException(null))
                .isInstanceOf(NullPointerException.class);
    }
}
