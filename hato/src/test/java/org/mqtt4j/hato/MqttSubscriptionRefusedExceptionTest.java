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
    void reports_the_brokers_reason_string() {
        // Act
        var sut = new MqttSubscriptionRefusedException(ReasonCode.NOT_AUTHORIZED, "no access");

        // Assert
        assertThat(sut.reasonString()).contains("no access");
        assertThat(sut).hasMessageContaining("NOT_AUTHORIZED").hasMessageContaining("no access");
    }

    @Test
    void a_refusal_without_a_reason_string_reports_none() {
        // Act
        var sut = new MqttSubscriptionRefusedException(ReasonCode.NOT_AUTHORIZED);

        // Assert
        assertThat(sut.reasonString()).isEmpty();
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the constructor's non-null contract.
    void a_broker_refusal_requires_a_reason() {
        // Act & Assert
        assertThatThrownBy(() -> new MqttSubscriptionRefusedException(null))
                .isInstanceOf(NullPointerException.class);
    }
}
