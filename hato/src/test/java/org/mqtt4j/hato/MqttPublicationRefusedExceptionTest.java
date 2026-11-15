package org.mqtt4j.hato;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttPublicationRefusedExceptionTest {

    @Test
    void reports_the_reason_code_and_the_brokers_reason_string() {
        // Act
        var sut = new MqttPublicationRefusedException(ReasonCode.QUOTA_EXCEEDED, "slow down");

        // Assert
        assertThat(sut.reasonCode()).isEqualTo(ReasonCode.QUOTA_EXCEEDED);
        assertThat(sut.reasonString()).contains("slow down");
        assertThat(sut).hasMessageContaining("QUOTA_EXCEEDED").hasMessageContaining("slow down");
    }

    @Test
    void a_refusal_without_a_reason_string_reports_none() {
        // Act
        var sut = new MqttPublicationRefusedException(ReasonCode.PACKET_TOO_LARGE);

        // Assert
        assertThat(sut.reasonString()).isEmpty();
        assertThat(sut).hasMessage("Publication refused: PACKET_TOO_LARGE");
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the constructor's non-null contract.
    void a_refusal_requires_a_reason_code() {
        // Act & Assert
        assertThatThrownBy(() -> new MqttPublicationRefusedException(null, "text"))
                .isInstanceOf(NullPointerException.class);
    }
}
