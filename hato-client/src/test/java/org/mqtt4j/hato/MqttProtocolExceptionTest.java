package org.mqtt4j.hato;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttProtocolExceptionTest {

    @ParameterizedTest
    @EnumSource(value = ReasonCode.class, names = {"SUCCESS", "NOT_AUTHORIZED"})
    void rejects_reason_codes_outside_local_packet_failures(ReasonCode reasonCode) {
        // Act & Assert
        assertThatThrownBy(() -> new MqttProtocolException("Invalid packet", reasonCode))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MqttProtocolException(
                "Invalid packet",
                reasonCode,
                new IllegalArgumentException("Invalid field")
        ))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the constructor's non-null contract.
    void requires_non_null_arguments_when_supplied() {
        // Act & Assert
        assertThatThrownBy(() -> new MqttProtocolException("Invalid packet", null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new MqttProtocolException(null, ReasonCode.PROTOCOL_ERROR))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new MqttProtocolException(
                "Invalid packet",
                ReasonCode.PROTOCOL_ERROR,
                null
        ))
                .isInstanceOf(NullPointerException.class);
    }
}
