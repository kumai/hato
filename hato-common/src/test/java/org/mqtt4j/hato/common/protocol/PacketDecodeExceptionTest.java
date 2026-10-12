package org.mqtt4j.hato.common.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.common.value.ReasonCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PacketDecodeExceptionTest {

    @ParameterizedTest
    @EnumSource(
            value = ReasonCode.class,
            names = {
                    "MALFORMED_PACKET", "PROTOCOL_ERROR", "PACKET_TOO_LARGE", "TOPIC_ALIAS_INVALID"
            }
    )
    void retains_the_reason_and_message(ReasonCode reasonCode) {
        var error = new PacketDecodeException(reasonCode, "Invalid packet");

        assertThat(error.reasonCode()).isSameAs(reasonCode);
        assertThat(error).hasMessage("Invalid packet").hasNoCause();
    }

    @Test
    void retains_the_original_cause() {
        var cause = new IllegalArgumentException("Invalid field");
        var error = new PacketDecodeException(
                ReasonCode.PROTOCOL_ERROR,
                "Invalid packet",
                cause
        );

        assertThat(error.reasonCode()).isSameAs(ReasonCode.PROTOCOL_ERROR);
        assertThat(error).hasMessage("Invalid packet").hasCause(cause);
    }

    @ParameterizedTest
    @EnumSource(
            value = ReasonCode.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {
                    "MALFORMED_PACKET", "PROTOCOL_ERROR", "PACKET_TOO_LARGE", "TOPIC_ALIAS_INVALID"
            }
    )
    void rejects_unrelated_reason_codes(ReasonCode reasonCode) {
        assertThatThrownBy(() -> new PacketDecodeException(reasonCode, "Invalid packet"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the constructor's non-null contract.
    void requires_a_reason_and_message() {
        assertThatThrownBy(() -> new PacketDecodeException(null, "Invalid packet"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new PacketDecodeException(ReasonCode.MALFORMED_PACKET, null))
                .isInstanceOf(NullPointerException.class);
    }
}
