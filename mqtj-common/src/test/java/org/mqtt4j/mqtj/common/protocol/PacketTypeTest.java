package org.mqtt4j.mqtj.common.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PacketTypeTest {

    @ParameterizedTest
    @CsvSource(textBlock = """
            1, CONNECT
            2, CONNACK
            3, PUBLISH
            4, PUBACK
            5, PUBREC
            6, PUBREL
            7, PUBCOMP
            8, SUBSCRIBE
            9, SUBACK
            10, UNSUBSCRIBE
            11, UNSUBACK
            12, PINGREQ
            13, PINGRESP
            14, DISCONNECT
            15, AUTH
            """)
    void packet_types_follow_the_wire_values(int value, PacketType expected) {
        // MQTT 5.0 Table 2-1; MQTT 3.1.1 Table 2.1 reserves 15 instead of defining AUTH.
        assertThat(PacketType.of(value)).isSameAs(expected);
        assertThat(expected.value()).isEqualTo(value);
    }

    @Test
    void each_wire_value_has_one_packet_type() {
        assertThat(PacketType.values())
                .extracting(PacketType::value)
                .hasSize(15)
                .doesNotHaveDuplicates();
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 16, 31, 0x30, 0xF0, 255, 256, Integer.MAX_VALUE})
    void reserved_and_out_of_range_type_values_are_rejected(int value) {
        // of() accepts the extracted type nibble, not the complete first header byte.
        assertThatThrownBy(() -> PacketType.of(value))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
            CONNECT, 0
            CONNACK, 0
            PUBACK, 0
            PUBREC, 0
            PUBREL, 2
            PUBCOMP, 0
            SUBSCRIBE, 2
            SUBACK, 0
            UNSUBSCRIBE, 2
            UNSUBACK, 0
            PINGREQ, 0
            PINGRESP, 0
            DISCONNECT, 0
            AUTH, 0
            """)
    void fixed_flags_follow_the_specification(PacketType type, int expected) {
        // MQTT 3.1.1 Table 2.2 and MQTT 5.0 Table 2-2.
        assertThat(type.fixedFlags()).hasValue(expected);
    }

    @Test
    void publish_flags_must_be_derived_from_the_message() {
        assertThat(PacketType.PUBLISH.fixedFlags()).isEmpty();
    }
}
