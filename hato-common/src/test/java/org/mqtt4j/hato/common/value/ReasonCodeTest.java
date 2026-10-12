package org.mqtt4j.hato.common.value;

import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReasonCodeTest {

    @ParameterizedTest
    @CsvSource(textBlock = """
            0x00, SUCCESS
            0x01, GRANTED_QOS_1
            0x02, GRANTED_QOS_2
            0x04, DISCONNECT_WITH_WILL_MESSAGE
            0x10, NO_MATCHING_SUBSCRIBERS
            0x11, NO_SUBSCRIPTION_EXISTED
            0x18, CONTINUE_AUTHENTICATION
            0x19, RE_AUTHENTICATE
            0x80, UNSPECIFIED_ERROR
            0x81, MALFORMED_PACKET
            0x82, PROTOCOL_ERROR
            0x83, IMPLEMENTATION_SPECIFIC_ERROR
            0x84, UNSUPPORTED_PROTOCOL_VERSION
            0x85, CLIENT_IDENTIFIER_NOT_VALID
            0x86, BAD_USER_NAME_OR_PASSWORD
            0x87, NOT_AUTHORIZED
            0x88, SERVER_UNAVAILABLE
            0x89, SERVER_BUSY
            0x8A, BANNED
            0x8B, SERVER_SHUTTING_DOWN
            0x8C, BAD_AUTHENTICATION_METHOD
            0x8D, KEEP_ALIVE_TIMEOUT
            0x8E, SESSION_TAKEN_OVER
            0x8F, TOPIC_FILTER_INVALID
            0x90, TOPIC_NAME_INVALID
            0x91, PACKET_IDENTIFIER_IN_USE
            0x92, PACKET_IDENTIFIER_NOT_FOUND
            0x93, RECEIVE_MAXIMUM_EXCEEDED
            0x94, TOPIC_ALIAS_INVALID
            0x95, PACKET_TOO_LARGE
            0x96, MESSAGE_RATE_TOO_HIGH
            0x97, QUOTA_EXCEEDED
            0x98, ADMINISTRATIVE_ACTION
            0x99, PAYLOAD_FORMAT_INVALID
            0x9A, RETAIN_NOT_SUPPORTED
            0x9B, QOS_NOT_SUPPORTED
            0x9C, USE_ANOTHER_SERVER
            0x9D, SERVER_MOVED
            0x9E, SHARED_SUBSCRIPTIONS_NOT_SUPPORTED
            0x9F, CONNECTION_RATE_EXCEEDED
            0xA0, MAXIMUM_CONNECT_TIME
            0xA1, SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED
            0xA2, WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED
            """)
    void reason_codes_follow_the_mqtt_5_wire_values(int value, ReasonCode expected) {
        // MQTT 5.0 section 2.4, Table 2-6: verify both directions against the specification.
        assertThat(ReasonCode.of(value)).isSameAs(expected);
        assertThat(expected.value()).isEqualTo(value);
    }

    @Test
    void each_wire_value_has_one_enum_constant() {
        // The three meanings of 0x00 share SUCCESS; there are 43 distinct wire values.
        assertThat(ReasonCode.values())
                .extracting(ReasonCode::value)
                .hasSize(43)
                .doesNotHaveDuplicates();
    }

    @ParameterizedTest
    @MethodSource("undefinedValues")
    void undefined_reason_codes_are_rejected(int value) {
        assertThatThrownBy(() -> ReasonCode.of(value))
                .isInstanceOf(IllegalArgumentException.class);
    }

    static IntStream undefinedValues() {
        // The gaps in MQTT 5.0 Table 2-6, independently of the enum's contents.
        return IntStream.rangeClosed(0, 0xFF)
                .filter(value -> value == 0x03
                        || (value >= 0x05 && value <= 0x0F)
                        || (value >= 0x12 && value <= 0x17)
                        || (value >= 0x1A && value <= 0x7F)
                        || value >= 0xA3);
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -256, -128, -1, 256, 257, 0x180, Integer.MAX_VALUE})
    void values_outside_an_unsigned_byte_are_rejected_without_truncation(int value) {
        assertThatThrownBy(() -> ReasonCode.of(value))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
