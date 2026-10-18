package org.mqtt4j.hato.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.hato.common.value.ReasonCode.MALFORMED_PACKET;
import static org.mqtt4j.hato.common.value.ReasonCode.PROTOCOL_ERROR;
import static org.mqtt4j.hato.common.value.ReasonCode.TOPIC_ALIAS_INVALID;

class Mqtt5PropertiesTest {

    @Test
    void all_seven_wire_types_preserve_unsigned_values_order_and_duplicates() {
        // Arrange: hand-written wire bytes exercise every property encoding kind.
        var user = new Property.UserProperty("k", "v");
        var expected = new Properties(
                List.of(
                        new Property.ByteValue(PropertyId.PAYLOAD_FORMAT_INDICATOR, 1),
                        new Property.FourByteValue(
                                PropertyId.MESSAGE_EXPIRY_INTERVAL,
                                0xFFFF_FFFFL
                        ),
                        new Property.StringValue(PropertyId.CONTENT_TYPE, "a"),
                        new Property.BinaryValue(
                                PropertyId.CORRELATION_DATA,
                                new byte[]{0, (byte) 0xFF}
                        ),
                        new Property.VariableByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER, 128),
                        new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, 65535),
                        user,
                        user
                )
        );
        byte[] wire = bytes("24010102ffffffff0300016109000200ff0b800123ffff"
                + "2600016b0001762600016b000176");
        var input = ByteBuffer.wrap(wire).order(ByteOrder.LITTLE_ENDIAN).asReadOnlyBuffer();
        var output = new ByteArrayOutputStream();

        // Act
        Properties decoded = Mqtt5Properties.read(input, PacketType.PUBLISH);
        Mqtt5Properties.write(output, expected, PacketType.PUBLISH);

        // Assert
        assertThat(decoded).isEqualTo(expected);
        assertThat(input.hasRemaining()).isFalse();
        assertThat(output.toByteArray()).isEqualTo(wire);
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
            PAYLOAD_FORMAT_INDICATOR, 0101, PUBLISH, true
            MESSAGE_EXPIRY_INTERVAL, 02ffffffff, PUBLISH, true
            CONTENT_TYPE, 030000, PUBLISH, true
            RESPONSE_TOPIC, 08000161, PUBLISH, true
            CORRELATION_DATA, 090000, PUBLISH, true
            SUBSCRIPTION_IDENTIFIER, 0b01, PUBLISH|SUBSCRIBE, false
            SESSION_EXPIRY_INTERVAL, 1100000000, CONNECT|CONNACK|DISCONNECT, false
            ASSIGNED_CLIENT_IDENTIFIER, 12000161, CONNACK, false
            SERVER_KEEP_ALIVE, 130000, CONNACK, false
            AUTHENTICATION_METHOD, 150000, CONNECT|CONNACK|AUTH, false
            AUTHENTICATION_DATA, 160000, CONNECT|CONNACK|AUTH, false
            REQUEST_PROBLEM_INFORMATION, 1700, CONNECT, false
            WILL_DELAY_INTERVAL, 1800000000, NONE, true
            REQUEST_RESPONSE_INFORMATION, 1900, CONNECT, false
            RESPONSE_INFORMATION, 1a0000, CONNACK, false
            SERVER_REFERENCE, 1c0000, CONNACK|DISCONNECT, false
            REASON_STRING, 1f0000, CONNACK|PUBACK|PUBREC|PUBREL|\
            PUBCOMP|SUBACK|UNSUBACK|DISCONNECT|AUTH, false
            RECEIVE_MAXIMUM, 210001, CONNECT|CONNACK, false
            TOPIC_ALIAS_MAXIMUM, 220000, CONNECT|CONNACK, false
            TOPIC_ALIAS, 230001, PUBLISH, false
            MAXIMUM_QOS, 2400, CONNACK, false
            RETAIN_AVAILABLE, 2500, CONNACK, false
            USER_PROPERTY, 2600000000, ALL, true
            MAXIMUM_PACKET_SIZE, 2700000001, CONNECT|CONNACK, false
            WILDCARD_SUBSCRIPTION_AVAILABLE, 2800, CONNACK, false
            SUBSCRIPTION_IDENTIFIER_AVAILABLE, 2900, CONNACK, false
            SHARED_SUBSCRIPTION_AVAILABLE, 2a00, CONNACK, false
            """)
    void identifiers_accept_only_their_packet_and_will_locations(
            PropertyId id,
            String valueHex,
            String allowed,
            boolean willAllowed
    ) {
        // Arrange
        byte[] value = bytes(valueHex);
        Properties misplaced = decodedWhereAllowed(valueHex, allowed);
        // Act & Assert: every packet location is checked against the specification's table.
        for (PacketType type : PacketType.values()) {
            boolean allowedHere = allowed.equals("ALL")
                    ? type != PacketType.PINGREQ && type != PacketType.PINGRESP
                    : Arrays.asList(allowed.split("\\|")).contains(type.name());
            // CONNECT Authentication Data also needs an Authentication Method in this packet.
            byte[] wire = block(valueHex + (id == PropertyId.AUTHENTICATION_DATA
                    && type == PacketType.CONNECT ? "150000" : ""));
            if (allowedHere) {
                Properties decoded = Mqtt5Properties.read(ByteBuffer.wrap(wire), type);
                assertThat(decoded.entries().getFirst().id()).isSameAs(id);
                var output = new ByteArrayOutputStream();
                Mqtt5Properties.write(output, decoded, type);
                assertThat(output.toByteArray()).isEqualTo(wire);
            } else {
                assertMalformed(() -> Mqtt5Properties.read(ByteBuffer.wrap(wire), type));
                assertThatThrownBy(() -> Mqtt5Properties.write(
                        new ByteArrayOutputStream(),
                        misplaced,
                        type
                )).isInstanceOf(IllegalArgumentException.class);
            }
        }
        if (willAllowed) {
            Properties decoded = Mqtt5Properties.readWill(ByteBuffer.wrap(block(valueHex)));
            assertThat(decoded.entries()).hasSize(1);
            assertThat(decoded.entries().getFirst().id().value()).isEqualTo(value[0] & 0xFF);
            var output = new ByteArrayOutputStream();
            Mqtt5Properties.writeWill(output, decoded);
            assertThat(output.toByteArray()).isEqualTo(block(valueHex));
        } else {
            assertMalformed(() -> Mqtt5Properties.readWill(ByteBuffer.wrap(block(valueHex))));
            assertThatThrownBy(() -> Mqtt5Properties.writeWill(
                    new ByteArrayOutputStream(),
                    misplaced
            )).isInstanceOf(IllegalArgumentException.class);
        }
    }

    // Decodes the property where one location permits it, so a test can write it elsewhere.
    private static Properties decodedWhereAllowed(String valueHex, String allowed) {
        var wire = ByteBuffer.wrap(block(valueHex));
        if (allowed.equals("NONE")) {
            return Mqtt5Properties.readWill(wire);
        }
        String[] types = allowed.split("\\|");
        PacketType type = allowed.equals("ALL")
                ? PacketType.DISCONNECT
                : PacketType.valueOf(types[types.length - 1]);
        return Mqtt5Properties.read(wire, type);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "", "80", "8000", "0201", "0104", "02ff7f", "03810001", "030b8100",
            "020900ff", "04030001ff", "0403000100", "032300", "04110000", "0126"
    })
    void invalid_lengths_identifiers_and_encodings_are_malformed(String wireHex) {
        assertMalformed(() -> Mqtt5Properties.read(
                ByteBuffer.wrap(bytes(wireHex)),
                PacketType.PUBLISH
        ));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "020102", "020b00", "0401010100", "03080000", "0408000123"
    })
    void invalid_values_and_singleton_duplicates_are_protocol_errors(String wireHex) {
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> Mqtt5Properties.read(
                        ByteBuffer.wrap(bytes(wireHex)),
                        PacketType.PUBLISH
                ))
                .satisfies(error -> assertThat(error.reasonCode()).isSameAs(PROTOCOL_ERROR));
    }

    @Test
    void a_zero_topic_alias_has_its_specific_disconnect_reason() {
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> Mqtt5Properties.read(
                        ByteBuffer.wrap(bytes("03230000")),
                        PacketType.PUBLISH
                ))
                .satisfies(error -> assertThat(error.reasonCode()).isSameAs(TOPIC_ALIAS_INVALID));
    }

    @Test
    void every_truncation_of_a_complete_property_block_is_malformed() {
        byte[] wire = bytes("17010102000000000300016109000200ff2600016b000176");
        assertThat(Mqtt5Properties.read(ByteBuffer.wrap(wire), PacketType.PUBLISH).entries())
                .hasSize(5);
        for (int limit = 0; limit < wire.length; limit++) {
            ByteBuffer input = ByteBuffer.wrap(wire).limit(limit);
            assertMalformed(() -> Mqtt5Properties.read(input, PacketType.PUBLISH));
        }
    }

    @Test
    void a_property_cannot_consume_bytes_beyond_the_declared_block() {
        // Arrange
        var input = ByteBuffer.wrap(bytes("010101"));

        // Act & Assert
        assertMalformed(() -> Mqtt5Properties.read(input, PacketType.PUBLISH));
        assertThat(input.position()).isEqualTo(2);
        assertThat(input.get()).isEqualTo((byte) 1);
    }

    @Test
    void properties_leave_the_following_payload_untouched() {
        // Arrange
        var input = ByteBuffer.wrap(bytes("aa020101bb"));
        input.position(1);

        // Act
        Properties decoded = Mqtt5Properties.read(input, PacketType.PUBLISH);

        // Assert
        assertThat(decoded.entries())
                .containsExactly(new Property.ByteValue(PropertyId.PAYLOAD_FORMAT_INDICATOR, 1));
        assertThat(input.get() & 0xFF).isEqualTo(0xBB);
    }

    @Test
    void subscription_identifiers_repeat_in_publish_but_not_in_subscribe() {
        // Arrange
        byte[] wire = bytes("040b010b02");

        // Act
        Properties decoded = Mqtt5Properties.read(ByteBuffer.wrap(wire), PacketType.PUBLISH);

        // Assert
        assertThat(decoded.entries()).containsExactly(
                new Property.VariableByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER, 1),
                new Property.VariableByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER, 2)
        );
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> Mqtt5Properties.read(ByteBuffer.wrap(wire), PacketType.SUBSCRIBE))
                .satisfies(error -> assertThat(error.reasonCode()).isSameAs(PROTOCOL_ERROR));
        assertThatThrownBy(() -> Mqtt5Properties.write(
                new ByteArrayOutputStream(),
                decoded,
                PacketType.SUBSCRIBE
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void connect_authentication_data_requires_a_method_in_either_order() {
        // Arrange
        byte[] dataOnly = bytes("03160000");
        var properties = new Properties(
                List.of(new Property.BinaryValue(PropertyId.AUTHENTICATION_DATA, new byte[0]))
        );

        // Act & Assert
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> Mqtt5Properties.read(
                        ByteBuffer.wrap(dataOnly),
                        PacketType.CONNECT
                ))
                .satisfies(error -> assertThat(error.reasonCode()).isSameAs(PROTOCOL_ERROR));
        assertThatThrownBy(() -> Mqtt5Properties.write(
                new ByteArrayOutputStream(),
                properties,
                PacketType.CONNECT
        )).isInstanceOf(IllegalArgumentException.class);
        Properties dataFirst = Mqtt5Properties.read(
                ByteBuffer.wrap(bytes("06160000150000")),
                PacketType.CONNECT
        );
        Properties methodFirst = Mqtt5Properties.read(
                ByteBuffer.wrap(bytes("06150000160000")),
                PacketType.CONNECT
        );
        assertThat(dataFirst.entries()).hasSize(2);
        assertThat(methodFirst.entries()).hasSize(2);
    }

    @Test
    void connack_authentication_data_does_not_require_a_method() {
        // The method was agreed in CONNECT, which this packet cannot see.
        var properties = new Properties(
                List.of(new Property.BinaryValue(PropertyId.AUTHENTICATION_DATA, new byte[0]))
        );

        assertThat(Mqtt5Properties.read(ByteBuffer.wrap(bytes("03160000")), PacketType.CONNACK))
                .isEqualTo(properties);
    }

    @Test
    void invalid_properties_are_rejected_before_writing_the_length_or_values() {
        // Arrange
        var property = new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, 1);
        var duplicate = new Properties(List.of(property, property));
        var malformedText = new Properties(List.of(new Property.UserProperty("k", "\uD800")));
        var output = new ByteArrayOutputStream();
        output.write(0xAA);

        // Act & Assert
        assertThatThrownBy(() -> Mqtt5Properties.write(output, duplicate, PacketType.PUBLISH))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Mqtt5Properties.write(output, malformedText, PacketType.PUBLISH))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Mqtt5Properties.write(output, duplicate, PacketType.CONNECT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(output.toByteArray()).containsExactly((byte) 0xAA);
    }

    private static byte[] bytes(String hex) {
        return HexFormat.of().parseHex(hex);
    }

    private static byte[] block(String valueHex) {
        byte[] values = bytes(valueHex);
        var output = new ByteArrayOutputStream();
        RemainingLength.write(output, values.length);
        output.writeBytes(values);
        return output.toByteArray();
    }

    private static void assertMalformed(Runnable action) {
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(action::run)
                .satisfies(error -> assertThat(error.reasonCode()).isSameAs(MALFORMED_PACKET));
    }
}
