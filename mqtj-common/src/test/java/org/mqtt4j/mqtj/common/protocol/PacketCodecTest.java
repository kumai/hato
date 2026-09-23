package org.mqtt4j.mqtj.common.protocol;

import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.mqtj.common.value.MqttVersion;
import org.mqtt4j.mqtj.common.value.Qos;
import org.mqtt4j.mqtj.common.value.ReasonCode;
import org.mqtt4j.mqtj.common.value.Topic;
import org.mqtt4j.mqtj.common.value.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.mqtj.common.protocol.Properties.EMPTY;
import static org.mqtt4j.mqtj.common.value.ReasonCode.MALFORMED_PACKET;
import static org.mqtt4j.mqtj.common.value.ReasonCode.SUCCESS;

class PacketCodecTest {

    private final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    @Test
    void mqtt_5_is_not_silently_encoded_as_mqtt_3_1_1() {
        assertThatThrownBy(() -> PacketCodec.of(MqttVersion.V5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> codec.encode(new Packet.Connect(
                "c", MqttVersion.V5, true, 0, null, null, null, EMPTY
        ))).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
            0, SUCCESS
            1, UNSUPPORTED_PROTOCOL_VERSION
            2, CLIENT_IDENTIFIER_NOT_VALID
            3, SERVER_UNAVAILABLE
            4, BAD_USER_NAME_OR_PASSWORD
            5, NOT_AUTHORIZED
            """)
    void connack_return_codes_map_to_mqtt_5_reason_codes(int code, ReasonCode reason) {
        var wire = new byte[]{0x20, 2, 0, (byte) code};
        var packet = new Packet.ConnAck(false, reason, EMPTY);

        assertThat(codec.decode(wire)).isEqualTo(packet);
        assertThat(codec.encode(packet)).isEqualTo(wire);
    }

    @ParameterizedTest
    @EnumSource(
            value = ReasonCode.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"SUCCESS", "UNSUPPORTED_PROTOCOL_VERSION", "CLIENT_IDENTIFIER_NOT_VALID",
                    "SERVER_UNAVAILABLE", "BAD_USER_NAME_OR_PASSWORD", "NOT_AUTHORIZED"}
    )
    void connack_reasons_without_a_return_code_are_rejected(ReasonCode reason) {
        assertThatThrownBy(() -> codec.encode(new Packet.ConnAck(false, reason, EMPTY)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(
            value = ReasonCode.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"SUCCESS", "GRANTED_QOS_1", "GRANTED_QOS_2", "UNSPECIFIED_ERROR"}
    )
    void suback_reasons_without_a_return_code_are_rejected(ReasonCode reason) {
        assertThatThrownBy(() -> codec.encode(new Packet.SubAck(1, List.of(reason), EMPTY)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "", "c0", "0000", "f000", "c001", "c00000", "c08080808000",
            "200100", "2003000000", "20020006", "200200ff",
            "400100", "4003000100", "c00100", "d00100", "e00100",
            "300100", "3003000261", "3003000100",
            "82050001000161", "9003000103", "90030001ff",
            "100d00044d51545404020000000100"
    })
    void malformed_framing_and_fields_are_rejected(String hex) {
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> codec.decode(HexFormat.of().parseHex(hex)))
                .satisfies(error -> assertThat(error.reasonCode()).isSameAs(MALFORMED_PACKET));
    }

    @ParameterizedTest(name = "{0}: {1} ({2})")
    @CsvSource(textBlock = """
            # Reserved QoS representations; PUBLISH classification follows MQTT 5.0.
            3603000161, MALFORMED_PACKET, MQTT 5.0 section 3.3.1.2
            3703000161, MALFORMED_PACKET, MQTT 5.0 section 3.3.1.2
            3e03000161, MALFORMED_PACKET, MQTT 5.0 section 3.3.1.2
            3f03000161, MALFORMED_PACKET, MQTT 5.0 section 3.3.1.2
            8206000100016103, MALFORMED_PACKET, MQTT-3-8.3-4 (QoS 3)
            8206000100016104, MALFORMED_PACKET, MQTT-3-8.3-4 (reserved bit)
            8206000100016180, MALFORMED_PACKET, MQTT-3-8.3-4 (reserved bit)
            82060001000161ff, MALFORMED_PACKET, MQTT-3-8.3-4 (reserved bits)
            # Zero identifiers and absent required payloads.
            32050001610000, PROTOCOL_ERROR, MQTT-2.3.1-1
            40020000, PROTOCOL_ERROR, MQTT-2.3.1-1
            50020000, PROTOCOL_ERROR, MQTT-2.3.1-1
            62020000, PROTOCOL_ERROR, MQTT-2.3.1-1
            70020000, PROTOCOL_ERROR, MQTT-2.3.1-1
            b0020000, PROTOCOL_ERROR, MQTT-2.3.1-1
            8206000000016100, PROTOCOL_ERROR, MQTT-2.3.1-1
            9003000000, PROTOCOL_ERROR, MQTT-2.3.1-1
            a2050000000161, PROTOCOL_ERROR, MQTT-2.3.1-1
            82020001, PROTOCOL_ERROR, MQTT-3.8.3-3
            90020001, PROTOCOL_ERROR, MQTT-3.8.4-5 (one result per subscription)
            a2020001, PROTOCOL_ERROR, MQTT-3.10.3-2
            # Names and filters are well-formed UTF-8 but violate topic rules.
            30020000, PROTOCOL_ERROR, MQTT-4.7.3-1 (empty PUBLISH topic)
            300300012b, PROTOCOL_ERROR, MQTT-4.7.1-1 (wildcard in PUBLISH topic)
            101100044d5154540406000000016300000000, PROTOCOL_ERROR, MQTT-4.7.3-1 (empty Will topic)
            101200044d5154540406000000016300012b0000, PROTOCOL_ERROR, MQTT-4.7.1-1 (Will wildcard)
            82050001000000, PROTOCOL_ERROR, MQTT-4.7.3-1 (empty SUBSCRIBE filter)
            820700010002612b00, PROTOCOL_ERROR, MQTT-4.7.1-3 (SUBSCRIBE wildcard)
            a20400010000, PROTOCOL_ERROR, MQTT-4.7.3-1 (empty UNSUBSCRIBE filter)
            a20600010002612b, PROTOCOL_ERROR, MQTT-4.7.1-3 (UNSUBSCRIBE wildcard)
            # Invalid UTF-8 stays malformed in each topic-reading path.
            30040002c080, MALFORMED_PACKET, MQTT-1.5.3-1 (PUBLISH topic)
            101300044d515454040600000001630002c0800000, MALFORMED_PACKET, MQTT-1.5.3-1 (Will topic)
            820700010002c08000, MALFORMED_PACKET, MQTT-1.5.3-1 (SUBSCRIBE filter)
            a20600010002c080, MALFORMED_PACKET, MQTT-1.5.3-1 (UNSUBSCRIBE filter)
            100c000458515454040200000000, PROTOCOL_ERROR, MQTT-3.1.2-1 (protocol name)
            """)
    void packet_violations_are_classified_by_the_wire_rule(
            String hex, ReasonCode reason, String rule
    ) {
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> codec.decode(HexFormat.of().parseHex(hex)))
                .satisfies(error -> assertThat(error.reasonCode())
                        .as("%s: %s", hex, rule)
                        .isSameAs(reason));
    }

    @ParameterizedTest
    @EnumSource(
            value = PacketType.class,
            names = {"PUBLISH", "AUTH"},
            mode = EnumSource.Mode.EXCLUDE
    )
    void every_invalid_fixed_flag_combination_is_rejected(PacketType type) {
        // [MQTT-2.2.2-2]: body validity cannot override invalid fixed flags.
        for (int flags = 0; flags < 16; flags++) {
            if (flags != type.fixedFlags().orElseThrow()) {
                var wire = new byte[]{(byte) ((type.value() << 4) | flags), 0};
                assertThatExceptionOfType(PacketDecodeException.class)
                        .isThrownBy(() -> codec.decode(wire))
                        .satisfies(error -> assertThat(error.reasonCode())
                                .isSameAs(MALFORMED_PACKET));
            }
        }
    }

    @ParameterizedTest(name = "flags {0}: {1} ({2})")
    @CsvSource(textBlock = """
            1, MALFORMED_PACKET, MQTT 5.0 section 3.1.2.3 (reserved bit)
            24, MALFORMED_PACKET, MQTT 5.0 section 3.1.2.6 (Will QoS 3)
            28, MALFORMED_PACKET, MQTT 5.0 section 3.1.2.6 (Will QoS 3)
            56, MALFORMED_PACKET, MQTT 5.0 section 3.1.2.6 (Will QoS 3)
            8, PROTOCOL_ERROR, MQTT-3.1.2-13 (Will QoS without Will)
            16, PROTOCOL_ERROR, MQTT-3.1.2-13 (Will QoS without Will)
            32, PROTOCOL_ERROR, MQTT-3.1.2-15 (Will Retain without Will)
            40, PROTOCOL_ERROR, MQTT-3.1.2-11 (Will fields without Will)
            64, PROTOCOL_ERROR, MQTT-3.1.2-22 (password without username)
            66, PROTOCOL_ERROR, MQTT-3.1.2-22 (password without username)
            """)
    void malformed_connect_bits_are_distinguished_from_invalid_flag_dependencies(
            int flags, ReasonCode reason, String rule
    ) {
        byte[] wire = HexFormat.of().parseHex("100c00044d515454040200000000");
        wire[9] = (byte) flags;

        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> codec.decode(wire))
                .satisfies(error -> assertThat(error.reasonCode())
                        .as("flags %s: %s", flags, rule)
                        .isSameAs(reason));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 3, 5, 6, 255})
    void unsupported_connect_levels_are_preserved_before_parsing_the_body(int level) {
        // [MQTT-3.1.2-2]: only the common prelude is interpreted for an unsupported level.
        byte[] wire = HexFormat.of().parseHex("100700044d51545404");
        wire[8] = (byte) level;

        assertThatExceptionOfType(UnsupportedProtocolVersionException.class)
                .isThrownBy(() -> PacketCodec.decodeConnect(wire))
                .satisfies(error -> assertThat(error.protocolLevel()).isEqualTo(level));
        assertThatExceptionOfType(UnsupportedProtocolVersionException.class)
                .isThrownBy(() -> codec.decode(wire))
                .satisfies(error -> assertThat(error.protocolLevel()).isEqualTo(level));
    }

    @Test
    void decode_connect_returns_a_typed_packet_and_rejects_other_packet_types() {
        byte[] wire = HexFormat.of().parseHex("100d00044d5154540402003c000163");
        assertThat(PacketCodec.decodeConnect(wire)).isEqualTo(codec.decode(wire));
        assertThatThrownBy(() -> PacketCodec.decodeConnect(new byte[]{(byte) 0xC0, 0}))
                .isInstanceOf(PacketDecodeException.class);
    }

    @Test
    void empty_persistent_client_id_is_retained_for_a_server_refusal_but_cannot_be_sent() {
        // [MQTT-3.1.3-8]: server must send CONNACK 0x02 instead of just dropping this packet.
        var packet = new Packet.Connect(
                "", MqttVersion.V3_1_1, false, 0, null, null, null, EMPTY
        );
        byte[] wire = HexFormat.of().parseHex("100c00044d515454040000000000");

        assertThat(PacketCodec.decodeConnect(wire)).isEqualTo(packet);
        assertThat(codec.decode(wire)).isEqualTo(packet);
        assertThatThrownBy(() -> codec.encode(packet)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void password_requires_username_when_encoding_mqtt_3_1_1() {
        assertThatThrownBy(() -> codec.encode(new Packet.Connect(
                "c", MqttVersion.V3_1_1, true, 0, null, null, new byte[0], EMPTY
        ))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void reception_ignores_sender_only_flags_and_encoding_canonicalizes_them() {
        assertThat(codec.decode(HexFormat.of().parseHex("2002fe00")))
                .isEqualTo(new Packet.ConnAck(false, SUCCESS, EMPTY));
        assertThat(codec.decode(HexFormat.of().parseHex("2002ff00")))
                .isEqualTo(new Packet.ConnAck(true, SUCCESS, EMPTY));
        assertThat(codec.decode(HexFormat.of().parseHex("20020105")))
                .isEqualTo(new Packet.ConnAck(false, ReasonCode.NOT_AUTHORIZED, EMPTY));
        assertThat(codec.encode(codec.decode(HexFormat.of().parseHex("3903000161"))))
                .isEqualTo(HexFormat.of().parseHex("3103000161"));
        assertThat(codec.encode(codec.decode(HexFormat.of().parseHex("c08000"))))
                .isEqualTo(HexFormat.of().parseHex("c000"));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 256})
    void unsupported_level_exception_requires_an_unsigned_byte(int level) {
        assertThatThrownBy(() -> new UnsupportedProtocolVersionException(level))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @MethodSource("packetsWithProperties")
    void properties_are_rejected_on_every_packet_and_on_the_will(Packet packet) {
        assertThatThrownBy(() -> codec.encode(packet))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("properties");
    }

    static Stream<Packet> packetsWithProperties() {
        var properties = new Properties(List.of(new Property.UserProperty("name", "value")));
        var subscription = new Packet.Subscription(
                new TopicFilter("#"), Qos.AT_MOST_ONCE, false, false, 0
        );
        return Stream.of(
                new Packet.Connect("c", MqttVersion.V3_1_1, true, 0,
                        null, null, null, properties
                ),
                new Packet.Connect("c", MqttVersion.V3_1_1, true, 0,
                        new Packet.Will(new Topic("a"), new byte[0],
                                Qos.AT_MOST_ONCE, false, properties
                        ), null, null, EMPTY
                ),
                new Packet.ConnAck(false, SUCCESS, properties),
                new Packet.Publish(new Topic("a"), new byte[0],
                        Qos.AT_MOST_ONCE, false, false, 0, properties
                ),
                new Packet.PubAck(1, SUCCESS, properties),
                new Packet.PubRec(1, SUCCESS, properties),
                new Packet.PubRel(1, SUCCESS, properties),
                new Packet.PubComp(1, SUCCESS, properties),
                new Packet.Subscribe(1, List.of(subscription), properties),
                new Packet.SubAck(1, List.of(SUCCESS), properties),
                new Packet.Unsubscribe(1, List.of(new TopicFilter("#")), properties),
                new Packet.UnsubAck(1, List.of(), properties),
                new Packet.Disconnect(SUCCESS, properties)
        );
    }

    @ParameterizedTest
    @EnumSource(value = ReasonCode.class, names = "SUCCESS", mode = EnumSource.Mode.EXCLUDE)
    void reasons_absent_from_mqtt_3_1_1_acknowledgements_cannot_be_discarded(ReasonCode reason) {
        List<Packet> packets = List.of(
                new Packet.PubAck(1, reason, EMPTY),
                new Packet.PubRec(1, reason, EMPTY),
                new Packet.PubRel(1, reason, EMPTY),
                new Packet.PubComp(1, reason, EMPTY),
                new Packet.Disconnect(reason, EMPTY)
        );
        for (Packet packet : packets) {
            assertThatThrownBy(() -> codec.encode(packet))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void auth_and_unsuback_reasons_cannot_be_encoded() {
        assertThatThrownBy(() -> codec.encode(new Packet.Auth(SUCCESS, EMPTY)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> codec.encode(new Packet.UnsubAck(1, List.of(SUCCESS), EMPTY)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({"a/#, 1", "a/#, 2", "a/#, 3", "a/#, 4", "$share/g/a, 1"})
    void mqtt_5_subscription_options_cannot_be_encoded(String pattern, int option) {
        var subscription = new Packet.Subscription(
                new TopicFilter(pattern), Qos.AT_LEAST_ONCE,
                option == 1, option == 2, option >= 3 ? option - 2 : 0
        );
        assertThatThrownBy(() -> codec.encode(new Packet.Subscribe(
                1, List.of(subscription), EMPTY
        ))).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
            $share/+/a, 820f0001000a2473686172652f2b2f6100, a20e0001000a2473686172652f2b2f61
            $share/g, 820d000100082473686172652f6700, a20c000100082473686172652f67
            $share/g/a, 820f0001000a2473686172652f672f6100, a20e0001000a2473686172652f672f61
            $share//a, 820e000100092473686172652f2f6100, a20d000100092473686172652f2f61
            $share/g/, 820e000100092473686172652f672f00, a20d000100092473686172652f672f
            """)
    void mqtt_3_1_1_preserves_dollar_prefixed_subscription_filters(
            String pattern, String subscribeHex, String unsubscribeHex
    ) {
        var filter = new TopicFilter(pattern);
        var subscription = new Packet.Subscription(filter, Qos.AT_MOST_ONCE, false, false, 0);
        var subscribe = new Packet.Subscribe(1, List.of(subscription), EMPTY);
        var unsubscribe = new Packet.Unsubscribe(1, List.of(filter), EMPTY);
        byte[] subscribeWire = HexFormat.of().parseHex(subscribeHex);
        byte[] unsubscribeWire = HexFormat.of().parseHex(unsubscribeHex);

        assertThat(codec.decode(subscribeWire)).isEqualTo(subscribe);
        assertThat(codec.encode(subscribe)).isEqualTo(subscribeWire);
        assertThat(codec.decode(unsubscribeWire)).isEqualTo(unsubscribe);
        assertThat(codec.encode(unsubscribe)).isEqualTo(unsubscribeWire);
    }

    @ParameterizedTest
    @ValueSource(strings = {"a\0b", "\uD800", "\uDC00"})
    void connect_text_is_validated_at_the_codec_boundary(String invalid) {
        assertThatThrownBy(() -> codec.encode(new Packet.Connect(
                invalid, MqttVersion.V3_1_1, true, 0, null, null, null, EMPTY
        ))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> codec.encode(new Packet.Connect(
                "c", MqttVersion.V3_1_1, true, 0, null, invalid, null, EMPTY
        ))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decoded_binary_fields_do_not_retain_the_input_array() {
        byte[] publishWire = HexFormat.of().parseHex("300500016100ff");
        var publish = (Packet.Publish) codec.decode(publishWire);
        publishWire[5] = 1;
        assertThat(publish.payload()).containsExactly((byte) 0, (byte) 0xFF);

        byte[] connectWire = HexFormat.of().parseHex(
                "101700044d51545404c60000000000016100010000000001ff"
        );
        Packet.Connect connect = PacketCodec.decodeConnect(connectWire);
        connectWire[19] = 1;
        connectWire[24] = 1;
        assertThat(connect.will()).isEqualTo(new Packet.Will(
                new Topic("a"), new byte[]{0}, Qos.AT_MOST_ONCE, false, EMPTY
        ));
        assertThat(connect.password()).containsExactly((byte) 0xFF);
    }
}
