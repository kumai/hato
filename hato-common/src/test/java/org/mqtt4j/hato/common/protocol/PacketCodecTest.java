package org.mqtt4j.hato.common.protocol;

import java.lang.management.ManagementFactory;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.ReasonCode;
import org.mqtt4j.hato.common.value.Topic;
import org.mqtt4j.hato.common.value.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.value.ReasonCode.MALFORMED_PACKET;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

class PacketCodecTest {

    private static final Set<MqttVersion> ALL_VERSIONS = EnumSet.allOf(MqttVersion.class);

    private final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    @Test
    void mqtt_5_is_not_silently_encoded_as_mqtt_3_1_1() {
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
    @ValueSource(ints = {0, 3, 6, 255})
    void unsupported_connect_levels_are_preserved_before_parsing_the_body(int level) {
        // [MQTT-3.1.2-2]: only the common prelude is interpreted for an unsupported level.
        byte[] wire = HexFormat.of().parseHex("100700044d51545404");
        wire[8] = (byte) level;

        assertThatExceptionOfType(UnsupportedProtocolVersionException.class)
                .isThrownBy(() -> PacketCodec.decodeConnect(wire, ALL_VERSIONS))
                .satisfies(error -> assertThat(error.protocolLevel()).isEqualTo(level));
        assertThatExceptionOfType(UnsupportedProtocolVersionException.class)
                .isThrownBy(() -> codec.decode(wire))
                .satisfies(error -> assertThat(error.protocolLevel()).isEqualTo(level));
    }

    @Test
    void decode_connect_returns_a_typed_packet_and_rejects_other_packet_types() {
        byte[] wire = HexFormat.of().parseHex("100d00044d5154540402003c000163");
        assertThat(PacketCodec.decodeConnect(wire, ALL_VERSIONS)).isEqualTo(codec.decode(wire));
        assertThatThrownBy(() -> PacketCodec.decodeConnect(
                new byte[]{(byte) 0xC0, 0},
                ALL_VERSIONS
        )).isInstanceOf(PacketDecodeException.class);
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void the_first_connect_selects_its_own_wire_version(MqttVersion version) {
        PacketCodec sut = PacketCodec.of(version);
        var connect = new Packet.Connect("c", version, true, 60, null, null, null, EMPTY);

        byte[] wire = sut.encode(connect);

        assertThat(PacketCodec.decodeConnect(wire, ALL_VERSIONS)).isEqualTo(connect);
    }

    @Test
    void selecting_mqtt_5_does_not_enable_it_in_the_mqtt_3_1_1_decoder() {
        byte[] wire = HexFormat.of().parseHex("100e00044d5154540502003c00000163");

        assertThat(PacketCodec.decodeConnect(wire, ALL_VERSIONS).version())
                .isEqualTo(MqttVersion.V5);
        assertThatExceptionOfType(UnsupportedProtocolVersionException.class)
                .isThrownBy(() -> codec.decode(wire))
                .satisfies(error -> assertThat(error.protocolLevel()).isEqualTo(5));
    }

    @Test
    void a_server_without_mqtt_5_refuses_level_5_before_reading_the_body() {
        // [MQTT-3.1.2-2]: the refusal does not depend on the body, here a Remaining Length that
        // only the MQTT 5.0 codec rejects for not being minimally encoded.
        byte[] wire = HexFormat.of().parseHex("108e0000044d5154540502003c00000163");

        assertThatExceptionOfType(UnsupportedProtocolVersionException.class)
                .isThrownBy(() -> PacketCodec.decodeConnect(
                        wire,
                        EnumSet.of(MqttVersion.V3_1_1)
                ))
                .satisfies(error -> assertThat(error.protocolLevel()).isEqualTo(5));
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> PacketCodec.decodeConnect(wire, ALL_VERSIONS))
                .satisfies(error -> assertThat(error.reasonCode()).isEqualTo(MALFORMED_PACKET));
    }

    @ParameterizedTest
    @CsvSource({
            "0000, MALFORMED_PACKET", "f000, MALFORMED_PACKET", "c100, MALFORMED_PACKET",
            "1100, MALFORMED_PACKET", "c000, PROTOCOL_ERROR", "3000, PROTOCOL_ERROR"
    })
    void the_first_packet_keeps_its_existing_header_error_classification(
            String hex,
            ReasonCode reason
    ) {
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> PacketCodec.decodeConnect(
                        HexFormat.of().parseHex(hex),
                        ALL_VERSIONS
                ))
                .satisfies(error -> assertThat(error.reasonCode()).isEqualTo(reason));
    }

    @Test
    void empty_persistent_client_id_is_retained_for_a_server_refusal_but_cannot_be_sent() {
        // [MQTT-3.1.3-8]: server must send CONNACK 0x02 instead of just dropping this packet.
        var packet = new Packet.Connect(
                "", MqttVersion.V3_1_1, false, 0, null, null, null, EMPTY
        );
        byte[] wire = HexFormat.of().parseHex("100c00044d515454040000000000");

        assertThat(PacketCodec.decodeConnect(wire, ALL_VERSIONS)).isEqualTo(packet);
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
        Packet.Connect connect = PacketCodec.decodeConnect(connectWire, ALL_VERSIONS);
        connectWire[19] = 1;
        connectWire[24] = 1;
        assertThat(connect.will()).isEqualTo(new Packet.Will(
                new Topic("a"), new byte[]{0}, Qos.AT_MOST_ONCE, false, EMPTY
        ));
        assertThat(connect.password()).containsExactly((byte) 0xFF);
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
            a, AT_MOST_ONCE, false, false, 0, 3005000161
            a, AT_MOST_ONCE, true, false, 0, 3105000161
            a, AT_LEAST_ONCE, false, false, 7, 32070001610007
            a, AT_LEAST_ONCE, true, true, 65535, 3b07000161ffff
            a, EXACTLY_ONCE, false, true, 42, 3c07000161002a
            a, EXACTLY_ONCE, true, false, 1, 35070001610001
            é, AT_MOST_ONCE, false, false, 0, 30060002c3a9
            """)
    void prepared_publish_headers_and_complete_packets_preserve_the_wire_format(
            String topicName,
            Qos qos,
            boolean retain,
            boolean dup,
            int packetId,
            String headerHex
    ) {
        // Arrange
        var sut = PacketCodec.of(MqttVersion.V3_1_1);
        var topic = new Topic(topicName);
        var payload = new byte[] {0, (byte) 0xFF};
        var packet = new Packet.Publish(topic, payload, qos, retain, dup, packetId, EMPTY);
        byte[] expectedHeader = HexFormat.of().parseHex(headerHex);
        byte[] expectedPacket = HexFormat.of().parseHex(headerHex + "00ff");

        // Act
        byte[] header = sut.encodePublishHeader(
                topic,
                qos,
                retain,
                dup,
                packetId,
                EMPTY,
                payload.length
        );
        byte[] assembled = sut.encodePublish(header, payload);
        byte[] encoded = sut.encode(packet);

        // Assert
        assertThat(header).isEqualTo(expectedHeader);
        assertThat(assembled).isEqualTo(expectedPacket);
        assertThat(encoded).isEqualTo(expectedPacket);
        assertThat(payload).containsExactly((byte) 0, (byte) 0xFF);
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
            AT_MOST_ONCE, 0, 3003000161
            AT_MOST_ONCE, 124, 307f000161
            AT_MOST_ONCE, 125, 308001000161
            AT_MOST_ONCE, 16380, 30ff7f000161
            AT_MOST_ONCE, 16381, 30808001000161
            AT_MOST_ONCE, 2097148, 30ffff7f000161
            AT_MOST_ONCE, 2097149, 3080808001000161
            AT_MOST_ONCE, 268435452, 30ffffff7f000161
            AT_LEAST_ONCE, 268435450, 32ffffff7f0001610001
            EXACTLY_ONCE, 268435450, 34ffffff7f0001610001
            """)
    void publish_headers_account_for_the_payload_at_every_remaining_length_boundary(
            Qos qos,
            int payloadSize,
            String headerHex
    ) {
        // Arrange: only the header is allocated, even at the maximum Remaining Length.
        var sut = PacketCodec.of(MqttVersion.V3_1_1);
        var topic = new Topic("a");
        int packetId = qos == Qos.AT_MOST_ONCE ? 0 : 1;
        byte[] expected = HexFormat.of().parseHex(headerHex);

        // Act
        byte[] header = sut.encodePublishHeader(
                topic,
                qos,
                false,
                false,
                packetId,
                EMPTY,
                payloadSize
        );

        // Assert
        assertThat(header).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
            AT_MOST_ONCE, -1
            AT_MOST_ONCE, 268435453
            AT_MOST_ONCE, 2147483647
            AT_LEAST_ONCE, 268435451
            EXACTLY_ONCE, 268435451
            """)
    void publish_headers_reject_payload_sizes_outside_the_remaining_length_limit(
            Qos qos,
            int payloadSize
    ) {
        // Arrange
        var sut = PacketCodec.of(MqttVersion.V3_1_1);
        var topic = new Topic("a");
        int packetId = qos == Qos.AT_MOST_ONCE ? 0 : 1;

        // Act & Assert
        assertThatThrownBy(() -> sut.encodePublishHeader(
                topic,
                qos,
                false,
                false,
                packetId,
                EMPTY,
                payloadSize
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
            AT_MOST_ONCE, true, 0
            AT_MOST_ONCE, false, 1
            AT_MOST_ONCE, false, -1
            AT_LEAST_ONCE, false, 0
            AT_LEAST_ONCE, false, 65536
            EXACTLY_ONCE, false, 0
            EXACTLY_ONCE, true, -1
            """)
    void publish_headers_reject_identifiers_and_dup_flags_inconsistent_with_qos(
            Qos qos,
            boolean dup,
            int packetId
    ) {
        // Arrange
        var sut = PacketCodec.of(MqttVersion.V3_1_1);
        var topic = new Topic("a");

        // Act & Assert
        assertThatThrownBy(() -> sut.encodePublishHeader(
                topic,
                qos,
                false,
                dup,
                packetId,
                EMPTY,
                0
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void mqtt_3_1_1_publish_headers_reject_properties() {
        // Arrange
        var sut = PacketCodec.of(MqttVersion.V3_1_1);
        var topic = new Topic("a");
        var properties = new Properties(List.of(new Property.UserProperty("name", "value")));

        // Act & Assert
        assertThatThrownBy(() -> sut.encodePublishHeader(
                topic,
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                properties,
                0
        )).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("properties");
    }

    @Test
    void an_assembled_publish_does_not_retain_its_header_or_payload() {
        // Arrange
        var sut = PacketCodec.of(MqttVersion.V3_1_1);
        byte[] header = sut.encodePublishHeader(
                new Topic("a"),
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY,
                2
        );
        var payload = new byte[] {0, (byte) 0xFF};
        byte[] expected = HexFormat.of().parseHex("300500016100ff");

        // Act
        byte[] encoded = sut.encodePublish(header, payload);
        header[0] = 0;
        payload[0] = 1;

        // Assert
        assertThat(encoded).isEqualTo(expected);
    }

    @Test
    void encoding_a_large_publish_allocates_only_one_payload_sized_buffer() {
        // Arrange: construct the input and warm up encoding before measuring its allocations.
        var sut = PacketCodec.of(MqttVersion.V3_1_1);
        int payloadSize = 4 * 1024 * 1024;
        var payload = new byte[payloadSize];
        payload[0] = 0x5A;
        payload[payloadSize - 1] = (byte) 0xFF;
        var packet = new Packet.Publish(
                new Topic("a"),
                payload,
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        );
        sut.encode(packet);
        var memory = (ThreadMXBean) ManagementFactory.getThreadMXBean();

        // Act
        long before = memory.getCurrentThreadAllocatedBytes();
        byte[] encoded = sut.encode(packet);
        long allocated = memory.getCurrentThreadAllocatedBytes() - before;

        // Assert: allow 1 MiB for header and JVM overhead, but not another full payload buffer.
        assertThat(before).as("per-thread allocation measurement").isNotNegative();
        assertThat(allocated).isLessThan(payloadSize + 1024 * 1024);
        assertThat(encoded).hasSize(payloadSize + 8);
        assertThat(encoded[8]).isEqualTo((byte) 0x5A);
        assertThat(encoded[encoded.length - 1]).isEqualTo((byte) 0xFF);
    }
}
