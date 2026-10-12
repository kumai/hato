package org.mqtt4j.hato.common.protocol;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.ReasonCode;
import org.mqtt4j.hato.common.value.Topic;
import org.mqtt4j.hato.common.value.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.value.ReasonCode.MALFORMED_PACKET;
import static org.mqtt4j.hato.common.value.ReasonCode.PROTOCOL_ERROR;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

class Mqtt5PacketCodecTest {

    private final PacketCodec sut = Mqtt5PacketCodec.instance;

    @ParameterizedTest
    @MethodSource("vectors")
    void reads_independent_wire_vectors(String hex, Packet expected) {
        assertThat(sut.decode(bytes(hex))).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("vectors")
    void writes_independent_wire_vectors(String hex, Packet packet) {
        assertThat(sut.encode(packet)).isEqualTo(bytes(hex));
    }

    @ParameterizedTest
    @MethodSource("vectors")
    void rejects_every_truncated_prefix_and_extra_bytes(String hex, Packet unused) {
        byte[] wire = bytes(hex);
        for (int length = 0; length < wire.length; length++) {
            byte[] truncated = Arrays.copyOf(wire, length);
            assertThatThrownBy(() -> sut.decode(truncated))
                    .as("%s truncated to %s", hex, length)
                    .isInstanceOf(PacketDecodeException.class);
        }
        assertThatThrownBy(() -> sut.decode(Arrays.copyOf(wire, wire.length + 1)))
                .isInstanceOf(PacketDecodeException.class);
    }

    static Stream<Arguments> vectors() {
        var topic = new Topic("a/b");
        var payload = new byte[]{0x68, 0x69};
        var alias = new Properties(
                List.of(new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, 1))
        );
        var method = new Properties(
                List.of(new Property.StringValue(PropertyId.AUTHENTICATION_METHOD, "m"))
        );
        return Stream.of(
                Arguments.of(
                        "101100044d5154540502003c0000046861746f",
                        new Packet.Connect(
                                "hato",
                                MqttVersion.V5,
                                true,
                                60,
                                null,
                                null,
                                null,
                                EMPTY
                        )
                ),
                Arguments.of(
                        "100d00044d51545405000000000000",
                        new Packet.Connect(
                                "",
                                MqttVersion.V5,
                                false,
                                0,
                                null,
                                null,
                                null,
                                EMPTY
                        )
                ),
                Arguments.of(
                        "101200044d5154540542000000000163000200ff",
                        new Packet.Connect(
                                "c",
                                MqttVersion.V5,
                                true,
                                0,
                                null,
                                null,
                                new byte[]{0, (byte) 0xFF},
                                EMPTY
                        )
                ),
                Arguments.of(
                        "2003000000",
                        new Packet.ConnAck(false, SUCCESS, EMPTY)
                ),
                Arguments.of(
                        "200600000321000a",
                        new Packet.ConnAck(
                                false,
                                SUCCESS,
                                new Properties(
                                        List.of(
                                                new Property.TwoByteValue(
                                                        PropertyId.RECEIVE_MAXIMUM,
                                                        10
                                                )
                                        )
                                )
                        )
                ),
                // Captured from Mosquitto 2.1.2 for CONNECT client id hato-five-wire.
                Arguments.of(
                        "200e00000b22000a27001e8480210014",
                        new Packet.ConnAck(
                                false,
                                SUCCESS,
                                new Properties(
                                        List.of(
                                                new Property.TwoByteValue(
                                                        PropertyId.TOPIC_ALIAS_MAXIMUM,
                                                        10
                                                ),
                                                new Property.FourByteValue(
                                                        PropertyId.MAXIMUM_PACKET_SIZE,
                                                        2_000_000
                                                ),
                                                new Property.TwoByteValue(
                                                        PropertyId.RECEIVE_MAXIMUM,
                                                        20
                                                )
                                        )
                                )
                        )
                ),
                Arguments.of(
                        "320a0003612f62000a006869",
                        new Packet.Publish(
                                topic,
                                payload,
                                Qos.AT_LEAST_ONCE,
                                false,
                                false,
                                10,
                                EMPTY
                        )
                ),
                Arguments.of(
                        "320d0003612f62000a032300016869",
                        new Packet.Publish(
                                topic,
                                payload,
                                Qos.AT_LEAST_ONCE,
                                false,
                                false,
                                10,
                                alias
                        )
                ),
                Arguments.of(
                        "30080000032300016869",
                        new Packet.AliasPublish(
                                payload,
                                Qos.AT_MOST_ONCE,
                                false,
                                false,
                                0,
                                alias
                        )
                ),
                Arguments.of(
                        "3d080003612f62ffff00",
                        new Packet.Publish(
                                topic,
                                new byte[0],
                                Qos.EXACTLY_ONCE,
                                true,
                                true,
                                65535,
                                EMPTY
                        )
                ),
                Arguments.of(
                        "4002000a",
                        new Packet.PubAck(10, SUCCESS, EMPTY)
                ),
                Arguments.of(
                        "4003000a10",
                        new Packet.PubAck(10, ReasonCode.NO_MATCHING_SUBSCRIBERS, EMPTY)
                ),
                Arguments.of(
                        "4009000a00051f00026f6b",
                        new Packet.PubAck(
                                10,
                                SUCCESS,
                                new Properties(
                                        List.of(
                                                new Property.StringValue(
                                                        PropertyId.REASON_STRING,
                                                        "ok"
                                                )
                                        )
                                )
                        )
                ),
                Arguments.of(
                        "5002000a",
                        new Packet.PubRec(10, SUCCESS, EMPTY)
                ),
                Arguments.of(
                        "6203000a92",
                        new Packet.PubRel(10, ReasonCode.PACKET_IDENTIFIER_NOT_FOUND, EMPTY)
                ),
                Arguments.of(
                        "7002000a",
                        new Packet.PubComp(10, SUCCESS, EMPTY)
                ),
                Arguments.of(
                        "8209000a000003612f622d",
                        new Packet.Subscribe(
                                10,
                                List.of(
                                        new Packet.Subscription(
                                                new TopicFilter("a/b"),
                                                Qos.AT_LEAST_ONCE,
                                                true,
                                                true,
                                                2
                                        )
                                ),
                                EMPTY
                        )
                ),
                Arguments.of(
                        "820f000a000003612f62050003632f641a",
                        new Packet.Subscribe(
                                10,
                                List.of(
                                        new Packet.Subscription(
                                                new TopicFilter("a/b"),
                                                Qos.AT_LEAST_ONCE,
                                                true,
                                                false,
                                                0
                                        ),
                                        new Packet.Subscription(
                                                new TopicFilter("c/d"),
                                                Qos.EXACTLY_ONCE,
                                                false,
                                                true,
                                                1
                                        )
                                ),
                                EMPTY
                        )
                ),
                Arguments.of(
                        "9005000a000102",
                        new Packet.SubAck(
                                10,
                                List.of(ReasonCode.GRANTED_QOS_1, ReasonCode.GRANTED_QOS_2),
                                EMPTY
                        )
                ),
                Arguments.of(
                        "a208000a000003612f62",
                        new Packet.Unsubscribe(
                                10,
                                List.of(
                                        new TopicFilter("a/b")
                                ),
                                EMPTY
                        )
                ),
                Arguments.of(
                        "b005000a000011",
                        new Packet.UnsubAck(
                                10,
                                List.of(SUCCESS, ReasonCode.NO_SUBSCRIPTION_EXISTED),
                                EMPTY
                        )
                ),
                Arguments.of(
                        "c000",
                        new Packet.PingReq()
                ),
                Arguments.of(
                        "d000",
                        new Packet.PingResp()
                ),
                Arguments.of(
                        "e000",
                        new Packet.Disconnect(SUCCESS, EMPTY)
                ),
                Arguments.of(
                        "e00104",
                        new Packet.Disconnect(
                                ReasonCode.DISCONNECT_WITH_WILL_MESSAGE,
                                EMPTY
                        )
                ),
                Arguments.of(
                        "f000",
                        new Packet.Auth(SUCCESS, EMPTY)
                ),
                Arguments.of(
                        "f00618041500016d",
                        new Packet.Auth(ReasonCode.CONTINUE_AUTHENTICATION, method)
                )
        );
    }

    @Test
    void places_will_properties_before_the_will_topic_and_binary_payload() {
        // Arrange: independent MQTT 5.0 CONNECT vector with every optional payload field.
        var willProperties = new Properties(
                List.of(new Property.FourByteValue(PropertyId.WILL_DELAY_INTERVAL, 2))
        );
        var will = new Packet.Will(
                new Topic("w/t"),
                new byte[]{0, (byte) 0xFF},
                Qos.EXACTLY_ONCE,
                true,
                willProperties
        );
        var connectProperties = new Properties(
                List.of(new Property.FourByteValue(PropertyId.SESSION_EXPIRY_INTERVAL, 60))
        );
        var expected = new Packet.Connect(
                "c",
                MqttVersion.V5,
                true,
                65535,
                will,
                "",
                new byte[]{(byte) 0x80, 0},
                connectProperties
        );
        byte[] wire = bytes(
                "102800044d51545405f6ffff05110000003c000163"
                        + "0518000000020003772f74000200ff000000028000"
        );
        // Act & Assert
        assertThat(sut.decode(wire)).isEqualTo(expected);
        assertThat(sut.encode(expected)).isEqualTo(wire);
    }

    @ParameterizedTest
    @CsvSource({
            "4003000a00, 4002000a",
            "4004000a0000, 4002000a",
            "5004000a1000, 5003000a10",
            "6204000a9200, 6203000a92",
            "7004000a0000, 7002000a",
            "e00100, e000",
            "e0020000, e000",
            "e0020400, e00104"
    })
    void normalizes_optional_empty_fields(String input, String canonical) {
        assertThat(sut.encode(sut.decode(bytes(input)))).isEqualTo(bytes(canonical));
    }

    @ParameterizedTest
    @ValueSource(strings = {"20020100", "20020000", "20020201", "2003018400"})
    void rejects_legacy_connack_forms_other_than_an_unsupported_version(String hex) {
        assertThatThrownBy(() -> sut.decode(bytes(hex))).isInstanceOf(PacketDecodeException.class);
    }

    @Test
    void understands_a_legacy_servers_unsupported_version_refusal() {
        var expected = new Packet.ConnAck(false, ReasonCode.UNSUPPORTED_PROTOCOL_VERSION, EMPTY);
        assertThat(sut.decode(bytes("20020001"))).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("badPackets")
    void reports_the_protocol_failure_reason(String hex, ReasonCode reason) {
        assertThatThrownBy(() -> sut.decode(bytes(hex)))
                .isInstanceOfSatisfying(PacketDecodeException.class, e ->
                        assertThat(e.reasonCode()).isEqualTo(reason));
    }

    static Stream<Arguments> badPackets() {
        return Stream.of(
                Arguments.of("0000", MALFORMED_PACKET),
                Arguments.of("c100", MALFORMED_PACKET),
                Arguments.of("c08000", MALFORMED_PACKET),
                Arguments.of("c00100", MALFORMED_PACKET),
                Arguments.of("400100", MALFORMED_PACKET),
                Arguments.of("40020000", PROTOCOL_ERROR),
                Arguments.of("400400010001", MALFORMED_PACKET),
                Arguments.of("360400016100", MALFORMED_PACKET),
                Arguments.of("380400016100", PROTOCOL_ERROR),
                Arguments.of("3003000000", PROTOCOL_ERROR),
                Arguments.of("300700016103230000", ReasonCode.TOPIC_ALIAS_INVALID),
                Arguments.of("300400012b00", PROTOCOL_ERROR),
                Arguments.of("3206000161000000", PROTOCOL_ERROR),
                Arguments.of("2003020000", MALFORMED_PACKET),
                Arguments.of("2003018000", PROTOCOL_ERROR),
                Arguments.of("8203000100", PROTOCOL_ERROR),
                Arguments.of("820700010000016103", PROTOCOL_ERROR),
                Arguments.of("820700010000016130", PROTOCOL_ERROR),
                Arguments.of("820700010000016140", MALFORMED_PACKET),
                Arguments.of("820700010000016180", MALFORMED_PACKET),
                Arguments.of("8210000100000a2473686172652f672f6104", PROTOCOL_ERROR),
                Arguments.of("a20d00010000082473686172652f67", PROTOCOL_ERROR),
                Arguments.of("9003000100", PROTOCOL_ERROR),
                Arguments.of("a203000100", PROTOCOL_ERROR),
                Arguments.of("b003000100", PROTOCOL_ERROR),
                Arguments.of("f00118", MALFORMED_PACKET),
                Arguments.of("f0021800", PROTOCOL_ERROR),
                Arguments.of("f0020000", PROTOCOL_ERROR),
                Arguments.of("100e00044d5154540503000000000163", MALFORMED_PACKET),
                Arguments.of("100e00044d515454050a000000000163", PROTOCOL_ERROR),
                Arguments.of("100e0004585154540502003c00000163", PROTOCOL_ERROR)
        );
    }

    @ParameterizedTest
    @CsvSource({
            "CONNACK, 00 80 81 82 83 84 85 86 87 88 89 8a 8c 90 95 97 99 9a 9b 9c 9d 9f",
            "PUBACK, 00 10 80 83 87 90 91 97 99",
            "PUBREC, 00 10 80 83 87 90 91 97 99",
            "PUBREL, 00 92",
            "PUBCOMP, 00 92",
            "SUBACK, 00 01 02 80 83 87 8f 91 97 9e a1 a2",
            "UNSUBACK, 00 11 80 83 87 8f 91",
            "DISCONNECT, 00 04 80 81 82 83 87 89 8b 8d 8e 8f 90 93 94 95 96 97 98 99 "
                    + "9a 9b 9c 9d 9e 9f a0 a1 a2",
            "AUTH, 00 18 19"
    })
    void accepts_exactly_the_packet_specific_reason_table(PacketType type, String allowedHex) {
        // Arrange: MQTT 5.0 section 3 packet-specific reason tables, including undefined bytes.
        Set<Integer> allowed = Stream.of(allowedHex.split(" "))
                .map(value -> Integer.parseInt(value, 16))
                .collect(Collectors.toSet());
        for (int value = 0; value <= 255; value++) {
            byte[] wire = reasonWire(type, value);
            // Act & Assert
            if (allowed.contains(value)) {
                Packet packet = reasonPacket(type, ReasonCode.of(value));
                assertThat(sut.decode(wire)).as("%s reason %s", type, value).isEqualTo(packet);
                assertThat(sut.decode(sut.encode(packet))).isEqualTo(packet);
            } else {
                assertThatThrownBy(() -> sut.decode(wire))
                        .as("%s reason %s", type, value)
                        .isInstanceOfSatisfying(PacketDecodeException.class, e ->
                                assertThat(e.reasonCode()).isEqualTo(PROTOCOL_ERROR));
            }
        }
        for (ReasonCode reason : ReasonCode.values()) {
            if (!allowed.contains(reason.value())) {
                assertThatThrownBy(() -> sut.encode(reasonPacket(type, reason)))
                        .as("%s reason %s", type, reason)
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    private static byte[] reasonWire(PacketType type, int reason) {
        String value = "%02x".formatted(reason);
        String wire = switch (type) {
            case CONNACK -> "200300" + value + "00";
            case PUBACK -> "40030001" + value;
            case PUBREC -> "50030001" + value;
            case PUBREL -> "62030001" + value;
            case PUBCOMP -> "70030001" + value;
            case SUBACK -> "9004000100" + value;
            case UNSUBACK -> "b004000100" + value;
            case DISCONNECT -> "e001" + value;
            case AUTH -> "f006" + value + "041500016d";
            case CONNECT, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PINGREQ, PINGRESP ->
                    throw new IllegalArgumentException("Packet has no reason code");
        };
        return bytes(wire);
    }

    private static Packet reasonPacket(PacketType type, ReasonCode reason) {
        return switch (type) {
            case CONNACK -> new Packet.ConnAck(false, reason, EMPTY);
            case PUBACK -> new Packet.PubAck(1, reason, EMPTY);
            case PUBREC -> new Packet.PubRec(1, reason, EMPTY);
            case PUBREL -> new Packet.PubRel(1, reason, EMPTY);
            case PUBCOMP -> new Packet.PubComp(1, reason, EMPTY);
            case SUBACK -> new Packet.SubAck(1, List.of(reason), EMPTY);
            case UNSUBACK -> new Packet.UnsubAck(1, List.of(reason), EMPTY);
            case DISCONNECT -> new Packet.Disconnect(reason, EMPTY);
            case AUTH -> {
                var properties = new Properties(
                        List.of(new Property.StringValue(PropertyId.AUTHENTICATION_METHOD, "m"))
                );
                yield new Packet.Auth(reason, properties);
            }
            case CONNECT, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PINGREQ, PINGRESP ->
                    throw new IllegalArgumentException("Packet has no reason code");
        };
    }

    @Test
    void includes_properties_and_payload_in_the_publish_header_length() {
        // Arrange
        var properties = new Properties(List.of(new Property.UserProperty("k", "v")));
        var topic = new Topic("a");
        var payload = new byte[120];
        var expected = new Packet.Publish(
                topic,
                payload,
                Qos.AT_LEAST_ONCE,
                false,
                false,
                1,
                properties
        );
        // Act
        byte[] header = sut.encodePublishHeader(
                topic,
                Qos.AT_LEAST_ONCE,
                false,
                false,
                1,
                properties,
                payload.length
        );
        var input = ByteBuffer.wrap(header);
        input.get();
        int remaining = RemainingLength.readMinimal(input);
        byte[] assembled = sut.encodePublish(header, payload);
        byte[] encoded = sut.encode(expected);
        // Assert
        assertThat(remaining).isEqualTo(133);
        assertThat(assembled).isEqualTo(encoded);
    }

    @Test
    void rejects_an_overflowing_publish_size_before_allocating_the_payload() {
        assertThatThrownBy(() -> sut.encodePublishHeader(
                new Topic("a"),
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY,
                Integer.MAX_VALUE
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requires_an_alias_for_an_omitted_publish_topic() {
        var packet = new Packet.AliasPublish(
                new byte[0],
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        );
        assertThatThrownBy(() -> sut.encode(packet)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refuses_no_local_for_shared_subscriptions() {
        var subscription = new Packet.Subscription(
                new TopicFilter("$share/g/a"),
                Qos.AT_LEAST_ONCE,
                true,
                false,
                0
        );
        var packet = new Packet.Subscribe(1, List.of(subscription), EMPTY);
        assertThatThrownBy(() -> sut.encode(packet)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requires_a_method_for_an_authentication_exchange() {
        var packet = new Packet.Auth(ReasonCode.CONTINUE_AUTHENTICATION, EMPTY);
        assertThatThrownBy(() -> sut.encode(packet)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refuses_a_property_outside_its_packet_type() {
        var alias = new Properties(List.of(new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, 1)));
        var packet = new Packet.PubAck(1, SUCCESS, alias);
        assertThatThrownBy(() -> sut.encode(packet)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void cannot_encode_a_connect_for_another_protocol_version() {
        var packet = new Packet.Connect("c", MqttVersion.V3_1_1, true, 0, null, null, null, EMPTY);
        assertThatThrownBy(() -> sut.encode(packet)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void an_unsuback_without_reason_codes_cannot_be_encoded() {
        var packet = new Packet.UnsubAck(1, List.of(), EMPTY);
        assertThatThrownBy(() -> sut.encode(packet)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refuses_an_ill_formed_shared_filter_in_unsubscribe() {
        var packet = new Packet.Unsubscribe(1, List.of(new TopicFilter("$share/g")), EMPTY);
        assertThatThrownBy(() -> sut.encode(packet)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejects_a_connect_for_another_protocol_level_before_reading_its_body() {
        // Arrange: an MQTT 3.1.1 CONNECT, whose body has no property length.
        byte[] wire = bytes("100d00044d5154540402003c000163");
        // Act & Assert
        assertThatThrownBy(() -> sut.decode(wire))
                .isInstanceOfSatisfying(UnsupportedProtocolVersionException.class, e ->
                        assertThat(e.protocolLevel()).isEqualTo(4));
    }

    @Test
    void publish_headers_accept_the_largest_remaining_length_and_reject_one_more() {
        // Arrange: topic "a" and no properties take four bytes of the variable header.
        var topic = new Topic("a");
        int largest = 268435455 - 4;
        // Act
        byte[] header = sut.encodePublishHeader(
                topic,
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY,
                largest
        );
        var input = ByteBuffer.wrap(header);
        input.get();
        // Assert
        assertThat(RemainingLength.readMinimal(input)).isEqualTo(268435455);
        assertThatThrownBy(() -> sut.encodePublishHeader(
                topic,
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY,
                largest + 1
        )).isInstanceOf(IllegalArgumentException.class);
    }

    private static byte[] bytes(String hex) {
        return HexFormat.of().parseHex(hex);
    }
}
