package org.mqtt4j.hato.common.protocol;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.ReasonCode;
import org.mqtt4j.hato.common.value.Topic;
import org.mqtt4j.hato.common.value.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

class PacketVectorsTest {

    private final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    @ParameterizedTest
    @MethodSource("vectors")
    void decodes_independent_wire_vectors(String hex, Packet expected) {
        assertThat(codec.decode(HexFormat.of().parseHex(hex))).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("vectors")
    void encodes_independent_wire_vectors(String hex, Packet packet) {
        assertThat(codec.encode(packet)).isEqualTo(HexFormat.of().parseHex(hex));
    }

    @ParameterizedTest
    @MethodSource("vectors")
    void every_truncated_prefix_is_rejected(String hex, Packet unused) {
        byte[] wire = HexFormat.of().parseHex(hex);
        for (int length = 0; length < wire.length; length++) {
            byte[] prefix = Arrays.copyOf(wire, length);
            assertThatThrownBy(() -> codec.decode(prefix))
                    .as("%s truncated to %s bytes", hex, length)
                    .isInstanceOf(PacketDecodeException.class);
        }
    }

    static Stream<Arguments> vectors() {
        // Independently calculated from MQTT 3.1.1 section 3, also listed in plan/0.1.md, which
        // still uses the product's former name as the first client ID.
        var topic = new Topic("a/b");
        var payload = new byte[]{0x68, 0x69};
        return Stream.of(
                Arguments.of("101000044d5154540402003c00046861746f", new Packet.Connect(
                        "hato", MqttVersion.V3_1_1, true, 60, null, null, null, EMPTY
                )),
                // All optional fields: Will QoS 2 + retain, binary Will/password, empty user name.
                Arguments.of("101c00044d51545404f6ffff0001630003772f74000200ff000000028000",
                        new Packet.Connect(
                                "c", MqttVersion.V3_1_1, true, 65535,
                                new Packet.Will(new Topic("w/t"), new byte[]{0, (byte) 0xFF},
                                        Qos.EXACTLY_ONCE, true, EMPTY
                                ),
                                "", new byte[]{(byte) 0x80, 0}, EMPTY
                        )
                ),
                Arguments.of("100c00044d515454040200000000", new Packet.Connect(
                        "", MqttVersion.V3_1_1, true, 0, null, null, null, EMPTY
                )),
                Arguments.of("20020000", new Packet.ConnAck(false, SUCCESS, EMPTY)),
                Arguments.of("20020100", new Packet.ConnAck(true, SUCCESS, EMPTY)),
                Arguments.of("32090003612f62000a6869", new Packet.Publish(
                        topic, payload, Qos.AT_LEAST_ONCE, false, false, 10, EMPTY
                )),
                Arguments.of("31070003612f626869", new Packet.Publish(
                        topic, payload, Qos.AT_MOST_ONCE, true, false, 0, EMPTY
                )),
                Arguments.of("3d070003612f62ffff", new Packet.Publish(
                        topic, new byte[0], Qos.EXACTLY_ONCE, true, true, 65535, EMPTY
                )),
                // Section 1.5.3 UTF-8 example used as a topic, followed by opaque bytes.
                Arguments.of("3009000541f0aa9b9400ff", new Packet.Publish(
                        new Topic("A𪛔"), new byte[]{0, (byte) 0xFF},
                        Qos.AT_MOST_ONCE, false, false, 0, EMPTY
                )),
                Arguments.of("4002000a", new Packet.PubAck(10, SUCCESS, EMPTY)),
                Arguments.of("5002000a", new Packet.PubRec(10, SUCCESS, EMPTY)),
                Arguments.of("6202000a", new Packet.PubRel(10, SUCCESS, EMPTY)),
                Arguments.of("7002000a", new Packet.PubComp(10, SUCCESS, EMPTY)),
                Arguments.of("820e000a0003612f62010003632f6402", new Packet.Subscribe(
                        10, List.of(
                                new Packet.Subscription(new TopicFilter("a/b"),
                                        Qos.AT_LEAST_ONCE, false, false, 0
                                ),
                                new Packet.Subscription(new TopicFilter("c/d"),
                                        Qos.EXACTLY_ONCE, false, false, 0
                                )
                        ), EMPTY
                )),
                Arguments.of("9004000a0102", new Packet.SubAck(
                        10, List.of(ReasonCode.GRANTED_QOS_1, ReasonCode.GRANTED_QOS_2), EMPTY
                )),
                Arguments.of("9006000a00010280", new Packet.SubAck(
                        10, List.of(SUCCESS, ReasonCode.GRANTED_QOS_1,
                                ReasonCode.GRANTED_QOS_2, ReasonCode.UNSPECIFIED_ERROR
                        ), EMPTY
                )),
                Arguments.of("a20c000a0003612f620003632f64", new Packet.Unsubscribe(
                        10, List.of(new TopicFilter("a/b"), new TopicFilter("c/d")), EMPTY
                )),
                Arguments.of("b002000a", new Packet.UnsubAck(10, List.of(), EMPTY)),
                Arguments.of("c000", new Packet.PingReq()),
                Arguments.of("d000", new Packet.PingResp()),
                Arguments.of("e000", new Packet.Disconnect(SUCCESS, EMPTY))
        );
    }
}
