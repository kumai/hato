package org.mqtt4j.mqtj.common.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.mqtj.common.value.MqttVersion;
import org.mqtt4j.mqtj.common.value.Qos;
import org.mqtt4j.mqtj.common.value.ReasonCode;
import org.mqtt4j.mqtj.common.value.Topic;
import org.mqtt4j.mqtj.common.value.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.mqtj.common.protocol.Properties.EMPTY;
import static org.mqtt4j.mqtj.common.value.ReasonCode.SUCCESS;

class PacketRoundTripTest {

    private static final List<String> TOPICS = List.of(
            "a", "/", "a//b/", " /あ", "$SYS/state", "cafe\u0301", "\uFEFFa", "𪛔/\uFFFF"
    );
    private static final List<String> FILTERS = List.of(
            "#", "+/+/", "a/#", "a//+", "$SYS/#", "$share/group/あ/+", " /𪛔", "\uFEFFa"
    );
    private static final List<ReasonCode> CONNACK_REASONS = List.of(
            SUCCESS,
            ReasonCode.UNSUPPORTED_PROTOCOL_VERSION,
            ReasonCode.CLIENT_IDENTIFIER_NOT_VALID,
            ReasonCode.SERVER_UNAVAILABLE,
            ReasonCode.BAD_USER_NAME_OR_PASSWORD,
            ReasonCode.NOT_AUTHORIZED
    );
    private static final List<ReasonCode> SUBACK_REASONS = List.of(
            SUCCESS, ReasonCode.GRANTED_QOS_1,
            ReasonCode.GRANTED_QOS_2, ReasonCode.UNSPECIFIED_ERROR
    );

    private final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void all_packet_types_round_trip_with_random_values(int seed) {
        var random = new Random(seed);
        for (Packet packet : randomPackets(random)) {
            byte[] encoded = codec.encode(packet);
            assertThat(codec.decode(encoded)).isEqualTo(packet);
        }
    }

    static IntStream seeds() {
        return IntStream.range(0, 200);
    }

    @ParameterizedTest
    @CsvSource({"124, 1", "125, 2", "16380, 2", "16381, 3", "2097148, 3", "2097149, 4"})
    void publish_crosses_every_remaining_length_encoding_boundary(int payloadSize, int lengthSize) {
        // Topic 'a' adds three bytes. PUBLISH payloads are not limited to 65535 bytes.
        var payload = new byte[payloadSize];
        Arrays.fill(payload, (byte) 0xA5);
        var packet = new Packet.Publish(
                new Topic("a"), payload, Qos.AT_MOST_ONCE, false, false, 0, EMPTY
        );

        byte[] encoded = codec.encode(packet);

        assertThat(encoded).hasSize(1 + lengthSize + 3 + payloadSize);
        assertThat(codec.decode(encoded)).isEqualTo(packet);
    }

    @Test
    void arbitrary_wire_bytes_only_produce_declared_decode_failures() {
        var random = new Random(0x4D515454);
        for (int i = 0; i < 10000; i++) {
            byte[] wire = randomBytes(random, random.nextInt(128));
            // Half the inputs have a valid length, exercising the body instead of only framing.
            if ((i & 1) == 0 && wire.length >= 2) {
                wire[1] = (byte) (wire.length - 2);
            }
            decodeOrExpectedFailure(wire);
        }
    }

    @Test
    void mutations_of_valid_packets_do_not_leak_implementation_exceptions() {
        var random = new Random(311);
        for (Packet packet : randomPackets(random)) {
            byte[] encoded = codec.encode(packet);
            for (int offset = 0; offset < encoded.length; offset++) {
                byte[] mutated = encoded.clone();
                mutated[offset] ^= (byte) (1 + random.nextInt(255));
                decodeOrExpectedFailure(mutated);
            }
        }
    }

    @Test
    void all_truncated_prefixes_of_random_packets_are_rejected() {
        for (Packet packet : randomPackets(new Random(0))) {
            byte[] encoded = codec.encode(packet);
            for (int length = 0; length < encoded.length; length++) {
                byte[] prefix = Arrays.copyOf(encoded, length);
                assertThatThrownBy(() -> codec.decode(prefix))
                        .as("Truncated %s at %s", packet, length)
                        .isInstanceOf(PacketDecodeException.class);
            }
        }
    }

    private void decodeOrExpectedFailure(byte[] wire) {
        try {
            assertThat(codec.decode(wire)).isNotNull();
        } catch (PacketDecodeException e) {
            assertThat(e.reasonCode()).isIn(ReasonCode.MALFORMED_PACKET, ReasonCode.PROTOCOL_ERROR);
        } catch (UnsupportedProtocolVersionException e) {
            assertThat(e.protocolLevel()).isBetween(0, 255).isNotEqualTo(4);
        }
    }

    private static List<Packet> randomPackets(Random random) {
        int id = 1 + random.nextInt(65535);
        boolean clean = random.nextBoolean();
        Packet.Will will = random.nextBoolean() ? new Packet.Will(
                new Topic(pick(random, TOPICS)), randomBytes(random, random.nextInt(64)),
                Qos.of(random.nextInt(3)), random.nextBoolean(), EMPTY
        ) : null;
        String username = random.nextBoolean()
                ? pick(random, List.of("", "user", "あ\uFEFF")) : null;
        byte[] password = username != null && random.nextBoolean()
                ? randomBytes(random, random.nextInt(64)) : null;
        var connect = new Packet.Connect(
                clean && random.nextBoolean() ? "" : pick(random, TOPICS),
                MqttVersion.V3_1_1, clean, random.nextInt(65536), will, username, password, EMPTY
        );
        ReasonCode connAckReason = pick(random, CONNACK_REASONS);
        Qos qos = Qos.of(random.nextInt(3));
        var subscriptions = new ArrayList<Packet.Subscription>();
        var filters = new ArrayList<TopicFilter>();
        var reasons = new ArrayList<ReasonCode>();
        int count = 1 + random.nextInt(5);
        for (int i = 0; i < count; i++) {
            var filter = new TopicFilter(pick(random, FILTERS));
            filters.add(filter);
            subscriptions.add(new Packet.Subscription(
                    filter, Qos.of(random.nextInt(3)), false, false, 0
            ));
            reasons.add(pick(random, SUBACK_REASONS));
        }
        return List.of(
                connect,
                new Packet.ConnAck(connAckReason == SUCCESS && random.nextBoolean(),
                        connAckReason, EMPTY
                ),
                new Packet.Publish(new Topic(pick(random, TOPICS)),
                        randomBytes(random, random.nextInt(2048)), qos, random.nextBoolean(),
                        qos != Qos.AT_MOST_ONCE && random.nextBoolean(),
                        qos == Qos.AT_MOST_ONCE ? 0 : id, EMPTY
                ),
                new Packet.PubAck(id, SUCCESS, EMPTY),
                new Packet.PubRec(id, SUCCESS, EMPTY),
                new Packet.PubRel(id, SUCCESS, EMPTY),
                new Packet.PubComp(id, SUCCESS, EMPTY),
                new Packet.Subscribe(id, subscriptions, EMPTY),
                new Packet.SubAck(id, reasons, EMPTY),
                new Packet.Unsubscribe(id, filters, EMPTY),
                new Packet.UnsubAck(id, List.of(), EMPTY),
                new Packet.PingReq(),
                new Packet.PingResp(),
                new Packet.Disconnect(SUCCESS, EMPTY)
        );
    }

    private static byte[] randomBytes(Random random, int size) {
        var bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    private static <T> T pick(Random random, List<T> values) {
        return values.get(random.nextInt(values.size()));
    }
}
