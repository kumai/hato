package org.mqtt4j.hato.common.protocol;

import java.util.*;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.common.value.*;

import static org.assertj.core.api.Assertions.*;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.protocol.PropertyId.*;
import static org.mqtt4j.hato.common.value.ReasonCode.*;

class Mqtt5RoundTripTest {

    private final PacketCodec sut = PacketCodec.of(MqttVersion.V5);

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void every_packet_round_trips_with_generated_property_combinations(int seed) {
        var random = new Random(seed);
        for (Packet packet : packets(random)) {
            byte[] encoded = sut.encode(packet);

            assertThat(sut.decode(encoded)).as("seed %s: %s", seed, packet).isEqualTo(packet);
        }
    }

    static IntStream seeds() {
        return IntStream.range(0, 200);
    }

    @Test
    void mutations_and_arbitrary_bytes_only_produce_declared_decode_failures() {
        var random = new Random(0x4D51545405L);
        for (Packet packet : packets(random)) {
            byte[] encoded = sut.encode(packet);
            for (int offset = 0; offset < encoded.length; offset++) {
                byte[] mutated = encoded.clone();
                mutated[offset] ^= (byte) (1 + random.nextInt(255));
                decodeOrExpectedFailure(mutated);
            }
        }
        for (int i = 0; i < 10000; i++) {
            byte[] wire = bytes(random, random.nextInt(128));
            if ((i & 1) == 0 && wire.length >= 2) {
                wire[1] = (byte) (wire.length - 2);
            }
            decodeOrExpectedFailure(wire);
        }
    }

    @Test
    void every_truncated_prefix_of_a_generated_packet_is_rejected() {
        for (Packet packet : packets(new Random(5))) {
            byte[] encoded = sut.encode(packet);
            for (int length = 0; length < encoded.length; length++) {
                byte[] prefix = Arrays.copyOf(encoded, length);
                assertThatThrownBy(() -> sut.decode(prefix))
                        .as("Truncated %s at %s", packet, length)
                        .isInstanceOf(PacketDecodeException.class);
            }
        }
    }

    private void decodeOrExpectedFailure(byte[] wire) {
        try {
            assertThat(sut.decode(wire)).isNotNull();
        } catch (PacketDecodeException e) {
            assertThat(e.reasonCode()).isIn(MALFORMED_PACKET, PROTOCOL_ERROR, TOPIC_ALIAS_INVALID);
        } catch (UnsupportedProtocolVersionException e) {
            assertThat(e.protocolLevel()).isBetween(0, 255).isNotEqualTo(5);
        }
    }

    private static List<Packet> packets(Random random) {
        int id = 1 + random.nextInt(65535);
        Qos qos = Qos.of(random.nextInt(3));
        int publishId = qos == Qos.AT_MOST_ONCE ? 0 : id;
        boolean dup = qos != Qos.AT_MOST_ONCE && random.nextBoolean();
        byte[] payload = bytes(random, random.nextInt(512));
        Properties connectProperties = properties(
                random,
                new Property.FourByteValue(SESSION_EXPIRY_INTERVAL, unsignedInt(random)),
                new Property.TwoByteValue(RECEIVE_MAXIMUM, id),
                new Property.FourByteValue(MAXIMUM_PACKET_SIZE, 1L + random.nextInt(65536)),
                new Property.TwoByteValue(TOPIC_ALIAS_MAXIMUM, random.nextInt(65536)),
                new Property.ByteValue(REQUEST_RESPONSE_INFORMATION, random.nextInt(2)),
                new Property.ByteValue(REQUEST_PROBLEM_INFORMATION, random.nextInt(2))
        );
        Properties willProperties = properties(
                random,
                new Property.FourByteValue(WILL_DELAY_INTERVAL, unsignedInt(random)),
                new Property.FourByteValue(MESSAGE_EXPIRY_INTERVAL, unsignedInt(random)),
                new Property.ByteValue(PAYLOAD_FORMAT_INDICATOR, random.nextInt(2)),
                new Property.StringValue(CONTENT_TYPE, "application/octet-stream"),
                new Property.StringValue(RESPONSE_TOPIC, "応答/𪛔"),
                new Property.BinaryValue(CORRELATION_DATA, bytes(random, random.nextInt(32)))
        );
        Properties connAckProperties = properties(
                random,
                new Property.TwoByteValue(RECEIVE_MAXIMUM, id),
                new Property.FourByteValue(MAXIMUM_PACKET_SIZE, 0xFFFF_FFFFL),
                new Property.ByteValue(MAXIMUM_QOS, random.nextInt(2)),
                new Property.ByteValue(RETAIN_AVAILABLE, random.nextInt(2)),
                new Property.ByteValue(WILDCARD_SUBSCRIPTION_AVAILABLE, random.nextInt(2)),
                new Property.ByteValue(SHARED_SUBSCRIPTION_AVAILABLE, random.nextInt(2)),
                new Property.ByteValue(SUBSCRIPTION_IDENTIFIER_AVAILABLE, random.nextInt(2)),
                new Property.StringValue(ASSIGNED_CLIENT_IDENTIFIER, "assigned-あ"),
                new Property.StringValue(RESPONSE_INFORMATION, "reply/"),
                new Property.StringValue(SERVER_REFERENCE, "mqtt.example:1883"),
                new Property.TwoByteValue(SERVER_KEEP_ALIVE, random.nextInt(65536))
        );
        Properties publishProperties = properties(
                random,
                new Property.FourByteValue(MESSAGE_EXPIRY_INTERVAL, unsignedInt(random)),
                new Property.ByteValue(PAYLOAD_FORMAT_INDICATOR, random.nextInt(2)),
                new Property.StringValue(CONTENT_TYPE, ""),
                new Property.StringValue(RESPONSE_TOPIC, "reply/a"),
                new Property.BinaryValue(CORRELATION_DATA, bytes(random, random.nextInt(32))),
                new Property.VariableByteValue(SUBSCRIPTION_IDENTIFIER, id),
                new Property.VariableByteValue(SUBSCRIPTION_IDENTIFIER, id)
        );
        var aliasEntries = new ArrayList<>(publishProperties.entries());
        aliasEntries.add(new Property.TwoByteValue(TOPIC_ALIAS, id));
        var aliasProperties = new Properties(aliasEntries);
        Properties acknowledgement = properties(
                random,
                new Property.StringValue(REASON_STRING, "reason/あ\uFEFF")
        );
        Properties authOptionalProperties = properties(
                random,
                new Property.BinaryValue(AUTHENTICATION_DATA, bytes(random, random.nextInt(32))),
                new Property.StringValue(REASON_STRING, "continue")
        );
        var authEntries = new ArrayList<>(authOptionalProperties.entries());
        authEntries.add(new Property.StringValue(AUTHENTICATION_METHOD, "method"));
        var authProperties = new Properties(authEntries);
        var will = new Packet.Will(
                new Topic("will/a"),
                bytes(random, random.nextInt(64)),
                Qos.of(random.nextInt(3)),
                random.nextBoolean(),
                willProperties
        );
        var subscriptions = new ArrayList<Packet.Subscription>();
        int subscriptionCount = 1 + random.nextInt(5);
        for (int i = 0; i < subscriptionCount; i++) {
            boolean shared = random.nextBoolean();
            subscriptions.add(new Packet.Subscription(
                    new TopicFilter(shared ? "$share/g/あ/+" : "あ/+/#"),
                    Qos.of(random.nextInt(3)),
                    // No Local is a protocol error on a shared subscription.
                    !shared && random.nextBoolean(),
                    random.nextBoolean(),
                    random.nextInt(3)
            ));
        }
        return List.of(
                new Packet.Connect(
                        random.nextBoolean() ? "client/𪛔" : "",
                        MqttVersion.V5,
                        random.nextBoolean(),
                        random.nextInt(65536),
                        random.nextBoolean() ? will : null,
                        random.nextBoolean() ? "user" : null,
                        random.nextBoolean() ? bytes(random, random.nextInt(64)) : null,
                        connectProperties
                ),
                new Packet.ConnAck(random.nextBoolean(), SUCCESS, connAckProperties),
                new Packet.Publish(
                        new Topic("topic/あ\uFEFF"),
                        payload,
                        qos,
                        random.nextBoolean(),
                        dup,
                        publishId,
                        publishProperties
                ),
                new Packet.AliasPublish(
                        payload,
                        qos,
                        random.nextBoolean(),
                        dup,
                        publishId,
                        aliasProperties
                ),
                new Packet.PubAck(id, NO_MATCHING_SUBSCRIBERS, acknowledgement),
                new Packet.PubRec(id, QUOTA_EXCEEDED, acknowledgement),
                new Packet.PubRel(id, PACKET_IDENTIFIER_NOT_FOUND, acknowledgement),
                new Packet.PubComp(id, SUCCESS, acknowledgement),
                new Packet.Subscribe(
                        id,
                        subscriptions,
                        properties(
                                random,
                                new Property.VariableByteValue(SUBSCRIPTION_IDENTIFIER, id)
                        )
                ),
                new Packet.SubAck(id, List.of(SUCCESS, GRANTED_QOS_2), acknowledgement),
                new Packet.Unsubscribe(
                        id,
                        subscriptions.stream().map(Packet.Subscription::filter).toList(),
                        properties(random)
                ),
                new Packet.UnsubAck(id, List.of(SUCCESS, NO_SUBSCRIPTION_EXISTED), acknowledgement),
                new Packet.PingReq(),
                new Packet.PingResp(),
                new Packet.Disconnect(SERVER_BUSY, acknowledgement),
                new Packet.Auth(CONTINUE_AUTHENTICATION, authProperties)
        );
    }

    private static Properties properties(Random random, Property... candidates) {
        var selected = new ArrayList<Property>();
        for (Property candidate : candidates) {
            if (random.nextBoolean()) {
                selected.add(candidate);
            }
        }
        if (random.nextBoolean()) {
            selected.add(new Property.UserProperty("key", "first"));
            selected.add(new Property.UserProperty("key", "second/あ"));
        }
        Collections.shuffle(selected, random);
        return selected.isEmpty() ? EMPTY : new Properties(selected);
    }

    private static long unsignedInt(Random random) {
        return Integer.toUnsignedLong(random.nextInt());
    }

    private static byte[] bytes(Random random, int count) {
        var bytes = new byte[count];
        random.nextBytes(bytes);
        return bytes;
    }
}
