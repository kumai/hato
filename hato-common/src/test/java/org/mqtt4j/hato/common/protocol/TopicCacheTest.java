package org.mqtt4j.hato.common.protocol;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.ReasonCode;
import org.mqtt4j.hato.common.value.Topic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;

class TopicCacheTest {

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_cache_decodes_every_publish_as_decoding_without_one_does(MqttVersion version) {
        // Arrange: more names than the cache holds at once, each coming back after others,
        // among names that differ only in their last byte and names in other scripts.
        PacketCodec codec = PacketCodec.of(version);
        var names = new ArrayList<String>();
        for (int i = 0; i < 100; i++) {
            names.add("sensors/" + i);
            names.add("sensors/" + (i % 10));
            names.add(i % 3 == 0 ? "温度/" + i : "a/b/" + (char) ('a' + i % 26));
        }
        List<byte[]> packets = names.stream()
                .map(name -> codec.encode(publish(name)))
                .toList();
        var sut = new TopicCache();
        // Act
        var decoded = new ArrayList<Packet>();
        for (byte[] packet : packets) {
            decoded.add(codec.decode(packet, 0, packet.length, sut));
        }
        // Assert
        assertThat(decoded).containsExactlyElementsOf(packets.stream()
                .map(codec::decode)
                .toList());
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_repeated_name_decodes_to_the_topic_decoded_before(MqttVersion version) {
        // Arrange
        PacketCodec codec = PacketCodec.of(version);
        byte[] packet = codec.encode(publish("sensors/1/temperature"));
        var sut = new TopicCache();
        var first = (Packet.Publish) codec.decode(packet, 0, packet.length, sut);
        // Act
        var second = (Packet.Publish) codec.decode(packet, 0, packet.length, sut);
        // Assert
        assertThat(second.topic()).isSameAs(first.topic());
        assertThat(second).isEqualTo(first);
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_refused_name_is_refused_again_rather_than_remembered(MqttVersion version) {
        // Arrange: the name "a/+", whose wildcard a PUBLISH must not carry. The last byte of the
        // name follows the fixed header and the name's two-byte length.
        PacketCodec codec = PacketCodec.of(version);
        byte[] invalid = codec.encode(publish("a/x"));
        invalid[6] = '+';
        var sut = new TopicCache();
        // Act & Assert
        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatExceptionOfType(PacketDecodeException.class)
                    .isThrownBy(() -> codec.decode(invalid, 0, invalid.length, sut))
                    .satisfies(error -> assertThat(error.reasonCode())
                            .isEqualTo(ReasonCode.PROTOCOL_ERROR));
        }
    }

    @Test
    void a_publish_that_names_its_topic_by_alias_alone_decodes_as_without_a_cache() {
        // Arrange
        PacketCodec codec = PacketCodec.of(MqttVersion.V5);
        byte[] packet = codec.encode(new Packet.AliasPublish(
                new byte[] {1, 2},
                Qos.AT_LEAST_ONCE,
                false,
                false,
                7,
                new Properties(List.of(new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, 3)))
        ));
        var sut = new TopicCache();
        // Act
        Packet decoded = codec.decode(packet, 0, packet.length, sut);
        // Assert
        assertThat(decoded).isEqualTo(codec.decode(packet));
    }

    private static Packet.Publish publish(String topic) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {1, 2},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        );
    }
}
