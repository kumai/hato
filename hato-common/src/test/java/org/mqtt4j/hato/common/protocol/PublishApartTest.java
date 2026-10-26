package org.mqtt4j.hato.common.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.ReasonCode;
import org.mqtt4j.hato.common.value.Topic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;

class PublishApartTest {

    record Case(MqttVersion version, Packet packet) {

        @Override
        public String toString() {
            return version + " " + packet;
        }
    }

    static Stream<Case> publications() {
        var userProperties = new Properties(List.of(
                new Property.UserProperty("trace", "a".repeat(300)),
                new Property.FourByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, 60),
                new Property.StringValue(PropertyId.CONTENT_TYPE, "text/plain")
        ));
        var cases = new ArrayList<Case>();
        for (MqttVersion version : MqttVersion.values()) {
            for (Qos qos : Qos.values()) {
                int packetId = qos == Qos.AT_MOST_ONCE ? 0 : 513;
                cases.add(new Case(version, publish("a", new byte[0], qos, packetId, EMPTY)));
                cases.add(new Case(version, publish(
                        "sensors/" + "x".repeat(300),
                        new byte[200],
                        qos,
                        packetId,
                        EMPTY
                )));
                if (version == MqttVersion.V5) {
                    cases.add(new Case(version, publish(
                            "温度",
                            new byte[] {1, 2, 3},
                            qos,
                            packetId,
                            userProperties
                    )));
                    cases.add(new Case(version, new Packet.AliasPublish(
                            new byte[] {4},
                            qos,
                            true,
                            false,
                            packetId,
                            new Properties(List.of(
                                    new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, 2)
                            ))
                    )));
                }
            }
        }
        return cases.stream();
    }

    @ParameterizedTest
    @MethodSource("publications")
    void a_publish_decodes_from_its_headers_and_its_payload_apart_as_it_does_whole(Case c) {
        // Arrange
        PacketCodec sut = PacketCodec.of(c.version());
        byte[] whole = sut.encode(c.packet());
        int headers = sut.publishHeadersSize(whole, 0, whole.length);
        // Act
        Packet decoded = sut.decodePublish(
                whole,
                0,
                headers,
                Arrays.copyOfRange(whole, headers, whole.length),
                new TopicCache()
        );
        // Assert
        assertThat(decoded).isEqualTo(sut.decode(whole));
    }

    @ParameterizedTest
    @MethodSource("publications")
    void the_headers_are_not_located_before_all_of_them_have_arrived(Case c) {
        // Arrange
        PacketCodec sut = PacketCodec.of(c.version());
        byte[] whole = sut.encode(c.packet());
        int headers = sut.publishHeadersSize(whole, 0, whole.length);
        // Act
        var sizes = new ArrayList<Integer>();
        for (int available = 0; available < headers; available++) {
            sizes.add(sut.publishHeadersSize(whole, 0, available));
        }
        // Assert
        assertThat(headers).isPositive();
        assertThat(sizes).containsOnly(-1);
    }

    @ParameterizedTest
    @MethodSource("publications")
    void a_payload_apart_that_its_headers_do_not_count_is_malformed(Case c) {
        // Arrange
        PacketCodec sut = PacketCodec.of(c.version());
        byte[] whole = sut.encode(c.packet());
        int headers = sut.publishHeadersSize(whole, 0, whole.length);
        var longer = new byte[whole.length - headers + 1];
        // Act & Assert
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> sut.decodePublish(whole, 0, headers, longer, new TopicCache()))
                .satisfies(error -> assertThat(error.reasonCode())
                        .isEqualTo(ReasonCode.MALFORMED_PACKET));
    }

    private static Packet.Publish publish(
            String topic,
            byte[] payload,
            Qos qos,
            int packetId,
            Properties properties
    ) {
        return new Packet.Publish(
                new Topic(topic),
                payload,
                qos,
                false,
                false,
                packetId,
                properties
        );
    }
}
