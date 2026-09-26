package org.mqtt4j.mqtj.client.internal;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketDecodeException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IncomingFramesTest {

    @Test
    void framing_accepts_split_headers_and_coalesced_packets() {
        // Arrange
        var packets = new ArrayList<Packet>();
        var sut = new IncomingFrames(
                16,
                PacketCodec.of(MqttVersion.V3_1_1),
                (packet, _) -> packets.add(packet)
        );

        // Act & Assert: the first packet spans chunks; later chunks combine packets.
        sut.accept(new byte[] {0x20});
        sut.accept(new byte[] {2, 0});
        assertThat(packets).isEmpty();
        sut.accept(new byte[] {0, (byte) 0xd0, 0, (byte) 0xd0});
        sut.accept(new byte[] {0});
        assertThat(packets).hasSize(3);
        assertThat(packets.getFirst()).isInstanceOf(Packet.ConnAck.class);
        assertThat(packets.subList(1, 3))
                .containsExactly(new Packet.PingResp(), new Packet.PingResp());
    }

    @Test
    void oversized_and_malformed_lengths_fail_before_the_body_arrives() {
        var sut = new IncomingFrames(16, PacketCodec.of(MqttVersion.V3_1_1), (_, _) -> { });
        assertThatThrownBy(() -> sut.accept(new byte[] {0x30, 127}))
                .isInstanceOf(PacketDecodeException.class);
        var malformed = new IncomingFrames(16, PacketCodec.of(MqttVersion.V3_1_1), (_, _) -> { });
        assertThatThrownBy(() -> malformed.accept(new byte[] {
                0x30, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff
        }))
                .isInstanceOf(PacketDecodeException.class);
    }
}
