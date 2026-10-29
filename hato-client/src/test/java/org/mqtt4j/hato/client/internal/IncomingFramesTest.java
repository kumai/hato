package org.mqtt4j.hato.client.internal;

import java.io.ByteArrayOutputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;

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

    @ParameterizedTest
    @ValueSource(ints = {1, 127, 16384, 100000})
    void packets_split_across_chunks_keep_payloads_sizes_and_the_next_packet(int chunkSize) {
        var codec = PacketCodec.of(MqttVersion.V3_1_1);
        var payload = new byte[65537];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }
        var publish = new Packet.Publish(new Topic("large"), payload, Qos.AT_MOST_ONCE,
                false, false, 0, Properties.EMPTY);
        byte[] packet = codec.encode(publish);
        byte[] followed = Arrays.copyOf(packet, packet.length + 2);
        followed[packet.length] = (byte) 0xd0;
        var packets = new ArrayList<Packet>();
        var sizes = new ArrayList<Integer>();
        var sut = new IncomingFrames(packet.length, codec, (received, size) -> {
            packets.add(received);
            sizes.add(size);
        });

        for (int offset = 0; offset < followed.length; offset += chunkSize) {
            sut.accept(Arrays.copyOfRange(followed, offset,
                    Math.min(followed.length, offset + chunkSize)));
        }

        assertThat(packets).containsExactly(publish, new Packet.PingResp());
        assertThat(sizes).containsExactly(packet.length, 2);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 127, 8192, 100000})
    void lent_chunks_decode_as_handed_over_ones_although_the_buffer_is_reused(int chunkSize) {
        // Arrange: small publications around one of 64 KiB, which gets storage of its size at
        // once, and one of 200 KiB, which keeps the parts that arrive, with PINGRESPs between.
        var codec = PacketCodec.of(MqttVersion.V3_1_1);
        List<Packet> sent = List.of(
                publish("a", 16, Qos.AT_MOST_ONCE, 0),
                publish("large", 65536, Qos.AT_LEAST_ONCE, 7),
                new Packet.PingResp(),
                publish("larger", 200 * 1024, Qos.AT_MOST_ONCE, 0),
                publish("a", 16, Qos.AT_LEAST_ONCE, 8),
                new Packet.PingResp()
        );
        var stream = new ByteArrayOutputStream();
        for (Packet packet : sent) {
            stream.writeBytes(codec.encode(packet));
        }
        byte[] bytes = stream.toByteArray();
        var received = new ArrayList<Packet>();
        var sut = new IncomingFrames(bytes.length, codec, (packet, _) -> received.add(packet));
        var buffer = new byte[chunkSize + 5];

        // Act: each chunk lies in the same buffer, after five other bytes, and the buffer is
        // overwritten once the call returns.
        for (int offset = 0; offset < bytes.length; offset += chunkSize) {
            int length = Math.min(chunkSize, bytes.length - offset);
            System.arraycopy(bytes, offset, buffer, 5, length);
            sut.acceptLent(buffer, 5, length);
            Arrays.fill(buffer, (byte) 0x55);
        }

        // Assert
        assertThat(received).containsExactlyElementsOf(sent);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 16383})
    void an_incomplete_frame_allocates_for_received_bytes_and_can_be_cleared(int received) {
        // Arrange
        var packets = new ArrayList<Packet>();
        var sut = new IncomingFrames(268_435_460, PacketCodec.of(MqttVersion.V3_1_1),
                (packet, _) -> packets.add(packet));
        byte[] header = HexFormat.of().parseHex("30ffffff7f");
        byte[] body = new byte[received];

        // Act: the maximum Remaining Length arrives byte by byte, then part of the body.
        long before = allocatedBytes();
        for (byte b : header) {
            sut.accept(new byte[] {b});
        }
        sut.accept(body);
        long allocated = allocatedBytes() - before;

        // Assert: the declared 256 MiB is not allocated up front. The bound leaves room for
        // one-time class loading, which this thread's measurement also counts.
        assertThat(before).as("per-thread allocation measurement").isNotNegative();
        assertThat(allocated).isLessThan(16L * 1024 * 1024);
        assertThat(packets).isEmpty();
        sut.clear();
        sut.accept(HexFormat.of().parseHex("d000"));
        assertThat(packets).containsExactly(new Packet.PingResp());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 16383})
    void a_publication_whose_headers_come_first_allocates_for_received_bytes(int received) {
        // Arrange
        var packets = new ArrayList<Packet>();
        var sut = new IncomingFrames(
                268_435_460,
                PacketCodec.of(MqttVersion.V3_1_1),
                (packet, _) -> packets.add(packet)
        );
        // A QoS 0 PUBLISH of the maximum Remaining Length to the topic "a", whose headers
        // locate its payload, followed by part of the payload.
        byte[] start = HexFormat.of().parseHex("30ffffff7f000161");
        byte[] chunk = Arrays.copyOf(start, start.length + received);

        // Act
        long before = allocatedBytes();
        sut.accept(chunk);
        long allocated = allocatedBytes() - before;

        // Assert: neither the declared packet nor its payload is allocated up front.
        assertThat(before).as("per-thread allocation measurement").isNotNegative();
        assertThat(allocated).isLessThan(16L * 1024 * 1024);
        assertThat(packets).isEmpty();
        sut.clear();
        sut.accept(HexFormat.of().parseHex("d000"));
        assertThat(packets).containsExactly(new Packet.PingResp());
    }

    @ParameterizedTest
    @ValueSource(strings = {"d08000", "d0808000", "d080808000"})
    void zero_length_frames_keep_nonminimal_headers_across_chunks(String hex) {
        var packets = new ArrayList<Packet>();
        var sizes = new ArrayList<Integer>();
        byte[] bytes = HexFormat.of().parseHex(hex);
        var sut = new IncomingFrames(bytes.length, PacketCodec.of(MqttVersion.V3_1_1),
                (packet, size) -> {
                    packets.add(packet);
                    sizes.add(size);
                });

        for (byte b : bytes) {
            sut.accept(new byte[] {b});
        }

        assertThat(packets).containsExactly(new Packet.PingResp());
        assertThat(sizes).containsExactly(bytes.length);
    }

    private static Packet.Publish publish(String topic, int size, Qos qos, int packetId) {
        var payload = new byte[size];
        for (int i = 0; i < size; i++) {
            payload[i] = (byte) (i % 251);
        }
        return new Packet.Publish(
                new Topic(topic),
                payload,
                qos,
                false,
                false,
                packetId,
                Properties.EMPTY
        );
    }

    // Heap bytes this thread has allocated so far, or -1 where the JVM cannot measure them.
    private static long allocatedBytes() {
        return ((ThreadMXBean) ManagementFactory.getThreadMXBean())
                .getCurrentThreadAllocatedBytes();
    }
}
