package org.mqtt4j.hato.common.io;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.common.value.ReasonCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FrameAssemblerTest {

    @Test
    void a_packet_comes_out_whole_wherever_its_bytes_are_split() {
        // Arrange: a fixed header of three bytes and a body of 200.
        byte[] packet = packet("30c801", 200);
        for (int split = 0; split < packet.length; split++) {
            var sut = new FrameAssembler(packet.length);

            // Act
            byte[] early = sut.next(ByteBuffer.wrap(packet, 0, split));
            byte[] result = sut.next(ByteBuffer.wrap(packet, split, packet.length - split));

            // Assert
            assertThat(early).as("after the first %d bytes", split).isNull();
            assertThat(result).as("split after %d bytes", split).isEqualTo(packet);
        }
    }

    @Test
    void bytes_that_arrive_one_at_a_time_make_the_same_packets() {
        // Arrange: a packet without a body, one with a body, and one with a longer header.
        byte[] first = HexFormat.of().parseHex("c000");
        byte[] second = packet("3005", 5);
        byte[] third = packet("30c801", 200);
        var stream = ByteBuffer.allocate(first.length + second.length + third.length);
        stream.put(first).put(second).put(third).flip();
        var sut = new FrameAssembler(third.length);

        // Act
        var packets = new ArrayList<byte[]>();
        while (stream.hasRemaining()) {
            byte[] packet = sut.next(stream.slice(stream.position(), 1));
            stream.position(stream.position() + 1);
            if (packet != null) {
                packets.add(packet);
            }
        }

        // Assert
        assertThat(packets).containsExactly(first, second, third);
    }

    @Test
    void packets_that_arrive_together_come_out_one_for_each_call() {
        // Arrange
        byte[] first = HexFormat.of().parseHex("c000");
        byte[] second = packet("3005", 5);
        var input = ByteBuffer.allocate(first.length + second.length);
        input.put(first).put(second).flip();
        var sut = new FrameAssembler(1024);

        // Act & Assert: a call takes no byte beyond the packet that it returns.
        assertThat(sut.next(input)).isEqualTo(first);
        assertThat(input.remaining()).isEqualTo(second.length);
        assertThat(sut.next(input)).isEqualTo(second);
        assertThat(sut.next(input)).isNull();
    }

    @ParameterizedTest
    @ValueSource(ints = {8192, 1, 5, 8200})
    void a_packet_that_arrives_in_many_parts_fills_an_array_of_its_size(int first) {
        // Arrange: larger than what a packet gets as its fixed header completes, in parts of
        // 8 KiB after a first part that need not line up with them.
        byte[] packet = packet("30a08d06", 100_000);
        var input = ByteBuffer.wrap(packet);
        var sut = new FrameAssembler(packet.length);

        // Act
        byte[] result = null;
        int part = first;
        while (result == null) {
            part = Math.min(part, input.remaining());
            result = sut.next(input.slice(input.position(), part));
            input.position(input.position() + part);
            part = 8192;
        }

        // Assert
        assertThat(result).isEqualTo(packet);
        assertThat(input.hasRemaining()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"c08000", "3081800061", "308180800061"})
    void a_remaining_length_longer_than_it_needs_to_be_is_kept_as_received(String hex) {
        byte[] packet = HexFormat.of().parseHex(hex);
        var sut = new FrameAssembler(1024);

        assertThat(sut.next(ByteBuffer.wrap(packet))).isEqualTo(packet);
    }

    @ParameterizedTest
    @ValueSource(strings = {"3080808080", "308080808000"})
    void a_length_that_continues_past_four_bytes_is_malformed(String hex) {
        var input = ByteBuffer.wrap(HexFormat.of().parseHex(hex));
        var sut = new FrameAssembler(1024);

        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> sut.next(input))
                .satisfies(error -> assertThat(error.reasonCode())
                        .isSameAs(ReasonCode.MALFORMED_PACKET));
        assertThat(input.position()).as("bytes taken").isEqualTo(5);
    }

    @ParameterizedTest
    @CsvSource({
            "c000, 1, 2",
            "2009, 10, 11",
            "3002, 3, 4",
            "308100, 3, 4",
            "308000, 2, 3",
            "30ffffff7f, 1024, 268435460"
    })
    void a_packet_beyond_the_size_limit_is_rejected_before_its_body_is_taken(
            String headerHex, int maxSize, int packetSize
    ) {
        // Arrange: one byte of the body follows the fixed header.
        var input = ByteBuffer.wrap(HexFormat.of().parseHex(headerHex + "ab"));
        var sut = new FrameAssembler(maxSize);

        // Act & Assert
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> sut.next(input))
                .withMessage("MQTT packet size " + packetSize + " exceeds maximum " + maxSize)
                .satisfies(error -> assertThat(error.reasonCode())
                        .isSameAs(ReasonCode.PACKET_TOO_LARGE));
        assertThat(input.remaining()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 16383, 100_000})
    void storage_follows_the_bytes_received_not_the_length_declared(int received) {
        // Arrange: the largest Remaining Length, followed by only part of the body.
        var input = ByteBuffer.allocate(5 + received);
        input.put(HexFormat.of().parseHex("30ffffff7f")).position(0);
        var sut = new FrameAssembler(268_435_460);

        // Act
        long before = allocatedBytes();
        byte[] result = sut.next(input);
        long allocated = allocatedBytes() - before;

        // Assert: the declared 256 MiB is not allocated up front. The bound leaves room for
        // one-time class loading, which this thread's measurement also counts.
        assertThat(before).as("per-thread allocation measurement").isNotNegative();
        assertThat(result).isNull();
        assertThat(sut.missing()).isEqualTo(268_435_455 - received);
        assertThat(allocated).isLessThan(16L * 1024 * 1024);
    }

    @Test
    void missing_counts_what_a_packet_lacks_once_its_fixed_header_is_complete() {
        var sut = new FrameAssembler(1024);

        // At a packet boundary, and partway through a fixed header.
        assertThat(sut.missing()).isZero();
        assertThat(sut.next(ByteBuffer.wrap(HexFormat.of().parseHex("30")))).isNull();
        assertThat(sut.missing()).isZero();

        // The fixed header completes with a Remaining Length of 5, and one byte of the body.
        assertThat(sut.next(ByteBuffer.wrap(HexFormat.of().parseHex("0561")))).isNull();
        assertThat(sut.missing()).isEqualTo(4);
    }

    @Test
    void a_new_size_limit_applies_to_the_packets_that_start_after_it() {
        // Arrange
        byte[] packet = HexFormat.of().parseHex("3002abcd");
        var sut = new FrameAssembler(packet.length);
        assertThat(sut.next(ByteBuffer.wrap(packet))).isEqualTo(packet);

        // Act
        sut.limit(packet.length - 1);

        // Assert
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> sut.next(ByteBuffer.wrap(packet)))
                .satisfies(error -> assertThat(error.reasonCode())
                        .isSameAs(ReasonCode.PACKET_TOO_LARGE));
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -1, 0})
    void the_size_limit_must_be_positive(int maxSize) {
        var sut = new FrameAssembler(1024);

        assertThatThrownBy(() -> new FrameAssembler(maxSize))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Maximum packet size must be positive");
        assertThatThrownBy(() -> sut.limit(maxSize))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Maximum packet size must be positive");
    }

    // A packet with the fixed header of `headerHex` and a body of `bodySize` bytes that differ
    // from their neighbours, so that a byte in the wrong place shows.
    private static byte[] packet(String headerHex, int bodySize) {
        byte[] header = HexFormat.of().parseHex(headerHex);
        var packet = new byte[header.length + bodySize];
        System.arraycopy(header, 0, packet, 0, header.length);
        for (int index = 0; index < bodySize; index++) {
            packet[header.length + index] = (byte) (index * 31 + 7);
        }
        return packet;
    }

    private static long allocatedBytes() {
        return ((ThreadMXBean) ManagementFactory.getThreadMXBean())
                .getCurrentThreadAllocatedBytes();
    }
}
