package org.mqtt4j.hato.common.io;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Optional;

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
import static org.assertj.core.api.Assertions.catchThrowable;

class FrameReaderTest {

    @Test
    void eof_before_a_packet_returns_empty() throws IOException {
        var sut = new FrameReader(InputStream.nullInputStream(), 1024);

        assertThat(sut.read()).isEmpty();
        assertThat(sut.read()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "c000", "4002000a", "c08000", "c0808000", "c080808000", "200900000621001422000a"
    })
    void packets_at_the_size_limit_keep_their_original_header(String hex) throws IOException {
        byte[] packet = HexFormat.of().parseHex(hex);
        var sut = new FrameReader(new ByteArrayInputStream(packet), packet.length);

        assertThat(sut.read().orElseThrow()).isEqualTo(packet);
        assertThat(sut.read()).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({
            "127, 7f", "128, 8001", "16380, fc7f", "16381, fd7f", "16382, fe7f",
            "16384, 808001", "32764, fcff01", "32765, fdff01", "2097152, 80808001"
    })
    void remaining_length_of_each_width_delimits_the_body(int size, String lengthHex)
            throws IOException {
        // Arrange
        byte[] header = HexFormat.of().parseHex("30" + lengthHex);
        byte[] packet = Arrays.copyOf(header, header.length + size);
        Arrays.fill(packet, header.length, packet.length, (byte) 0x5A);
        var sut = new FrameReader(new ByteArrayInputStream(packet), packet.length);

        // Act & Assert
        assertThat(sut.read().orElseThrow()).isEqualTo(packet);
        assertThat(sut.read()).isEmpty();
    }

    @Test
    void concatenated_packets_are_returned_separately() throws IOException {
        var input = new ByteArrayInputStream(HexFormat.of().parseHex("c0004002000ad000"));
        var sut = new FrameReader(input, 1024);

        assertThat(sut.read().orElseThrow()).isEqualTo(HexFormat.of().parseHex("c000"));
        assertThat(sut.read().orElseThrow()).isEqualTo(HexFormat.of().parseHex("4002000a"));
        assertThat(sut.read().orElseThrow()).isEqualTo(HexFormat.of().parseHex("d000"));
        assertThat(sut.read()).isEmpty();
    }

    @Test
    void a_growing_frame_does_not_consume_the_next_packet() throws IOException {
        byte[] header = HexFormat.of().parseHex("30808004");
        byte[] packet = Arrays.copyOf(header, header.length + 65536);
        Arrays.fill(packet, header.length, packet.length, (byte) 0x5A);
        var input = new SequenceInputStream(new ByteArrayInputStream(packet),
                new ByteArrayInputStream(HexFormat.of().parseHex("c08000")));
        var sut = new FrameReader(input, packet.length);

        assertThat(sut.read().orElseThrow()).isEqualTo(packet);
        assertThat(sut.read().orElseThrow()).isEqualTo(HexFormat.of().parseHex("c08000"));
        assertThat(sut.read()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 16383})
    void an_incomplete_frame_allocates_for_received_bytes_not_its_declared_length(int received) {
        // Arrange: the maximum Remaining Length, followed by only part of the body.
        var input = new SequenceInputStream(
                new ByteArrayInputStream(HexFormat.of().parseHex("30ffffff7f")),
                new ByteArrayInputStream(new byte[received])
        );
        var sut = new FrameReader(input, 268_435_460);

        // Act
        long before = allocatedBytes();
        Throwable failure = catchThrowable(sut::read);
        long allocated = allocatedBytes() - before;

        // Assert: the declared 256 MiB is not allocated up front. The bound leaves room for
        // one-time class loading, which this thread's measurement also counts.
        assertThat(before).as("per-thread allocation measurement").isNotNegative();
        assertThat(failure).isInstanceOf(EOFException.class);
        assertThat(allocated).isLessThan(16L * 1024 * 1024);
    }

    @Test
    void one_byte_reads_are_assembled_without_using_available_or_closing_the_input()
            throws IOException {
        // Arrange: force short body reads regardless of the requested length.
        byte[] packet = HexFormat.of().parseHex("3289000003612f62000a6869");
        var input = new FilterInputStream(new ByteArrayInputStream(packet)) {
            @Override
            public int read(byte[] bytes, int offset, int length) throws IOException {
                return super.read(bytes, offset, Math.min(length, 1));
            }

            @Override
            public int available() {
                return 0;
            }

            @Override
            public void close() {
                throw new AssertionError("The reader must not close its borrowed input");
            }
        };
        var sut = new FrameReader(input, packet.length);

        // Act
        Optional<byte[]> result = sut.read();

        // Assert
        assertThat(result.orElseThrow()).isEqualTo(packet);
        assertThat(sut.read()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"30", "3080", "308080", "30808080", "3002", "3002ab"})
    void eof_partway_through_a_packet_is_an_io_failure(String hex) {
        var sut = new FrameReader(new ByteArrayInputStream(HexFormat.of().parseHex(hex)), 1024);

        assertThatThrownBy(sut::read).isInstanceOf(EOFException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"3080808080", "308080808000"})
    void a_length_continuing_past_four_bytes_is_rejected_without_reading_a_fifth(String hex) {
        byte[] bytes = HexFormat.of().parseHex(hex);
        var input = new ByteArrayInputStream(bytes);
        var sut = new FrameReader(input, 1024);

        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(sut::read)
                .satisfies(error -> assertThat(error.reasonCode())
                        .isSameAs(ReasonCode.MALFORMED_PACKET));
        assertThat(input.available()).isEqualTo(bytes.length - 5);
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
    void oversized_packets_are_rejected_after_the_header_without_consuming_body_bytes(
            String headerHex, int maxSize, int packetSize
    ) {
        // Arrange: the sentinel must remain unread even for the largest declared length.
        var input = new ByteArrayInputStream(HexFormat.of().parseHex(headerHex + "ab"));
        var sut = new FrameReader(input, maxSize);

        // Act & Assert
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(sut::read)
                .withMessage("MQTT packet size " + packetSize + " exceeds maximum " + maxSize)
                .satisfies(error -> assertThat(error.reasonCode())
                        .isSameAs(ReasonCode.PACKET_TOO_LARGE));
        assertThat(input.read()).isEqualTo(0xAB);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "30", "3080", "3002ab"})
    void input_failures_propagate_from_each_part_of_a_packet(String prefixHex) {
        var failure = new IOException("Read failed");
        var failingInput = new InputStream() {
            @Override
            public int read() throws IOException {
                throw failure;
            }
        };
        var input = new SequenceInputStream(
                new ByteArrayInputStream(HexFormat.of().parseHex(prefixHex)), failingInput
        );
        var sut = new FrameReader(input, 1024);

        assertThatThrownBy(sut::read).isSameAs(failure);
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -1, 0})
    void the_size_limit_must_be_positive(int maxSize) {
        var sut = new FrameReader(InputStream.nullInputStream(), 1024);

        assertThatThrownBy(() -> new FrameReader(InputStream.nullInputStream(), maxSize))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.limit(maxSize))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void a_new_size_limit_applies_to_the_packets_read_after_it() throws IOException {
        // Arrange: two packets of four bytes, the first read under the original limit.
        var input = new ByteArrayInputStream(HexFormat.of().parseHex("3002abcd3002abcd"));
        var sut = new FrameReader(input, 1024);
        assertThat(sut.read()).hasValueSatisfying(packet -> assertThat(packet).hasSize(4));

        // Act
        sut.limit(3);

        // Assert
        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(sut::read)
                .withMessage("MQTT packet size 4 exceeds maximum 3");
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the constructor's non-null contract.
    void an_input_stream_is_required() {
        assertThatThrownBy(() -> new FrameReader(null, 1024))
                .isInstanceOf(NullPointerException.class);
    }

    // Heap bytes this thread has allocated so far, or -1 where the JVM cannot measure them.
    private static long allocatedBytes() {
        return ((ThreadMXBean) ManagementFactory.getThreadMXBean())
                .getCurrentThreadAllocatedBytes();
    }
}
