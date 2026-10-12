package org.mqtt4j.hato.common.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FrameWriterTest {

    @Test
    void each_packet_is_written_in_one_call_then_flushed_without_closing_the_output()
            throws IOException {
        // Arrange
        var bytes = new ByteArrayOutputStream();
        var calls = new ArrayList<String>();
        var output = new OutputStream() {
            @Override
            public void write(int value) {
                throw new AssertionError("Use one bulk write for each packet");
            }

            @Override
            public void write(byte[] packet, int offset, int length) {
                calls.add("write " + length);
                bytes.write(packet, offset, length);
            }

            @Override
            public void flush() {
                calls.add("flush");
            }

            @Override
            public void close() {
                throw new AssertionError("The writer must not close its borrowed output");
            }
        };
        var sut = new FrameWriter(output);

        // Act
        sut.write(HexFormat.of().parseHex("c000"));
        sut.write(HexFormat.of().parseHex("4002000a"));

        // Assert
        assertThat(bytes.toByteArray()).isEqualTo(HexFormat.of().parseHex("c0004002000a"));
        assertThat(calls).containsExactly("write 2", "flush", "write 4", "flush");
    }

    @Test
    void a_write_failure_is_propagated_without_flushing_or_retrying() {
        var failure = new IOException("Write failed");
        var calls = new ArrayList<String>();
        var output = new OutputStream() {
            @Override
            public void write(int value) throws IOException {
                calls.add("write");
                throw failure;
            }

            @Override
            public void flush() {
                calls.add("flush");
            }
        };
        var sut = new FrameWriter(output);

        assertThatThrownBy(() -> sut.write(HexFormat.of().parseHex("c000"))).isSameAs(failure);
        assertThat(calls).containsExactly("write");
    }

    @Test
    void a_flush_failure_is_propagated_after_writing_the_packet() {
        var failure = new IOException("Flush failed");
        var output = new ByteArrayOutputStream() {
            @Override
            public void flush() throws IOException {
                throw failure;
            }
        };
        var sut = new FrameWriter(output);

        assertThatThrownBy(() -> sut.write(HexFormat.of().parseHex("c000"))).isSameAs(failure);
        assertThat(output.toByteArray()).isEqualTo(HexFormat.of().parseHex("c000"));
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates non-null contracts.
    void the_output_and_packet_are_required() {
        var output = new ByteArrayOutputStream();
        var sut = new FrameWriter(output);

        assertThatThrownBy(() -> new FrameWriter(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> sut.write(null)).isInstanceOf(NullPointerException.class);
        assertThat(output.size()).isZero();
    }
}
