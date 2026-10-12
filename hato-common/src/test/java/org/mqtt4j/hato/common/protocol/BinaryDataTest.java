package org.mqtt4j.hato.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.common.value.ReasonCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinaryDataTest {

    @ParameterizedTest
    @CsvSource({"'', 0000", "00, 000100", "0080ff, 00030080ff", "c080, 0002c080"})
    void reads_and_writes_opaque_bytes(String contentHex, String wireHex) {
        byte[] content = HexFormat.of().parseHex(contentHex);
        byte[] wire = HexFormat.of().parseHex(wireHex);
        var input = ByteBuffer.wrap(wire);
        var output = new ByteArrayOutputStream();

        assertThat(BinaryData.read(input)).isEqualTo(content);
        assertThat(input.hasRemaining()).isFalse();
        BinaryData.write(output, content);
        assertThat(output.toByteArray()).isEqualTo(wire);
    }

    @ParameterizedTest
    @CsvSource({"255, 00ff", "256, 0100", "32768, 8000", "65535, ffff"})
    void length_is_an_unsigned_big_endian_byte_count(int length, String prefixHex) {
        var content = new byte[length];
        for (int i = 0; i < length; i++) {
            content[i] = (byte) i;
        }
        var output = new ByteArrayOutputStream();
        BinaryData.write(output, content);

        assertThat(output.toByteArray()).startsWith(HexFormat.of().parseHex(prefixHex));
        assertThat(output.size()).isEqualTo(length + 2);
        var input = ByteBuffer.wrap(output.toByteArray()).order(ByteOrder.LITTLE_ENDIAN);
        assertThat(BinaryData.read(input)).isEqualTo(content);
        assertThat(input.order()).isSameAs(ByteOrder.LITTLE_ENDIAN);
        assertThat(input.hasRemaining()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "00", "0001", "000200", "ffff00"})
    void truncated_fields_are_malformed_packets(String hex) {
        var input = ByteBuffer.wrap(HexFormat.of().parseHex(hex));

        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> BinaryData.read(input))
                .satisfies(error -> assertThat(error.reasonCode())
                        .isSameAs(ReasonCode.MALFORMED_PACKET));
    }

    @Test
    void reads_an_independent_array_and_leaves_the_next_field_unread() {
        byte[] wire = HexFormat.of().parseHex("aa000100bb");
        var input = ByteBuffer.wrap(wire).asReadOnlyBuffer();
        input.position(1);

        byte[] decoded = BinaryData.read(input);
        assertThat(decoded).containsExactly((byte) 0);
        decoded[0] = 0x7F;
        assertThat(wire[3]).isZero();
        assertThat(input.position()).isEqualTo(4);
        assertThat(input.get() & 0xFF).isEqualTo(0xBB);
    }

    @Test
    void the_buffer_limit_bounds_the_field() {
        var input = ByteBuffer.wrap(HexFormat.of().parseHex("00020102"));
        input.limit(3);

        assertThatThrownBy(() -> BinaryData.read(input))
                .isInstanceOf(PacketDecodeException.class);
    }

    @Test
    void too_large_values_are_rejected_without_writing() {
        var output = new ByteArrayOutputStream();
        output.write(0xAA);

        assertThatThrownBy(() -> BinaryData.write(output, new byte[65536]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(output.toByteArray()).containsExactly((byte) 0xAA);
    }

    @Test
    void writing_copies_the_bytes_to_the_output() {
        var content = new byte[]{1};
        var output = new ByteArrayOutputStream();
        output.write(0xAA);

        BinaryData.write(output, content);
        content[0] = 2;

        assertThat(output.toByteArray()).isEqualTo(HexFormat.of().parseHex("aa000101"));
    }
}
