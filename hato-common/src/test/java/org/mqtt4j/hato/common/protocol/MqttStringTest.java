package org.mqtt4j.hato.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.common.value.ReasonCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttStringTest {

    @ParameterizedTest
    @CsvSource(textBlock = """
            '', 0000
            MQTT, 00044d515454
            A𪛔, 000541f0aa9b94
            é, 0002c3a9
            あ, 0003e38182
            """)
    void reads_and_writes_known_utf8_encodings(String value, String hex) {
        // Includes the MQTT 3.1.1 section 1.5.3 example A + U+2A6D4.
        byte[] wire = HexFormat.of().parseHex(hex);
        var input = ByteBuffer.wrap(wire);
        var output = new ByteArrayOutputStream();

        assertThat(MqttString.read(input)).isEqualTo(value);
        assertThat(input.hasRemaining()).isFalse();
        MqttString.write(output, value);
        assertThat(output.toByteArray()).isEqualTo(wire);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "\uFEFF", "\uFEFFa", "a\uFEFFb", "\u0001\u001F\u007F\u0080\u009F",
            "\uFDD0\uFDEF\uFFFE\uFFFF", "\uDBFF\uDFFF", "cafe\u0301", " /+# "
    })
    void preserves_bom_controls_noncharacters_and_text_without_normalization(String value) {
        // SHOULD NOT is not MUST NOT; [MQTT-1.5.3-3] (Errata 01) preserves U+FEFF.
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        var expected = ByteBuffer.allocate(bytes.length + 2);
        expected.putShort((short) bytes.length).put(bytes).flip();
        var output = new ByteArrayOutputStream();

        assertThat(MqttString.read(expected)).isEqualTo(value);
        MqttString.write(output, value);
        assertThat(output.toByteArray()).isEqualTo(expected.array());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "000180", "0002c080", "0002c1bf", "0003e08080", "0004f0808080",
            "0003eda080", "0003edbfbf", "0004f4908080", "0004f5808080",
            "0001ff", "0002c241", "0002e282", "0003f09080"
    })
    void malformed_utf8_is_rejected_instead_of_replaced(String hex) {
        // [MQTT-1.5.3-1]: overlong forms, surrogates, out-of-range and incomplete sequences.
        var input = ByteBuffer.wrap(HexFormat.of().parseHex(hex));

        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> MqttString.read(input))
                .withCauseInstanceOf(CharacterCodingException.class)
                .satisfies(error -> assertThat(error.reasonCode())
                        .isSameAs(ReasonCode.MALFORMED_PACKET));
    }

    @Test
    void decoding_agrees_with_a_strict_utf8_decoder_on_short_sequences() {
        // Arrange: every sequence of one or two bytes, and after each byte that can lead a three-
        // or four-byte sequence, every second byte with later bytes at the edges of the ranges
        // that decide well-formedness.
        int[] edges = {0x00, 0x41, 0x7F, 0x80, 0x8F, 0x90, 0x9F, 0xA0, 0xBF, 0xC0, 0xFF};
        var mismatches = new ArrayList<String>();
        // Act
        for (int first = 0; first < 256; first++) {
            check(bytes(first), mismatches);
            for (int second = 0; second < 256; second++) {
                check(bytes(first, second), mismatches);
                if (first < 0xE0) {
                    continue;
                }
                for (int third : edges) {
                    check(bytes(first, second, third), mismatches);
                    if (first < 0xF0) {
                        continue;
                    }
                    for (int fourth : edges) {
                        check(bytes(first, second, third, fourth), mismatches);
                    }
                }
            }
        }
        // Assert
        assertThat(mismatches).isEmpty();
    }

    // Adds `content` to `mismatches` unless MqttString reads it as a field exactly as a strict
    // decoder reads it, under MQTT's rule against U+0000.
    private static void check(byte[] content, List<String> mismatches) {
        ByteBuffer field = ByteBuffer.allocate(content.length + 2)
                .putShort((short) content.length)
                .put(content)
                .flip();
        @Nullable String actual;
        try {
            actual = MqttString.read(field);
        } catch (PacketDecodeException _) {
            actual = null;
        }
        if (!Objects.equals(actual, strictlyDecoded(content))) {
            mismatches.add(HexFormat.of().formatHex(content));
        }
    }

    private static byte[] bytes(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return bytes;
    }

    // What a strict UTF-8 decoder makes of `content`, or null when MQTT rejects it.
    private static @Nullable String strictlyDecoded(byte[] content) {
        try {
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(content))
                    .toString();
            return value.indexOf('\0') >= 0 ? null : value;
        } catch (CharacterCodingException _) {
            return null;
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"000100", "0003610062", "", "00", "0001", "000261"})
    void nul_and_truncated_fields_are_malformed_packets(String hex) {
        var input = ByteBuffer.wrap(HexFormat.of().parseHex(hex));

        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> MqttString.read(input))
                .satisfies(error -> assertThat(error.reasonCode())
                        .isSameAs(ReasonCode.MALFORMED_PACKET));
    }

    @ParameterizedTest
    @ValueSource(strings = {"\0", "a\0b", "\uD800", "\uDC00", "a\uD800b", "\uDC00\uD800"})
    void invalid_java_strings_are_rejected_without_writing(String value) {
        var output = new ByteArrayOutputStream();
        output.write(0xAA);

        assertThatThrownBy(() -> MqttString.write(output, value))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(output.toByteArray()).containsExactly((byte) 0xAA);
    }

    @ParameterizedTest
    @CsvSource({"a, 1", "é, 2", "あ, 3", "𪛔, 4"})
    void accepts_65535_utf8_bytes_and_rejects_65536(String character, int bytesPerCharacter) {
        String value = character.repeat(65535 / bytesPerCharacter)
                + "a".repeat(65535 % bytesPerCharacter);
        var output = new ByteArrayOutputStream();
        MqttString.write(output, value);

        assertThat(output.size()).isEqualTo(65537);
        assertThat(output.toByteArray()).startsWith((byte) 0xFF, (byte) 0xFF);
        assertThat(MqttString.read(ByteBuffer.wrap(output.toByteArray()))).isEqualTo(value);

        byte[] before = output.toByteArray();
        assertThatThrownBy(() -> MqttString.write(output, value + "a"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(output.toByteArray()).isEqualTo(before);
    }

    @Test
    void multiple_fields_can_be_written_and_read_in_sequence() {
        var output = new ByteArrayOutputStream();
        output.write(0xAA);
        MqttString.write(output, "A");
        MqttString.write(output, "");
        MqttString.write(output, "あ");
        output.write(0xBB);
        var input = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();
        input.position(1);

        assertThat(MqttString.read(input)).isEqualTo("A");
        assertThat(MqttString.read(input)).isEmpty();
        assertThat(MqttString.read(input)).isEqualTo("あ");
        assertThat(input.get() & 0xFF).isEqualTo(0xBB);
        assertThat(input.hasRemaining()).isFalse();
    }
}
