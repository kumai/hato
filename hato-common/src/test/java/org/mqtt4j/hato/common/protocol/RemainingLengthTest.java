package org.mqtt4j.hato.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.common.value.ReasonCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemainingLengthTest {

    @ParameterizedTest
    @CsvSource(textBlock = """
            0, 00
            64, 40
            127, 7f
            128, 8001
            321, c102
            16383, ff7f
            16384, 808001
            2097151, ffff7f
            2097152, 80808001
            268435455, ffffff7f
            """)
    void reads_and_writes_known_wire_encodings(int value, String hex) {
        // MQTT 3.1.1 section 2.2.3: boundaries and the specification's examples.
        byte[] expected = HexFormat.of().parseHex(hex);
        var input = ByteBuffer.wrap(expected);
        var output = new ByteArrayOutputStream();

        assertThat(RemainingLength.read(input)).isEqualTo(value);
        assertThat(input.hasRemaining()).isFalse();
        RemainingLength.write(output, value);
        assertThat(output.toByteArray()).isEqualTo(expected);
        assertThat(RemainingLength.encodedSize(value)).isEqualTo(expected.length);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "80", "8080", "808080", "80808080", "ffffffff00"})
    void truncated_or_overlong_fields_are_malformed_packets(String hex) {
        var input = ByteBuffer.wrap(HexFormat.of().parseHex(hex));

        assertThatExceptionOfType(PacketDecodeException.class)
                .isThrownBy(() -> RemainingLength.read(input))
                .satisfies(error -> assertThat(error.reasonCode())
                        .isSameAs(ReasonCode.MALFORMED_PACKET));
        assertThat(input.position()).isLessThanOrEqualTo(4);
    }

    @ParameterizedTest
    @CsvSource({"8000, 0", "ff00, 127", "808000, 0", "80808000, 0"})
    void mqtt_3_1_1_accepts_non_minimal_encodings_within_four_bytes(String hex, int expected) {
        assertThat(RemainingLength.read(ByteBuffer.wrap(HexFormat.of().parseHex(hex))))
                .isEqualTo(expected);
    }

    @Test
    void reads_only_the_field_at_the_current_position() {
        var input = ByteBuffer.wrap(HexFormat.of().parseHex("aac102bb")).asReadOnlyBuffer();
        input.position(1);

        assertThat(RemainingLength.read(input)).isEqualTo(321);
        assertThat(input.position()).isEqualTo(3);
        assertThat(input.get() & 0xFF).isEqualTo(0xBB);
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -1, 268435456, Integer.MAX_VALUE})
    void invalid_values_are_rejected_without_writing(int value) {
        var output = new ByteArrayOutputStream();
        output.write(0xAA);

        assertThatThrownBy(() -> RemainingLength.write(output, value))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RemainingLength.encodedSize(value))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(output.toByteArray()).containsExactly((byte) 0xAA);
    }

    @Test
    void arbitrary_values_round_trip_in_the_reported_size() {
        var random = new Random(0x4D515454);
        for (int i = 0; i < 1000; i++) {
            int value = random.nextInt(268435456);
            var output = new ByteArrayOutputStream();
            RemainingLength.write(output, value);
            assertThat(output.size()).isEqualTo(RemainingLength.encodedSize(value));
            assertThat(RemainingLength.read(ByteBuffer.wrap(output.toByteArray())))
                    .isEqualTo(value);
        }
    }
}
