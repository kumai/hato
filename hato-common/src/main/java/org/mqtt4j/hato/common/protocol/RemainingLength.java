package org.mqtt4j.hato.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.common.value.ReasonCode.MALFORMED_PACKET;

/// Reads and writes MQTT Remaining Length values, from 0 to 268435455, in one to four bytes.
///
/// Reading advances the buffer's position past this field only. On malformed input, its
/// position may have advanced. The decoder follows MQTT 3.1.1 and accepts non-minimal encodings
/// of at most four bytes. [#readMinimal] additionally enforces the MQTT 5.0 minimum-length
/// rule; writing always uses the minimum number of bytes.
///
/// See [MQTT 3.1.1 Plus Errata 01, section 2.2.3: Remaining Length](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html).
public final class RemainingLength {

    static final int MAX_VALUE = 268435455;

    private RemainingLength() {}

    /// Reads one variable byte integer from the buffer's current position.
    ///
    /// @throws PacketDecodeException if the field is truncated or continues beyond four bytes
    public static int read(ByteBuffer input) {
        requireNonNull(input, "input must not be null");
        int value = 0;
        for (int shift = 0; shift < 28; shift += 7) {
            if (!input.hasRemaining()) {
                throw new PacketDecodeException(MALFORMED_PACKET, "Truncated Remaining Length");
            }
            int digit = input.get() & 0xFF;
            value |= (digit & 0x7F) << shift;
            if ((digit & 0x80) == 0) {
                return value;
            }
        }
        throw new PacketDecodeException(MALFORMED_PACKET, "Remaining Length exceeds four bytes");
    }

    /// Reads a minimally encoded variable byte integer, as required by MQTT 5.0.
    ///
    /// @throws PacketDecodeException if truncated, over four bytes, or not minimally encoded
    public static int readMinimal(ByteBuffer input) {
        requireNonNull(input, "input must not be null");
        int start = input.position();
        int value = read(input);
        if (input.position() - start != encodedSize(value)) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Non-minimal variable byte integer");
        }
        return value;
    }

    /// Appends the shortest encoding of `value`, validating it before writing any bytes.
    ///
    /// @throws IllegalArgumentException if `value` is outside 0 to 268435455
    public static void write(ByteArrayOutputStream output, int value) {
        requireNonNull(output, "output must not be null");
        checkValue(value);
        int remaining = value;
        do {
            int digit = remaining & 0x7F;
            remaining >>>= 7;
            if (remaining != 0) {
                digit |= 0x80;
            }
            output.write(digit);
        } while (remaining != 0);
    }

    /// Writes the shortest encoding of `value` into `output` from `offset`, which must have room
    /// for it, and returns the offset after it.
    ///
    /// @throws IllegalArgumentException if `value` is outside 0 to 268435455
    static int write(byte[] output, int offset, int value) {
        checkValue(value);
        int at = offset;
        int remaining = value;
        do {
            int digit = remaining & 0x7F;
            remaining >>>= 7;
            if (remaining != 0) {
                digit |= 0x80;
            }
            output[at++] = (byte) digit;
        } while (remaining != 0);
        return at;
    }

    /// Returns the number of bytes in the shortest encoding of `value`.
    ///
    /// @throws IllegalArgumentException if `value` is outside 0 to 268435455
    public static int encodedSize(int value) {
        checkValue(value);
        int size = 1;
        for (int remaining = value >>> 7; remaining != 0; remaining >>>= 7) {
            size++;
        }
        return size;
    }

    private static void checkValue(int value) {
        if (value < 0 || value > MAX_VALUE) {
            throw new IllegalArgumentException("Remaining Length must be between 0 and 268435455");
        }
    }
}
