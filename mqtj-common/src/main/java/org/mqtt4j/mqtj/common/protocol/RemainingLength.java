package org.mqtt4j.mqtj.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.mqtj.common.value.ReasonCode.MALFORMED_PACKET;

/// Reads and writes MQTT Remaining Length values, from 0 to 268435455, in one to four bytes.
///
/// Reading advances the buffer's position past this field only. On malformed input, its
/// position may have advanced. The decoder follows MQTT 3.1.1 and accepts non-minimal encodings
/// of at most four bytes; writing always uses the minimum number of bytes. A future MQTT 5.0
/// codec must additionally verify the minimum-length rule `MQTT-1.5.5-1`.
///
/// See [MQTT 3.1.1 Plus Errata 01, section 2.2.3: Remaining Length](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html).
public final class RemainingLength {

    private static final int MAX_VALUE = 268435455;

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
