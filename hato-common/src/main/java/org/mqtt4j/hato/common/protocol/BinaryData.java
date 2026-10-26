package org.mqtt4j.hato.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.common.value.ReasonCode.MALFORMED_PACKET;

/// Reads and writes an unsigned two-byte length followed by zero to 65535 opaque bytes.
///
/// The length is always big-endian, regardless of the input buffer's byte order. Reading
/// returns a new array and advances the buffer past this field only. On malformed input,
/// its position may have advanced. The bytes are neither interpreted as text nor normalized.
///
/// This format is used by MQTT 3.1.1 Will Message and Password fields, and named Binary Data
/// in [MQTT 5.0, section 1.5.6](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
public final class BinaryData {

    private BinaryData() {}

    /// Reads one length-prefixed field from the buffer's current position into a new array.
    ///
    /// @throws PacketDecodeException if the length prefix or the declared data is truncated
    public static byte[] read(ByteBuffer input) {
        var value = new byte[readLength(input)];
        input.get(value);
        return value;
    }

    /// Reads the length prefix of one field and checks that the field's bytes follow it, leaving
    /// the buffer at the first of them.
    ///
    /// @throws PacketDecodeException if the length prefix or the declared data is truncated
    static int readLength(ByteBuffer input) {
        requireNonNull(input, "input must not be null");
        if (input.remaining() < 2) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Truncated binary data length");
        }
        int length = ((input.get() & 0xFF) << 8) | (input.get() & 0xFF);
        if (input.remaining() < length) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Truncated binary data");
        }
        return length;
    }

    /// Appends the length prefix and a copy of `value`, validating before writing any bytes.
    ///
    /// @throws IllegalArgumentException if `value` contains more than 65535 bytes
    public static void write(ByteArrayOutputStream output, byte[] value) {
        requireNonNull(output, "output must not be null");
        requireNonNull(value, "value must not be null");
        if (value.length > 65535) {
            throw new IllegalArgumentException("Binary data exceeds 65535 bytes");
        }
        output.write(value.length >>> 8);
        output.write(value.length & 0xFF);
        output.writeBytes(value);
    }
}
