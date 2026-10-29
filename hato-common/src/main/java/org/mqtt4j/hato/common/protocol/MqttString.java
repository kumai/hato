package org.mqtt4j.hato.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.common.value.ReasonCode.MALFORMED_PACKET;

/// Reads and writes MQTT UTF-8 encoded strings, including their two-byte byte-length prefix.
///
/// Empty strings are allowed. The encoded content must fit in 65535 bytes, contain no U+0000,
/// and be well-formed UTF-8. BOM characters are preserved; control characters and Unicode
/// noncharacters are accepted because MQTT recommends, but does not require, their rejection.
/// Topic-specific restrictions belong to the topic value types.
///
/// Reading advances the buffer past this field only. On malformed input, its position may
/// have advanced. No Unicode normalization is performed in either direction.
///
/// See [MQTT 3.1.1 Plus Errata 01, section 1.5.3: UTF-8 encoded strings](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html#_Toc442180829).
public final class MqttString {

    private MqttString() {}

    /// Reads and validates one length-prefixed UTF-8 string.
    ///
    /// @throws PacketDecodeException if the field is truncated, malformed UTF-8,
    ///         or contains U+0000
    public static String read(ByteBuffer input) {
        int length = BinaryData.readLength(input);
        int position = input.position();
        String value;
        if (input.hasArray()) {
            // Checked and decoded where the field lies, without copying it out first.
            value = decode(input.array(), input.arrayOffset() + position, length);
        } else {
            var bytes = new byte[length];
            input.get(position, bytes);
            value = decode(bytes, 0, length);
        }
        input.position(position + length);
        return value;
    }

    /// Decodes the `length` bytes of `bytes` from `offset` as the content of an MQTT UTF-8
    /// string, which [#read(ByteBuffer)] reads after its length prefix.
    ///
    /// @throws PacketDecodeException if the bytes are malformed UTF-8 or contain U+0000
    static String decode(byte[] bytes, int offset, int length) {
        // [MQTT-1.5.3-1]: never silently replace malformed UTF-8. One scan checks the bytes
        // against the well-formed sequences of the Unicode standard (table 3-7), as a strict
        // decoder does: it rejects overlong forms, encoded surrogates, values beyond U+10FFFF,
        // and truncated sequences. Decoding checked bytes then cannot replace anything.
        int end = offset + length;
        boolean nul = false;
        int index = offset;
        while (index < end) {
            int lead = bytes[index] & 0xFF;
            if (lead < 0x80) {
                if (lead == 0) {
                    nul = true;
                }
                index++;
                continue;
            }
            // How many continuation bytes the lead byte announces, and the range that the first
            // of them must lie in; the rest lie in 80..BF.
            int continuations;
            int low = 0x80;
            int high = 0xBF;
            if (lead >= 0xC2 && lead <= 0xDF) {
                continuations = 1;
            } else if (lead == 0xE0) {
                continuations = 2;
                low = 0xA0;
            } else if (lead == 0xED) {
                continuations = 2;
                high = 0x9F;
            } else if (lead >= 0xE1 && lead <= 0xEF) {
                continuations = 2;
            } else if (lead == 0xF0) {
                continuations = 3;
                low = 0x90;
            } else if (lead == 0xF4) {
                continuations = 3;
                high = 0x8F;
            } else if (lead >= 0xF1 && lead <= 0xF3) {
                continuations = 3;
            } else {
                throw malformed();
            }
            if (end - index <= continuations) {
                throw malformed();
            }
            int first = bytes[index + 1] & 0xFF;
            if (first < low || first > high) {
                throw malformed();
            }
            for (int at = index + 2; at <= index + continuations; at++) {
                int continuation = bytes[at] & 0xFF;
                if (continuation < 0x80 || continuation > 0xBF) {
                    throw malformed();
                }
            }
            index += continuations + 1;
        }
        // [MQTT-1.5.3-2]
        if (nul) {
            throw new PacketDecodeException(MALFORMED_PACKET, "MQTT string contains NUL");
        }
        // [MQTT-1.5.3-3] (Errata 01): U+FEFF remains part of the string.
        return new String(bytes, offset, length, StandardCharsets.UTF_8);
    }

    private static PacketDecodeException malformed() {
        return new PacketDecodeException(
                MALFORMED_PACKET,
                "Malformed MQTT UTF-8 string",
                new MalformedInputException(1)
        );
    }

    /// Appends the byte-length prefix and UTF-8 content, validating before writing any bytes.
    ///
    /// @throws IllegalArgumentException if `value` contains U+0000 or an unpaired surrogate,
    ///         or exceeds 65535 bytes in UTF-8
    public static void write(ByteArrayOutputStream output, String value) {
        requireNonNull(output, "output must not be null");
        validate(value);
        // getBytes would replace an unpaired surrogate, but validation has rejected them all.
        BinaryData.write(output, value.getBytes(StandardCharsets.UTF_8));
    }

    /// Writes the byte-length prefix and UTF-8 content of `value`, which [#encodedSize] has
    /// checked, into `output` from `offset`, and returns the offset after them.
    static int write(byte[] output, int offset, String value) {
        // getBytes would replace an unpaired surrogate, but sizing has rejected them all.
        return BinaryData.write(output, offset, value.getBytes(StandardCharsets.UTF_8));
    }

    /// Checks that `value` can be written as an MQTT UTF-8 string, without encoding it.
    ///
    /// @throws IllegalArgumentException if `value` contains U+0000 or an unpaired surrogate,
    ///         or exceeds 65535 bytes in UTF-8
    public static void validate(String value) {
        int _ = encodedSize(value);
    }

    /// The bytes that `value` takes as an MQTT UTF-8 string, its byte-length prefix included,
    /// after checking it as [#validate] does, without encoding it.
    ///
    /// @throws IllegalArgumentException if `value` contains U+0000 or an unpaired surrogate,
    ///         or exceeds 65535 bytes in UTF-8
    public static int encodedSize(String value) {
        requireNonNull(value, "value must not be null");
        // Shared text constraints also appear in value.TopicText; that layer cannot use protocol.
        // One scan counts the UTF-8 length: a char below U+0080 takes one byte, below U+0800
        // two, and any other three, except that a surrogate pair takes four for its two chars.
        long bytes = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x80) {
                // [MQTT-1.5.3-2]
                if (c == '\0') {
                    throw new IllegalArgumentException("MQTT string must not contain NUL");
                }
                bytes += 1;
            } else if (c < 0x800) {
                bytes += 2;
            } else if (!Character.isSurrogate(c)) {
                bytes += 3;
            } else if (Character.isHighSurrogate(c)
                    && i + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                bytes += 4;
                i++;
            } else {
                // [MQTT-1.5.3-1]: an unpaired surrogate has no UTF-8 encoding.
                throw new IllegalArgumentException("MQTT string contains an unpaired surrogate");
            }
        }
        if (bytes > 65535) {
            throw new IllegalArgumentException("MQTT string exceeds 65535 bytes in UTF-8");
        }
        return 2 + (int) bytes;
    }
}
