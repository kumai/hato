package org.mqtt4j.mqtj.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.mqtj.common.value.ReasonCode.MALFORMED_PACKET;

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
        byte[] bytes = BinaryData.read(input);
        try {
            // [MQTT-1.5.3-1]: never silently replace malformed UTF-8, including surrogates.
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
            // [MQTT-1.5.3-2]
            if (value.indexOf('\0') >= 0) {
                throw new PacketDecodeException(MALFORMED_PACKET, "MQTT string contains NUL");
            }
            // [MQTT-1.5.3-3] (Errata 01): U+FEFF remains part of the string.
            return value;
        } catch (CharacterCodingException e) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Malformed MQTT UTF-8 string", e);
        }
    }

    /// Appends the byte-length prefix and UTF-8 content, validating before writing any bytes.
    ///
    /// @throws IllegalArgumentException if `value` contains U+0000 or an unpaired surrogate,
    ///         or exceeds 65535 bytes in UTF-8
    public static void write(ByteArrayOutputStream output, String value) {
        requireNonNull(output, "output must not be null");
        requireNonNull(value, "value must not be null");
        // Shared text constraints also appear in value.TopicText; that layer cannot use protocol.
        // [MQTT-1.5.3-2]
        if (value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("MQTT string must not contain NUL");
        }
        try {
            // [MQTT-1.5.3-1]: reject unpaired UTF-16 surrogates instead of replacing them.
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            var bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            BinaryData.write(output, bytes);
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("MQTT string contains an unpaired surrogate", e);
        }
    }
}
