package org.mqtt4j.mqtj.common.value;

import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/// Validates the text constraints shared by topic names and topic filters.
///
/// - [MQTT 3.1.1 Plus Errata 01, section 1.5.3: UTF-8 encoded strings](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html#_Toc442180829)
/// - [MQTT 3.1.1 Plus Errata 01, section 4.7: Topic Names and Topic Filters](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html#_Toc442180919)
final class TopicText {

    private TopicText() {}

    /// Validates the complete text, using `label` to identify it in exception messages.
    static void validate(String value, String label) {
        Objects.requireNonNull(value, label + " must not be null");
        // [MQTT-4.7.3-1]
        if (value.isEmpty()) {
            throw new IllegalArgumentException(label + " must not be empty");
        }

        // [MQTT-4.7.3-2]
        if (value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(label + " must not contain NUL character");
        }

        // [MQTT-1.5.3-1]: report malformed UTF-16 instead of replacing unpaired surrogates.
        try {
            int encodedLength = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value))
                    .remaining();
            // [MQTT-4.7.3-3]: the limit applies to the complete UTF-8 encoding.
            if (encodedLength > 65535) {
                throw new IllegalArgumentException(label + " exceeds 65535 bytes in UTF-8");
            }
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(label + " contains an unpaired surrogate", e);
        }
    }
}
