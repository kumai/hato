package org.mqtt4j.hato.common.value;

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

        // One scan counts the UTF-8 length without encoding the text, which every received
        // topic name would otherwise pay for: a char below U+0080 takes one byte, below U+0800
        // two, and any other three, except that a surrogate pair takes four for its two chars.
        // [MQTT-1.5.3-1]: an unpaired surrogate has no UTF-8 encoding, so it is reported rather
        // than replaced.
        long encodedLength = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x80) {
                encodedLength += 1;
            } else if (c < 0x800) {
                encodedLength += 2;
            } else if (!Character.isSurrogate(c)) {
                encodedLength += 3;
            } else if (Character.isHighSurrogate(c)
                    && i + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                encodedLength += 4;
                i++;
            } else {
                throw new IllegalArgumentException(label + " contains an unpaired surrogate");
            }
        }
        // [MQTT-4.7.3-3]: the limit applies to the complete UTF-8 encoding.
        if (encodedLength > 65535) {
            throw new IllegalArgumentException(label + " exceeds 65535 bytes in UTF-8");
        }
    }
}
