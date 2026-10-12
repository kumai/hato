package org.mqtt4j.hato.common.value;

/// A validated MQTT topic name used when publishing a message.
///
/// Names are non-empty, contain neither NUL nor the wildcards `+` and `#`, and must encode
/// as well-formed UTF-8 in at most 65535 bytes. Names are case-sensitive and preserved exactly:
/// whitespace, empty levels separated by `/`, and a byte order mark are not removed or normalized.
///
/// The validation rules follow MQTT 3.1.1 Plus Errata 01:
///
/// - [Section 1.5.3: UTF-8 encoded strings](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html#_Toc442180829)
/// - [Section 4.7: Topic Names and Topic Filters](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html#_Toc442180919)
///
/// @param name the topic name
public record Topic(String name) {

    /// Creates a topic after validating its name.
    ///
    /// @throws NullPointerException if `name` is null
    /// @throws IllegalArgumentException if `name` is empty, contains NUL, a wildcard, or an
    ///         unpaired surrogate, or exceeds 65535 bytes in UTF-8
    public Topic {
        TopicText.validate(name, "Topic name");

        // [MQTT-4.7.1-1]
        if (name.indexOf('+') >= 0 || name.indexOf('#') >= 0) {
            throw new IllegalArgumentException("Topic name must not contain wildcards");
        }
    }

    /// Returns the topic name unchanged.
    @Override
    public String toString() {
        return name;
    }
}
