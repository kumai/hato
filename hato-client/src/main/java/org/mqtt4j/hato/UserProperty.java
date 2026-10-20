package org.mqtt4j.hato;

import java.io.ByteArrayOutputStream;

import org.mqtt4j.hato.client.internal.common.protocol.MqttString;

import static java.util.Objects.requireNonNull;

/// An MQTT 5.0 User Property of a message: a name and a value that the application chooses.
/// Both are MQTT UTF-8 strings of at most 65535 bytes and may be empty. A message keeps its
/// User Properties in order and may repeat a name; brokers forward them in the same order.
///
/// @param name the property name
/// @param value the property value
public record UserProperty(String name, String value) {

    /// Validates both strings.
    ///
    /// @throws IllegalArgumentException if either string contains U+0000 or an unpaired
    ///     surrogate, or exceeds 65535 bytes in UTF-8
    public UserProperty {
        requireNonNull(name, "User Property name must not be null");
        requireNonNull(value, "User Property value must not be null");
        MqttString.write(new ByteArrayOutputStream(), name);
        MqttString.write(new ByteArrayOutputStream(), value);
    }
}
