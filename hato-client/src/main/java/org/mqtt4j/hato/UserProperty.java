package org.mqtt4j.hato;

import java.util.Objects;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.common.protocol.MqttString;
import org.mqtt4j.hato.client.internal.common.protocol.Property;

import static java.util.Objects.requireNonNull;

/// An MQTT 5.0 User Property of a message: a name and a value that the application chooses.
/// Both are MQTT UTF-8 strings of at most 65535 bytes and may be empty. A message keeps its
/// User Properties in order and may repeat a name; brokers forward them in the same order.
/// Two User Properties are equal when their names are equal and their values are equal.
public final class UserProperty {

    private final String name;

    private final String value;

    /// Validates both strings.
    ///
    /// @param name the property name
    /// @param value the property value
    /// @throws IllegalArgumentException if either string contains U+0000 or an unpaired
    ///     surrogate, or exceeds 65535 bytes in UTF-8
    public UserProperty(String name, String value) {
        requireNonNull(name, "User Property name must not be null");
        requireNonNull(value, "User Property value must not be null");
        MqttString.validate(name);
        MqttString.validate(value);
        this.name = name;
        this.value = value;
    }

    // The codec validated both strings while decoding them, so they are not checked again.
    private UserProperty(Property.UserProperty received) {
        name = received.name();
        value = received.value();
    }

    /// Wraps a User Property that the client received; the builder hands this to the client.
    static UserProperty received(Property.UserProperty property) {
        return new UserProperty(property);
    }

    /// The property name, possibly empty.
    public String name() {
        return name;
    }

    /// The property value, possibly empty.
    public String value() {
        return value;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof UserProperty that
                && name.equals(that.name)
                && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return "UserProperty[name=" + name + ", value=" + value + "]";
    }
}
