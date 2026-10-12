package org.mqtt4j.hato.common.protocol;

import static java.util.Objects.requireNonNull;

/// MQTT 5.0 properties. Further property types are added with the MQTT 5.0 codec.
///
/// See [MQTT 5.0, section 2.2.2: Properties](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
public sealed interface Property {

    /// A User Property (identifier 0x26). Names and values may be empty; duplicates are allowed.
    /// The codec validates the UTF-8 representation of both strings.
    record UserProperty(String name, String value) implements Property {

        public UserProperty {
            requireNonNull(name, "User Property name must not be null");
            requireNonNull(value, "User Property value must not be null");
        }
    }
}
