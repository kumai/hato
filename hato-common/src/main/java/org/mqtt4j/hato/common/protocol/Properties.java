package org.mqtt4j.hato.common.protocol;

import java.util.List;
import java.util.Optional;

/// An immutable property sequence preserving wire order and duplicates.
/// Packet-specific restrictions are checked by the codec.
public record Properties(List<Property> entries) {

    /// The absence of MQTT 5.0 properties, also used by MQTT 3.1.1 packets.
    public static final Properties EMPTY = new Properties(List.of());

    /// Copies the sequence and rejects null entries.
    public Properties {
        entries = List.copyOf(entries);
    }

    /// Whether the packet has no properties.
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /// The first property with `id`, or empty when the packet has none.
    public Optional<Property> find(PropertyId id) {
        // A loop rather than a stream: the client looks up properties for every PUBLISH it
        // receives, and an MQTT 3.1.1 PUBLISH has none.
        for (int i = 0; i < entries.size(); i++) {
            Property property = entries.get(i);
            if (property.id() == id) {
                return Optional.of(property);
            }
        }
        return Optional.empty();
    }
}
