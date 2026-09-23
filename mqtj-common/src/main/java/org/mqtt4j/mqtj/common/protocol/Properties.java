package org.mqtt4j.mqtj.common.protocol;

import java.util.List;

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
}
