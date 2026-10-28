package org.mqtt4j.hato.common.value;

/// The MQTT protocol versions supported by Hato.
///
/// MQTT 5.0 features are rejected when used in a 3.1.1 session.
public enum MqttVersion {

    /// MQTT 3.1.1. Protocol level 4 on the wire.
    V3_1_1(4),

    /// MQTT 5.0. Protocol level 5 on the wire.
    V5(5);

    private final int protocolLevel;

    MqttVersion(int protocolLevel) {
        this.protocolLevel = protocolLevel;
    }

    /// {@return the protocol level sent in the CONNECT packet}
    public int protocolLevel() {
        return protocolLevel;
    }

    /// Resolves the protocol level from a CONNECT packet.
    ///
    /// @param protocolLevel
    ///     the protocol level
    /// @return
    ///     the version with that protocol level
    /// @throws IllegalArgumentException
    ///     if the protocol level is unsupported
    public static MqttVersion of(int protocolLevel) {
        return switch (protocolLevel) {
            case 4 -> V3_1_1;
            case 5 -> V5;
            default -> throw new IllegalArgumentException(
                    "Unsupported MQTT protocol level: " + protocolLevel
            );
        };
    }
}
