package org.mqtt4j.hato.common.value;

/// The MQTT protocol versions supported by Hato.
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
}
