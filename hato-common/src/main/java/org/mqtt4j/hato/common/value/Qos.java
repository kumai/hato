package org.mqtt4j.hato.common.value;

/// MQTT quality of service levels.
public enum Qos {

    /// QoS 0: delivered at most once, with no acknowledgement.
    AT_MOST_ONCE(0),

    /// QoS 1: delivered at least once, acknowledged with PUBACK.
    AT_LEAST_ONCE(1),

    /// QoS 2: delivered exactly once, through the PUBREC, PUBREL, and PUBCOMP handshake.
    EXACTLY_ONCE(2);

    private final int level;

    Qos(int level) {
        this.level = level;
    }

    /// The numeric level as it appears on the wire.
    public int level() {
        return level;
    }

    /// Returns the QoS for a wire-level value.
    ///
    /// @throws IllegalArgumentException if `level` is not 0, 1, or 2
    public static Qos of(int level) {
        return switch (level) {
            case 0 -> AT_MOST_ONCE;
            case 1 -> AT_LEAST_ONCE;
            case 2 -> EXACTLY_ONCE;
            default -> throw new IllegalArgumentException("QoS level must be 0, 1, or 2: " + level);
        };
    }
}
