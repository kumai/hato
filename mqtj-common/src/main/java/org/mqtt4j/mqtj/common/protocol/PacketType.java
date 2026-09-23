package org.mqtt4j.mqtj.common.protocol;

import java.util.OptionalInt;

/// MQTT Control Packet types encoded in bits 7 to 4 of the first fixed-header byte.
///
/// Values are unshifted numbers from 1 to 15. AUTH is included for MQTT 5.0; the MQTT 3.1.1
/// codec must reject it. The remaining four bits are flags, fixed except for PUBLISH.
///
/// - [MQTT 3.1.1 Plus Errata 01, sections 2.2.1 and 2.2.2: types and flags](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html)
/// - [MQTT 5.0, sections 2.1.2 and 2.1.3: types and flags](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)
public enum PacketType {

    /// CONNECT — requests a connection.
    CONNECT(1),

    /// CONNACK — reports the connection result.
    CONNACK(2),

    /// PUBLISH — carries an application message.
    PUBLISH(3),

    /// PUBACK — acknowledges a QoS 1 publication.
    PUBACK(4),

    /// PUBREC — acknowledges receipt of a QoS 2 publication.
    PUBREC(5),

    /// PUBREL — releases a QoS 2 publication after PUBREC.
    PUBREL(6),

    /// PUBCOMP — completes the QoS 2 exchange after PUBREL.
    PUBCOMP(7),

    /// SUBSCRIBE — requests subscriptions.
    SUBSCRIBE(8),

    /// SUBACK — reports subscription results.
    SUBACK(9),

    /// UNSUBSCRIBE — requests removal of subscriptions.
    UNSUBSCRIBE(10),

    /// UNSUBACK — acknowledges removal of subscriptions.
    UNSUBACK(11),

    /// PINGREQ — requests a keep-alive response.
    PINGREQ(12),

    /// PINGRESP — responds to PINGREQ.
    PINGRESP(13),

    /// DISCONNECT — notifies the peer that the connection is closing.
    DISCONNECT(14),

    /// AUTH — exchanges authentication data in MQTT 5.0.
    AUTH(15);

    private final int value;

    PacketType(int value) {
        this.value = value;
    }

    /// Returns the unshifted four-bit wire value, excluding flags.
    public int value() {
        return value;
    }

    /// Returns the required low four header bits, or empty for PUBLISH.
    ///
    /// PUBREL, SUBSCRIBE, and UNSUBSCRIBE require `0010`; other fixed flags are `0000`.
    /// PUBLISH derives its flags from DUP, QoS, and RETAIN. An empty result does not mean
    /// that arbitrary PUBLISH flags are valid; the codec must check those fields separately.
    public OptionalInt fixedFlags() {
        // MQTT 3.1.1 [MQTT-2.2.2-1] and MQTT 5.0 [MQTT-2.1.3-1].
        return switch (this) {
            case PUBLISH -> OptionalInt.empty();
            case PUBREL,
                 SUBSCRIBE,
                 UNSUBSCRIBE -> OptionalInt.of(0b0010);
            case CONNECT,
                 CONNACK,
                 PUBACK,
                 PUBREC,
                 PUBCOMP,
                 SUBACK,
                 UNSUBACK,
                 PINGREQ,
                 PINGRESP,
                 DISCONNECT,
                 AUTH -> OptionalInt.of(0b0000);
        };
    }

    /// Resolves the packet type after the codec extracts bits 7 to 4 of the first header byte.
    ///
    /// @param value the unshifted type value, not the complete header byte
    /// @return the corresponding packet type, including AUTH for 15
    /// @throws IllegalArgumentException if `value` is reserved (0) or outside 1 to 15
    public static PacketType of(int value) {
        return switch (value) {
            case 1 -> CONNECT;
            case 2 -> CONNACK;
            case 3 -> PUBLISH;
            case 4 -> PUBACK;
            case 5 -> PUBREC;
            case 6 -> PUBREL;
            case 7 -> PUBCOMP;
            case 8 -> SUBSCRIBE;
            case 9 -> SUBACK;
            case 10 -> UNSUBSCRIBE;
            case 11 -> UNSUBACK;
            case 12 -> PINGREQ;
            case 13 -> PINGRESP;
            case 14 -> DISCONNECT;
            case 15 -> AUTH;
            default -> throw new IllegalArgumentException("Undefined MQTT packet type: " + value);
        };
    }
}
