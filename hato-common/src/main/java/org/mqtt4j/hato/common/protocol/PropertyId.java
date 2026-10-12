package org.mqtt4j.hato.common.protocol;

/// MQTT 5.0 property identifiers and their wire types.
///
/// See [MQTT 5.0, table 2-4](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
public enum PropertyId {

    PAYLOAD_FORMAT_INDICATOR(0x01, Kind.BYTE),
    MESSAGE_EXPIRY_INTERVAL(0x02, Kind.FOUR_BYTE),
    CONTENT_TYPE(0x03, Kind.STRING),
    RESPONSE_TOPIC(0x08, Kind.STRING),
    CORRELATION_DATA(0x09, Kind.BINARY),
    SUBSCRIPTION_IDENTIFIER(0x0B, Kind.VARIABLE_BYTE),
    SESSION_EXPIRY_INTERVAL(0x11, Kind.FOUR_BYTE),
    ASSIGNED_CLIENT_IDENTIFIER(0x12, Kind.STRING),
    SERVER_KEEP_ALIVE(0x13, Kind.TWO_BYTE),
    AUTHENTICATION_METHOD(0x15, Kind.STRING),
    AUTHENTICATION_DATA(0x16, Kind.BINARY),
    REQUEST_PROBLEM_INFORMATION(0x17, Kind.BYTE),
    WILL_DELAY_INTERVAL(0x18, Kind.FOUR_BYTE),
    REQUEST_RESPONSE_INFORMATION(0x19, Kind.BYTE),
    RESPONSE_INFORMATION(0x1A, Kind.STRING),
    SERVER_REFERENCE(0x1C, Kind.STRING),
    REASON_STRING(0x1F, Kind.STRING),
    RECEIVE_MAXIMUM(0x21, Kind.TWO_BYTE),
    TOPIC_ALIAS_MAXIMUM(0x22, Kind.TWO_BYTE),
    TOPIC_ALIAS(0x23, Kind.TWO_BYTE),
    MAXIMUM_QOS(0x24, Kind.BYTE),
    RETAIN_AVAILABLE(0x25, Kind.BYTE),
    USER_PROPERTY(0x26, Kind.STRING_PAIR),
    MAXIMUM_PACKET_SIZE(0x27, Kind.FOUR_BYTE),
    WILDCARD_SUBSCRIPTION_AVAILABLE(0x28, Kind.BYTE),
    SUBSCRIPTION_IDENTIFIER_AVAILABLE(0x29, Kind.BYTE),
    SHARED_SUBSCRIPTION_AVAILABLE(0x2A, Kind.BYTE);

    enum Kind {
        BYTE, TWO_BYTE, FOUR_BYTE, VARIABLE_BYTE, BINARY, STRING, STRING_PAIR
    }

    private final int value;
    private final Kind kind;

    PropertyId(int value, Kind kind) {
        this.value = value;
        this.kind = kind;
    }

    /// Returns the numeric identifier that precedes the property value on the wire.
    public int value() {
        return value;
    }

    /// Resolves a defined MQTT 5.0 identifier.
    ///
    /// @throws IllegalArgumentException if the identifier is unknown
    public static PropertyId of(int value) {
        return switch (value) {
            case 0x01 -> PAYLOAD_FORMAT_INDICATOR;
            case 0x02 -> MESSAGE_EXPIRY_INTERVAL;
            case 0x03 -> CONTENT_TYPE;
            case 0x08 -> RESPONSE_TOPIC;
            case 0x09 -> CORRELATION_DATA;
            case 0x0B -> SUBSCRIPTION_IDENTIFIER;
            case 0x11 -> SESSION_EXPIRY_INTERVAL;
            case 0x12 -> ASSIGNED_CLIENT_IDENTIFIER;
            case 0x13 -> SERVER_KEEP_ALIVE;
            case 0x15 -> AUTHENTICATION_METHOD;
            case 0x16 -> AUTHENTICATION_DATA;
            case 0x17 -> REQUEST_PROBLEM_INFORMATION;
            case 0x18 -> WILL_DELAY_INTERVAL;
            case 0x19 -> REQUEST_RESPONSE_INFORMATION;
            case 0x1A -> RESPONSE_INFORMATION;
            case 0x1C -> SERVER_REFERENCE;
            case 0x1F -> REASON_STRING;
            case 0x21 -> RECEIVE_MAXIMUM;
            case 0x22 -> TOPIC_ALIAS_MAXIMUM;
            case 0x23 -> TOPIC_ALIAS;
            case 0x24 -> MAXIMUM_QOS;
            case 0x25 -> RETAIN_AVAILABLE;
            case 0x26 -> USER_PROPERTY;
            case 0x27 -> MAXIMUM_PACKET_SIZE;
            case 0x28 -> WILDCARD_SUBSCRIPTION_AVAILABLE;
            case 0x29 -> SUBSCRIPTION_IDENTIFIER_AVAILABLE;
            case 0x2A -> SHARED_SUBSCRIPTION_AVAILABLE;
            default -> throw new IllegalArgumentException("Unknown MQTT property: " + value);
        };
    }

    Kind kind() {
        return kind;
    }

    void requireKind(Kind expected) {
        if (kind != expected) {
            throw new IllegalArgumentException(this + " requires " + kind);
        }
    }

    boolean allowedIn(PacketType type) {
        return switch (this) {
            case PAYLOAD_FORMAT_INDICATOR,
                 MESSAGE_EXPIRY_INTERVAL,
                 CONTENT_TYPE,
                 RESPONSE_TOPIC,
                 CORRELATION_DATA,
                 TOPIC_ALIAS -> {
                yield type == PacketType.PUBLISH;
            }
            case SUBSCRIPTION_IDENTIFIER -> {
                yield type == PacketType.PUBLISH || type == PacketType.SUBSCRIBE;
            }
            case SESSION_EXPIRY_INTERVAL -> {
                yield type == PacketType.CONNECT
                        || type == PacketType.CONNACK
                        || type == PacketType.DISCONNECT;
            }
            case ASSIGNED_CLIENT_IDENTIFIER,
                 SERVER_KEEP_ALIVE,
                 RESPONSE_INFORMATION,
                 MAXIMUM_QOS,
                 RETAIN_AVAILABLE,
                 WILDCARD_SUBSCRIPTION_AVAILABLE,
                 SUBSCRIPTION_IDENTIFIER_AVAILABLE,
                 SHARED_SUBSCRIPTION_AVAILABLE -> {
                yield type == PacketType.CONNACK;
            }
            case AUTHENTICATION_METHOD,
                 AUTHENTICATION_DATA -> {
                yield type == PacketType.CONNECT
                        || type == PacketType.CONNACK
                        || type == PacketType.AUTH;
            }
            case REQUEST_PROBLEM_INFORMATION,
                 REQUEST_RESPONSE_INFORMATION -> {
                yield type == PacketType.CONNECT;
            }
            case WILL_DELAY_INTERVAL -> {
                yield false;
            }
            case SERVER_REFERENCE -> {
                yield type == PacketType.CONNACK || type == PacketType.DISCONNECT;
            }
            case REASON_STRING -> {
                yield switch (type) {
                    case CONNACK,
                         PUBACK,
                         PUBREC,
                         PUBREL,
                         PUBCOMP,
                         SUBACK,
                         UNSUBACK,
                         DISCONNECT,
                         AUTH -> true;
                    case CONNECT,
                         PUBLISH,
                         SUBSCRIBE,
                         UNSUBSCRIBE,
                         PINGREQ,
                         PINGRESP -> false;
                };
            }
            case RECEIVE_MAXIMUM,
                 TOPIC_ALIAS_MAXIMUM,
                 MAXIMUM_PACKET_SIZE -> {
                yield type == PacketType.CONNECT || type == PacketType.CONNACK;
            }
            case USER_PROPERTY -> {
                yield type != PacketType.PINGREQ && type != PacketType.PINGRESP;
            }
        };
    }

    boolean allowedInWill() {
        return switch (this) {
            case PAYLOAD_FORMAT_INDICATOR,
                 MESSAGE_EXPIRY_INTERVAL,
                 CONTENT_TYPE,
                 RESPONSE_TOPIC,
                 CORRELATION_DATA,
                 WILL_DELAY_INTERVAL,
                 USER_PROPERTY -> true;
            case SUBSCRIPTION_IDENTIFIER,
                 SESSION_EXPIRY_INTERVAL,
                 ASSIGNED_CLIENT_IDENTIFIER,
                 SERVER_KEEP_ALIVE,
                 AUTHENTICATION_METHOD,
                 AUTHENTICATION_DATA,
                 REQUEST_PROBLEM_INFORMATION,
                 REQUEST_RESPONSE_INFORMATION,
                 RESPONSE_INFORMATION,
                 SERVER_REFERENCE,
                 REASON_STRING,
                 RECEIVE_MAXIMUM,
                 TOPIC_ALIAS_MAXIMUM,
                 TOPIC_ALIAS,
                 MAXIMUM_QOS,
                 RETAIN_AVAILABLE,
                 MAXIMUM_PACKET_SIZE,
                 WILDCARD_SUBSCRIPTION_AVAILABLE,
                 SUBSCRIPTION_IDENTIFIER_AVAILABLE,
                 SHARED_SUBSCRIPTION_AVAILABLE -> false;
        };
    }
}
