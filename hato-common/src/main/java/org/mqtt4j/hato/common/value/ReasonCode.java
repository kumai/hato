package org.mqtt4j.hato.common.value;

/// MQTT 5.0 reason codes shared by acknowledgements, disconnection, and authentication.
///
/// Each distinct wire value has one constant. In particular, [SUCCESS][#SUCCESS] represents `0x00`:
/// Success in acknowledgements and AUTH, Normal disconnection in DISCONNECT, and Granted
/// QoS 0 in SUBACK. The packet determines which meaning applies.
///
/// A value defined here is not necessarily valid for every packet type. An MQTT 3.1.1
/// connection reports its return codes as the corresponding values here.
///
/// See [MQTT 5.0, section 2.4, Table 2-6: Reason Codes](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
public enum ReasonCode {

    /// Success, normal disconnection, or granted QoS 0, depending on the packet.
    SUCCESS(0x00),

    /// The subscription is accepted with a maximum QoS of 1.
    GRANTED_QOS_1(0x01),

    /// The subscription is accepted with a maximum QoS of 2.
    GRANTED_QOS_2(0x02),

    /// The client requests disconnection and publication of its Will.
    DISCONNECT_WITH_WILL_MESSAGE(0x04),

    /// The publication is accepted but has no matching subscribers.
    NO_MATCHING_SUBSCRIBERS(0x10),

    /// Unsubscription completed without finding a matching subscription.
    NO_SUBSCRIPTION_EXISTED(0x11),

    /// Authentication requires another exchange.
    CONTINUE_AUTHENTICATION(0x18),

    /// The client starts authentication again on an existing connection.
    RE_AUTHENTICATE(0x19),

    /// The operation failed without a more specific reason.
    UNSPECIFIED_ERROR(0x80),

    /// The packet cannot be parsed according to the protocol's format.
    MALFORMED_PACKET(0x81),

    /// The packet violates a protocol rule or the expected exchange sequence.
    PROTOCOL_ERROR(0x82),

    /// The implementation declines an otherwise valid packet.
    IMPLEMENTATION_SPECIFIC_ERROR(0x83),

    /// The server does not support the requested MQTT version.
    UNSUPPORTED_PROTOCOL_VERSION(0x84),

    /// The server rejects the client identifier.
    CLIENT_IDENTIFIER_NOT_VALID(0x85),

    /// The server rejects the supplied user name or password.
    BAD_USER_NAME_OR_PASSWORD(0x86),

    /// The operation is not permitted for this client.
    NOT_AUTHORIZED(0x87),

    /// The server cannot currently accept a connection.
    SERVER_UNAVAILABLE(0x88),

    /// The server cannot currently accept work because it is busy.
    SERVER_BUSY(0x89),

    /// The server has banned the client.
    BANNED(0x8A),

    /// The server is closing the connection as part of its shutdown.
    SERVER_SHUTTING_DOWN(0x8B),

    /// The authentication method is unsupported or inconsistent with the exchange.
    BAD_AUTHENTICATION_METHOD(0x8C),

    /// The server received no packet within the permitted keep-alive interval.
    KEEP_ALIVE_TIMEOUT(0x8D),

    /// Another connection using the same client identifier has replaced this connection.
    SESSION_TAKEN_OVER(0x8E),

    /// The receiver does not accept the topic filter.
    TOPIC_FILTER_INVALID(0x8F),

    /// The receiver does not accept the topic name.
    TOPIC_NAME_INVALID(0x90),

    /// The packet identifier is already assigned to an unfinished exchange.
    PACKET_IDENTIFIER_IN_USE(0x91),

    /// The receiver has no exchange for the packet identifier.
    PACKET_IDENTIFIER_NOT_FOUND(0x92),

    /// Too many QoS 1 or 2 publications are awaiting acknowledgement.
    RECEIVE_MAXIMUM_EXCEEDED(0x93),

    /// The topic alias is zero or exceeds the negotiated maximum.
    TOPIC_ALIAS_INVALID(0x94),

    /// The packet exceeds the receiver's permitted size.
    PACKET_TOO_LARGE(0x95),

    /// The publication rate exceeds the receiver's capacity or policy.
    MESSAGE_RATE_TOO_HIGH(0x96),

    /// A resource or administrative quota has been exhausted.
    QUOTA_EXCEEDED(0x97),

    /// An administrative decision causes disconnection.
    ADMINISTRATIVE_ACTION(0x98),

    /// The payload does not conform to its declared format.
    PAYLOAD_FORMAT_INVALID(0x99),

    /// The server does not support retained publications.
    RETAIN_NOT_SUPPORTED(0x9A),

    /// The requested QoS exceeds what the server supports.
    QOS_NOT_SUPPORTED(0x9B),

    /// The client should try a different server.
    USE_ANOTHER_SERVER(0x9C),

    /// The server has moved to a different address.
    SERVER_MOVED(0x9D),

    /// The server does not support shared subscriptions.
    SHARED_SUBSCRIPTIONS_NOT_SUPPORTED(0x9E),

    /// The rate of new connections exceeds the server's limit.
    CONNECTION_RATE_EXCEEDED(0x9F),

    /// The connection has reached the server's permitted lifetime.
    MAXIMUM_CONNECT_TIME(0xA0),

    /// The server does not support subscription identifiers.
    SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED(0xA1),

    /// The server does not support wildcard subscriptions.
    WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED(0xA2);

    private final int value;

    ReasonCode(int value) {
        this.value = value;
    }

    /// {@return the unsigned byte value used by MQTT 5.0 on the wire}
    public int value() {
        return value;
    }

    /// Resolves an unsigned MQTT 5.0 wire value to its reason code.
    ///
    /// Zero always resolves to [SUCCESS][#SUCCESS], including for DISCONNECT and SUBACK. The
    /// caller must separately check whether the code is permitted for the packet being decoded.
    ///
    /// @param value
    ///     the unsigned byte value, represented as an `int`
    /// @return
    ///     the reason code for that value
    /// @throws IllegalArgumentException
    ///     if `value` is outside 0 to 255 or is undefined
    public static ReasonCode of(int value) {
        return switch (value) {
            case 0x00 -> SUCCESS;
            case 0x01 -> GRANTED_QOS_1;
            case 0x02 -> GRANTED_QOS_2;
            case 0x04 -> DISCONNECT_WITH_WILL_MESSAGE;
            case 0x10 -> NO_MATCHING_SUBSCRIBERS;
            case 0x11 -> NO_SUBSCRIPTION_EXISTED;
            case 0x18 -> CONTINUE_AUTHENTICATION;
            case 0x19 -> RE_AUTHENTICATE;
            case 0x80 -> UNSPECIFIED_ERROR;
            case 0x81 -> MALFORMED_PACKET;
            case 0x82 -> PROTOCOL_ERROR;
            case 0x83 -> IMPLEMENTATION_SPECIFIC_ERROR;
            case 0x84 -> UNSUPPORTED_PROTOCOL_VERSION;
            case 0x85 -> CLIENT_IDENTIFIER_NOT_VALID;
            case 0x86 -> BAD_USER_NAME_OR_PASSWORD;
            case 0x87 -> NOT_AUTHORIZED;
            case 0x88 -> SERVER_UNAVAILABLE;
            case 0x89 -> SERVER_BUSY;
            case 0x8A -> BANNED;
            case 0x8B -> SERVER_SHUTTING_DOWN;
            case 0x8C -> BAD_AUTHENTICATION_METHOD;
            case 0x8D -> KEEP_ALIVE_TIMEOUT;
            case 0x8E -> SESSION_TAKEN_OVER;
            case 0x8F -> TOPIC_FILTER_INVALID;
            case 0x90 -> TOPIC_NAME_INVALID;
            case 0x91 -> PACKET_IDENTIFIER_IN_USE;
            case 0x92 -> PACKET_IDENTIFIER_NOT_FOUND;
            case 0x93 -> RECEIVE_MAXIMUM_EXCEEDED;
            case 0x94 -> TOPIC_ALIAS_INVALID;
            case 0x95 -> PACKET_TOO_LARGE;
            case 0x96 -> MESSAGE_RATE_TOO_HIGH;
            case 0x97 -> QUOTA_EXCEEDED;
            case 0x98 -> ADMINISTRATIVE_ACTION;
            case 0x99 -> PAYLOAD_FORMAT_INVALID;
            case 0x9A -> RETAIN_NOT_SUPPORTED;
            case 0x9B -> QOS_NOT_SUPPORTED;
            case 0x9C -> USE_ANOTHER_SERVER;
            case 0x9D -> SERVER_MOVED;
            case 0x9E -> SHARED_SUBSCRIPTIONS_NOT_SUPPORTED;
            case 0x9F -> CONNECTION_RATE_EXCEEDED;
            case 0xA0 -> MAXIMUM_CONNECT_TIME;
            case 0xA1 -> SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED;
            case 0xA2 -> WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED;
            default -> throw new IllegalArgumentException(
                    "Undefined MQTT 5.0 reason code: " + value
            );
        };
    }
}
