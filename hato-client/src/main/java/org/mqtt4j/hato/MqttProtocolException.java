package org.mqtt4j.hato;

import java.io.Serial;

import static java.util.Objects.requireNonNull;

/// A locally detected failure to process an incoming MQTT packet.
/// The reason distinguishes malformed input, protocol rule violations, receive size limits, and
/// more unacknowledged MQTT 5.0 QoS 1 and QoS 2 publications than the client's Receive Maximum
/// ([ReasonCode#RECEIVE_MAXIMUM_EXCEEDED]). [ReasonCode#PACKET_TOO_LARGE] can indicate a local
/// limit rather than a peer protocol violation.
///
/// Reported as [Disconnection#cause()] or as the cause of [MqttConnectException] or
/// [MqttConnectionLostException]. Before such a failure closes an established MQTT 5.0
/// connection, the client sends the broker DISCONNECT with this reason code, which does not stop
/// the broker from publishing the Will. A QoS 2 publication that the broker acknowledges with
/// PUBACK fails with this exception directly, without ending the connection; see
/// [MqttPublisher#publish(MqttMessage)]. A SUBACK that violates the protocol also ends its
/// subscription with this exception; see [Subscription#established()]. An underlying cause is
/// retained for diagnostics; applications should use this exception and [#reasonCode()] to
/// classify the failure.
public final class MqttProtocolException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ReasonCode reasonCode;

    /// A failure without an underlying cause.
    ///
    /// @param message
    ///     the detail message
    /// @param reasonCode
    ///     the local failure classification
    /// @throws IllegalArgumentException
    ///     if the reason is not [MALFORMED_PACKET][ReasonCode#MALFORMED_PACKET],
    ///     [PROTOCOL_ERROR][ReasonCode#PROTOCOL_ERROR],
    ///     [PACKET_TOO_LARGE][ReasonCode#PACKET_TOO_LARGE],
    ///     [TOPIC_ALIAS_INVALID][ReasonCode#TOPIC_ALIAS_INVALID], or
    ///     [RECEIVE_MAXIMUM_EXCEEDED][ReasonCode#RECEIVE_MAXIMUM_EXCEEDED]
    public MqttProtocolException(String message, ReasonCode reasonCode) {
        super(requireNonNull(message, "message must not be null"));
        this.reasonCode = requireNonNull(reasonCode, "reasonCode must not be null");
        if (reasonCode != ReasonCode.MALFORMED_PACKET
                && reasonCode != ReasonCode.PROTOCOL_ERROR
                && reasonCode != ReasonCode.PACKET_TOO_LARGE
                && reasonCode != ReasonCode.TOPIC_ALIAS_INVALID
                && reasonCode != ReasonCode.RECEIVE_MAXIMUM_EXCEEDED
        ) {
            throw new IllegalArgumentException(
                    "Expected MALFORMED_PACKET, PROTOCOL_ERROR, PACKET_TOO_LARGE, "
                            + "TOPIC_ALIAS_INVALID, or RECEIVE_MAXIMUM_EXCEEDED"
            );
        }
    }

    /// A failure with an underlying cause, which is retained for diagnostics.
    ///
    /// @param message
    ///     the detail message
    /// @param reasonCode
    ///     the local failure classification
    /// @param cause
    ///     the underlying cause
    /// @throws IllegalArgumentException
    ///     if the reason is not [MALFORMED_PACKET][ReasonCode#MALFORMED_PACKET],
    ///     [PROTOCOL_ERROR][ReasonCode#PROTOCOL_ERROR],
    ///     [PACKET_TOO_LARGE][ReasonCode#PACKET_TOO_LARGE],
    ///     [TOPIC_ALIAS_INVALID][ReasonCode#TOPIC_ALIAS_INVALID], or
    ///     [RECEIVE_MAXIMUM_EXCEEDED][ReasonCode#RECEIVE_MAXIMUM_EXCEEDED]
    public MqttProtocolException(
            String message,
            ReasonCode reasonCode,
            Throwable cause
    ) {
        this(message, reasonCode);
        initCause(requireNonNull(cause, "cause must not be null"));
    }

    /// {@return the local failure classification, including for MQTT 3.1.1 connections}
    /// This is not a reason code received from the broker.
    public ReasonCode reasonCode() {
        return reasonCode;
    }
}
