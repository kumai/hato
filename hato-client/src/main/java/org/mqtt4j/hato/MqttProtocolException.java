package org.mqtt4j.hato;

import java.io.Serial;

import static java.util.Objects.requireNonNull;

/// A locally detected failure to process an incoming MQTT packet.
/// The reason distinguishes malformed input, protocol rule violations, and receive size limits.
/// [ReasonCode#PACKET_TOO_LARGE] can indicate a local limit rather than a peer protocol violation.
///
/// Reported as [Disconnection#cause] or as the cause of [MqttConnectException] or
/// [MqttConnectionLostException]. An underlying cause is retained for diagnostics;
/// applications should use this exception and [#reasonCode] to classify the failure.
public final class MqttProtocolException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ReasonCode reasonCode;

    /// @throws IllegalArgumentException if the reason is not `MALFORMED_PACKET`,
    ///         `PROTOCOL_ERROR`, or `PACKET_TOO_LARGE`
    public MqttProtocolException(String message, ReasonCode reasonCode) {
        super(requireNonNull(message, "message must not be null"));
        this.reasonCode = requireNonNull(reasonCode, "reasonCode must not be null");
        if (reasonCode != ReasonCode.MALFORMED_PACKET
                && reasonCode != ReasonCode.PROTOCOL_ERROR
                && reasonCode != ReasonCode.PACKET_TOO_LARGE
        ) {
            throw new IllegalArgumentException(
                    "Expected MALFORMED_PACKET, PROTOCOL_ERROR, or PACKET_TOO_LARGE"
            );
        }
    }

    /// @throws IllegalArgumentException if the reason is not `MALFORMED_PACKET`,
    ///         `PROTOCOL_ERROR`, or `PACKET_TOO_LARGE`
    public MqttProtocolException(
            String message,
            ReasonCode reasonCode,
            Throwable cause
    ) {
        this(message, reasonCode);
        initCause(requireNonNull(cause, "cause must not be null"));
    }

    /// Returns the local failure classification, including for MQTT 3.1.1 connections.
    /// This is not a reason code received from the broker.
    public ReasonCode reasonCode() {
        return reasonCode;
    }
}
