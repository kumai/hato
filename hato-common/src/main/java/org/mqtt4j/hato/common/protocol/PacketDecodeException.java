package org.mqtt4j.hato.common.protocol;

import java.io.Serial;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.common.value.ReasonCode;

import static java.util.Objects.requireNonNull;

/// Indicates that received bytes cannot be decoded into an acceptable MQTT packet.
///
/// It is thrown while a received packet is framed and decoded: by the frame reader for the
/// fixed header and the receive size limit, and by [PacketCodec#decode] for the packet
/// contents. Encoders never throw it; invalid values supplied to an encoder cause
/// [IllegalArgumentException] instead. Stream failures are reported separately as
/// [java.io.IOException].
///
/// The supported reasons are [ReasonCode#MALFORMED_PACKET] (invalid packet structure),
/// [ReasonCode#PROTOCOL_ERROR] (a protocol rule violation), and [ReasonCode#PACKET_TOO_LARGE]
/// (the packet exceeds the configured receive limit). [ReasonCode#TOPIC_ALIAS_INVALID]
/// identifies an invalid MQTT 5.0 Topic Alias.
///
/// MQTT 5.0 reason codes also classify MQTT 3.1.1 failures internally. In MQTT 3.1.1, the
/// receive size limit is local and exceeding it does not necessarily violate the protocol.
/// The connection owner decides how to disconnect and whether to reconnect; this exception
/// does not perform I/O.
///
/// The name avoids [java.net.ProtocolException]: it is an `IOException`, which the receive loop
/// catches as well, so a mistaken import would still compile. `MqttProtocolException` is kept
/// for the public API.
///
/// See [MQTT 5.0, section 4.13: Handling errors](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
public final class PacketDecodeException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ReasonCode reasonCode;

    /// Creates a received-packet failure with no underlying cause.
    ///
    /// @throws IllegalArgumentException if `reasonCode` is not `MALFORMED_PACKET`,
    ///         `PROTOCOL_ERROR`, `PACKET_TOO_LARGE`, or `TOPIC_ALIAS_INVALID`
    public PacketDecodeException(ReasonCode reasonCode, String message) {
        this(reasonCode, message, null);
    }

    /// Creates a received-packet failure, preserving the underlying cause when present.
    ///
    /// @throws IllegalArgumentException if `reasonCode` is not `MALFORMED_PACKET`,
    ///         `PROTOCOL_ERROR`, `PACKET_TOO_LARGE`, or `TOPIC_ALIAS_INVALID`
    public PacketDecodeException(
            ReasonCode reasonCode,
            String message,
            @Nullable Throwable cause
    ) {
        super(requireNonNull(message, "message must not be null"), cause);
        this.reasonCode = requireNonNull(reasonCode, "reasonCode must not be null");
        if (reasonCode != ReasonCode.MALFORMED_PACKET
                && reasonCode != ReasonCode.PROTOCOL_ERROR
                && reasonCode != ReasonCode.PACKET_TOO_LARGE
                && reasonCode != ReasonCode.TOPIC_ALIAS_INVALID) {
            throw new IllegalArgumentException(
                    "Expected MALFORMED_PACKET, PROTOCOL_ERROR, PACKET_TOO_LARGE, "
                            + "or TOPIC_ALIAS_INVALID"
            );
        }
    }

    /// Returns the MQTT 5.0 reason code, also used to classify MQTT 3.1.1 receive failures.
    public ReasonCode reasonCode() {
        return reasonCode;
    }
}
