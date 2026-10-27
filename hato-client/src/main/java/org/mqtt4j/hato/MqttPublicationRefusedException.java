package org.mqtt4j.hato;

import java.io.Serial;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// The broker refused a publication, or would refuse it. A refusal arrives in PUBACK or PUBREC
/// with a reason code of 0x80 or greater, such as [ReasonCode#NOT_AUTHORIZED] or
/// [ReasonCode#QUOTA_EXCEEDED]; the broker did not accept the message, and the exchange is over.
/// A publication that exceeds a limit that the broker announced for the connection is not sent
/// at all and fails with the reason code that the broker would use:
/// [ReasonCode#QOS_NOT_SUPPORTED] above [ConnAck#maximumQos()],
/// [ReasonCode#RETAIN_NOT_SUPPORTED] without [ConnAck#retainAvailable()], and
/// [ReasonCode#PACKET_TOO_LARGE] beyond [ConnAck#maximumPacketSize()]. A publication too large
/// for the client to send fails with [ReasonCode#PACKET_TOO_LARGE] in the same way, with any
/// broker. Either way the connection goes on. Only MQTT 5.0 brokers refuse publications or
/// announce these limits.
@SuppressWarnings("exports") // Nullness annotations are compile-time only.
public final class MqttPublicationRefusedException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ReasonCode reasonCode;

    private final @Nullable String reasonString;

    /// A refusal without an explanation, such as the one that the client makes itself.
    ///
    /// @param reasonCode
    ///     the reason for the refusal
    public MqttPublicationRefusedException(ReasonCode reasonCode) {
        this(reasonCode, null);
    }

    /// A refusal with the broker's explanation, if the broker sent one.
    ///
    /// @param reasonCode
    ///     the reason for the refusal
    /// @param reasonString
    ///     the broker's explanation, or null when it sent none
    public MqttPublicationRefusedException(
            ReasonCode reasonCode,
            @Nullable String reasonString
    ) {
        super(message(reasonCode, reasonString));
        this.reasonCode = reasonCode;
        this.reasonString = reasonString;
    }

    /// {@return the reason for the refusal, received in PUBACK or PUBREC, or the one that the
    /// broker would use for a publication that exceeds its announced limits}
    public ReasonCode reasonCode() {
        return reasonCode;
    }

    /// {@return the broker's explanation for people diagnosing the refusal, received in PUBACK
    /// or PUBREC, or empty when the broker sent none or the client refused without sending}
    public Optional<String> reasonString() {
        return Optional.ofNullable(reasonString);
    }

    private static String message(ReasonCode reasonCode, @Nullable String reasonString) {
        String message = "Publication refused: "
                + requireNonNull(reasonCode, "reasonCode must not be null");
        return reasonString == null ? message : message + " (" + reasonString + ")";
    }
}
