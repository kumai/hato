package org.mqtt4j.hato;

import java.io.Serial;

import static java.util.Objects.requireNonNull;

/// The broker refused a publication, or would refuse it. A refusal arrives in PUBACK or PUBREC
/// with a reason code of 0x80 or greater, such as [ReasonCode#NOT_AUTHORIZED] or
/// [ReasonCode#QUOTA_EXCEEDED]; the broker did not accept the message, and the exchange is over.
/// A publication that exceeds a limit that the broker announced for the connection is not sent
/// at all and fails with the reason code that the broker would use:
/// [ReasonCode#QOS_NOT_SUPPORTED] above [ConnAck#maximumQos()],
/// [ReasonCode#RETAIN_NOT_SUPPORTED] without [ConnAck#retainAvailable()], and
/// [ReasonCode#PACKET_TOO_LARGE] beyond [ConnAck#maximumPacketSize()]. Either way the
/// connection goes on. Only MQTT 5.0 brokers refuse publications or announce these limits.
public final class MqttPublicationRefusedException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ReasonCode reasonCode;

    public MqttPublicationRefusedException(ReasonCode reasonCode) {
        super("Publication refused: " + requireNonNull(reasonCode, "reasonCode must not be null"));
        this.reasonCode = reasonCode;
    }

    /// The reason for the refusal, received in PUBACK or PUBREC, or the one that the broker
    /// would use for a publication that exceeds its announced limits.
    public ReasonCode reasonCode() {
        return reasonCode;
    }
}
