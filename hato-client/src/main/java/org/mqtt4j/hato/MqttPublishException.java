package org.mqtt4j.hato;

import java.io.Serial;

import static java.util.Objects.requireNonNull;

/// The broker refused a QoS 1 or QoS 2 publication: its PUBACK or PUBREC carried a reason code
/// of 0x80 or greater, such as [ReasonCode#NOT_AUTHORIZED] or [ReasonCode#QUOTA_EXCEEDED]. The
/// broker did not accept the message, and the exchange is over. Only MQTT 5.0 brokers refuse
/// publications.
public final class MqttPublishException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ReasonCode reasonCode;

    public MqttPublishException(ReasonCode reasonCode) {
        super("Broker refused publication: " + requireNonNull(reasonCode,
                "reasonCode must not be null"));
        this.reasonCode = reasonCode;
    }

    /// The refusal reason received in PUBACK or PUBREC.
    public ReasonCode reasonCode() {
        return reasonCode;
    }
}
