package org.mqtt4j.mqtj;

import java.io.Serial;

import static java.util.Objects.requireNonNull;

/// The broker refused a subscription. MQTT 3.1.1 SUBACK failure (`0x80`) is represented by
/// [ReasonCode#UNSPECIFIED_ERROR]. The failed subscription is removed locally.
public final class MqttSubscribeException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ReasonCode reasonCode;

    public MqttSubscribeException(ReasonCode reasonCode) {
        super("Broker refused subscription: " + requireNonNull(reasonCode,
                "reasonCode must not be null"));
        this.reasonCode = reasonCode;
    }

    /// The refusal reason received in SUBACK.
    public ReasonCode reasonCode() {
        return reasonCode;
    }
}
