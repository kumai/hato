package org.mqtt4j.hato;

import java.io.Serial;

import static java.util.Objects.requireNonNull;

/// The broker refused a subscription: its SUBACK carried a reason code of 0x80 or greater. The
/// MQTT 3.1.1 failure (`0x80`) is represented by [ReasonCode#UNSPECIFIED_ERROR]. The failed
/// subscription is removed locally, and the connection goes on.
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
