package org.mqtt4j.hato;

import java.io.Serial;

import static java.util.Objects.requireNonNull;

/// The broker refused a subscription, or would refuse it. A refusal arrives in SUBACK with a
/// reason code of 0x80 or greater; the MQTT 3.1.1 failure (`0x80`) is represented by
/// [ReasonCode#UNSPECIFIED_ERROR]. A subscription that the broker announced it does not support
/// is not sent and fails with the reason code that the broker would use:
/// [ReasonCode#WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED] without
/// [ConnAck#wildcardSubscriptionAvailable()], and
/// [ReasonCode#SHARED_SUBSCRIPTIONS_NOT_SUPPORTED] without
/// [ConnAck#sharedSubscriptionAvailable()]. Either way the failed subscription is removed
/// locally, and the connection goes on.
public final class MqttSubscriptionRefusedException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ReasonCode reasonCode;

    public MqttSubscriptionRefusedException(ReasonCode reasonCode) {
        super("Subscription refused: " + requireNonNull(reasonCode, "reasonCode must not be null"));
        this.reasonCode = reasonCode;
    }

    /// The reason for the refusal, received in SUBACK, or the one that the broker would use for
    /// a subscription that it announced it does not support.
    public ReasonCode reasonCode() {
        return reasonCode;
    }
}
