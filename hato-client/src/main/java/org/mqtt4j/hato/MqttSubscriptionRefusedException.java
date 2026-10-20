package org.mqtt4j.hato;

import java.io.Serial;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

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
@SuppressWarnings("exports") // Nullness annotations are compile-time only.
public final class MqttSubscriptionRefusedException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ReasonCode reasonCode;

    private final @Nullable String reasonString;

    /// A refusal without an explanation, such as the one that the client makes itself.
    public MqttSubscriptionRefusedException(ReasonCode reasonCode) {
        this(reasonCode, null);
    }

    /// A refusal with the broker's explanation, if the broker sent one.
    public MqttSubscriptionRefusedException(
            ReasonCode reasonCode,
            @Nullable String reasonString
    ) {
        super(message(reasonCode, reasonString));
        this.reasonCode = reasonCode;
        this.reasonString = reasonString;
    }

    /// The reason for the refusal, received in SUBACK, or the one that the broker would use for
    /// a subscription that it announced it does not support.
    public ReasonCode reasonCode() {
        return reasonCode;
    }

    /// The broker's explanation for people diagnosing the refusal, received in SUBACK, or
    /// empty when the broker sent none or the client refused without sending.
    public Optional<String> reasonString() {
        return Optional.ofNullable(reasonString);
    }

    private static String message(ReasonCode reasonCode, @Nullable String reasonString) {
        String message = "Subscription refused: "
                + requireNonNull(reasonCode, "reasonCode must not be null");
        return reasonString == null ? message : message + " (" + reasonString + ")";
    }
}
