package org.mqtt4j.hato;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// Successful completion of a publication: the transport write for QoS 0, receipt of PUBACK
/// for QoS 1, or receipt of PUBCOMP for QoS 2. None confirms completion of any subscriber's
/// application processing.
public final class PublishResult {

    // The two results that a successful PUBACK or PUBREC can report, shared by every
    // publication.
    private static final PublishResult SUCCESS = new PublishResult(ReasonCode.SUCCESS);

    private static final PublishResult NO_MATCHING_SUBSCRIBERS =
            new PublishResult(ReasonCode.NO_MATCHING_SUBSCRIBERS);

    private final ReasonCode reasonCode;

    private PublishResult(ReasonCode reasonCode) {
        this.reasonCode = reasonCode;
    }

    /// Describes a successfully completed publication with [ReasonCode#SUCCESS].
    public static PublishResult of() {
        return SUCCESS;
    }

    /// Describes a successfully completed publication with `reasonCode`.
    ///
    /// @throws IllegalArgumentException unless `reasonCode` is [ReasonCode#SUCCESS] or
    ///     [ReasonCode#NO_MATCHING_SUBSCRIBERS]
    public static PublishResult of(ReasonCode reasonCode) {
        return switch (requireNonNull(reasonCode, "reasonCode must not be null")) {
            case SUCCESS -> SUCCESS;
            case NO_MATCHING_SUBSCRIBERS -> NO_MATCHING_SUBSCRIBERS;
            default -> throw new IllegalArgumentException(
                    "A successful publication reports SUCCESS or NO_MATCHING_SUBSCRIBERS, not "
                            + reasonCode
            );
        };
    }

    /// The reason code of the MQTT 5.0 broker's acknowledgement: that of PUBACK for QoS 1, and
    /// that of PUBREC for QoS 2. [ReasonCode#NO_MATCHING_SUBSCRIBERS] means that the broker
    /// accepted the message but had no subscription to deliver it to. Brokers may also report
    /// [ReasonCode#SUCCESS] in that case. QoS 0 and MQTT 3.1.1 report [ReasonCode#SUCCESS].
    public ReasonCode reasonCode() {
        return reasonCode;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof PublishResult that && reasonCode == that.reasonCode;
    }

    @Override
    public int hashCode() {
        return reasonCode.hashCode();
    }

    @Override
    public String toString() {
        return "PublishResult[reasonCode=" + reasonCode + "]";
    }
}
