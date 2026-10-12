package org.mqtt4j.hato;

import org.jspecify.annotations.Nullable;

/// Successful completion of a publication: the transport write for QoS 0, receipt of PUBACK
/// for QoS 1, or receipt of PUBCOMP for QoS 2. None confirms completion of any subscriber's
/// application processing.
public final class PublishResult {

    // Stateless results are shared; a later field can make of() return new instances.
    private static final PublishResult SUCCESS = new PublishResult();

    private PublishResult() {}

    /// Describes a successfully completed publication.
    public static PublishResult of() {
        return SUCCESS;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof PublishResult;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "PublishResult[]";
    }
}
