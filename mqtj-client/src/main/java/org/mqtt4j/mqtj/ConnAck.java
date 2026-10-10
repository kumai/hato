package org.mqtt4j.mqtj;

import org.jspecify.annotations.Nullable;

/// The broker's successful connection acknowledgement.
public final class ConnAck {

    private final boolean sessionPresent;

    private ConnAck(boolean sessionPresent) {
        this.sessionPresent = sessionPresent;
    }

    /// Describes a successful acknowledgement and whether the broker resumed a session.
    public static ConnAck of(boolean sessionPresent) {
        return new ConnAck(sessionPresent);
    }

    /// Whether the broker resumed an existing session.
    public boolean sessionPresent() {
        return sessionPresent;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof ConnAck that && sessionPresent == that.sessionPresent;
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(sessionPresent);
    }

    @Override
    public String toString() {
        return "ConnAck[sessionPresent=" + sessionPresent + "]";
    }
}
