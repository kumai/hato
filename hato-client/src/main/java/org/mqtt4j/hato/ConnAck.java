package org.mqtt4j.hato;

import org.jspecify.annotations.Nullable;

/// The broker's successful connection acknowledgement.
public final class ConnAck {

    private final boolean sessionPresent;

    private final long attempt;

    private ConnAck(boolean sessionPresent, long attempt) {
        this.sessionPresent = sessionPresent;
        this.attempt = attempt;
    }

    /// Describes a successful acknowledgement of connection attempt number `attempt`, and
    /// whether the broker resumed a session.
    ///
    /// @throws IllegalArgumentException if `attempt` is not positive
    public static ConnAck of(boolean sessionPresent, long attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("Attempt numbers start at 1");
        }
        return new ConnAck(sessionPresent, attempt);
    }

    /// Whether the broker resumed an existing session.
    public boolean sessionPresent() {
        return sessionPresent;
    }

    /// The number of the connection attempt that this acknowledgement established. A client
    /// numbers its attempts consecutively from 1, whether the application's
    /// [MqttClient#connect()] or an automatic reconnection started them.
    public long attempt() {
        return attempt;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof ConnAck that
                && sessionPresent == that.sessionPresent
                && attempt == that.attempt;
    }

    @Override
    public int hashCode() {
        return 31 * Boolean.hashCode(sessionPresent) + Long.hashCode(attempt);
    }

    @Override
    public String toString() {
        return "ConnAck[sessionPresent=" + sessionPresent + ", attempt=" + attempt + "]";
    }
}
