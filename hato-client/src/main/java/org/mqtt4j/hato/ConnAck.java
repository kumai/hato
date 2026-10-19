package org.mqtt4j.hato;

import org.jspecify.annotations.Nullable;

/// The broker's successful connection acknowledgement.
public final class ConnAck {

    private final boolean sessionPresent;

    private final long connectionAttempt;

    private ConnAck(boolean sessionPresent, long connectionAttempt) {
        this.sessionPresent = sessionPresent;
        this.connectionAttempt = connectionAttempt;
    }

    /// Describes a successful acknowledgement of connection attempt number `connectionAttempt`,
    /// and whether the broker resumed a session.
    ///
    /// @throws IllegalArgumentException if `connectionAttempt` is not positive
    public static ConnAck of(boolean sessionPresent, long connectionAttempt) {
        if (connectionAttempt < 1) {
            throw new IllegalArgumentException("Attempt numbers start at 1");
        }
        return new ConnAck(sessionPresent, connectionAttempt);
    }

    /// Whether the broker resumed an existing session.
    public boolean sessionPresent() {
        return sessionPresent;
    }

    /// The number of the connection attempt that this acknowledgement established. A client
    /// numbers its attempts consecutively from 1, whether the application's
    /// [MqttClient#connect()] or an automatic reconnection started them.
    public long connectionAttempt() {
        return connectionAttempt;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof ConnAck that
                && sessionPresent == that.sessionPresent
                && connectionAttempt == that.connectionAttempt;
    }

    @Override
    public int hashCode() {
        return 31 * Boolean.hashCode(sessionPresent) + Long.hashCode(connectionAttempt);
    }

    @Override
    public String toString() {
        return "ConnAck[sessionPresent=" + sessionPresent + ", connectionAttempt="
                + connectionAttempt + "]";
    }
}
