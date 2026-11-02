package org.mqtt4j.hato;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/// The end of an established connection, or of a connection attempt that established none.
/// An empty cause means no failure is reported; it does not indicate a graceful MQTT
/// disconnect. [userRequested][#userRequested()] identifies disconnect or close, including a
/// failed DISCONNECT write. These fields do not distinguish a successful disconnect from an
/// immediate close.
/// Locally detected packet failures use [MqttProtocolException] as the cause, and a DISCONNECT
/// from an MQTT 5.0 broker uses [MqttBrokerDisconnectException].
/// [#reconnectDelay()] tells whether the client scheduled another attempt.
/// Equality compares the cause by identity, because exceptions have no value equality.
public final class Disconnection {

    private final @Nullable Throwable cause;

    private final boolean userRequested;

    private final long connectionAttempt;

    private final @Nullable Duration reconnectDelay;

    private Disconnection(
            @Nullable Throwable cause,
            boolean userRequested,
            long connectionAttempt,
            @Nullable Duration reconnectDelay
    ) {
        if (connectionAttempt < 1) {
            throw new IllegalArgumentException("Attempt numbers start at 1");
        }
        if (reconnectDelay != null && reconnectDelay.isNegative()) {
            throw new IllegalArgumentException("Reconnect delay must not be negative");
        }
        this.cause = cause;
        this.userRequested = userRequested;
        this.connectionAttempt = connectionAttempt;
        this.reconnectDelay = reconnectDelay;
    }

    /// The ending of connection attempt number `connectionAttempt` with the original `cause`, or
    /// no reported failure when it is null, after which the client attempts to connect again
    /// after `reconnectDelay`, or does not when it is null. `userRequested` is true when
    /// disconnect or close was requested, even if that failed. Only the client creates endings;
    /// the builder hands this to it.
    static Disconnection of(
            @Nullable Throwable cause,
            boolean userRequested,
            long connectionAttempt,
            @Nullable Duration reconnectDelay
    ) {
        return new Disconnection(cause, userRequested, connectionAttempt, reconnectDelay);
    }

    /// {@return the original failure, or empty when no failure is reported}
    public Optional<Throwable> cause() {
        return Optional.ofNullable(cause);
    }

    /// {@return `true` if the application requested disconnect or close, even if that failed}
    public boolean userRequested() {
        return userRequested;
    }

    /// {@return the number of the attempt that established the ended connection or that
    /// failed} When a stop cancels a wait to reconnect, it is the number reserved for the
    /// cancelled attempt. See [ConnAck#connectionAttempt()].
    public long connectionAttempt() {
        return connectionAttempt;
    }

    /// {@return the delay, counted from when the client handled this ending, until its next
    /// connection attempt, or empty when it will not attempt to connect again unless the
    /// application calls [MqttClient#connect()]} A slow listener can receive the ending after
    /// the delay has passed.
    public Optional<Duration> reconnectDelay() {
        return Optional.ofNullable(reconnectDelay);
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof Disconnection that
                && Objects.equals(cause, that.cause)
                && userRequested == that.userRequested
                && connectionAttempt == that.connectionAttempt
                && Objects.equals(reconnectDelay, that.reconnectDelay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cause, userRequested, connectionAttempt, reconnectDelay);
    }

    @Override
    public String toString() {
        return "Disconnection[cause=" + cause + ", userRequested=" + userRequested
                + ", connectionAttempt=" + connectionAttempt + ", reconnectDelay=" + reconnectDelay
                + "]";
    }
}
