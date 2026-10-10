package org.mqtt4j.mqtj;

import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/// The end of an established connection. An empty cause means no failure is reported;
/// it does not indicate a graceful MQTT disconnect.
/// `userRequested` identifies disconnect or close, including a failed DISCONNECT write.
/// These fields do not distinguish a successful disconnect from an immediate close.
/// Locally detected packet failures use [MqttProtocolException] as the cause.
/// Equality compares the cause by identity, because exceptions have no value equality.
public final class Disconnection {

    private final @Nullable Throwable cause;

    private final boolean userRequested;

    private Disconnection(@Nullable Throwable cause, boolean userRequested) {
        this.cause = cause;
        this.userRequested = userRequested;
    }

    /// Describes an ending with the original `cause`, or no reported failure when it is null.
    /// `userRequested` is true when disconnect or close was requested, even if that failed.
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public static Disconnection of(@Nullable Throwable cause, boolean userRequested) {
        return new Disconnection(cause, userRequested);
    }

    /// The original failure, or empty when no failure is reported.
    public Optional<Throwable> cause() {
        return Optional.ofNullable(cause);
    }

    /// Whether disconnect or close was requested by the application.
    public boolean userRequested() {
        return userRequested;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof Disconnection that
                && Objects.equals(cause, that.cause)
                && userRequested == that.userRequested;
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hashCode(cause) + Boolean.hashCode(userRequested);
    }

    @Override
    public String toString() {
        return "Disconnection[cause=" + cause + ", userRequested=" + userRequested + "]";
    }
}
