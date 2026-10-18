package org.mqtt4j.hato;

import java.time.Duration;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.ReconnectBackoff;

import static java.util.Objects.requireNonNull;

/// Decides whether and when a client connects again after an established connection ends
/// without a request to stop. The policy applies only while the application's connection
/// request stands: [MqttClient#disconnect()] and [MqttClient#close()] withdraw it, and so does
/// a handler failure that stops the client; see [MqttSubscriber].
///
/// With exponential backoff, the delay before the first reconnection attempt is chosen
/// uniformly at random between zero and `initialDelay`, and each further consecutive attempt
/// doubles that bound up to `maxDelay`, without a limit on the number of attempts. A connection
/// that stays established for a while resets the sequence; a successful CONNACK alone does not,
/// so two clients that share a client identifier and keep taking over each other's connection
/// slow down. Reconnecting stops at a refusal that the same CONNECT would meet again
/// (unsupported protocol version or identifier rejected), at a broker session without local
/// state (see [MqttClient#connect()]), and when the client's
/// [timer scheduler][org.mqtt4j.hato.spi.TimerScheduler] fails. Other refusals and failures,
/// including bad credentials or missing authorization, are retried.
/// [ConnectionListener#onConnectFailed(Disconnection)] reports each failed attempt, so an
/// application that wants to stop on a particular cause can call [MqttClient#disconnect()].
///
/// By default, the first failure of a connection request ends that request and fails its
/// [MqttClient#connect()] future, because it usually reveals a configuration error at startup.
/// [#withInitialRetry()] retries that failure with the same backoff instead.
///
/// Instances are immutable values with equality.
public final class ReconnectPolicy {

    private static final Duration MAX_DELAY = Duration.ofMillis(Integer.MAX_VALUE);

    private static final ReconnectPolicy DISABLED = new ReconnectPolicy(null, null, false);

    private final @Nullable Duration initialDelay;

    private final @Nullable Duration maxDelay;

    private final boolean initialRetry;

    private ReconnectPolicy(
            @Nullable Duration initialDelay,
            @Nullable Duration maxDelay,
            boolean initialRetry
    ) {
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
        this.initialRetry = initialRetry;
    }

    /// Reconnects with exponential backoff and full jitter, doubling the bound of the random
    /// delay from `initialDelay` up to `maxDelay`. Both must be positive, `maxDelay` must not be
    /// shorter than `initialDelay`, and neither may exceed 2147483647 milliseconds.
    ///
    /// @throws IllegalArgumentException if a delay is out of range
    public static ReconnectPolicy exponentialBackoff(Duration initialDelay, Duration maxDelay) {
        requireNonNull(initialDelay, "initialDelay");
        requireNonNull(maxDelay, "maxDelay");
        if (!initialDelay.isPositive()
                || maxDelay.compareTo(initialDelay) < 0
                || maxDelay.compareTo(MAX_DELAY) > 0) {
            throw new IllegalArgumentException(
                    "Reconnect delays must be positive, ordered, and <= "
                            + MAX_DELAY.toMillis() + " milliseconds"
            );
        }
        return new ReconnectPolicy(initialDelay, maxDelay, false);
    }

    /// Never reconnects automatically. A connection that ends leaves the client disconnected
    /// until the application calls [MqttClient#connect()] again.
    public static ReconnectPolicy disabled() {
        return DISABLED;
    }

    /// Returns a policy that also retries a connection request's failures before its first
    /// successful connection, so its [MqttClient#connect()] future waits, possibly without
    /// limit, until a connection is established or the client gives the request up.
    ///
    /// @throws IllegalStateException for [#disabled()], which never retries
    public ReconnectPolicy withInitialRetry() {
        if (initialDelay == null) {
            throw new IllegalStateException("A disabled policy does not retry");
        }
        return new ReconnectPolicy(initialDelay, maxDelay, true);
    }

    /// A fresh delay sequence for one client, which draws from its own random source.
    ReconnectBackoff backoff() {
        return (initialDelay == null)
                ? ReconnectBackoff.disabled()
                : ReconnectBackoff.exponential(initialDelay, requireNonNull(maxDelay));
    }

    boolean initialRetry() {
        return initialRetry;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof ReconnectPolicy that
                && Objects.equals(initialDelay, that.initialDelay)
                && Objects.equals(maxDelay, that.maxDelay)
                && initialRetry == that.initialRetry;
    }

    @Override
    public int hashCode() {
        return Objects.hash(initialDelay, maxDelay, initialRetry);
    }

    @Override
    public String toString() {
        if (initialDelay == null) {
            return "ReconnectPolicy[disabled]";
        }
        return "ReconnectPolicy[initialDelay=" + initialDelay + ", maxDelay=" + maxDelay
                + ", initialRetry=" + initialRetry + "]";
    }
}
