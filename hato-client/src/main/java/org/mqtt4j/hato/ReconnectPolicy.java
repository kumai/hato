package org.mqtt4j.hato;

import java.time.Duration;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.ReconnectBackoff;
import org.mqtt4j.hato.client.internal.RetryRules;

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
/// slow down. Reconnecting stops when the broker's reason code, in a CONNACK refusal or an
/// MQTT 5.0 DISCONNECT, means that the same connection would be refused again or that the
/// client should keep away or use another server: an unsupported protocol version, an invalid
/// client identifier, a ban, an unsupported authentication method, a session taken over by a
/// client with the same identifier, an administrative action, a redirection, and the reason
/// codes for malformed packets, protocol errors, invalid topics or payloads, and broker limits
/// or features that the client exceeded (0x81, 0x82, 0x8F, 0x90, 0x93 to 0x95, 0x99 to 0x9B,
/// 0x9E, 0xA1, and 0xA2). It also stops when the client's
/// [timer scheduler][org.mqtt4j.hato.spi.TimerScheduler] fails. Other refusals and failures
/// are retried, including bad credentials, missing authorization, an unspecified error, and a
/// reason code that this client does not know.
/// [ConnectionListener#onConnectFailed(Disconnection)] reports each failed attempt, so an
/// application that wants to stop on a particular cause can call [MqttClient#disconnect()].
///
/// Two of these classifications depend on how an application is run, and a policy can change
/// them: [#withAdministrativeActionRetry()] also reconnects after an administrative action, and
/// [#withoutUnspecifiedErrorRetry()] stops at an unspecified error. Only MQTT 5.0 brokers send
/// these reason codes, so the two settings change nothing on MQTT 3.1.1 connections. Every
/// other classification is fixed.
///
/// By default, the first failure of a connection request ends that request and fails its
/// [MqttClient#connect()] future, because it usually reveals a configuration error at startup.
/// [#withInitialRetry()] retries that failure with the same backoff instead, unless its reason
/// code stops reconnecting.
///
/// Instances are immutable values with equality.
public final class ReconnectPolicy {

    private static final Duration MAX_DELAY = Duration.ofMillis(Integer.MAX_VALUE);

    private static final ReconnectPolicy DISABLED =
            new ReconnectPolicy(null, null, RetryRules.DEFAULT);

    private final @Nullable Duration initialDelay;

    private final @Nullable Duration maxDelay;

    private final RetryRules rules;

    private ReconnectPolicy(
            @Nullable Duration initialDelay,
            @Nullable Duration maxDelay,
            RetryRules rules
    ) {
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
        this.rules = rules;
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
        return new ReconnectPolicy(initialDelay, maxDelay, RetryRules.DEFAULT);
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
        return with(rules.withInitialRetry());
    }

    /// Returns a policy that also reconnects after an MQTT 5.0 broker ends the connection with
    /// DISCONNECT 0x98 (Administrative action), with the same backoff as after other failures.
    /// By default, the client stays disconnected after an administrative action, because an
    /// administrator ended the connection on purpose.
    ///
    /// @throws IllegalStateException for [#disabled()], which never retries
    public ReconnectPolicy withAdministrativeActionRetry() {
        return with(rules.withAdministrativeActionRetry());
    }

    /// Returns a policy that stops reconnecting when an MQTT 5.0 broker refuses a connection or
    /// ends one with reason code 0x80 (Unspecified error), in CONNACK or DISCONNECT. By default,
    /// the client retries an unspecified error, because the reason code does not say whether its
    /// cause lasts.
    ///
    /// @throws IllegalStateException for [#disabled()], which never retries
    public ReconnectPolicy withoutUnspecifiedErrorRetry() {
        return with(rules.withoutUnspecifiedErrorRetry());
    }

    private ReconnectPolicy with(RetryRules changed) {
        if (initialDelay == null) {
            throw new IllegalStateException("A disabled policy does not retry");
        }
        return new ReconnectPolicy(initialDelay, maxDelay, changed);
    }

    /// A fresh delay sequence for one client, which draws from its own random source.
    ReconnectBackoff backoff() {
        return (initialDelay == null)
                ? ReconnectBackoff.disabled()
                : ReconnectBackoff.exponential(initialDelay, requireNonNull(maxDelay));
    }

    RetryRules rules() {
        return rules;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof ReconnectPolicy that
                && Objects.equals(initialDelay, that.initialDelay)
                && Objects.equals(maxDelay, that.maxDelay)
                && rules.equals(that.rules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(initialDelay, maxDelay, rules);
    }

    @Override
    public String toString() {
        if (initialDelay == null) {
            return "ReconnectPolicy[disabled]";
        }
        return "ReconnectPolicy[initialDelay=" + initialDelay + ", maxDelay=" + maxDelay
                + ", initialRetry=" + rules.initialRetry()
                + ", administrativeActionRetry=" + rules.administrativeActionRetry()
                + ", unspecifiedErrorRetry=" + rules.unspecifiedErrorRetry() + "]";
    }
}
