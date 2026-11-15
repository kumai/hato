package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttBrokerDisconnectException;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.ReasonCode;

/// The application's standing request for a connection, which the client keeps across
/// connection ends while its policy reconnects. The session core owns it on the actor: the
/// request decides whether a failure retries and numbers the attempts, but sends nothing.
final class ConnectionRequest {

    private final ReconnectBackoff backoff;

    private final RetryRules rules;

    private boolean requested;

    // Before the request's first successful connection, a failure ends the request unless the
    // policy also retries initial failures.
    private boolean established;

    // connect() calls waiting for the request's result.
    private final List<Long> connections = new ArrayList<>();

    // disconnect() calls waiting for the pending graceful disconnect.
    private final List<Long> disconnections = new ArrayList<>();

    // The number of the latest attempt. Attempts are numbered per client from one.
    private long attempts;

    ConnectionRequest(ReconnectBackoff backoff, RetryRules rules) {
        this.backoff = backoff;
        this.rules = rules;
    }

    boolean requested() {
        return requested;
    }

    /// Stands the request. A request that was not standing starts over, so its first failure
    /// is again an initial one; one that waits to reconnect keeps its history.
    void raise() {
        if (!requested) {
            established = false;
        }
        requested = true;
    }

    void withdraw() {
        requested = false;
    }

    /// A successful CONNACK has met the request at least once.
    void establish() {
        established = true;
    }

    long nextAttempt() {
        return ++attempts;
    }

    long attempt() {
        return attempts;
    }

    void awaitConnection(long operation) {
        connections.add(operation);
    }

    /// Returns and forgets the waiting connect() calls, which their caller completes.
    List<Long> takeConnections() {
        List<Long> taken = List.copyOf(connections);
        connections.clear();
        return taken;
    }

    void awaitDisconnection(long operation) {
        disconnections.add(operation);
    }

    /// Returns and forgets the waiting disconnect() calls, which their caller completes.
    List<Long> takeDisconnections() {
        List<Long> taken = List.copyOf(disconnections);
        disconnections.clear();
        return taken;
    }

    /// Whether the policy reconnects at all.
    boolean reconnects() {
        return backoff.enabled();
    }

    /// Whether a connection failure keeps the request and schedules another attempt.
    boolean retries(@Nullable Throwable cause) {
        return requested
                && backoff.enabled()
                && (established || rules.initialRetry())
                && reconnectable(cause, rules);
    }

    Duration nextDelay() {
        return backoff.next();
    }

    /// A stable connection resets the delay sequence.
    void stable() {
        backoff.reset();
    }

    /// Whether the client may connect again after `cause` ended a connection or an attempt. An
    /// ending without a failure stops; a broker's reason code, in a CONNACK refusal or an MQTT 5.0
    /// DISCONNECT, decides by [#reconnects(ReasonCode, RetryRules)]; every other failure
    /// reconnects.
    static boolean reconnectable(@Nullable Throwable cause, RetryRules rules) {
        return switch (cause) {
            case null -> false;
            case MqttConnectException refusal -> refusal.reasonCode()
                    .map(reason -> reconnects(reason, rules))
                    .orElse(true);
            case MqttBrokerDisconnectException ending -> reconnects(ending.reasonCode(), rules);
            default -> true;
        };
    }

    /// The one table for both protocol versions, which map MQTT 3.1.1 CONNACK return codes to the
    /// MQTT 5.0 reason codes of the same meaning. The client stays away when the same CONNECT or
    /// the same session would meet the refusal again, and when the broker asks it to keep away
    /// or to use another server. Every other reason, including one that a later MQTT version
    /// adds, reconnects: bad credentials and missing authorization too, because each attempt
    /// asks for credentials again. The policy decides the two reasons whose answer depends on how
    /// the application is run: by default, an administrative action keeps the client away,
    /// because an administrator chose it, and an unspecified error reconnects, because it does
    /// not say whether its cause lasts.
    private static boolean reconnects(ReasonCode reason, RetryRules rules) {
        return switch (reason) {
            case ADMINISTRATIVE_ACTION -> rules.administrativeActionRetry();
            case UNSPECIFIED_ERROR -> rules.unspecifiedErrorRetry();
            case MALFORMED_PACKET,
                 PROTOCOL_ERROR,
                 UNSUPPORTED_PROTOCOL_VERSION,
                 CLIENT_IDENTIFIER_NOT_VALID,
                 BANNED,
                 BAD_AUTHENTICATION_METHOD,
                 SESSION_TAKEN_OVER,
                 TOPIC_FILTER_INVALID,
                 TOPIC_NAME_INVALID,
                 RECEIVE_MAXIMUM_EXCEEDED,
                 TOPIC_ALIAS_INVALID,
                 PACKET_TOO_LARGE,
                 PAYLOAD_FORMAT_INVALID,
                 RETAIN_NOT_SUPPORTED,
                 QOS_NOT_SUPPORTED,
                 USE_ANOTHER_SERVER,
                 SERVER_MOVED,
                 SHARED_SUBSCRIPTIONS_NOT_SUPPORTED,
                 SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED,
                 WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED -> false;
            default -> true;
        };
    }
}
