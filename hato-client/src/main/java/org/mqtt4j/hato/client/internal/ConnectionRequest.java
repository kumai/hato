package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.ReasonCode;

/// The application's standing request for a connection, which the client keeps across
/// connection ends while its policy reconnects. The session core owns it on the actor: the
/// request decides whether a failure retries and numbers the attempts, but sends nothing.
final class ConnectionRequest {

    private final ReconnectBackoff backoff;

    private final boolean initialRetry;

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

    ConnectionRequest(ReconnectBackoff backoff, boolean initialRetry) {
        this.backoff = backoff;
        this.initialRetry = initialRetry;
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
                && (established || initialRetry)
                && reconnectable(cause);
    }

    Duration nextDelay() {
        return backoff.next();
    }

    /// A stable connection resets the delay sequence.
    void stable() {
        backoff.reset();
    }

    /// Only refusals that the same CONNECT meets again, and endings without a failure, stop.
    static boolean reconnectable(@Nullable Throwable cause) {
        if (cause == null) {
            return false;
        }
        if (cause instanceof MqttConnectException failure) {
            return failure.reasonCode()
                    .map(reason ->
                            (reason != ReasonCode.UNSUPPORTED_PROTOCOL_VERSION)
                                    && (reason != ReasonCode.CLIENT_IDENTIFIER_NOT_VALID)
                    )
                    .orElse(true);
        }
        return true;
    }
}
