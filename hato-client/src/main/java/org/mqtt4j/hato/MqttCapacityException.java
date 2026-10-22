package org.mqtt4j.hato;

import java.io.Serial;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// A bounded resource has no available capacity.
///
/// As an operation's failure, this means the operation was not admitted; the rejection
/// does not close an existing connection, and [#limit()] names the limit that refused it. An
/// observation that fails this way, such as one returned by [Subscription#unsubscribe()],
/// leaves the request it observes in place.
///
/// As a [Disconnection#cause], or the cause of [MqttConnectException] or
/// [MqttConnectionLostException], this means overload ended the affected connection, and
/// [#limit()] is empty; [ClientStats#overloads()] counts such endings by cause.
/// Pending operations fail with that connection's termination.
public final class MqttCapacityException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final @Nullable CapacityLimit limit;

    /// Describes an overload that ended a connection.
    public MqttCapacityException(String message) {
        super(message);
        limit = null;
    }

    /// Describes an operation that `limit` refused.
    public MqttCapacityException(String message, CapacityLimit limit) {
        super(message);
        this.limit = requireNonNull(limit, "limit");
    }

    /// The limit that refused the operation, or empty when overload ended a connection.
    public Optional<CapacityLimit> limit() {
        return Optional.ofNullable(limit);
    }
}
