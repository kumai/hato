package org.mqtt4j.hato;

import java.io.Serial;

/// A bounded resource has no available capacity.
///
/// As an operation's failure, this means the operation was not admitted; the rejection
/// does not close an existing connection.
///
/// As a [Disconnection#cause], or the cause of [MqttConnectException] or
/// [MqttConnectionLostException], this means overload ended the affected connection.
/// Pending operations fail with that connection's termination.
public final class MqttCapacityException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    public MqttCapacityException(String message) {
        super(message);
    }
}
