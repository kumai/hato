package org.mqtt4j.hato;

import java.io.Serial;

import static java.util.Objects.requireNonNull;

/// Base class for failures of MQTT operations.
public class MqttException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    /// A failure with a detail message.
    ///
    /// @param message
    ///     the detail message
    protected MqttException(String message) {
        super(message);
    }

    /// A failure with a detail message and a cause.
    ///
    /// @param message
    ///     the detail message
    /// @param cause
    ///     the cause of the failure
    protected MqttException(String message, Throwable cause) {
        super(message, requireNonNull(cause, "cause must not be null"));
    }
}
