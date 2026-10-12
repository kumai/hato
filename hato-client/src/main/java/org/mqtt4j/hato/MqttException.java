package org.mqtt4j.hato;

import java.io.Serial;

import static java.util.Objects.requireNonNull;

/// Base class for failures of MQTT operations.
public class MqttException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    protected MqttException(String message) {
        super(message);
    }

    protected MqttException(String message, Throwable cause) {
        super(message, requireNonNull(cause, "cause must not be null"));
    }
}
