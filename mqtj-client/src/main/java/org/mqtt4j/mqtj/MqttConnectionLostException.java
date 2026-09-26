package org.mqtt4j.mqtj;

import java.io.Serial;

/// A connection ended before an operation's outcome could be confirmed.
/// Locally detected packet failures carry [MqttProtocolException] as their cause.
public final class MqttConnectionLostException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    public MqttConnectionLostException() {
        super("Connection ended before the operation completed");
    }

    public MqttConnectionLostException(Throwable cause) {
        super("Connection ended before the operation completed", cause);
    }
}
