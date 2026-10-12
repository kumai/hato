package org.mqtt4j.hato;

import java.io.Serial;

/// An operation requires an established connection.
public final class MqttNotConnectedException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    public MqttNotConnectedException() {
        super("Client is not connected");
    }
}
