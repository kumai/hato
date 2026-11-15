package org.mqtt4j.hato;

import java.io.Serial;

/// An operation requires an established connection. Publications fail this way while the
/// client is disconnected, attempting to connect, waiting to reconnect, or completing a
/// requested disconnect; they are not queued for a later connection.
public final class MqttNotConnectedException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    /// A failure of an operation attempted without an established connection.
    public MqttNotConnectedException() {
        super("Client is not connected");
    }
}
