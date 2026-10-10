package org.mqtt4j.mqtj;

import java.io.Serial;

/// A connection ended before an operation's outcome could be confirmed. This includes an
/// initial subscribe awaiting SUBACK or an unsubscribe awaiting removal, including one
/// observing removal scheduled by [Subscription#close()]. That background removal remains
/// pending after its observer fails. Established subscriptions survive connection termination,
/// and [Subscription#receive()] keeps waiting. A pull subscription throws it only when an
/// internal failure closes the client, carrying that failure as its cause.
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
