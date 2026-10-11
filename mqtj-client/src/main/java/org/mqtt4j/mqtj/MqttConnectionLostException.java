package org.mqtt4j.mqtj;

import java.io.Serial;

/// A connection ended before an operation's outcome could be confirmed. This includes an
/// unacknowledged publication, whose outcome is unknown even if its exchange is later discarded.
/// In a persistent session, QoS 1 exchanges survive connection termination in the same client.
/// Its next successful [MqttClient#connect()] reports [ConnAck#sessionPresent()] true when those
/// exchanges resume, or false when they are discarded without replay. Previously failed futures
/// remain failed; neither Session Present value proves that a publication was never received.
/// Closing the client discards its local exchanges.
///
/// An initial subscribe awaiting SUBACK or an unsubscribe awaiting removal also fails this way,
/// including a call observing removal scheduled by [Subscription#close()]. That removal remains
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
