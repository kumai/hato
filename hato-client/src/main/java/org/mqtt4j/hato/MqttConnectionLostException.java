package org.mqtt4j.hato;

import java.io.Serial;

/// A connection ended before an operation's outcome could be confirmed. This includes an
/// unacknowledged publication, whose outcome is unknown even if its exchange is later discarded.
/// In a persistent session, QoS 1 and QoS 2 exchanges survive connection termination in the
/// same client, and their futures wait instead of failing while the client reconnects
/// automatically. The next connection reports [ConnAck#sessionPresent()] true when those
/// exchanges resume, or false when they are discarded without replay, which fails the waiting
/// futures with this exception. Previously failed futures remain failed; neither Session
/// Present value proves that a publication was never received. Closing the client discards its
/// local exchanges.
///
/// Subscription registrations and removals survive connection termination: their observations
/// do not fail when a connection ends, and [Subscription#receive()] keeps waiting.
/// [Subscription#established()], [Subscription#unsubscribe()], and pull receives report this
/// exception only when an internal failure closes the client, carrying that failure as its cause.
/// Locally detected packet failures carry [MqttProtocolException] as their cause, and a
/// DISCONNECT from an MQTT 5.0 broker carries [MqttBrokerDisconnectException].
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
