package org.mqtt4j.hato.client.internal;

import java.time.Duration;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttCredentials;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static org.mqtt4j.hato.client.internal.SessionCore.protocolError;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// The session core's part for establishing connections: what each connection of an attempt
/// sends in CONNECT, and what its CONNACK means for the attempt. The core keeps the attempt's
/// lifecycle. It opens and closes the connections, sends the CONNECT built here, and acts on
/// the answer. This part keeps no state: the core passes each connection's step, the attempt's
/// credentials, and whether this client object has started a session.
final class ConnectionEstablishment {

    /// The connections of one attempt. A broker session without local state cannot continue,
    /// so an attempt that finds one replaces it, using up to three connections. As MQTT 3.1.1
    /// describes for discarding session state on both sides, it disconnects, connects with Clean
    /// Session 1, and disconnects again; then it starts a new persistent session.
    enum Step {
        /// The attempt's first connection, which may find a broker session without local state.
        FIRST,
        /// A Clean Session 1 connection without a Will, which discards that session.
        DISCARD,
        /// The connection that starts a new persistent session once the old one is discarded.
        NEW_SESSION;

        /// The step of the connection that follows an abandoned one.
        Step next() {
            return this == FIRST ? DISCARD : NEW_SESSION;
        }
    }

    /// What a CONNACK means for the attempt.
    sealed interface Outcome {

        /// The broker refused the connection, or its CONNACK breaks the protocol.
        record Failed(RuntimeException cause) implements Outcome {}

        /// The attempt gives the connection up, closing it with DISCONNECT so that the broker
        /// discards its Will. Without `refusal`, the attempt continues on its next connection
        /// once DISCONNECT is written; with it, the attempt fails then.
        record Abandoned(@Nullable MqttConnectException refusal) implements Outcome {}

        /// The connection is established. It continues the session when `sessionPresent`, and
        /// otherwise starts a new one.
        record Established(boolean sessionPresent) implements Outcome {}
    }

    private static final Log log = Log.of(ConnectionEstablishment.class);

    private final String clientId;

    private final MqttVersion version;

    private final int keepAliveSeconds;

    // Zero session expiry starts a new session with every connection.
    private final boolean cleanStart;

    private final Packet.@Nullable Will will;

    ConnectionEstablishment(
            String clientId,
            MqttVersion version,
            Duration keepAlive,
            Duration sessionExpiry,
            Packet.@Nullable Will will
    ) {
        this.clientId = clientId;
        this.version = version;
        keepAliveSeconds = (int) keepAlive.toSeconds();
        cleanStart = sessionExpiry.isZero();
        this.will = will;
    }

    /// The CONNECT of a connection at `step` that sends the attempt's `credentials`, if any.
    Packet.Connect connect(Step step, @Nullable MqttCredentials credentials) {
        // The connection that discards a broker session carries no Will, so losing it before
        // its DISCONNECT publishes nothing.
        boolean discarding = step == Step.DISCARD;
        return new Packet.Connect(
                clientId,
                version,
                cleanStart || discarding,
                keepAliveSeconds,
                discarding ? null : will,
                credentials == null ? null : credentials.username(),
                credentials == null ? null : credentials.password().orElse(null),
                EMPTY
        );
    }

    /// What `ack` means for the connection at `step`. `sessionEstablished` tells whether this
    /// client object has started a session that a Session Present can continue.
    Outcome accept(Packet.ConnAck ack, Step step, boolean sessionEstablished) {
        if (ack.reasonCode() != ReasonCode.SUCCESS) {
            return new Outcome.Failed(new MqttConnectException(
                    "Broker refused connection",
                    ack.reasonCode()
            ));
        }
        if (ack.sessionPresent() && (cleanStart || step == Step.DISCARD)) {
            return new Outcome.Failed(protocolError("Session Present with Clean Session"));
        }
        if (step == Step.DISCARD) {
            log.info("Discarded broker session without local state for {}", clientId);
            return new Outcome.Abandoned(null);
        }
        if (ack.sessionPresent() && !sessionEstablished && step == Step.FIRST) {
            // The attempt goes on to discard the broker's session.
            return new Outcome.Abandoned(null);
        }
        if (ack.sessionPresent() && !sessionEstablished) {
            // Only one discard per attempt: a session found again fails the attempt.
            return new Outcome.Abandoned(
                    new MqttConnectException("Broker session present after discarding it")
            );
        }
        return new Outcome.Established(ack.sessionPresent());
    }
}
