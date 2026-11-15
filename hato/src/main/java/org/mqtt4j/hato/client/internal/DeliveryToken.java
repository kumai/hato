package org.mqtt4j.hato.client.internal;

/// A receipt's origin remains available after its PUBACK or PUBREC is released, including QoS 0.
/// The acknowledgement token is zero for QoS 0, otherwise unique for the client's lifetime.
/// `receivedAt` is when the transport handed the PUBLISH to the client, on the client's clock,
/// for the timing of its deliveries; it is [#UNTIMED] when no listener asked for that timing.
record DeliveryToken(Origin origin, long acknowledgement, long receivedAt) {

    /// The reception time of a PUBLISH whose reception was not timed.
    static final long UNTIMED = Long.MIN_VALUE;

    long session() {
        return origin.session();
    }

    long connection() {
        return origin.connection();
    }

    long attempt() {
        return origin.attempt();
    }

    /// What the receptions of one connection share. The session number identifies the receiving
    /// session without exposing its actor-owned state. The attempt is the number of the
    /// connection attempt that established the receiving connection, as
    /// [org.mqtt4j.hato.ConnAck#connectionAttempt()] reports it.
    record Origin(long session, long connection, long attempt) {
    }
}
