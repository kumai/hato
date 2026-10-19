package org.mqtt4j.hato.client.internal;

/// A receipt's origin remains available after its PUBACK or PUBREC is released, including QoS 0.
/// The session number identifies the receiving session without exposing its actor-owned state.
/// The attempt is the number of the connection attempt that established the receiving
/// connection, as [org.mqtt4j.hato.ConnAck#connectionAttempt()] reports it.
/// The acknowledgement token is zero for QoS 0, otherwise unique for the client's lifetime.
record DeliveryToken(long session, long connection, long attempt, long acknowledgement) {
}
