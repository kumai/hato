package org.mqtt4j.mqtj.client.internal;

/// A receipt's origin remains available after its PUBACK is released, including for QoS 0.
/// The session number identifies the receiving session without exposing its actor-owned state.
/// The acknowledgement token is zero for QoS 0, otherwise unique for the client's lifetime.
record DeliveryToken(long session, long connection, long acknowledgement) {
}
