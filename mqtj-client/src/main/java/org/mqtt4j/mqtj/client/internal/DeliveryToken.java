package org.mqtt4j.mqtj.client.internal;

/// A receipt's origin remains available after its PUBACK is released, including for QoS 0.
/// The acknowledgement token is zero for QoS 0, otherwise unique for the client's lifetime.
/// Clean Session currently makes the receiving connection the boundary for handler failures.
record DeliveryToken(long connection, long acknowledgement) {
}
