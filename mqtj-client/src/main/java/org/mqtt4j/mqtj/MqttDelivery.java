package org.mqtt4j.mqtj;

/// An application delivery for one subscription. Matching subscriptions share the immutable
/// message but have independent acknowledgement conditions.
public interface MqttDelivery {

    /// The received message, whose payload accessor returns an independent copy.
    MqttMessage message();

    /// Whether the broker marked the publication as a duplicate. Always false for QoS 0.
    boolean dup();

    /// Acknowledges this subscription's QoS 1 delivery in [AckMode#MANUAL] mode.
    /// Safe to call from any thread, including before the handler returns. Repeated calls,
    /// QoS 0, other acknowledgement modes, and calls after the connection ends have no effect.
    /// This does not wait for a PUBACK write or acknowledge another matching subscription.
    /// A push delivery keeps its subscription capacity until its handler returns and its PUBACK
    /// can be sent; a pull delivery releases it at this call. Either way, the PUBACK waits for
    /// other matching subscriptions and for earlier QoS 1 messages; see [AckMode].
    void ack();
}
