package org.mqtt4j.hato;

/// An application delivery for one subscription. Each matching subscription has an independent
/// acknowledgement condition.
public interface MqttDelivery {

    /// The received message, whose payload accessor returns an independent copy.
    MqttMessage message();

    /// Whether the broker marked the publication as a duplicate. Always false for QoS 0.
    boolean dup();

    /// Acknowledges this subscription's QoS 1 delivery in [AckMode#MANUAL] mode.
    /// Safe to call from any thread, including before the handler returns. Repeated calls,
    /// QoS 0, other acknowledgement modes, and calls after the receiving session ends have no
    /// effect. In a continued session, a call after connection loss records completion; PUBACK
    /// waits for the broker to retransmit that message on the new connection.
    /// A failure of this delivery's handler, even while disconnected, voids earlier and later
    /// calls unless its PUBACK has already entered the send queue. A failure that ends the
    /// connection also invalidates other unfinished deliveries. Their old `ack()` calls and
    /// handler returns cannot acknowledge a replacement after reconnecting.
    /// This does not wait for a PUBACK write or acknowledge another matching subscription.
    /// A push delivery keeps its subscription capacity until its handler returns and its PUBACK
    /// enters the send queue; a pull delivery releases it at this call. Either way, PUBACK waits
    /// for other matching subscriptions and for earlier QoS 1 messages; see [AckMode].
    void ack();
}
