package org.mqtt4j.hato;

/// An application delivery for one subscription. Each matching subscription has an independent
/// acknowledgement condition.
public interface MqttDelivery {

    /// The received message, whose payload accessor returns an independent copy.
    MqttMessage message();

    /// Whether the broker marked the publication as a duplicate. Always false for QoS 0.
    boolean dup();

    /// Acknowledges this subscription's QoS 1 or QoS 2 delivery in [AckMode#MANUAL] mode.
    /// Safe to call from any thread, including before the handler returns. Repeated calls,
    /// QoS 0, other acknowledgement modes, and calls after the receiving session ends have no
    /// effect. In a continued session, a call after connection loss records completion; the
    /// acknowledgement waits for the broker to retransmit that message on the new connection.
    /// A failure of this delivery's handler before this call, even while disconnected, stops
    /// the client under [FailurePolicy#STOP], which voids later calls, or acknowledges the
    /// delivery under [FailurePolicy#SKIP]. A failure after this call keeps the acknowledgement.
    /// A failure that ends the connection also invalidates other deliveries whose
    /// acknowledgement condition is unmet. Their old `ack()` calls and handler returns cannot
    /// acknowledge a replacement after reconnecting.
    /// This does not wait for an acknowledgement write or acknowledge another subscription.
    /// A push delivery keeps its subscription capacity until its handler returns and its PUBACK
    /// or PUBREC enters the send queue; a pull delivery releases it at this call. Either way,
    /// acknowledgements wait for other matching subscriptions and earlier QoS 1 or 2 messages;
    /// see [AckMode].
    void ack();
}
