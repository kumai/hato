package org.mqtt4j.hato;

/// An application delivery for one subscription. Each matching subscription has an independent
/// acknowledgement condition.
public interface MqttDelivery {

    /// {@return the received message, whose payload accessor returns an independent copy}
    MqttMessage message();

    /// {@return whether the broker marked the publication as a duplicate} Always false for QoS 0.
    boolean dup();

    /// {@return the number of the connection attempt that established the connection delivering
    /// this message, as [ConnAck#connectionAttempt()] reports it} A retransmission on a later
    /// connection carries the later attempt. An application that keeps deliveries after their
    /// connection ends can tell them from the current connection's by comparing the two.
    long connectionAttempt();

    /// {@return the original filter of the subscription that received this delivery, including any
    /// shared group name, as [Subscription#filter()] returns it} A handler or hand-off acceptor
    /// shared by several subscriptions can tell their deliveries apart by it, because the client
    /// reserves a filter for one subscription while it is pending, active, or awaiting removal.
    /// Deliveries that a removed subscription accepted can still run or be received after the
    /// same filter is registered again; the deliveries of both subscriptions then name that
    /// filter. A copy that reaches overlapping subscriptions becomes one delivery for each of
    /// them, each naming its own subscription. Where the client cannot tell which subscription a
    /// broker meant a copy for, as described in [MqttSubscriber], this names the subscription that
    /// the copy reached.
    TopicFilter filter();

    /// Acknowledges this subscription's QoS 1 or QoS 2 delivery in [AckMode#MANUAL] mode.
    /// Safe to call from any thread, including before the handler returns. Repeated calls,
    /// QoS 0, other acknowledgement modes, and calls after the receiving session ends have no
    /// effect. In a continued session, a call after connection loss records completion; the
    /// acknowledgement waits for the broker to retransmit that message on the new connection.
    /// A failure of this delivery's handler before this call, even while disconnected, stops
    /// the client under [FailurePolicy#STOP], which voids later calls, or acknowledges the
    /// delivery under [FailurePolicy#SKIP]. A failure after this call keeps the acknowledgement.
    /// A call made before a hand-off subscription's acceptor returns counts only if the
    /// acceptor accepts the delivery.
    /// A failure that ends the connection also invalidates other deliveries whose
    /// acknowledgement condition is unmet. Their old `ack()` calls and handler returns cannot
    /// acknowledge a replacement after reconnecting.
    /// This does not wait for an acknowledgement write or acknowledge another subscription.
    /// A push delivery keeps its subscription capacity until its handler returns and, for QoS 1
    /// and 2, its acknowledgement has its turn; a pull delivery releases it at this call, and a
    /// hand-off delivery released it when accepted. In each case, acknowledgements wait for other
    /// matching subscriptions and earlier QoS 1 or 2 messages; see [AckMode].
    void ack();
}
