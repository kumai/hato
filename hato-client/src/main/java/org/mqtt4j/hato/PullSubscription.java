package org.mqtt4j.hato;

import java.time.Duration;

/// A subscription whose deliveries the application takes by calling [#receive()], as returned by
/// [MqttSubscriber#subscribe(Subscribe)]. A subscription with a handler or a hand-off acceptor
/// is a plain [Subscription], which cannot receive.
public interface PullSubscription extends Subscription {

    /// Waits interruptibly for the next delivery. Multiple receiving threads share one FIFO; each
    /// delivery is handed to one caller only. Concurrent callers can return and finish processing
    /// in a different order.
    ///
    /// [AckMode#ON_RECEIVE] permits acknowledgement at handoff; [AckMode#MANUAL] waits for
    /// [MqttDelivery#ack()]. PUBACK and PUBREC still follow receive order; see [AckMode].
    /// Returns [ReceiveResult.Received] at handoff, or [ReceiveResult.Ended] after [#close()],
    /// after [#unsubscribe()] once removal is confirmed and every delivery it accepted has been
    /// received, or once [MqttClient#close()] has been called. Connection termination
    /// discards buffered deliveries but leaves this call waiting for delivery after the next
    /// connection. This applies to both requested disconnection and connection failure.
    /// A refusal ends this subscription and throws [MqttSubscriptionRefusedException]; a refusal
    /// after local closure or a requested removal only confirms that removal, so this call ends
    /// with [ReceiveResult.Ended] as described above. A SUBACK that violates the protocol throws
    /// [MqttProtocolException]. Rejected registration throws its original exception, including
    /// [IllegalStateException] for duplicate or overlapping shared filters and
    /// [MqttCapacityException] for capacity. An internal failure that closes the client throws
    /// [MqttConnectionLostException] with that failure as its cause. The first permanent ending
    /// decides every later result; a later close or refusal does not change it. This overload
    /// waits without a deadline and never returns [ReceiveResult.TimedOut].
    ///
    /// @throws InterruptedException if interrupted before claiming a delivery
    /// @throws MqttSubscriptionRefusedException if the subscription was refused before its removal
    ///         was requested
    /// @throws MqttProtocolException if a SUBACK for this subscription violated the protocol
    /// @throws MqttConnectionLostException if an internal failure closed the client first
    ReceiveResult receive() throws InterruptedException;

    /// Like [#receive()], but returns [ReceiveResult.TimedOut] if the wait expires while the
    /// subscription remains active. Zero polls without waiting for a delivery; a negative
    /// duration is rejected. A timeout does not end the subscription. A normally ended
    /// subscription returns [ReceiveResult.Ended] immediately, even for a zero-duration poll.
    /// A disconnected subscription can time out while waiting for the next connection.
    /// Registration rejection, a refusal, a SUBACK that violates the protocol, and an
    /// internal failure that closes the client throw as they do for [#receive()].
    ///
    /// @throws IllegalArgumentException if `timeout` is negative
    /// @throws InterruptedException if interrupted before claiming a delivery
    /// @throws MqttSubscriptionRefusedException if the subscription was refused before its removal
    ///         was requested
    /// @throws MqttProtocolException if a SUBACK for this subscription violated the protocol
    /// @throws MqttConnectionLostException if an internal failure closed the client first
    ReceiveResult receive(Duration timeout) throws InterruptedException;
}
