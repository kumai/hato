package org.mqtt4j.mqtj;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/// One subscription established by a successful SUBACK. The same handle survives connection
/// termination. The client's next successful [MqttClient#connect()] resumes it when the broker
/// retained the session, or re-creates it when [ConnAck#sessionPresent()] is false.
/// Removal, local closure, permanent client closure, or a broker refusal during re-creation
/// ends the subscription permanently.
public interface Subscription extends AutoCloseable {

    /// The original filter sent to the broker, including any shared group name.
    TopicFilter filter();

    /// The maximum QoS granted by the most recent successful SUBACK, which can be lower than
    /// requested. Re-creation can update this value; it retains its last value while disconnected.
    Qos grantedQos();

    /// Waits interruptibly for the next delivery of a pull subscription. Multiple receiving
    /// threads share one FIFO; each delivery is handed to one caller only. Concurrent callers
    /// can return and finish processing in a different order.
    ///
    /// [AckMode#ON_RECEIVE] permits acknowledgement at handoff; [AckMode#MANUAL] waits for
    /// [MqttDelivery#ack()]. Actual PUBACK writes still follow publication receive order.
    /// Returns [ReceiveResult.Received] at handoff, or [ReceiveResult.Ended] after [#close()],
    /// a completed [#unsubscribe()], or [MqttClient#close()]. Connection termination
    /// discards buffered deliveries but leaves this call waiting for delivery after the next
    /// connection. This applies to both requested disconnection and connection failure.
    /// A broker refusal during re-creation ends this subscription and throws
    /// [MqttSubscribeException]. An internal failure that closes the client throws
    /// [MqttConnectionLostException] with that failure as its cause. The first permanent ending
    /// decides every later result; a later close or refusal does not change it. This overload
    /// waits without a deadline and never returns [ReceiveResult.TimedOut]. The default
    /// implementation throws [UnsupportedOperationException].
    ///
    /// @throws InterruptedException if interrupted before claiming a delivery
    /// @throws UnsupportedOperationException for a subscription with a handler
    /// @throws MqttSubscribeException if the broker refused re-creation before local closure
    /// @throws MqttConnectionLostException if an internal failure closed the client first
    default ReceiveResult receive() throws InterruptedException {
        throw new UnsupportedOperationException("This subscription does not support receiving");
    }

    /// Like [#receive()], but returns [ReceiveResult.TimedOut] if the wait expires while the
    /// subscription remains active. Zero polls without waiting for a delivery; a negative
    /// duration is rejected. A timeout does not end the subscription. A normally ended
    /// subscription returns [ReceiveResult.Ended] immediately, even for a zero-duration poll.
    /// A disconnected subscription can time out while waiting for the next connection.
    /// A broker refusal during re-creation and an internal failure that closes the client still
    /// throw as they do for [#receive()]. The default implementation throws
    /// [UnsupportedOperationException].
    ///
    /// @throws IllegalArgumentException if `timeout` is negative
    /// @throws InterruptedException if interrupted before claiming a delivery
    /// @throws UnsupportedOperationException for a subscription with a handler
    /// @throws MqttSubscribeException if the broker refused re-creation before local closure
    /// @throws MqttConnectionLostException if an internal failure closed the client first
    default ReceiveResult receive(Duration timeout) throws InterruptedException {
        throw new UnsupportedOperationException("This subscription does not support receiving");
    }

    /// Stops new local delivery when called and completes after UNSUBACK.
    /// Already queued deliveries may run or be received while acknowledgement is pending; any
    /// still queued when it completes are discarded. Handlers already running can finish.
    /// QoS 1 messages arriving while removal is pending are neither run nor received, while
    /// QoS 0 messages arriving meanwhile are dropped.
    /// Discarded deliveries release this subscription's acknowledgement condition.
    /// Removing this subscription does not acknowledge already delivered MANUAL messages.
    /// Their [MqttDelivery#ack()] rules continue to apply after removal or handler return.
    ///
    /// If capacity prevents admitting the removal, the future fails with [MqttCapacityException]
    /// and the removal can be retried. Unless [#close()] was called, delivery then resumes,
    /// including the QoS 1 messages that waited. QoS 0 messages dropped during the attempted
    /// removal are not replayed.
    /// While disconnected or connecting, a new removal fails with
    /// [MqttNotConnectedException], including an observation of removal scheduled by [#close()].
    /// An open subscription is retained for the next connection; a pending close remains pending
    /// and prevents re-creation.
    /// While a disconnect is pending, a new removal attempt fails with
    /// [MqttNotConnectedException]. Additional observations of an existing removal can still be
    /// admitted on that connection and fail when it ends.
    /// If the connection ends during removal, the future fails with
    /// [MqttConnectionLostException] and this subscription is retained unless [#close()] was
    /// called. After CONNACK, a removal requested while re-creation awaits SUBACK waits for
    /// that response before sending UNSUBSCRIBE. A refusal of re-creation also completes
    /// removal successfully, since the broker has not established the subscription.
    /// Each call returns an independent future; changing it does not alter the wire request or
    /// later results. Each admitted future belongs to its current connection, including one
    /// observing removal scheduled by [#close()], and does not wait for a later connection.
    /// Removal scheduled by [#close()] continues even if an observing future fails when the
    /// connection ends. Calls during one attempt observe the same wire exchange. Each admitted
    /// call holds operation capacity until the client finishes it, including continuations
    /// that its completion runs, even if the application completes or cancels the future first.
    /// Retrying after a timeout therefore cannot speed up removal and may exhaust that capacity
    /// for other operations. Rejection of an additional call leaves the pending removal
    /// unchanged.
    /// A call admitted before permanent client closure can be rejected if it has not started
    /// observing that exchange.
    /// A repeated request after successful removal is harmless.
    /// The same filter can be registered again after removal completes.
    ///
    /// Calls after removal has completed, re-creation was refused, or client shutdown has been
    /// requested complete locally and discard queued deliveries. This does not change the
    /// outcome of a removal already pending when the connection ends.
    CompletableFuture<Void> unsubscribe();

    /// Permanently stops local delivery, discards queued messages, releases their
    /// acknowledgement conditions, and wakes waiting receivers. Schedules broker removal without
    /// waiting for the broker or application code. Running handlers may finish. Local closure
    /// does not acknowledge handed-off MANUAL deliveries; their [MqttDelivery#ack()] rules
    /// continue to apply.
    /// Calling close again has no further effect.
    /// A closed subscription is never re-created, including when closed while disconnected.
    ///
    /// Removal waits for send capacity and does not consume operation capacity. It continues
    /// across connection termination until removal is confirmed.
    /// [#unsubscribe()] can observe the pending removal using operation capacity; rejection
    /// or failure of that future neither cancels removal nor resumes local delivery.
    /// The closed subscription continues to count against subscription capacity and
    /// prevents registering the same filter again or, when either filter is shared, an
    /// overlapping one until UNSUBACK or a successful CONNACK with [ConnAck#sessionPresent()]
    /// false confirms its removal. Messages that still arrive for it are discarded, and QoS 1
    /// messages are acknowledged. This applies to both push and pull modes.
    @Override
    void close();
}
