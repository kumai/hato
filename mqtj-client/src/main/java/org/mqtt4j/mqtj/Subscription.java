package org.mqtt4j.mqtj;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/// One subscription established by a successful SUBACK.
public interface Subscription extends AutoCloseable {

    /// The original filter sent to the broker, including any shared group name.
    TopicFilter filter();

    /// The maximum QoS granted by the broker, which can be lower than requested.
    Qos grantedQos();

    /// Waits interruptibly for the next delivery of a pull subscription. Multiple receiving
    /// threads share one FIFO; each delivery is handed to one caller only. Concurrent callers
    /// can return and finish processing in a different order.
    ///
    /// [AckMode#ON_RECEIVE] permits acknowledgement at handoff; [AckMode#MANUAL] waits for
    /// [MqttDelivery#ack()]. Actual PUBACK writes still follow publication receive order.
    /// Returns [ReceiveResult.Received] at handoff, or [ReceiveResult.Ended] once the
    /// subscription has ended normally: after [#close()] or a completed [#unsubscribe()], or
    /// when its connection ends without a [Disconnection#cause()]. Connection termination
    /// discards buffered deliveries; a connection end with a cause throws
    /// [MqttConnectionLostException] with that cause. The first of these endings decides every
    /// later result; a later [#close()], [#unsubscribe()], or connection end does not change it.
    /// A retired subscription never resumes after the client reconnects. This overload waits
    /// without a deadline and never returns [ReceiveResult.TimedOut]. The default
    /// implementation throws [UnsupportedOperationException].
    ///
    /// @throws InterruptedException if interrupted before claiming a delivery
    /// @throws UnsupportedOperationException for a subscription with a handler
    /// @throws MqttConnectionLostException if a connection end with a cause came first
    default ReceiveResult receive() throws InterruptedException {
        throw new UnsupportedOperationException("This subscription does not support receiving");
    }

    /// Like [#receive()], but returns [ReceiveResult.TimedOut] if the wait expires while the
    /// subscription remains active. Zero polls without waiting for a delivery; a negative
    /// duration is rejected. A timeout does not end the subscription. A normally ended
    /// subscription returns [ReceiveResult.Ended] immediately, even for a zero-duration poll.
    /// Connection failures still throw with their cause. The default implementation throws
    /// [UnsupportedOperationException].
    ///
    /// @throws IllegalArgumentException if `timeout` is negative
    /// @throws InterruptedException if interrupted before claiming a delivery
    /// @throws UnsupportedOperationException for a subscription with a handler
    /// @throws MqttConnectionLostException if a connection end with a cause came first
    default ReceiveResult receive(Duration timeout) throws InterruptedException {
        throw new UnsupportedOperationException("This subscription does not support receiving");
    }

    /// Stops new local delivery when called and completes after UNSUBACK.
    /// Already queued deliveries may run or be received while acknowledgement is pending; any
    /// still queued when it completes are discarded. Handlers already running can finish.
    /// QoS 1 messages arriving while removal is pending are neither run nor received, while
    /// QoS 0 messages arriving meanwhile are dropped.
    /// Discarded deliveries release this subscription's acknowledgement condition.
    /// An already delivered MANUAL message still needs [MqttDelivery#ack()] while its connection
    /// remains active, even if its handler has returned and this subscription has been removed.
    ///
    /// If capacity prevents admitting the removal, the future fails with [MqttCapacityException]
    /// and the removal can be retried. Unless [#close()] was called, delivery then resumes,
    /// including the QoS 1 messages that waited. QoS 0 messages dropped during the attempted
    /// removal are not replayed.
    /// While a disconnect is pending, the removal fails with [MqttNotConnectedException].
    /// Each call returns an independent future; changing it does not alter the wire request or
    /// later results. Calls during one attempt observe the same wire exchange and use the
    /// operation budget until it ends, even if their futures are completed or cancelled first.
    /// Retrying after a timeout therefore cannot speed up removal and may exhaust the budget
    /// for other operations. Rejection of an additional call leaves the pending removal
    /// unchanged.
    /// A call admitted before permanent client closure can be rejected if it has not started
    /// observing that exchange.
    /// A repeated request after successful removal is harmless.
    /// The same filter can be registered again after removal completes.
    ///
    /// Connection termination retires this handle. Subsequent calls complete locally without
    /// sending an UNSUBSCRIBE, even after reconnecting and registering the same filter again.
    /// Calls after client shutdown has been requested also complete locally and discard queued
    /// deliveries. This does not change the outcome of a removal already pending when the
    /// connection ends.
    CompletableFuture<Void> unsubscribe();

    /// Permanently stops local delivery, discards queued messages, releases their
    /// acknowledgement conditions, and wakes waiting receivers. Schedules broker removal without
    /// waiting for the broker or application code. Running handlers may finish; handed-off
    /// MANUAL deliveries still require [MqttDelivery#ack()] while their connection is active.
    /// Calling close again has no further effect.
    ///
    /// Removal waits for send capacity and proceeds independently of the operation budget.
    /// [#unsubscribe()] can observe the pending removal using the operation budget; rejection
    /// of that call neither cancels removal nor resumes local delivery. Until removal succeeds
    /// or the connection ends, the broker registration still counts against subscription
    /// capacity and prevents registering the same filter again or, when either filter is
    /// shared, an overlapping one. Messages that still arrive for it are discarded, and QoS 1
    /// ones are acknowledged. This applies to both push and pull modes.
    @Override
    void close();
}
