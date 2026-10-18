package org.mqtt4j.hato;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/// One subscription registration, which can await its first connection or SUBACK.
/// The same handle survives connection termination. The client's next connection, including an
/// automatic reconnection, resumes an established subscription when the broker retained the
/// session; otherwise, including before the first successful SUBACK, it sends SUBSCRIBE again.
/// Removal, local closure, permanent client closure, registration rejection, a broker refusal,
/// or a SUBACK that violates the protocol ends the subscription permanently.
public interface Subscription extends AutoCloseable {

    /// The original filter sent to the broker, including any shared group name.
    TopicFilter filter();

    /// The maximum QoS granted by the most recent successful SUBACK, which can be lower than
    /// requested. Re-creation can update this value; it retains its last value while disconnected
    /// or after the subscription ends.
    ///
    /// @throws IllegalStateException if no successful SUBACK has granted a QoS yet
    Qos grantedQos();

    /// Observes establishment on the current or next connection without starting a connection.
    /// Returns an already completed future with the granted QoS when this subscription is
    /// established on the current connection and no requested disconnection is pending.
    /// Otherwise it waits for successful SUBACK, or for a later connection that resumes the
    /// broker's existing subscription. Connection loss, requested disconnection, and failed
    /// connection attempts do not fail the observation.
    ///
    /// Each call returns an independent future. Completing, cancelling, or timing out that future
    /// changes neither the registration nor another observation. A broker refusal fails it with
    /// [MqttSubscribeException]. A SUBACK that violates the protocol fails it with
    /// [MqttProtocolException] and also ends the connection. Registration rejection reports its
    /// original cause; see [MqttSubscriber]. Requested removal, local closure, or an ordinary
    /// client close fails it with [IllegalStateException]. An internal failure that permanently
    /// closes the client fails it with [MqttConnectionLostException] carrying that failure. The
    /// first permanent ending decides every later result.
    ///
    /// Pending observations share a limit of 1,024 per client, independent of operation capacity.
    /// This includes [#unsubscribe()] and [MqttClient#whenConnected()] observations. A full limit
    /// returns a future already failed with [MqttCapacityException], which is done, without
    /// changing the subscription. A slot remains held through completion and any synchronous
    /// continuations run by the client. An already completed result needs no slot. Application
    /// completion or cancellation does not release a pending slot before the client resolves the
    /// observation, so each call made while the subscription is not established holds one: a
    /// periodic check keeps one pending future and calls again only after it completes.
    CompletableFuture<Qos> established();

    /// Waits interruptibly for the next delivery of a pull subscription. Multiple receiving
    /// threads share one FIFO; each delivery is handed to one caller only. Concurrent callers
    /// can return and finish processing in a different order.
    ///
    /// [AckMode#ON_RECEIVE] permits acknowledgement at handoff; [AckMode#MANUAL] waits for
    /// [MqttDelivery#ack()]. PUBACK and PUBREC still follow receive order; see [AckMode].
    /// Returns [ReceiveResult.Received] at handoff, or [ReceiveResult.Ended] after [#close()],
    /// a completed [#unsubscribe()], or [MqttClient#close()]. Connection termination
    /// discards buffered deliveries but leaves this call waiting for delivery after the next
    /// connection. This applies to both requested disconnection and connection failure.
    /// A broker refusal ends this subscription and throws [MqttSubscribeException]; a refusal
    /// after local closure or a requested removal only confirms that removal, which returns
    /// [ReceiveResult.Ended]. A SUBACK that violates the protocol throws
    /// [MqttProtocolException]. Rejected registration throws its original exception, including
    /// [IllegalStateException] for duplicate or overlapping shared filters and
    /// [MqttCapacityException] for capacity. An internal failure that closes the client throws
    /// [MqttConnectionLostException] with that failure as its cause. The first permanent ending
    /// decides every later result; a later close or refusal does not change it. This overload
    /// waits without a deadline and never returns [ReceiveResult.TimedOut]. The default
    /// implementation throws [UnsupportedOperationException].
    ///
    /// @throws InterruptedException if interrupted before claiming a delivery
    /// @throws UnsupportedOperationException for a subscription with a handler
    /// @throws MqttSubscribeException if the broker refused the subscription before its removal
    ///         was requested
    /// @throws MqttProtocolException if a SUBACK for this subscription violated the protocol
    /// @throws MqttConnectionLostException if an internal failure closed the client first
    default ReceiveResult receive() throws InterruptedException {
        throw new UnsupportedOperationException("This subscription does not support receiving");
    }

    /// Like [#receive()], but returns [ReceiveResult.TimedOut] if the wait expires while the
    /// subscription remains active. Zero polls without waiting for a delivery; a negative
    /// duration is rejected. A timeout does not end the subscription. A normally ended
    /// subscription returns [ReceiveResult.Ended] immediately, even for a zero-duration poll.
    /// A disconnected subscription can time out while waiting for the next connection.
    /// Registration rejection, a broker refusal, a SUBACK that violates the protocol, and an
    /// internal failure that closes the client throw as they do for [#receive()]. The default
    /// implementation throws [UnsupportedOperationException].
    ///
    /// @throws IllegalArgumentException if `timeout` is negative
    /// @throws InterruptedException if interrupted before claiming a delivery
    /// @throws UnsupportedOperationException for a subscription with a handler
    /// @throws MqttSubscribeException if the broker refused the subscription before its removal
    ///         was requested
    /// @throws MqttProtocolException if a SUBACK for this subscription violated the protocol
    /// @throws MqttConnectionLostException if an internal failure closed the client first
    default ReceiveResult receive(Duration timeout) throws InterruptedException {
        throw new UnsupportedOperationException("This subscription does not support receiving");
    }

    /// Stops new local delivery when called and observes confirmed removal.
    /// Already queued deliveries may run or be received while acknowledgement is pending; any
    /// still queued when it completes are discarded. Handlers already running can finish.
    /// Messages arriving while removal is pending are dropped without running or being received,
    /// and this subscription does not hold back the acknowledgement of QoS 1 and 2 among them.
    /// Discarded deliveries release this subscription's acknowledgement condition.
    /// Removing this subscription does not acknowledge already delivered MANUAL messages.
    /// Their [MqttDelivery#ack()] rules continue to apply after removal or handler return.
    ///
    /// Removal is accepted while disconnected or connecting and survives connection loss.
    /// With no connection and a nonpersistent session it completes locally. A persistent session
    /// waits for a later connection: UNSUBACK confirms removal when the broker retained the
    /// session, and a CONNACK with [ConnAck#sessionPresent()] false confirms it otherwise.
    /// Removal waits for send capacity. When SUBACK is pending, removal waits for that response
    /// before sending UNSUBSCRIBE; a refusal also confirms removal.
    ///
    /// If [MqttClient#close()] finds removal pending, a nonpersistent session confirms it,
    /// because the broker discards that session, while a persistent session fails it with
    /// [IllegalStateException]; that broker may still hold the subscription. If an internal
    /// failure closes the client instead, the observation fails with
    /// [MqttConnectionLostException] carrying that failure.
    ///
    /// Each call returns an independent future observing the same removal. Changing a future
    /// does not alter the request or later results. Observations share the client-wide limit
    /// described by [#established()] and do not consume operation capacity. If the observation
    /// limit is full, its future fails with [MqttCapacityException]; removal still proceeds.
    /// A repeated request after successful removal is harmless. The same filter can be
    /// registered again after removal completes.
    ///
    /// Calls after removal has completed, registration was refused, or client shutdown has been
    /// requested complete locally and discard queued deliveries. Handlers already running can
    /// finish.
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
    /// [#unsubscribe()] can observe the pending removal within the shared observation limit;
    /// rejection or failure of that future neither cancels removal nor resumes local delivery.
    /// The closed subscription continues to count against subscription capacity and
    /// prevents registering the same filter again or, when either filter is shared, an
    /// overlapping one until removal is confirmed: UNSUBACK, a successful CONNACK with
    /// [ConnAck#sessionPresent()] false, or the end of a nonpersistent connection. Without a
    /// connection, a nonpersistent session confirms removal locally. Messages that still arrive
    /// for it are discarded, and QoS 1 and 2 messages are acknowledged. This applies to both
    /// push and pull modes.
    /// To keep using the same filter without waiting for removal, retain the subscription and
    /// switch the handler's delegate, for example through an application-owned atomic reference.
    @Override
    void close();
}
