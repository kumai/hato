package org.mqtt4j.hato;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/// One subscription registration, which can await its first connection or SUBACK.
/// The same handle survives connection termination. The client's next connection, including an
/// automatic reconnection, resumes an established subscription when the broker retained the
/// session; otherwise, including before the first successful SUBACK, it sends SUBSCRIBE again.
/// Removal, local closure, permanent client closure, registration rejection, a refusal,
/// or a SUBACK that violates the protocol ends the subscription permanently.
public interface Subscription extends AutoCloseable {

    /// The original filter sent to the broker, including any shared group name.
    TopicFilter filter();

    /// The maximum QoS granted by the most recent successful SUBACK, which can be lower than
    /// requested, or empty until a successful SUBACK has granted one. Re-creation can update
    /// this value; it retains its last value while disconnected or after the subscription ends.
    Optional<Qos> grantedQos();

    /// Observes establishment on the current or next connection without starting a connection.
    /// Returns an already completed future with the granted QoS when this subscription is
    /// established on the current connection and no requested disconnection is pending.
    /// Otherwise it waits for successful SUBACK, or for a later connection that resumes the
    /// broker's existing subscription. Connection loss, requested disconnection, and failed
    /// connection attempts do not fail the observation.
    ///
    /// Each call returns an independent future. Completing, cancelling, or timing out that future
    /// changes neither the registration nor another observation. A refusal fails it with
    /// [MqttSubscriptionRefusedException]. A SUBACK that violates the protocol fails it with
    /// [MqttProtocolException] and also ends the connection. Registration rejection reports its
    /// original cause; see [MqttSubscriber]. Requested removal, local closure, or an ordinary
    /// client close fails it with [IllegalStateException]. An internal failure that permanently
    /// closes the client fails it with [MqttConnectionLostException] carrying that failure. The
    /// first permanent ending decides every later result.
    ///
    /// Pending observations share a limit per client, independent of operation capacity, which
    /// can be lower than the number of subscriptions that the client holds.
    /// This includes [#unsubscribe()] and [MqttClient#whenConnected()] observations. A full limit
    /// returns a future already failed with [MqttCapacityException], which is done, without
    /// changing the subscription. A slot remains held through completion and any synchronous
    /// continuations run by the client. An already completed result needs no slot. Application
    /// completion or cancellation does not release a pending slot before the client resolves the
    /// observation, so each call made while the subscription is not established holds one: a
    /// periodic check keeps one pending future and calls again only after it completes.
    CompletableFuture<Qos> established();

    /// Stops new local delivery when called and observes confirmed removal. Deliveries already
    /// queued when this is called are accepted: they still run or can still be received, also
    /// after removal is confirmed, and the subscription ends once removal is confirmed and they
    /// are done, which [#terminated()] observes. Call [#close()] to discard them instead. A
    /// connection end discards them as it does for every subscription. Handlers already running
    /// can finish. Messages arriving after this call are dropped without running or being
    /// received, and this subscription does not hold back the acknowledgement of QoS 1 and 2
    /// among them. Discarded deliveries release this subscription's acknowledgement condition.
    /// Removing this subscription does not acknowledge already delivered MANUAL messages.
    /// Their [MqttDelivery#ack()] rules continue to apply after removal or handler return.
    ///
    /// Removal is accepted while disconnected or connecting and survives connection loss.
    /// With no connection and a nonpersistent session it completes locally. A persistent session
    /// waits for a later connection: UNSUBACK confirms removal when the broker retained the
    /// session, and a CONNACK with [ConnAck#sessionPresent()] false confirms it otherwise.
    /// Removal waits for send capacity. When SUBACK is pending, removal waits for that response
    /// before sending UNSUBSCRIBE; a refusal also confirms removal. An UNSUBSCRIBE larger than
    /// [ConnAck#maximumPacketSize()] is not sent, and removal is confirmed locally; only a
    /// continued session whose broker announces a smaller limit than the one that took the
    /// SUBSCRIBE comes to this. That broker may still hold the subscription, and the messages
    /// that it sends for it are acknowledged without being delivered.
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
    /// Calls after removal has completed or registration was refused complete locally and change
    /// nothing; deliveries an earlier call accepted still finish. Calls after client shutdown
    /// has been requested complete locally and discard queued deliveries. Handlers already
    /// running can finish.
    CompletableFuture<Void> unsubscribe();

    /// Observes the end of this subscription's local work without requesting it. The future
    /// completes once no handler of this subscription is running and no delivery remains for it
    /// to run or to hand to a receiver: after [#close()]; after [#unsubscribe()], once removal is
    /// confirmed and the deliveries it accepted have run or been received; after any other
    /// permanent end, such as a refusal; or after the client closes. Beyond the removal
    /// confirmation that [#unsubscribe()] waits for, it does not wait for the broker: after
    /// [#close()] it can complete before UNSUBACK, and it also completes while disconnected. It
    /// does not wait for [MqttDelivery#ack()] of MANUAL deliveries already handed off; their
    /// rules continue to apply.
    ///
    /// Each call returns an independent future; an already terminated subscription returns a
    /// completed one. Pending observations share the client-wide limit described by
    /// [#established()], and a full limit returns a future already failed with
    /// [MqttCapacityException]; the future fails in no other way. A handler that never returns
    /// keeps it pending.
    ///
    /// Start shutdown work for resources the handler uses from a continuation of this future.
    /// Waiting for it inside this subscription's handler, or on a thread the handler needs in
    /// order to return, never completes. The default implementation throws
    /// [UnsupportedOperationException].
    ///
    /// @throws UnsupportedOperationException if this implementation cannot observe termination
    default CompletableFuture<Void> terminated() {
        throw new UnsupportedOperationException("This subscription does not observe termination");
    }

    /// Permanently stops local delivery, discards queued messages, including those an earlier
    /// [#unsubscribe()] accepted, releases their acknowledgement conditions, and wakes waiting
    /// receivers. No delivery is handed off after this returns. Schedules broker removal without
    /// waiting for the broker or application code. Running handlers may finish;
    /// [#terminated()] observes their end. Local closure does not acknowledge handed-off MANUAL
    /// deliveries; their [MqttDelivery#ack()] rules continue to apply.
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
