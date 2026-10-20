package org.mqtt4j.hato;

/// Receives connection and subscription notifications serially, independently of I/O and
/// operation completion. Every connection attempt, whether [MqttClient#connect()] or an
/// automatic reconnection started it, produces either [#onConnected(ConnAck)] or
/// [#onConnectFailed(Disconnection)]. An established connection is followed by
/// [#onDisconnected(Disconnection)] once, even if it ends before the listener runs. Attempt
/// numbers in [ConnAck#connectionAttempt()] and [Disconnection#connectionAttempt()] are
/// consecutive. When an attempt replaces a broker session without local state, as
/// [MqttClient#connect()] describes, the connections that it closes on the way produce no
/// notifications and take no attempt number; the attempt reports only its outcome.
///
/// All callbacks share the same serial order. Notifications wait in a bounded queue: when a
/// slow callback lets it fill, the oldest notification not yet delivered is dropped, so the
/// listener never delays connection attempts or operations. Notifications that arrive are in
/// order and never repeated, so each callback below runs at most once for its event. A missing
/// attempt number, or an established connection without its ending, reveals a dropped
/// connection notification. A dropped re-creation refusal leaves no such gap, but
/// [Subscription#established()] and pull receives still report it. Release resources when the
/// futures that own them complete, for example in a continuation of [MqttClient#disconnect()],
/// not only in [#onDisconnected(Disconnection)], which can be among the dropped notifications.
///
/// Listener exceptions are logged and do not affect the client. Interrupt status left by a
/// callback is cleared before the next callback. Ordering relative to future completion is
/// unspecified. The default methods do nothing, so a lambda can still receive just
/// disconnections.
@FunctionalInterface
public interface ConnectionListener {

    /// Called for each established connection. Subscription re-creation may still be pending.
    /// Client operations, including publishing, can be called here without waiting for the
    /// connect future, subject to normal admission limits. Like any client call, they act on
    /// the client's current connection: this call can run after its connection has ended, and
    /// operations then reach a later connection or fail with [MqttNotConnectedException].
    default void onConnected(ConnAck connAck) {
    }

    /// Called for each established connection that ends, including local disconnect or
    /// close, peer closure, protocol errors, overload, and internal failure.
    /// [Disconnection#reconnectDelay()] tells when the client attempts to connect again.
    void onDisconnected(Disconnection disconnection);

    /// Called for each connection attempt that ends without an established connection,
    /// for example because the broker cannot be reached or refuses the connection.
    /// [Disconnection#reconnectDelay()] is the delay before the next attempt; when it is empty,
    /// the client has given up and a pending [MqttClient#connect()] fails. A handler failure
    /// that stops the client while it waits to reconnect is reported here too, for the attempt
    /// that it cancels; see [MqttSubscriber].
    ///
    /// The cause is the handler's failure when a handler stopped the attempt. Otherwise it is
    /// the same [MqttConnectException] with which the attempt fails a pending
    /// [MqttClient#connect()]; a broker refusal carries its reason code. To stop retrying on a
    /// particular cause, call [MqttClient#disconnect()] from here.
    default void onConnectFailed(Disconnection failure) {
    }

    /// Called when re-creation of an established subscription is refused, unless
    /// [Subscription#close()] or [Subscription#unsubscribe()] requested its removal first.
    /// `subscription` is the original handle and is permanently ended; other subscriptions and
    /// the connection continue. Waiting and later pull receives throw
    /// [MqttSubscriptionRefusedException] with the same refusal reason. The same filter can be
    /// registered again at once, also from within this callback.
    default void onResubscribeFailed(
            Subscription subscription,
            MqttSubscriptionRefusedException failure
    ) {
    }
}
