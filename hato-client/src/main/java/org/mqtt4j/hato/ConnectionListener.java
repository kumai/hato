package org.mqtt4j.hato;

/// Receives connection and subscription notifications serially, independently of I/O and
/// operation completion. An established connection produces [#onConnected(ConnAck)] followed
/// by [#onDisconnected(Disconnection)], once each, even if it ends before the listener runs.
/// Failed application connection attempts are reported only by the connect future.
///
/// All callbacks share the same serial order. A slow callback delays later callbacks;
/// listener exceptions are logged and do not affect the client. Interrupt status left by a
/// callback is cleared before the next callback. Ordering relative to future completion is
/// unspecified. The default methods do nothing, so a lambda can still receive just
/// disconnections.
/// Pending callbacks retain capacity until they return; slow callbacks can cause new connection
/// attempts or subscription registrations to fail with [MqttCapacityException].
@FunctionalInterface
public interface ConnectionListener {

    /// Called for each established connection. Subscription re-creation may still be pending.
    /// Client operations, including publishing, can be called here without waiting for the
    /// connect future, subject to normal admission limits. Like any client call, they act on
    /// the client's current connection: this call can run after its connection has ended, and
    /// operations then reach a later connection or fail with [MqttNotConnectedException].
    default void onConnected(ConnAck connAck) {
    }

    /// Called once for each established connection that ends, including local disconnect or
    /// close, peer closure, protocol errors, overload, and internal failure.
    void onDisconnected(Disconnection disconnection);

    /// Called once when the broker refuses re-creation of an established subscription, unless
    /// [Subscription#close()] or [Subscription#unsubscribe()] requested its removal first.
    /// `subscription` is the original handle and is permanently ended; other subscriptions and
    /// the connection continue. Waiting and later pull receives throw [MqttSubscribeException]
    /// with the same refusal reason.
    default void onResubscribeFailed(Subscription subscription, MqttSubscribeException failure) {
    }
}
