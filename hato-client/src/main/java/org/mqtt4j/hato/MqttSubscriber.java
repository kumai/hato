package org.mqtt4j.hato;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/// The subscribing role of a client, without access to its connection lifecycle.
public interface MqttSubscriber {

    /// Registers a pull subscription, whose deliveries are consumed by the application's
    /// receiving threads. Completes after a successful SUBACK. Publications received before
    /// SUBACK are buffered within the subscription's capacity.
    ///
    /// [AckMode#AFTER_HANDLER] fails with [IllegalArgumentException] through the returned
    /// future. Other registration restrictions and delivery rules, apart from those about the
    /// handler, are as for [#subscribe(Subscribe, Consumer)].
    ///
    /// The default implementation fails with [UnsupportedOperationException], so implementations
    /// of the subscribing role that only provide handlers remain source compatible.
    default CompletableFuture<Subscription> subscribe(Subscribe request) {
        return CompletableFuture.failedFuture(
                new UnsupportedOperationException("Pull subscriptions are not supported")
        );
    }

    /// Registers one subscription and completes after a successful SUBACK.
    /// Supports QoS 0, QoS 1, and QoS 2.
    ///
    /// Deliveries can reach the handler before the future completes. Each subscription's
    /// handler runs serially, independently of I/O and operation completion. A handler failure
    /// is logged and ends the active connection of its receiving session, notifying
    /// [ConnectionListener]. This includes a handler started on an earlier connection when the
    /// session continues. During a connection attempt, such a failure fails the attempt instead:
    /// [MqttClient#connect()] fails with [MqttConnectException] caused by it, and the listener is
    /// not notified. While disconnected, or after that session ends, a failure is logged without
    /// a disconnection notification.
    /// Exhausted receive capacity also ends the connection.
    /// On connection termination, queued deliveries are discarded; a running handler can finish.
    /// An established subscription survives termination and resumes on the same handle after
    /// the client's next successful [MqttClient#connect()]. New deliveries wait for a previous
    /// connection's running handler to finish, preserving serial execution. A handler failure
    /// also retains the subscription for that next connection.
    /// Interrupt status left by a normally returning handler is cleared before the next delivery.
    /// QoS 1 and QoS 2 acknowledgement follows the request's [AckMode]. QoS 1 messages can be
    /// delivered more than once. Within a session, a QoS 2 message reaches each subscription at
    /// most once, except that a delivery invalidated by a handler failure is delivered again
    /// from the broker's retransmission unless its PUBREC had reached the transport; see
    /// [MqttDelivery#ack()].
    ///
    /// Every matching subscription receives each incoming publication. MQTT 3.1.1 cannot
    /// distinguish broker copies for overlapping subscriptions, so each copy is routed again.
    /// A retransmission joining a continued QoS 1 or 2 reception goes only to its original matching
    /// subscriptions whose deliveries were discarded; handed-off deliveries keep their state.
    /// Shared subscriptions are matched against the filter after their shared group name.
    /// A shared subscription cannot overlap any other subscription on the same client, including
    /// another shared subscription. Such combinations fail with [IllegalStateException] in
    /// either registration order, including subscriptions awaiting SUBACK or UNSUBACK.
    /// Use separate clients when those filters are needed together. Ordinary subscriptions may
    /// overlap each other.
    ///
    /// The same original filter fails with [IllegalStateException] while it is pending, active,
    /// or awaiting unsubscribe acknowledgement. A broker refusal fails with
    /// [MqttSubscribeException]. An unconnected client fails with [MqttNotConnectedException];
    /// admission failures use [MqttCapacityException].
    /// If the connection ends before the initial SUBACK, the future fails with
    /// [MqttConnectionLostException] and the registration is not retained. A broker refusal
    /// during re-creation ends only that established subscription; other subscriptions and the
    /// connection continue. A pull subscription reports that refusal from its receive methods.
    CompletableFuture<Subscription> subscribe(Subscribe request, Consumer<MqttDelivery> handler);
}
