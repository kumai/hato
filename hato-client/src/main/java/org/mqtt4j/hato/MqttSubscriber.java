package org.mqtt4j.hato;

import java.util.function.Consumer;

/// The subscribing role of a client, without access to its connection lifecycle.
public interface MqttSubscriber {

    /// Registers a pull subscription, whose deliveries are consumed by the application's
    /// receiving threads. Returns the handle immediately; [Subscription#established()] observes
    /// broker acceptance. Publications received before SUBACK are buffered within the
    /// subscription's capacity.
    ///
    /// [AckMode#AFTER_HANDLER] throws [IllegalArgumentException]. Other registration
    /// restrictions and delivery rules, apart from those about the handler, are as for
    /// [#subscribe(Subscribe, Consumer)].
    ///
    /// The default implementation throws [UnsupportedOperationException], so implementations
    /// of the subscribing role that only provide handlers need not implement pull delivery.
    default Subscription subscribe(Subscribe request) {
        throw new UnsupportedOperationException("Pull subscriptions are not supported");
    }

    /// Registers one subscription and returns its handle immediately, including while the
    /// client is disconnected or connecting. Supports QoS 0, QoS 1, and QoS 2.
    /// [Subscription#established()] observes broker acceptance. Registration does not start a
    /// connection: SUBSCRIBE is sent on the current connection or on the next established one,
    /// including an automatic reconnection. The registration survives connection loss, requested
    /// disconnection, and failed connection attempts, including when its first SUBACK has not
    /// arrived.
    ///
    /// Deliveries can reach the handler before establishment completes. Each subscription's
    /// handler runs serially, independently of I/O and operation completion. A handler failure
    /// is logged and stops the client: it withdraws the connection request, so the client does
    /// not reconnect until the application calls [MqttClient#connect()] again. The failure ends
    /// the active connection of its receiving session, reported by
    /// [ConnectionListener#onDisconnected(Disconnection)], or a connection attempt or a wait to
    /// reconnect, reported by [ConnectionListener#onConnectFailed(Disconnection)]. Either way the
    /// failure is the cause, no reconnection delay is announced, and a pending
    /// [MqttClient#connect()] fails with [MqttConnectException] caused by it. This includes a
    /// handler started on an earlier connection when the session continues. Without a connection
    /// request, or after that session ends, a failure is only logged.
    /// To retry a transient failure, repeat the work inside the handler, which keeps the
    /// subscription's order and holds back its acknowledgement meanwhile. To give up a message,
    /// catch the failure and return normally, or call [MqttDelivery#ack()] with
    /// [AckMode#MANUAL]; the message then counts as handled.
    /// Exhausted receive capacity also ends the connection.
    /// On connection termination, queued deliveries are discarded; a running handler can finish.
    /// A subscription survives termination and resumes on the same handle after
    /// the client's next connection, including an automatic reconnection. New deliveries wait
    /// for a previous connection's running handler to finish, preserving serial execution. A
    /// handler failure also retains the subscription for that next connection.
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
    /// Null arguments throw [NullPointerException], and calls after client closure throw
    /// [IllegalStateException], synchronously. Duplicate filters, shared-filter overlap, and
    /// registration capacity are checked asynchronously: the returned handle ends, and
    /// [Subscription#established()] fails with [IllegalStateException] for duplicates or overlap,
    /// or [MqttCapacityException] for capacity. Pull receive methods report the same registration
    /// failure. The client permits 256 registrations, including ones awaiting establishment or
    /// broker removal. Registration does not consume operation capacity.
    /// The same original filter remains reserved while pending, active, or awaiting removal.
    ///
    /// A broker refusal fails establishment with [MqttSubscribeException] and permanently ends
    /// that subscription. Other subscriptions and the connection continue. A refusal during
    /// re-creation is also reported by
    /// [ConnectionListener#onResubscribeFailed(Subscription, MqttSubscribeException)]; pull
    /// receive methods report both initial and re-creation refusals. A refusal that arrives after
    /// the application closed the subscription or requested its removal only confirms that
    /// removal. A SUBACK that violates the protocol ends both the subscription, with
    /// [MqttProtocolException], and the connection, so a later connection does not send the same
    /// SUBSCRIBE again.
    Subscription subscribe(Subscribe request, Consumer<MqttDelivery> handler);

    /// Registers a push subscription like [#subscribe(Subscribe, Consumer)], except that `options`
    /// decides where and how its handler runs; see [DeliveryOptions]. Every rule of that method
    /// applies, including registration failures and how a handler failure stops the client, and
    /// [DeliveryOptions#serial()] behaves exactly as it. Returns the handle immediately.
    ///
    /// The default implementation throws [UnsupportedOperationException], so implementations of
    /// the subscribing role need not place handlers.
    default Subscription subscribe(
            Subscribe request,
            DeliveryOptions options,
            Consumer<MqttDelivery> handler
    ) {
        throw new UnsupportedOperationException("Delivery options are not supported");
    }
}
