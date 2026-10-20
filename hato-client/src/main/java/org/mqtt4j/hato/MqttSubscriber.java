package org.mqtt4j.hato;

import java.util.function.Consumer;
import java.util.function.Predicate;

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
    /// handler runs serially, independently of I/O and operation completion. What a handler
    /// failure does depends on the request's [FailurePolicy]. Under the default
    /// [FailurePolicy#STOP], a failure of a QoS 1 or 2 delivery that still holds back its
    /// acknowledgement ([AckMode#AFTER_HANDLER], or [AckMode#MANUAL] before [MqttDelivery#ack()])
    /// is logged and stops the client: it withdraws the connection request, so the client does
    /// not reconnect until the application calls [MqttClient#connect()] again. The failure ends
    /// the active connection of its receiving session, reported by
    /// [ConnectionListener#onDisconnected(Disconnection)], or a connection attempt or a wait to
    /// reconnect, reported by [ConnectionListener#onConnectFailed(Disconnection)]. Either way the
    /// failure is the cause, no reconnection delay is announced, and a pending
    /// [MqttClient#connect()] fails with [MqttConnectException] caused by it. This includes a
    /// handler started on an earlier connection when the session continues. Without a connection
    /// request, or after that session ends, such a failure is only logged. Any other failure is
    /// skipped: the delivery counts as handled, and the subscription and the connection go on.
    /// To retry a transient failure, repeat the work inside the handler, which keeps the
    /// subscription's order and holds back its acknowledgement meanwhile. To give up a message,
    /// catch the failure and return normally, call [MqttDelivery#ack()] with [AckMode#MANUAL], or
    /// use [FailurePolicy#SKIP]; the message then counts as handled.
    /// A QoS 1 or 2 message that exhausts receive capacity also ends the connection, while a
    /// QoS 0 one is dropped; see [Subscribe#bufferCapacity()].
    /// On connection termination, queued deliveries are discarded; a running handler can finish.
    /// A subscription survives termination and resumes on the same handle after
    /// the client's next connection, including an automatic reconnection. New deliveries wait
    /// for a previous connection's running handler to finish, preserving serial execution. A
    /// handler failure also retains the subscription for that next connection.
    /// Interrupt status left by a normally returning handler is cleared before the next delivery.
    /// QoS 1 and QoS 2 acknowledgement follows the request's [AckMode]. QoS 1 messages can be
    /// delivered more than once. Within a session, a QoS 2 message reaches each subscription at
    /// most once, except that a delivery whose handler failure stopped the client is delivered
    /// again from the broker's retransmission; see [MqttDelivery#ack()].
    ///
    /// Every matching subscription receives each incoming publication. The client does not tell
    /// apart the copies that a broker sends for overlapping subscriptions, so each copy is
    /// routed again.
    /// A retransmission joining a continued QoS 1 or 2 reception goes only to its original matching
    /// subscriptions whose deliveries were discarded; handed-off deliveries keep their state.
    /// Shared subscriptions are matched against the filter after their shared group name.
    /// A shared subscription cannot overlap any other subscription on the same client, including
    /// another shared subscription. Such combinations fail with [IllegalStateException] in
    /// either registration order, including subscriptions awaiting SUBACK or UNSUBACK.
    /// Use separate clients when those filters are needed together. Ordinary subscriptions may
    /// overlap each other.
    ///
    /// Null arguments throw [NullPointerException], calls after client closure throw
    /// [IllegalStateException], and on an MQTT 3.1.1 client a request that sets a subscription
    /// option throws [IllegalArgumentException], all synchronously and without registering.
    /// Duplicate filters, including the same filter with other subscription options,
    /// shared-filter overlap, and registration capacity are checked asynchronously: the returned
    /// handle ends, and [Subscription#established()] fails with [IllegalStateException] for
    /// duplicates or overlap, or [MqttCapacityException] for capacity. Pull receive methods
    /// report the same registration failure. The client permits 256 registrations, including
    /// ones awaiting establishment or broker removal. Registration does not consume operation
    /// capacity.
    /// The same original filter remains reserved while pending, active, or awaiting removal.
    ///
    /// A refusal fails establishment with [MqttSubscriptionRefusedException] and permanently ends
    /// that subscription. Other subscriptions and the connection continue. A refusal during
    /// re-creation is also reported by
    /// [ConnectionListener#onResubscribeFailed(Subscription, MqttSubscriptionRefusedException)];
    /// pull receive methods report both initial and re-creation refusals. A refusal that arrives
    /// after the application closed the subscription or requested its removal only confirms that
    /// removal. A SUBACK that violates the protocol ends both the subscription, with
    /// [MqttProtocolException], and the connection, so a later connection does not send the same
    /// SUBSCRIBE again.
    Subscription subscribe(Subscribe request, Consumer<MqttDelivery> handler);

    /// Registers a push subscription like [#subscribe(Subscribe, Consumer)], except that `options`
    /// decides where and how its handler runs; see [DeliveryOptions]. Every other rule of that
    /// method applies, including registration failures and how a handler failure stops the
    /// client, and [DeliveryOptions#serial()] behaves exactly as it. Returns the handle
    /// immediately.
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

    /// Registers a hand-off subscription, which hands each delivery to `acceptor` for the
    /// application to process elsewhere, such as in its own mailbox: `mailbox::offer`. Returns
    /// the handle immediately; registration and its failures are as for
    /// [#subscribe(Subscribe, Consumer)].
    ///
    /// One internal thread of the client calls the acceptors of all its hand-off subscriptions,
    /// one delivery at a time, in receive order across them; a publication that several of them
    /// match is offered to each in turn. Deliveries waiting for hand-off count against
    /// [Subscribe#bufferCapacity()]. The acceptor returns true to accept a delivery: from then on
    /// the application owns it, and its capacity returns. Returning false leaves it unaccepted
    /// and counts as an overflow, as when the buffer is full: a QoS 0 delivery is dropped for
    /// this subscription, and a QoS 1 or 2 delivery ends the connection. The client does not
    /// offer a refused delivery again. An exception is a handler failure under the request's
    /// [FailurePolicy]; a QoS 1 or 2 delivery that was not accepted still holds back its
    /// acknowledgement, so [FailurePolicy#STOP] stops the client. Once it has accepted a
    /// delivery, the acceptor must neither return false nor throw, because the client cannot
    /// take the delivery back.
    ///
    /// The acceptor must not wait, as `BlockingQueue.put` does; use `offer`. While it waits, the
    /// client's other hand-off subscriptions wait too, and their deliveries can overflow. Waiting
    /// there for the termination of this subscription or the client never ends.
    ///
    /// [AckMode#ON_RECEIVE], the default, acknowledges a QoS 1 or 2 delivery when it is accepted.
    /// With [AckMode#MANUAL], the application calls [MqttDelivery#ack()] once it has processed
    /// the delivery, even before the acceptor returns; a call made before a refusal or a failure
    /// has no effect. The capacity still returns at acceptance, and the open acknowledgement
    /// holds back later ones as [AckMode] describes. [AckMode#AFTER_HANDLER] throws
    /// [IllegalArgumentException], because the client cannot see when processing ends.
    ///
    /// [Subscription#close()], [Subscription#unsubscribe()], and closing the client end new
    /// hand-offs; `unsubscribe()` first hands off the deliveries it already accepted. A call that
    /// started before the stop can still return after it. Once [Subscription#terminated()]
    /// completes, the acceptor is not called again, and the application can drain or discard its
    /// mailbox. A connection's end discards the deliveries still waiting for hand-off. Each
    /// delivery reports its [MqttDelivery#connectionAttempt()], so deliveries kept from an
    /// earlier connection can be told apart.
    ///
    /// The default implementation throws [UnsupportedOperationException], so implementations of
    /// the subscribing role need not hand off deliveries.
    default Subscription handOff(Subscribe request, Predicate<MqttDelivery> acceptor) {
        throw new UnsupportedOperationException("Hand-off subscriptions are not supported");
    }
}
