package org.mqtt4j.hato;

/// When a subscription makes a QoS 1 delivery eligible for acknowledgement. QoS 0 deliveries
/// need none.
///
/// PUBACK follows receive order and waits until every subscription that received the message
/// meets its condition. A delivery that has not met its condition therefore holds back the
/// PUBACK of every later QoS 1 message on the connection, including other subscriptions'
/// messages. Push subscriptions keep a delivery's capacity until its handler finishes and its
/// PUBACK enters the send queue. Pull subscriptions release it at handoff (ON_RECEIVE) or at
/// [MqttDelivery#ack()] (MANUAL). QoS 1 messages whose PUBACK is pending are also bounded per
/// session, including across connections that continue it. Exhausting either bound ends the
/// connection. Connection loss alone does not release a push delivery's capacity in a continued
/// session. Acknowledgement never releases a running handler's capacity.
public enum AckMode {

    /// Immediately before invoking the handler, or when a pull receive hands off the delivery.
    /// The default for a pull subscription. Does not wait for application processing.
    /// A handler failure withdraws it unless its PUBACK has already entered the send queue.
    ON_RECEIVE,

    /// After the handler returns normally. The default for a subscription with a handler;
    /// rejected for pull subscriptions.
    AFTER_HANDLER,

    /// When the application calls [MqttDelivery#ack()]. Call it for every delivery; it has no
    /// effect where no acknowledgement is needed. A subscription that requested QoS 0 still
    /// receives QoS 1 messages that also match another subscription, so [MqttMessage#qos()]
    /// can exceed [Subscription#grantedQos()].
    MANUAL
}
