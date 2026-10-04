package org.mqtt4j.mqtj;

/// When a subscription makes a QoS 1 delivery eligible for acknowledgement. QoS 0 deliveries
/// need none.
///
/// PUBACK follows receive order and waits until every subscription that received the message
/// meets its condition. A delivery that has not met its condition therefore holds back the
/// PUBACK of every later QoS 1 message on the connection, including other subscriptions'
/// messages, which keep their subscription capacity meanwhile. Exhausted capacity ends the
/// connection. Acknowledgement does not release capacity still occupied by a running handler.
public enum AckMode {

    /// Immediately before invoking the handler, without waiting for it to return.
    ON_RECEIVE,

    /// After the handler returns normally. The default for a subscription with a handler.
    AFTER_HANDLER,

    /// When the application calls [MqttDelivery#ack()]. Call it for every delivery; it has no
    /// effect where no acknowledgement is needed. A subscription that requested QoS 0 still
    /// receives QoS 1 messages that also match another subscription, so [MqttMessage#qos()]
    /// can exceed [Subscription#grantedQos()].
    MANUAL
}
