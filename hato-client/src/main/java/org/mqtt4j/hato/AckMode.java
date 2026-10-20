package org.mqtt4j.hato;

/// When a subscription makes a QoS 1 or QoS 2 delivery eligible for acknowledgement. QoS 0
/// deliveries need none.
///
/// PUBACK (QoS 1) or PUBREC (QoS 2) waits until every subscription that received the message
/// meets its condition. PUBACK and PUBREC follow one receive-order sequence. A delivery that has
/// not met its condition therefore holds back the acknowledgements of every later QoS 1 and
/// QoS 2 message on the connection, including other subscriptions' messages. PUBCOMP does not
/// wait for such a delivery.
/// Push subscriptions keep a delivery's capacity until its handler finishes and its PUBACK or
/// PUBREC enters the send queue. Pull subscriptions release it at handoff (ON_RECEIVE) or at
/// [MqttDelivery#ack()] (MANUAL), and hand-off subscriptions when their acceptor accepts the
/// delivery, in either mode. At most 4,096 receptions may await protocol completion in one
/// session, including QoS 2 exchanges and connections that continue the session. A QoS 1 or 2
/// message that exhausts either bound ends the connection; see [Subscribe#bufferCapacity()].
/// Connection loss alone does not release a push delivery's capacity in a continued session.
/// Acknowledgement never releases a running handler's capacity.
public enum AckMode {

    /// Immediately before invoking the handler, when a pull receive hands off the delivery, or
    /// when a hand-off subscription's acceptor accepts it. The default for pull and hand-off
    /// subscriptions. Does not wait for application processing, and a handler failure does not
    /// withdraw it; see [FailurePolicy].
    ON_RECEIVE,

    /// After the handler returns normally. The default for a subscription with a handler;
    /// rejected for pull and hand-off subscriptions.
    AFTER_HANDLER,

    /// When the application calls [MqttDelivery#ack()]. Call it for every delivery; it has no
    /// effect where no acknowledgement is needed. A subscription that requested QoS 0 still
    /// receives QoS 1 or 2 messages that also match another subscription, so [MqttMessage#qos()]
    /// can exceed [Subscription#grantedQos()].
    MANUAL
}
