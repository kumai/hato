package org.mqtt4j.hato;

/// What a push subscription does when its handler throws for a delivery. Set it with
/// [Subscribe#withFailurePolicy(FailurePolicy)]; pull subscriptions have no handler and accept
/// only [#STOP].
///
/// Either policy applies only to exceptions that escape the handler. To retry a transient
/// failure, catch it and repeat the work inside the handler, which keeps the subscription's
/// order and holds back the acknowledgement meanwhile. A skipped failure is logged as a warning
/// the first time for each subscription, and at debug level after that.
public enum FailurePolicy {

    /// Stops the client when the failed delivery still holds back its acknowledgement: a QoS 1
    /// or 2 delivery with [AckMode#AFTER_HANDLER], or with [AckMode#MANUAL] before
    /// [MqttDelivery#ack()]. The failure ends the connection and withdraws the connection
    /// request, as [MqttSubscriber#subscribe(Subscribe, java.util.function.Consumer)] describes,
    /// so the unacknowledged message can come back in a continued session instead of being lost
    /// silently. A failed delivery that holds back no acknowledgement (QoS 0,
    /// [AckMode#ON_RECEIVE], or MANUAL after `ack()`) has nothing to protect, so it is skipped
    /// as [#SKIP] skips it. The default.
    STOP,

    /// Skips every failed delivery: a QoS 1 or 2 delivery is acknowledged without processing,
    /// and a QoS 0 delivery is dropped. The subscription and the connection continue, and a
    /// message that matched other subscriptions still reaches them. Use it when the handler's
    /// failures are deterministic, so a redelivery would fail again.
    SKIP
}
