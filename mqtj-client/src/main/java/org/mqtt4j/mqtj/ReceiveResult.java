package org.mqtt4j.mqtj;

import static java.util.Objects.requireNonNull;

/// The outcome of one pull receive. A timeout leaves the subscription active; normal
/// termination ends it permanently. Connection failures and interruption are thrown by
/// [Subscription#receive()].
public sealed interface ReceiveResult {

    /// One delivery handed to this caller. Its acknowledgement follows the subscription's
    /// [AckMode]; receiving it does not imply that application processing has finished.
    record Received(MqttDelivery delivery) implements ReceiveResult {
        public Received {
            requireNonNull(delivery, "delivery");
        }
    }

    /// No delivery became available before the wait expired. Also returned by a zero-duration
    /// poll of an active subscription with no available delivery.
    record TimedOut() implements ReceiveResult {
    }

    /// The subscription ended normally. No subsequent receive on this handle can deliver a
    /// message, including after the client reconnects.
    record Ended() implements ReceiveResult {
    }
}
