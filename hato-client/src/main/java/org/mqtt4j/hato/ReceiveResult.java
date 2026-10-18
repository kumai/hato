package org.mqtt4j.hato;

import static java.util.Objects.requireNonNull;

/// The outcome of one pull receive. A timeout leaves the subscription active; normal
/// termination ends it permanently. Connection termination leaves receiving subscriptions
/// waiting for a later connection. Failures that end the subscription, and interruption, are
/// thrown by [Subscription#receive()], which lists them.
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

    /// The subscription ended through its close, a successful unsubscribe once the deliveries it
    /// accepted have been received, or [MqttClient#close()]. No subsequent receive on this handle
    /// can deliver a message, including after the client reconnects.
    record Ended() implements ReceiveResult {
    }
}
