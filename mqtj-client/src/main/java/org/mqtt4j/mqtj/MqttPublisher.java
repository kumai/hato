package org.mqtt4j.mqtj;

import java.util.concurrent.CompletableFuture;

/// The publishing role of a client, without access to its connection lifecycle.
@FunctionalInterface
public interface MqttPublisher {

    /// Publishes a message and completes after its transport write for QoS 0, or PUBACK for QoS 1.
    /// Neither completion confirms a subscriber's application processing.
    /// The current client supports QoS 0 and QoS 1; QoS 2 fails with
    /// [IllegalArgumentException] through the returned future.
    ///
    /// An unconnected client fails with [MqttNotConnectedException]. Admission capacity
    /// failures use [MqttCapacityException] and leave the connection usable.
    /// Cancelling or completing the returned future does not cancel the publication.
    /// Connection termination before completion fails with [MqttConnectionLostException];
    /// the publication may already have reached the broker, so its outcome is unknown.
    CompletableFuture<PublishResult> publish(MqttMessage message);
}
