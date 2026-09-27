package org.mqtt4j.mqtj;

import java.util.concurrent.CompletableFuture;

/// The publishing role of a client, without access to its connection lifecycle.
@FunctionalInterface
public interface MqttPublisher {

    /// Publishes a message and completes after the transport has written it.
    /// The current client supports only QoS 0; other QoS values fail with
    /// [IllegalArgumentException] through the returned future.
    ///
    /// An unconnected client fails with [MqttNotConnectedException]. Admission capacity
    /// failures use [MqttCapacityException] and leave the connection usable.
    /// Cancelling or completing the returned future does not cancel the publication.
    CompletableFuture<PublishResult> publish(MqttMessage message);
}
