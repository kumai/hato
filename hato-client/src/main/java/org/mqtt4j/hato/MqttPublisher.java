package org.mqtt4j.hato;

import java.util.concurrent.CompletableFuture;

/// The publishing role of a client, without access to its connection lifecycle.
@FunctionalInterface
public interface MqttPublisher {

    /// Publishes a message and completes after its transport write for QoS 0, PUBACK for QoS 1,
    /// or PUBCOMP for QoS 2. Completion does not confirm a subscriber's application processing.
    /// QoS 2 requires the full exchange; a broker replying with PUBACK instead of PUBREC ends
    /// the connection with [MqttProtocolException] as the cause. That PUBACK has ended the
    /// broker's exchange, so a persistent session does not resume it.
    ///
    /// An unconnected client fails with [MqttNotConnectedException]. Admission capacity
    /// failures use [MqttCapacityException] and leave the connection usable.
    /// Cancelling or completing the returned future does not cancel the publication.
    /// Connection termination before completion fails with [MqttConnectionLostException];
    /// the publication may already have reached the broker, so its outcome is unknown.
    /// For a persistent session, incomplete QoS 1 and QoS 2 exchanges remain in the same client.
    /// A later [MqttClient#connect()] with [ConnAck#sessionPresent()] true resumes them;
    /// false discards them without replay. A failed future stays failed even if its exchange
    /// later completes. Neither result proves that a publication never reached the broker.
    /// Closing the client discards its local exchanges. QoS 0 is never replayed.
    /// New publications fail with [MqttCapacityException] while replay is waiting for send
    /// capacity after reconnecting. Subscription re-creation waiting for SUBACK does not itself
    /// block publication. Retained QoS 1 and QoS 2 exchanges also consume admission capacity
    /// until completed or discarded, even after their futures have failed.
    CompletableFuture<PublishResult> publish(MqttMessage message);
}
