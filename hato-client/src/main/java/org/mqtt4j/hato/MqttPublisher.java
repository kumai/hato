package org.mqtt4j.hato;

import java.util.concurrent.CompletableFuture;

/// The publishing role of a client, without access to its connection lifecycle.
@FunctionalInterface
public interface MqttPublisher {

    /// Publishes a message and completes after its transport write for QoS 0, PUBACK for QoS 1,
    /// or PUBCOMP for QoS 2. Completion does not confirm a subscriber's application processing.
    /// QoS 2 requires the full exchange. A broker without QoS 2, such as RabbitMQ with
    /// MQTT 3.1.1, replies with PUBACK instead of PUBREC: it has accepted the message at QoS 1
    /// and may deliver it more than once. The publication then fails with
    /// [MqttProtocolException] while the connection stays usable, and a persistent session does
    /// not resume it.
    ///
    /// An MQTT 5.0 broker acknowledges a QoS 1 or QoS 2 publication with a reason code, which
    /// [PublishResult#reasonCode()] reports, for example when no subscription matched. It can
    /// also refuse the publication in PUBACK or PUBREC. The publication then fails with
    /// [MqttPublicationRefusedException], which carries the reason code and any reason string,
    /// and the connection stays usable. A publication that exceeds a limit that the
    /// broker announced in [ConnAck] fails the same way without being sent. QoS 1 and QoS 2
    /// publications beyond the broker's Receive Maximum wait in publish order until the broker
    /// acknowledges earlier ones: this method still returns at once, the future completes
    /// after the acknowledgement, and waiting publications count toward the admission capacity.
    /// QoS 0 publications do not wait.
    ///
    /// The MQTT 5.0 properties of the message, such as [MqttMessage#contentType()], travel with
    /// the PUBLISH. An MQTT 3.1.1 client cannot carry them: a message with any of them fails
    /// with [IllegalArgumentException] without being sent.
    ///
    /// A client without a connection, including one waiting to reconnect or with a requested
    /// disconnect pending, fails with [MqttNotConnectedException]. Admission capacity failures
    /// use [MqttCapacityException] and leave the connection usable.
    /// Cancelling or completing the returned future does not cancel the publication.
    ///
    /// When the connection ends before completion, the publication may already have reached
    /// the broker, so its outcome is unknown. QoS 0 then fails with
    /// [MqttConnectionLostException] and is never replayed. Incomplete QoS 1 and QoS 2 exchanges
    /// of a persistent session remain in the same client. While the client reconnects
    /// automatically, their futures keep waiting: a connection with
    /// [ConnAck#sessionPresent()] true resumes the exchanges and completes the futures as they
    /// finish, and false discards the exchanges without replay and fails the futures with
    /// [MqttConnectionLostException]. Otherwise the future fails with
    /// [MqttConnectionLostException]: when the connection ends, if the session is nonpersistent
    /// or the client schedules no reconnection; or, for a future kept across a reconnection, as
    /// soon as the client gives the reconnection up, through [MqttClient#disconnect()], closing,
    /// a handler failure, or a failure that [ReconnectPolicy] does not retry. A later
    /// [MqttClient#connect()] with Session Present still resumes a persistent exchange, but a
    /// failed future stays failed even if its exchange later completes. Neither result proves
    /// that a publication never reached the broker. Closing the client discards its local
    /// exchanges.
    /// New publications fail with [MqttCapacityException] while an earlier exchange waits for
    /// send capacity, as replay can after reconnecting. Subscription re-creation waiting for
    /// SUBACK does not itself block publication. Retained QoS 1 and QoS 2 exchanges also
    /// consume admission capacity until completed or discarded, even after their futures have
    /// failed.
    ///
    /// @param message
    ///     the message to publish
    /// @return
    ///     a future that completes with the result of the publication
    CompletableFuture<PublishResult> publish(MqttMessage message);
}
