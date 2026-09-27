package org.mqtt4j.mqtj;

import java.util.concurrent.CompletableFuture;

/// Owns an MQTT connection. A disconnected client can connect again until it is closed.
/// Its methods and those of its subscriptions are safe to call concurrently from any thread.
///
/// Operations report failure through their returned futures. Admission can fail immediately
/// with [MqttCapacityException], or with [IllegalStateException] after the client is closed.
/// A publication that exceeds the send capacity on its own always fails with
/// [MqttCapacityException], whatever maximum incoming packet size is configured.
/// Cancelling or completing a returned future does not cancel the underlying operation.
///
/// The client completes operations independently of I/O and listener callbacks. One blocked
/// continuation does not prevent another operation from completing. Slow continuations and
/// listeners retain capacity until they return, so further operations may be rejected.
/// A returned future may already be complete; subsequently attached continuations follow the
/// usual [CompletableFuture] execution rules and may run on the attaching application thread.
public interface MqttClient extends MqttPublisher, MqttSubscriber, AutoCloseable {

    /// Starts configuration without opening a connection.
    static MqttClientBuilder builder() {
        return new MqttClientBuilder();
    }

    /// Establishes a clean session and completes after a successful CONNACK.
    ///
    /// The future fails with [MqttConnectException] if establishment fails, the broker refuses
    /// the connection, or the connection deadline expires. Connecting while already connected
    /// or while another attempt is pending fails with [IllegalStateException].
    /// If [#close] wins before the attempt starts, it also fails with [IllegalStateException].
    CompletableFuture<ConnAck> connect();

    /// Sends DISCONNECT and completes after writing it and requesting transport closure.
    /// Completion does not confirm that the broker received DISCONNECT.
    ///
    /// The future fails with [MqttNotConnectedException] when there is no established connection
    /// or a disconnect is already pending. A connection failure before the write completes is
    /// reported as [MqttConnectionLostException]. The write is bounded by the configured
    /// [connection timeout][MqttClientBuilder#connectionTimeout(java.time.Duration)].
    CompletableFuture<Void> disconnect();

    /// Requests immediate, permanent shutdown without sending DISCONNECT or waiting for I/O
    /// or application callbacks. Repeated calls have no effect.
    /// A Will registered with the broker may be published. For a graceful disconnect,
    /// wait for [#disconnect] to complete before calling this method.
    @Override
    void close();
}
