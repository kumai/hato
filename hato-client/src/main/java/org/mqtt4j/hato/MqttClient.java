package org.mqtt4j.hato;

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
/// Within one connection, a QoS 1 retransmission marked DUP joins the original delivery while
/// that delivery's PUBACK is still pending. Any other publication on that connection, including
/// one without DUP that reuses a pending packet identifier, starts a new delivery. After
/// reconnection, a DUP-marked retransmission continues the earlier delivery only when a
/// persistent session resumes, a subscription still holds that delivery, and its PUBACK had
/// not reached the transport; otherwise it starts a new delivery. See [MqttSubscriber].
/// This does not guarantee exactly-once processing.
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

    /// Establishes a connection and completes after a successful CONNACK.
    /// With an existing local persistent session, [ConnAck#sessionPresent()] true continues
    /// pending QoS 1 and QoS 2 exchanges and established subscriptions. False discards the old
    /// exchanges and re-creates subscriptions using their existing handles. Previously failed
    /// publication futures remain failed. This future does not wait for replay or SUBACK; a
    /// re-creation refusal ends only the affected subscription. A pull subscription reports it
    /// through [Subscription#receive()].
    ///
    /// Local session state is held only in this client object's memory. On a persistent
    /// connection, if the broker reports Session Present without matching local state, the
    /// client sends DISCONNECT, closes that connection, and fails this attempt with
    /// [MqttConnectException]. Its message preserves the session refusal; a failure while
    /// closing is attached as a suppressed exception. The broker keeps that session, so later
    /// attempts fail the same way until it is discarded. Connecting a client built with the
    /// same identifier and the default [MqttClientBuilder#cleanStart(boolean)] discards it.
    ///
    /// The future fails with [MqttConnectException] if establishment fails, the broker refuses
    /// the connection, the connection deadline expires, or a handler of a continued session
    /// fails during the attempt; see [MqttSubscriber]. Connecting while already connected or
    /// while another attempt is pending fails with [IllegalStateException].
    /// If [#close] wins before the attempt starts, it also fails with [IllegalStateException].
    CompletableFuture<ConnAck> connect();

    /// Sends DISCONNECT and completes after writing it and requesting transport closure.
    /// Completion does not confirm that the broker received DISCONNECT.
    /// This does not wait for PUBACK or PUBCOMP, although either can complete its publication
    /// before the connection ends. Other pending QoS 1 and QoS 2 publications fail with
    /// [MqttConnectionLostException] when the connection ends; their outcome is unknown.
    /// A persistent session retains their exchanges for a later [#connect()], without changing
    /// those failed futures. A nonpersistent session discards them.
    /// Wait for each publication's future before disconnecting to observe its acknowledgement.
    ///
    /// The future fails with [MqttNotConnectedException] when there is no established connection
    /// or a disconnect is already pending. A connection failure before the write completes is
    /// reported as [MqttConnectionLostException]. The write is bounded by the configured
    /// [connection timeout][MqttClientBuilder#connectionTimeout(java.time.Duration)].
    CompletableFuture<Void> disconnect();

    /// Requests immediate, permanent shutdown without sending DISCONNECT or waiting for I/O
    /// or application callbacks. Repeated calls have no effect.
    /// Discards local session state; a persistent session may remain on the broker.
    /// A Will registered with the broker may be published. For a graceful disconnect,
    /// wait for [#disconnect] to complete before calling this method.
    @Override
    void close();
}
