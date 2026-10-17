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
/// Subscription registration returns its handle immediately, including while disconnected;
/// [Subscription#established()] observes acceptance. See [MqttSubscriber] for synchronous
/// argument and closure failures, and asynchronous registration rejection.
///
/// Within one connection, a QoS 1 retransmission marked DUP joins the original delivery while
/// that delivery's PUBACK is still pending. A QoS 1 publication without DUP, including one that
/// reuses a pending QoS 1 packet identifier, starts a new delivery. After reconnection, a
/// DUP-marked retransmission continues the earlier delivery only when a
/// persistent session resumes, a subscription still holds that delivery, and its PUBACK had
/// not reached the transport; otherwise it starts a new delivery. See [MqttSubscriber].
/// QoS 2 publications that reuse the identifier of an unfinished QoS 2 reception join it until
/// PUBREL, regardless of DUP. This also applies across connections that continue the same
/// session. A QoS 1 publication on such an identifier, or a QoS 2 publication marked DUP on the
/// identifier of an unacknowledged QoS 1 reception, ends the connection with
/// [MqttProtocolException]; without DUP, that QoS 2 publication starts a new delivery.
/// [MqttSubscriber] states when a retransmission reaches the application again. Session state
/// is held in memory, so this does not guarantee exactly-once application processing.
///
/// The client completes operations independently of I/O and listener callbacks. One blocked
/// continuation does not prevent another operation from completing. Slow continuations and
/// listeners retain capacity until they return, so further operations may be rejected.
/// A returned future may already be complete; subsequently attached continuations follow the
/// usual [CompletableFuture] execution rules and may run on the attaching application thread.
///
/// Once a packet requiring SUBACK, UNSUBACK, PUBACK, PUBREC, or PUBCOMP has been written,
/// its response must arrive within 60 seconds. Each QoS 2 stage is timed separately;
/// other responses and PINGRESP do not extend the deadline. Expiry ends the connection with
/// [java.util.concurrent.TimeoutException] as its cause, even when keep-alive is disabled.
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
    /// re-creation refusal ends only the affected subscription and is reported by
    /// [ConnectionListener#onResubscribeFailed(Subscription, MqttSubscribeException)]. A pull
    /// subscription also reports it through [Subscription#receive()].
    /// Registrations still awaiting their first SUBACK are sent after CONNACK as well.
    /// [Subscription#established()] can observe each subscription independently of this future.
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
    /// fails during the attempt; see [MqttSubscriber]. Calls during an attempt join its result.
    /// While connected, returns the current successful CONNACK without opening another connection.
    /// Calls while a disconnect is pending fail with [IllegalStateException].
    /// If [#close] wins before the attempt starts, it also fails with [IllegalStateException].
    CompletableFuture<ConnAck> connect();

    /// Sends DISCONNECT and completes after writing it and requesting transport closure.
    /// Completion does not confirm that the broker received DISCONNECT.
    /// Wait for successful completion before calling [#close()]; closing earlier may discard
    /// the pending DISCONNECT and cause the broker to publish the Will.
    /// This does not wait for PUBACK or PUBCOMP, although either can complete its publication
    /// before the connection ends. Other pending QoS 1 and QoS 2 publications fail with
    /// [MqttConnectionLostException] when the connection ends; their outcome is unknown.
    /// A persistent session retains their exchanges for a later [#connect()], without changing
    /// those failed futures. A nonpersistent session discards them.
    /// Wait for each publication's future before disconnecting to observe its acknowledgement.
    /// Subscription registrations and pending establishment observations survive disconnection.
    /// Pending removals complete when a nonpersistent connection ends; a persistent session's
    /// removals wait for confirmation on a later connection.
    ///
    /// With no connection or attempt, completes locally. During an attempt, waits for CONNACK
    /// and, on success, sends DISCONNECT before closing; the pending connect requests fail.
    /// If the attempt ends without a connection, for example because the broker refuses it,
    /// this completes normally. Concurrent calls observe the same disconnection. A failure of
    /// an established connection before write completion is reported as
    /// [MqttConnectionLostException]. Its write deadline is the configured
    /// [connection timeout][MqttClientBuilder#connectionTimeout(java.time.Duration)].
    CompletableFuture<Void> disconnect();

    /// Requests immediate, permanent shutdown without sending DISCONNECT or waiting for I/O
    /// or application callbacks. Repeated calls have no effect.
    /// Discards local session state; a persistent session may remain on the broker.
    /// A Will registered with the broker may be published. For a graceful disconnect,
    /// wait for [#disconnect] to complete successfully before calling this method.
    @Override
    void close();
}
