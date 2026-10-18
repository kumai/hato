package org.mqtt4j.hato;

import java.util.concurrent.CompletableFuture;

/// Owns an MQTT connection. [#connect()] requests a connection, which the client keeps
/// re-establishing after a connection ends, as its [ReconnectPolicy] allows, until
/// [#disconnect()] or [#close()] withdraws the request or a failure that the policy does not
/// retry ends it; see [MqttClientBuilder#reconnectPolicy].
/// A disconnected client can connect again until it is closed.
/// Its methods and those of its subscriptions are safe to call concurrently from any thread.
///
/// Operations report failure through their returned futures. Admission can fail immediately
/// with [MqttCapacityException], or with [IllegalStateException] after the client is closed.
/// One [#connect()] call and one [#disconnect()] call are admitted even while publications fill
/// the operation capacity; further concurrent calls share that capacity. A publication that
/// exceeds the send capacity on its own always fails with
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
/// continuation does not prevent another operation from completing. Slow continuations
/// retain capacity until they return, so further operations may be rejected; slow listeners
/// lose their oldest notifications instead; see [ConnectionListener].
/// A returned future may already be complete; subsequently attached continuations follow the
/// usual [CompletableFuture] execution rules and may run on the attaching application thread.
///
/// Once a packet requiring SUBACK, UNSUBACK, PUBACK, PUBREC, or PUBCOMP has been written,
/// its response must arrive within the
/// [response timeout][MqttClientBuilder#responseTimeout(java.time.Duration)], 60 seconds by
/// default. Each QoS 2 stage is timed separately; other responses and PINGRESP do not extend
/// the deadline. Expiry ends the connection with [java.util.concurrent.TimeoutException] as
/// its cause, even when keep-alive is disabled, and the client reconnects as after other
/// connection failures. The next connection repeats an unanswered publication stage when it
/// resumes a persistent session, and an unanswered SUBSCRIBE in either kind of session.
public interface MqttClient extends MqttPublisher, MqttSubscriber, AutoCloseable {

    /// Starts configuration without opening a connection.
    static MqttClientBuilder builder() {
        return new MqttClientBuilder();
    }

    /// Requests a connection and completes with the successful CONNACK that establishes it.
    /// The client keeps the request until [#disconnect()] or [#close()], and reconnects after
    /// the connection ends as its [ReconnectPolicy] allows.
    ///
    /// Without a connection or a standing request, this starts an attempt. A call during an
    /// attempt, or while the client waits to reconnect, joins the standing request: its future
    /// completes with the next established connection and fails only when the client gives the
    /// request up. While the client waits to reconnect, this also cancels the wait and attempts
    /// at once, but does not shorten later reconnection delays. While connected, this returns
    /// the current successful CONNACK without opening another connection. Calls while a
    /// disconnect is pending, or after [#close()], fail with [IllegalStateException]; so can a
    /// call admitted just before [#close()].
    ///
    /// The future fails with [MqttConnectException] when the client gives up the request:
    /// when the first attempt of a request fails, unless the policy retries initial failures;
    /// when the broker refuses the connection with a reason that reconnecting cannot change, or
    /// keeps a session without local state, as described below; when the client's
    /// [org.mqtt4j.hato.spi.TimerScheduler] fails; or when [#disconnect()], [#close()], or a
    /// handler failure withdraws the request first. Its message or cause tells which, as for
    /// [ConnectionListener#onConnectFailed(Disconnection)]. With
    /// [ReconnectPolicy#withInitialRetry()], and for a call that joins an automatic
    /// reconnection, the future can wait without a deadline. Timing out the returned future
    /// does not stop the attempts; [#disconnect()] does.
    ///
    /// With an existing local persistent session, [ConnAck#sessionPresent()] true continues
    /// pending QoS 1 and QoS 2 exchanges and established subscriptions. False discards the old
    /// exchanges, failing their pending publications, and re-creates subscriptions using their
    /// existing handles. Previously failed publication futures remain failed. This future does
    /// not wait for replay or SUBACK; a re-creation refusal ends only the affected subscription
    /// and is reported by
    /// [ConnectionListener#onResubscribeFailed(Subscription, MqttSubscribeException)]. A pull
    /// subscription also reports it through [Subscription#receive()].
    /// Registrations still awaiting their first SUBACK are sent after CONNACK as well.
    /// [Subscription#established()] can observe each subscription independently of this future.
    ///
    /// Local session state is held only in this client object's memory. On a persistent
    /// connection, if the broker reports Session Present without matching local state, the
    /// client sends DISCONNECT, closes that connection, and gives up the request with
    /// [MqttConnectException]. Its message preserves the session refusal; a failure while
    /// closing is attached as a suppressed exception. The broker keeps that session, so later
    /// attempts fail the same way until it is discarded. Connecting a client built with the
    /// same identifier and the default [MqttClientBuilder#cleanStart(boolean)] discards it.
    CompletableFuture<ConnAck> connect();

    /// Observes the current or next established connection without requesting one. Returns an
    /// already completed future with the current CONNACK while connected and no requested
    /// disconnection is pending. Otherwise it completes with the CONNACK of the next
    /// established connection, after which operations are admitted on that connection.
    /// Failed attempts and connection loss do not fail the observation. After an ordinary
    /// [#close()], it fails with [IllegalStateException]; after an internal failure that
    /// permanently closes the client, with [MqttConnectionLostException] carrying that failure.
    ///
    /// Each call returns an independent future. Completing, cancelling, or timing out that future
    /// changes neither the connection nor another observation. Pending observations share the
    /// limit of 1,024 per client described by [Subscription#established()]: a full limit returns
    /// a future already failed with [MqttCapacityException], and each call made while
    /// disconnected holds a slot until the client resolves it. To check the state periodically,
    /// keep one pending future and call again only after it completes. A future that then
    /// completed normally means that the client is connected; a failed one means that the client
    /// is closed or the limit is full.
    CompletableFuture<ConnAck> whenConnected();

    /// Withdraws the connection request: the client stops reconnecting, and an established
    /// connection ends with DISCONNECT. Completes after writing DISCONNECT and requesting
    /// transport closure. Completion does not confirm that the broker received DISCONNECT.
    /// Wait for successful completion before calling [#close()]; closing earlier may discard
    /// the pending DISCONNECT and cause the broker to publish the Will.
    /// A pending [#connect()] fails.
    ///
    /// With no connection or attempt, including while the client waits to reconnect, completes
    /// at once. During an attempt, waits for CONNACK and, on success, sends DISCONNECT before
    /// closing. If the attempt ends without a connection, for example because the broker
    /// refuses it, this completes normally. Concurrent calls observe the same disconnection. A
    /// failure of an established connection before write completion is reported as
    /// [MqttConnectionLostException]. Its write deadline is the configured
    /// [connection timeout][MqttClientBuilder#connectionTimeout(java.time.Duration)].
    ///
    /// This does not wait for PUBACK or PUBCOMP, although either can complete its publication
    /// before the connection ends. Other pending QoS 1 and QoS 2 publications fail with
    /// [MqttConnectionLostException], including those waiting for a reconnection; their outcome
    /// is unknown. A persistent session retains their exchanges for a later [#connect()],
    /// without changing those failed futures. A nonpersistent session discards them.
    /// Wait for each publication's future before disconnecting to observe its acknowledgement.
    ///
    /// When this completes, queued deliveries that no handler or receiver has taken have been
    /// discarded, and no further delivery of the ended connection is handed off. Handlers that
    /// already took a delivery can still run.
    /// Subscription registrations and pending establishment observations survive disconnection.
    /// Pending removals complete when a nonpersistent connection ends; a persistent session's
    /// removals wait for confirmation on a later connection.
    CompletableFuture<Void> disconnect();

    /// Requests immediate, permanent shutdown without sending DISCONNECT or waiting for I/O
    /// or application callbacks. Repeated calls have no effect. This also stops reconnection
    /// and fails pending connection requests and publications.
    /// Discards local session state; a persistent session may remain on the broker.
    /// A Will registered with the broker may be published. For a graceful disconnect,
    /// wait for [#disconnect] to complete successfully before calling this method.
    @Override
    void close();
}
