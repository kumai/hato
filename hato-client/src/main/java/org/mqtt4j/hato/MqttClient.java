package org.mqtt4j.hato;

import java.util.concurrent.CompletableFuture;

/// Owns an MQTT connection. [#connect()] requests a connection, which the client keeps
/// re-establishing after a connection ends, as its [ReconnectPolicy] allows, until
/// [#disconnect()] or [#close()] withdraws the request or a failure that the policy does not
/// retry ends it; see [MqttClientBuilder#reconnectPolicy(ReconnectPolicy)].
/// A disconnected client can connect again until it is closed.
/// Its methods and those of its subscriptions are safe to call concurrently from any thread.
///
/// Operations report failure through their returned futures. Admission can fail immediately
/// with [MqttCapacityException], or with [IllegalStateException] after the client is closed.
/// One [#connect()] call and one [#disconnect()] call are admitted even while publications fill
/// the operation capacity; further concurrent calls share that capacity. A publication too
/// large for the client to send fails with [MqttPublicationRefusedException] without being sent,
/// whatever maximum incoming packet size is configured.
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
/// continuation does not prevent another operation from completing. Slow continuations of
/// observations, such as [Subscription#established()], retain their slots until they return,
/// so further observations may be rejected; slow listeners lose their oldest notifications
/// instead; see [ConnectionListener].
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
    ///
    /// @return
    ///     a new builder
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
    /// when the broker refuses the connection with a reason that reconnecting cannot change;
    /// when the client's [TimerScheduler] fails; or when [#disconnect()], [#close()], or a handler
    /// failure withdraws the request first. Its reason code or cause tells which, as for
    /// [ConnectionListener#onConnectFailed(Disconnection)]; a request that [#disconnect()] or
    /// [#close()] withdraws carries neither. With
    /// [ReconnectPolicy#withInitialRetry()], and for a call that joins an automatic reconnection,
    /// the future can wait without a deadline. Timing out the returned future does not stop the
    /// attempts; [#disconnect()] does.
    ///
    /// With an existing local persistent session, [ConnAck#sessionPresent()] true continues
    /// pending QoS 1 and QoS 2 exchanges and established subscriptions. False discards the old
    /// exchanges, failing their pending publications, and re-creates subscriptions using their
    /// existing handles. Previously failed publication futures remain failed. This future does
    /// not wait for replay or SUBACK; a re-creation refusal ends only the affected subscription
    /// and is reported by
    /// [ConnectionListener#onResubscribeFailed(Subscription, MqttSubscriptionRefusedException)].
    /// A pull subscription also reports it through [PullSubscription#receive()].
    /// Registrations still awaiting their first SUBACK are sent after CONNACK as well.
    /// [Subscription#established()] can observe each subscription independently of this future.
    ///
    /// Local session state is held only in this client object's memory, so a new client object,
    /// for example after the application restarts, has none for a session that the broker
    /// kept. Its first successful attempt replaces such a session with a new one, so messages
    /// that the broker stored for the old session, including those that arrived while the
    /// application was stopped, are not received. With MQTT 5.0, the attempt connects with
    /// Clean Start, which makes the broker discard the old session. With MQTT 3.1.1, the broker
    /// reports the old session with Session Present, and the attempt replaces it before it
    /// completes: the client sends DISCONNECT, connects with Clean Session 1 and without the
    /// Will to discard the session, sends DISCONNECT again, and connects as configured to start
    /// a new session. The broker may still publish the Will of the first connection if that
    /// connection fails before its DISCONNECT is written, or if the broker handles the next
    /// connection before that DISCONNECT. These connections share the attempt's number and
    /// credentials, and the listener is notified only of the attempt's outcome. Each
    /// connection and each DISCONNECT write has the whole
    /// [connection timeout][MqttClientBuilder#connectionTimeout(java.time.Duration)], and a
    /// failure on any of them fails the attempt as on an ordinary connection. This future
    /// completes with the last CONNACK, whose [ConnAck#sessionPresent()] is false. If the
    /// connection that starts the new session finds a session again, the client sends
    /// DISCONNECT and the attempt fails with [MqttConnectException], which
    /// [ReconnectPolicy#withInitialRetry()] retries like other initial failures; a failure
    /// while closing that connection is attached as a suppressed exception. A broker that
    /// limits how often one client identifier may connect, as AWS IoT Core allows one CONNECT
    /// per second, may refuse or close the later connections and fail the attempt; MQTT 5.0
    /// needs only one connection.
    ///
    /// @return
    ///     a future that completes with the successful CONNACK that establishes the connection
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
    /// per-client limit described by [Subscription#established()]: a full limit returns
    /// a future already failed with [MqttCapacityException], and each call made while
    /// disconnected holds a slot until the client resolves it. To check the state periodically,
    /// keep one pending future and call again only after it completes. A future that then
    /// completed normally means that the client is connected; a failed one means that the client
    /// is closed or the limit is full.
    ///
    /// @return
    ///     a future that completes with the CONNACK of the current or next established connection
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
    /// refuses it, this completes normally. An attempt that is replacing a broker session
    /// without local state, as [#connect()] describes, stops in the same way at its current
    /// connection and opens no further one. The old session then remains on the broker unless
    /// the broker accepts the connection that discards it, which this call does not prevent
    /// once that connection has started; an attempt that stops at the connection that starts the
    /// new session keeps that session, as any other attempt would. Concurrent calls observe the
    /// same disconnection. If a connection that the broker accepted, including one that such an
    /// attempt is closing, fails before its DISCONNECT is written, this fails with
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
    ///
    /// @return
    ///     a future that completes when the client has disconnected
    CompletableFuture<Void> disconnect();

    /// Requests immediate, permanent shutdown without sending DISCONNECT or waiting for I/O
    /// or application callbacks. Repeated calls have no effect. This also stops reconnection
    /// and fails pending connection requests and publications.
    /// No delivery is handed off after this returns: handlers start and receivers return only
    /// for deliveries taken before. Those handlers, and other application code that the client
    /// is already running, may still run; [#terminated()] observes the end of all of it.
    /// Discards local session state; a persistent session may remain on the broker until a
    /// later client object with the same identifier replaces it on connecting.
    /// A Will registered with the broker may be published. For a graceful disconnect,
    /// wait for [#disconnect()] to complete successfully before calling this method.
    @Override
    void close();

    /// Observes the end of this client's work without requesting it; it completes only after the
    /// client is closed. The future completes once the client runs no application code and calls
    /// none later: every subscription has terminated, as [Subscription#terminated()] observes, so
    /// no handler runs; the [ConnectionListener] has returned from every notification it receives,
    /// including the last one that closing produces, while notifications dropped because it fell
    /// behind never arrive; a credentials supplier call, which closing interrupts, has returned;
    /// every transport has finished closing; and the synchronous continuations of the futures that
    /// the client completes have returned, except those of this method. Afterwards the client calls
    /// no handler, executor chosen with [DeliveryOptions], listener, supplier, [TimerScheduler],
    /// [Transport], or continuation, and a reconnection timer that fires late does nothing.
    ///
    /// It does not wait for work that application code started, such as tasks submitted to the
    /// application's own executors, nor for the application's own calls into the client, such as
    /// a waiting [PullSubscription#receive()], which closing ends promptly. Code that never returns
    /// keeps the future pending; it never completes on a timeout.
    ///
    /// Each call returns an independent future; after termination, a completed one. Pending
    /// observations, including those made before [#close()], share the client-wide limit
    /// described by [Subscription#established()], and a full limit returns a future already
    /// failed with [MqttCapacityException]; the future fails in no other way.
    ///
    /// Release what the client's callbacks use, such as an injected [TimerScheduler] or the
    /// resources of a custom transport, from a continuation of this future or after it completes.
    /// Waiting for it inside a handler, listener, supplier, or continuation that the client runs,
    /// or on a thread that such code needs in order to return, never completes.
    ///
    /// @return
    ///     a future that completes at the end of this client's work
    CompletableFuture<Void> terminated();

    /// Takes a snapshot of what this client holds and has counted, which only the client can
    /// know; see [ClientStats]. The client collects it at one point, so its values agree with
    /// each other. Calling it often does not crowd the client. After the client is closed, the
    /// future completes with the snapshot collected when the client stopped, which no longer
    /// changes.
    ///
    /// Each call returns an independent future. Pending calls share the client-wide limit
    /// described by [Subscription#established()], and a full limit returns a future already
    /// failed with [MqttCapacityException]. A runtime exception from the time read of the
    /// client's [TimerScheduler] fails the pending calls with that exception and affects the
    /// connection as that interface describes. A client that keeps no statistics, such as an
    /// application's test double, returns a future failed with [UnsupportedOperationException], as
    /// this default method does.
    ///
    /// @return
    ///     a future that completes with the snapshot
    default CompletableFuture<ClientStats> stats() {
        return CompletableFuture.failedFuture(
                new UnsupportedOperationException("This client keeps no statistics")
        );
    }
}
