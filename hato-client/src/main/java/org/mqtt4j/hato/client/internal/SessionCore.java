package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeoutException;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttCredentials;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttSubscriptionRefusedException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.client.internal.ConnectionEstablishment.Outcome;
import org.mqtt4j.hato.client.internal.ConnectionEstablishment.Step;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.client.internal.common.protocol.PacketIds;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A single-owner state machine. Time is input; outputs are nonblocking commands.
/// Operation tokens belong to the shell, which alone owns and completes futures.
///
/// The core keeps the connection's lifecycle and decides the order across three parts:
/// [Publications] for outgoing exchanges, [Subscriptions] for registrations, and [Receptions]
/// for incoming publications. The parts never call one another. They reach the core through
/// [Context], and each replaces its own state of a connection or session that the core ends.
/// A fourth part, [ConnectionEstablishment], keeps no state: it builds each connection's CONNECT
/// and tells the core what its CONNACK means for the attempt.
final class SessionCore {

    /// DISCONNECTING follows an accepted disconnect(), both while its attempt still waits for
    /// CONNACK or for the DISCONNECT of a connection that it abandoned, and while the
    /// established connection writes DISCONNECT.
    enum State {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
        DISCONNECTING,
        RECONNECT_WAIT,
        CLOSED
    }

    enum Timer {
        CONNECT,
        KEEP_ALIVE,
        PING,
        DISCONNECT,
        RECONNECT,
        STABLE,
        RESPONSE
    }

    private enum EndReason {
        CONNECTION_FAILURE,
        DISCONNECT_WRITTEN,
        HANDLER_FAILURE,
        SCHEDULER_FAILURE,
        CLIENT_CLOSE
    }

    interface Port {

        void beginAttempt(long generation, ConnectionAttempt attempt, long attemptNumber);

        /// Moves the current attempt to its next connection, numbered `generation`, without
        /// announcing the end of the previous one. The previous connection's queued and late
        /// events no longer reach the core. It started no session and handed nothing to the
        /// application, so none of the cleanup that [#end] performs is due.
        void continueAttempt(long generation);

        void connectionRequested(boolean requested);

        void evaluateCredentials(ConnectionAttempt attempt);

        /// Called when the attempt ends before its credentials arrive.
        void cancelCredentials(ConnectionAttempt attempt);

        void openTransport(long generation, Duration timeout);

        /// Opens admission for the connection a CONNACK established. Continued subscriptions are
        /// reconciled next, so observers completed there can already use the connection.
        void admit(long generation);

        /// Announces the admitted connection once continued subscriptions are reconciled.
        void connected(long generation, ConnAck ack);

        /// A requested disconnect is pending; established observations wait for a later
        /// connection.
        void disconnecting();

        /// Releases lanes paused by a stop that a withdrawn reconnect wait discarded.
        void stopDiscarded();

        void resubscribeFailed(Subscription handle, MqttSubscriptionRefusedException failure);

        /// Reports queue admission, not write completion. The core handles refusal per operation.
        SendQueue.OfferResult send(
                long generation,
                long ticket,
                Packet packet,
                @Nullable SendReceipt receipt
        );

        /// Reports admission at the exact wire size, not write completion. The payload is copied
        /// only after admission. DUP comes from `receipt` when the packet is encoded.
        SendQueue.OfferResult publish(
                long generation,
                long ticket,
                MqttMessage message,
                int packetId,
                @Nullable SendReceipt receipt
        );

        void schedule(long generation, Timer timer, Duration delay);

        void cancel(Timer timer);

        void ignoredPingResponse();

        void complete(long operation, @Nullable Object result, @Nullable Throwable failure);

        Optional<Subscription> registerSubscription(long id);

        void subscribed(Subscription handle, Qos granted);

        boolean deliver(
                DeliveryToken token,
                List<Long> ids,
                MqttMessage message,
                boolean dup,
                int bytes
        );

        void releaseDelivery(long token);

        void forgetDelivery(long token);

        /// Maps each reception handed to at least one recipient to whether every recipient has
        /// met its acknowledgement condition. Receptions handed to no recipient are omitted.
        Map<Long, Boolean> receiveState();

        boolean readyDelivery(long token);

        /// The numbered session replaced the previous one, whose receptions and failures lapse.
        void sessionChanged(long session);

        void removeSubscription(long id, Subscription handle, @Nullable Throwable cause);

        /// Returns [#receiveState()] after discarding queued deliveries. Without
        /// `preserveSession` no reception remains; a stop also invalidates unmet conditions.
        /// `disconnection` describes the ended connection when `established`, and otherwise the
        /// failed attempt, or the attempt that a stop while waiting to reconnect cancelled.
        Map<Long, Boolean> end(
                long generation,
                boolean established,
                Disconnection disconnection,
                boolean preserveSession,
                boolean stopped
        );
    }

    /// What the core lends its parts. The core owns every value behind it; a part keeps no value
    /// that a connection end or a new session replaces.
    interface Context {

        State state();

        long generation();

        /// The number of the attempt that established the current connection.
        long attempt();

        long sessionNumber();

        /// Numbers a write; tickets identify write completions across the client.
        long nextTicket();

        /// Ends the current connection with `cause`.
        void fail(Throwable cause);

        /// The response deadlines of the current connection.
        ResponseDeadlines responses();

        /// Takes a session packet identifier that no unknown PUBREC still holds.
        OptionalInt allocatePacketId();

        void releasePacketId(int packetId);

        /// Sends queued exchanges, PUBREC responses, and registrations, in that order.
        void drain();

        /// Lets acknowledgements, removals, and other queued work use returned send capacity.
        void sendCapacityAvailable();

        /// An acknowledgement took the reserved send quota, so PUBREL goes first next time.
        void acknowledgementAdmitted();

        /// A PUBREL took the reserved send quota, so acknowledgements go first next time.
        void releaseAdmitted();

        /// The registrations whose filter matches `topic`, except those being removed.
        List<Long> matching(Topic topic);

        /// The limits that the broker announced for the established connection. Only valid
        /// while a connection is established.
        ConnAck limits();

        /// How many QoS 1 and QoS 2 publications the broker takes at once on the established
        /// connection: its Receive Maximum, which is 65,535 when it announced none. Only valid
        /// while a connection is established.
        int receiveMaximum();
    }

    private static final Log log = Log.of(SessionCore.class);

    private static final Duration STABLE_CONNECTION = Duration.ofSeconds(30);

    private final Port port;

    private final String clientId;

    private final Duration timeout;

    private final Duration responseTimeout;

    // Zero session expiry starts a new session with every connection.
    private final boolean cleanStart;

    private final boolean credentialsRequired;

    private final ConnectionRequest request;

    private final ConnectionEstablishment establishment;

    private final Receptions receptions;

    private final Subscriptions subscriptions;

    private final Publications publications;

    private State state = State.DISCONNECTED;

    private long generation;

    // The core's state of the current connection, including one that its attempt is still
    // opening. Ending the connection replaces it, so the next one starts from fresh values; the
    // parts replace their own. The attempt's credentials stay outside, because an attempt can
    // outlive a connection.
    private Connection current;

    // The attempt that has not established a connection yet, including while it closes a
    // connection that it abandoned, and null once a CONNACK establishes its connection or the
    // attempt ends.
    private @Nullable ConnectionAttempt connecting;

    // The connecting attempt's credentials. Every connection of the attempt sends the same
    // value, and the attempt's end drops it.
    private boolean credentialsReady;

    private @Nullable MqttCredentials attemptCredentials;

    private long nextTicket;

    private long connectionDeadline;

    private Session session = new Session();

    // Identifies the current session to delivery and mailbox code, which run outside the actor
    // and must not reach its state. Every replacement of the session advances it.
    private long sessionNumber = 1;

    // Incoming acknowledgements and outgoing PUBREL share the reserved send quota. Returned
    // room goes first to the side that did not take the last slot, so neither can starve.
    private boolean releasesFirst;

    SessionCore(
            Port port,
            String clientId,
            Duration timeout,
            Duration keepAlive,
            MqttVersion version,
            Duration sessionExpiry,
            int maxPacketSize,
            Packet.@Nullable Will will,
            boolean credentialsRequired,
            ReconnectBackoff backoff,
            boolean initialRetry,
            Duration responseTimeout
    ) {
        this.port = port;
        this.clientId = clientId;
        this.timeout = timeout;
        this.responseTimeout = responseTimeout;
        cleanStart = sessionExpiry.isZero();
        this.credentialsRequired = credentialsRequired;
        request = new ConnectionRequest(backoff, initialRetry);
        current = new Connection(responseTimeout, Step.FIRST);
        establishment = new ConnectionEstablishment(
                clientId,
                version,
                keepAlive,
                sessionExpiry,
                maxPacketSize,
                will
        );
        var context = new PartContext();
        receptions = new Receptions(port, context, clientId);
        subscriptions = new Subscriptions(port, context, clientId, cleanStart);
        publications = new Publications(
                port,
                context,
                clientId,
                cleanStart,
                PacketCodec.of(version)
        );
    }

    State state() {
        return state;
    }

    /// Whether an attempt has not established its connection yet, possibly with a requested
    /// disconnect pending.
    private boolean attempting() {
        return connecting != null;
    }

    /// Whether a successful CONNACK established the current connection, which may be
    /// disconnecting.
    private boolean established() {
        return current.ack != null;
    }

    /// Whether the current generation has an attempt or connection that input can still reach.
    private boolean connectionActive() {
        return state == State.CONNECTING
                || state == State.CONNECTED
                || state == State.DISCONNECTING;
    }

    long sessionNumber() {
        return sessionNumber;
    }

    void connect(ConnectionAttempt attempt, long now) {
        if (state == State.CLOSED || state == State.DISCONNECTING) {
            if (attempt.operation != 0) {
                port.complete(
                        attempt.operation,
                        null,
                        new IllegalStateException(
                                state == State.CLOSED ? "Client is closed" : "Disconnect is pending"
                        )
                );
            }
            return;
        }
        if (state == State.CONNECTED) {
            if (attempt.operation != 0) {
                port.complete(attempt.operation, requireNonNull(current.ack), null);
            }
            return;
        }
        if (attempt.operation != 0) {
            request.awaitConnection(attempt.operation);
        }
        if (state == State.CONNECTING) {
            return;
        }
        if (state == State.RECONNECT_WAIT) {
            port.cancel(Timer.RECONNECT);
        }
        request.raise();
        port.connectionRequested(true);
        startAttempt(attempt, now);
    }

    private void startAttempt(ConnectionAttempt attempt, long now) {
        state = State.CONNECTING;
        generation++;
        long number = request.nextAttempt();
        connecting = attempt;
        credentialsReady = !credentialsRequired;
        attemptCredentials = null;
        current.lastSent = now;
        connectionDeadline = now + timeout.toNanos();
        port.beginAttempt(generation, attempt, number);
        port.schedule(generation, Timer.CONNECT, timeout);
        if (credentialsReady) {
            port.openTransport(generation, timeout);
        } else {
            port.evaluateCredentials(attempt);
        }
    }

    /// Opens the attempt's next connection after the previous one wrote its DISCONNECT. The
    /// attempt keeps its number, credentials, and waiting calls, and neither the application nor
    /// the listener sees the replaced connection. The parts keep no state of a connection before
    /// a CONNACK establishes it, so the replaced connection leaves them nothing to settle.
    private void openNextConnection(long now) {
        Step next = current.step.next();
        port.cancel(Timer.DISCONNECT);
        current = new Connection(responseTimeout, next);
        current.lastSent = now;
        generation++;
        port.continueAttempt(generation);
        // Each connection of the attempt has the whole connection timeout.
        port.schedule(generation, Timer.CONNECT, timeout);
        port.openTransport(generation, timeout);
    }

    void opened(long connection) {
        if (connection == generation && attempting()) {
            current.transportConnected = true;
            sendConnect();
        }
    }

    void credentials(
            ConnectionAttempt attempt,
            @Nullable MqttCredentials value,
            @Nullable Throwable failure,
            long now
    ) {
        if (connecting != attempt || credentialsReady) {
            return;
        }
        if (failure != null) {
            failed(generation, new MqttConnectException("Cannot obtain credentials", failure));
            return;
        }
        long remaining = connectionDeadline - now;
        if (remaining <= 0) {
            failed(generation, new TimeoutException("Credentials deadline expired"));
            return;
        }
        attemptCredentials = requireNonNull(value);
        credentialsReady = true;
        port.openTransport(generation, Duration.ofNanos(remaining));
    }

    private void sendConnect() {
        if (current.transportConnected && credentialsReady && !current.connectSent) {
            current.connectSent = true;
            sendRequired(++nextTicket, establishment.connect(
                    current.step,
                    attemptCredentials,
                    session.established,
                    receptions.room()
            ));
        }
    }

    void received(long connection, Packet packet, long now, int bytes) {
        if (connection != generation || !connectionActive()) {
            return;
        }
        if (current.abandoned) {
            // The attempt has given this connection up; only its DISCONNECT may progress, so
            // nothing it receives is delivered or acknowledged.
            return;
        }
        if (attempting()) {
            if (!credentialsReady) {
                failed(connection, protocolError("Received packet before credentials were ready"));
            } else if (!(packet instanceof Packet.ConnAck ack)) {
                failed(connection, protocolError("Expected CONNACK"));
            } else {
                switch (establishment.accept(
                        ack,
                        current.step,
                        session.established,
                        request.attempt()
                )) {
                    case Outcome.Failed(var cause) -> failed(connection, cause);
                    case Outcome.Abandoned(var refusal) -> abandon(refusal);
                    case Outcome.Established(var result, var keepAlive, var receiveMaximum) ->
                            establish(result, keepAlive, receiveMaximum, now);
                }
            }
        } else if (packet instanceof Packet.PubAck ack) {
            // Publication responses neither send nor close while disconnecting, so they can
            // progress without disturbing a pending shutdown.
            publications.acknowledged(ack);
        } else if (packet instanceof Packet.PubRec ack) {
            publications.release(ack);
        } else if (packet instanceof Packet.PubComp ack) {
            publications.completed(ack.packetId());
        } else if (state == State.DISCONNECTING) {
            // Other input already in flight cannot change a local shutdown's write/close sequence.
            return;
        } else if (packet instanceof Packet.PingResp) {
            if (current.awaitingPing) {
                current.awaitingPing = false;
                port.cancel(Timer.PING);
            } else {
                port.ignoredPingResponse();
            }
        } else if (packet instanceof Packet.SubAck ack) {
            subscriptions.subscribed(ack);
        } else if (packet instanceof Packet.UnsubAck ack) {
            subscriptions.unsubscribed(ack);
        } else if (packet instanceof Packet.Publish publish
                && publish.properties().find(PropertyId.TOPIC_ALIAS).isEmpty()) {
            // Other MQTT 5.0 properties do not change how the message is delivered.
            receptions.deliver(publish, bytes);
        } else if (packet instanceof Packet.Publish || packet instanceof Packet.AliasPublish) {
            // The client announces no Topic Alias Maximum, so the broker may use no alias.
            failed(connection, new PacketDecodeException(
                    ReasonCode.TOPIC_ALIAS_INVALID,
                    "Topic Alias without Topic Alias Maximum"
            ));
        } else if (packet instanceof Packet.PubRel release) {
            receptions.released(release.packetId());
        } else {
            failed(connection, protocolError("Unexpected " + packet.getClass().getSimpleName()));
        }
        // Acknowledgements can return packet identifiers or the close exchange window.
        subscriptions.drainClosures();
        drainDeferred();
    }

    /// Establishes the current connection with `result`, which continues the session when it has
    /// Session Present and otherwise starts a new one, and keeps the connection alive at
    /// `keepAlive`. The broker takes up to `receiveMaximum` QoS 1 and QoS 2 publications at once.
    private void establish(ConnAck result, Duration keepAlive, int receiveMaximum, long now) {
        boolean sessionPresent = result.sessionPresent();
        if (!sessionPresent) {
            discardSession();
            session.established = true;
            if (!cleanStart) {
                log.info("Started session for {}", clientId);
            }
        } else {
            receptions.reconcile(port.receiveState(), false);
            publications.resume();
        }
        if (state == State.CONNECTING) {
            state = State.CONNECTED;
        }
        current.ack = result;
        current.keepAlive = keepAlive;
        current.receiveMaximum = receiveMaximum;
        request.establish();
        port.admit(generation);
        // Reconcile before announcing, so connect() and onConnected already find continued
        // subscriptions established on this connection.
        subscriptions.connectionEstablished(sessionPresent);
        port.connected(generation, result);
        port.cancel(Timer.CONNECT);
        connecting = null;
        attemptCredentials = null;
        if (state == State.DISCONNECTING) {
            beginDisconnect();
        } else {
            completeConnections(result, null);
            if (!current.keepAlive.isZero()) {
                scheduleKeepAlive(now);
            }
            if (request.reconnects()) {
                current.stableDeadline = now + STABLE_CONNECTION.toNanos();
                port.schedule(generation, Timer.STABLE, STABLE_CONNECTION);
            }
        }
    }

    void sent(long connection, long ticket, long now) {
        sent(connection, ticket, now, now);
    }

    void sent(long connection, long ticket, long writtenAt, long now) {
        if (connection != generation || !connectionActive()) {
            return;
        }
        current.lastSent = writtenAt;
        if (state == State.CONNECTED) {
            current.responses.written(ticket, writtenAt, now).ifPresent(delay -> {
                port.schedule(generation, Timer.RESPONSE, delay);
            });
        }
        if (publications.written(ticket)) {
            drainDeferred();
        }
        if (ticket == current.disconnectTicket) {
            MqttConnectException refusal = current.sessionRefusal;
            // Unless a disconnect() was requested meanwhile, the attempt goes on.
            if (current.abandoned && refusal == null && state == State.CONNECTING) {
                openNextConnection(now);
                return;
            }
            // The written DISCONNECT satisfies a pending disconnect(), even with a refusal.
            List<Long> operations = request.takeDisconnections();
            if (refusal != null) {
                // The refusal fails the attempt, which the policy may retry like other failures.
                end(refusal, EndReason.CONNECTION_FAILURE);
            } else {
                end(null, EndReason.DISCONNECT_WRITTEN);
            }
            for (long operation : operations) {
                port.complete(operation, null, null);
            }
        }
    }

    /// Write completion retires PUBACK/PUBCOMP, while PUBREC retains the id for PUBREL.
    void written(long connection, long ticket) {
        if (connection == generation && established()) {
            receptions.written(ticket);
            sendCapacityAvailable(connection);
        }
    }

    void publish(long operation, MqttMessage message) {
        publications.publish(operation, message);
    }

    void subscribe(long operation, Subscribe request) {
        subscriptions.subscribe(operation, request);
    }

    void unsubscribe(long operation, long subscriptionId) {
        subscriptions.unsubscribe(operation, subscriptionId);
    }

    /// Sends queued work in order: the session's queued exchanges, then PUBREC responses, then
    /// registrations, which use no application operation slots and resume when capacity returns.
    private void drainDeferred() {
        if (state != State.CONNECTED) {
            return;
        }
        // An exchange waiting for capacity holds back everything after it, unlike publications
        // that wait only for the broker's Receive Maximum. PUBREC responses use the reserved
        // quota, so registrations in the regular quota proceed even when they wait.
        if (publications.sendQueued()) {
            publications.sendReleases();
            subscriptions.sendRegistrations();
        }
    }

    private OptionalInt allocatePacketId() {
        // Skip the ids that an unknown PUBREC holds until its PUBCOMP. The cursor advances on
        // every attempt, so the 65,535-id space bounds the scan; move exclusions into PacketIds
        // if that adversarial bound becomes a throughput concern.
        for (int remaining = 65_535; remaining > 0; remaining--) {
            OptionalInt allocated = session.packetIds.allocate();
            if (allocated.isEmpty() || !publications.holds(allocated.getAsInt())) {
                return allocated;
            }
            session.packetIds.release(allocated.getAsInt());
        }
        return OptionalInt.empty();
    }

    void deliveryReady(long token) {
        deliveriesReady(List.of(token));
    }

    /// QoS 0 deliveries and receptions whose PUBACK or PUBCOMP was written have no table entry.
    void deliveryFailed(DeliveryToken token, Throwable cause) {
        // A stop also reaches a pending disconnect, whose request is already withdrawn.
        boolean admitted = request.requested() || state == State.DISCONNECTING;
        if (token.session() == sessionNumber && admitted) {
            end(cause, EndReason.HANDLER_FAILURE);
        }
    }

    /// Tokens belong to the client lifetime; only entries still in the receive table can advance.
    void deliveriesReady(List<Long> tokens) {
        if (state == State.CLOSED) {
            return;
        }
        receptions.ready(tokens);
        sendCapacityAvailable(generation);
    }

    void sendCapacityAvailable(long connection) {
        if (connection != generation || state != State.CONNECTED) {
            return;
        }
        // Incoming acknowledgements and PUBREL take turns at returned room in the reserved quota.
        // Removals and the rest of the queued work use the regular quota.
        if (releasesFirst) {
            drainDeferred();
        }
        receptions.sendAcknowledgements();
        subscriptions.drainClosures();
        drainDeferred();
    }

    void timer(long connection, Timer timer, long now) {
        if (timer == Timer.RECONNECT) {
            if (connection == 0 && state == State.RECONNECT_WAIT && request.requested()) {
                startAttempt(new ConnectionAttempt(0), now);
            }
            return;
        }
        if (connection != generation) {
            return;
        }
        switch (timer) {
            case CONNECT -> {
                if (attempting()) {
                    String phase = !credentialsReady
                            ? "Credentials"
                            : current.transportConnected ? "CONNACK" : "Transport connection";
                    failed(connection, new TimeoutException(phase + " deadline expired"));
                }
            }
            case KEEP_ALIVE -> {
                if (state == State.CONNECTED) {
                    long remaining = current.keepAlive.toNanos() - (now - current.lastSent);
                    if (current.awaitingPing) {
                        port.schedule(generation, Timer.KEEP_ALIVE, current.keepAlive);
                    } else if (remaining > 0) {
                        port.schedule(generation, Timer.KEEP_ALIVE, Duration.ofNanos(remaining));
                    } else {
                        current.awaitingPing = true;
                        current.pingTicket = ++nextTicket;
                        // Include a stalled write in the deadline; otherwise it can wait forever.
                        port.schedule(generation, Timer.PING, current.keepAlive);
                        if (sendRequired(current.pingTicket, new Packet.PingReq())) {
                            port.schedule(generation, Timer.KEEP_ALIVE, current.keepAlive);
                        }
                    }
                }
            }
            case PING -> {
                if (state == State.CONNECTED && current.awaitingPing) {
                    failed(connection, new TimeoutException("PINGRESP deadline expired"));
                }
            }
            case DISCONNECT -> {
                if (state == State.DISCONNECTING || current.abandoned) {
                    failed(connection, new TimeoutException("DISCONNECT write deadline expired"));
                }
            }
            case STABLE -> {
                if (state == State.CONNECTED) {
                    long remaining = current.stableDeadline - now;
                    if (remaining > 0) {
                        port.schedule(generation, Timer.STABLE, Duration.ofNanos(remaining));
                    } else {
                        request.stable();
                    }
                }
            }
            case RESPONSE -> {
                if (state == State.CONNECTED) {
                    current.responses.expired(now).ifPresent(delay -> {
                        if (delay.isZero()) {
                            failed(connection, new TimeoutException(
                                    current.responses.headKind() + " deadline expired"
                            ));
                        } else {
                            port.schedule(generation, Timer.RESPONSE, delay);
                        }
                    });
                }
            }
            case RECONNECT -> throw new AssertionError("Reconnect timer handled above");
        }
    }

    private void scheduleKeepAlive(long now) {
        long remaining = current.keepAlive.toNanos() - (now - current.lastSent);
        port.schedule(generation, Timer.KEEP_ALIVE, Duration.ofNanos(Math.max(0, remaining)));
    }

    void disconnect(long operation) {
        if (state == State.CLOSED) {
            port.complete(operation, null, new IllegalStateException("Client is closed"));
        } else {
            // Withdrawal prevents retry immediately; stop admission stays open until the
            // outstanding attempt or graceful disconnect has ended.
            request.withdraw();
            if (state == State.DISCONNECTED || state == State.RECONNECT_WAIT) {
                port.connectionRequested(false);
                if (state == State.RECONNECT_WAIT) {
                    // Closing admission discards a stop admitted while waiting, and no
                    // connection end follows to release the lanes its failure paused.
                    port.stopDiscarded();
                }
                port.cancel(Timer.RECONNECT);
                state = State.DISCONNECTED;
                completeConnections(null, new MqttConnectException("Connection request withdrawn"));
                // Publications held for the withdrawn reconnection keep an unknown outcome; a
                // persistent session retains their exchanges.
                publications.failPublications(new MqttConnectionLostException());
                port.complete(operation, null, null);
                return;
            }
            request.awaitDisconnection(operation);
            if (state == State.DISCONNECTING) {
                return;
            }
            boolean connected = state == State.CONNECTED;
            state = State.DISCONNECTING;
            port.disconnecting();
            if (connected) {
                beginDisconnect();
            }
        }
    }

    /// Gives up the attempt's connection after its successful CONNACK, closing it with
    /// DISCONNECT so that the broker discards its Will. Without `refusal`, the attempt continues
    /// on its next connection once DISCONNECT is written; with it, the attempt fails then.
    private void abandon(@Nullable MqttConnectException refusal) {
        current.abandoned = true;
        current.sessionRefusal = refusal;
        port.cancel(Timer.CONNECT);
        beginDisconnect();
    }

    private void beginDisconnect() {
        port.cancel(Timer.KEEP_ALIVE);
        port.cancel(Timer.PING);
        port.cancel(Timer.STABLE);
        port.cancel(Timer.RESPONSE);
        current.disconnectTicket = ++nextTicket;
        port.schedule(generation, Timer.DISCONNECT, timeout);
        sendRequired(current.disconnectTicket, new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY));
    }

    private boolean sendRequired(long ticket, Packet packet) {
        return switch (port.send(generation, ticket, packet, null)) {
            case ACCEPTED -> true;
            case FULL -> {
                failed(generation, new MqttCapacityException("Reserved send queue capacity"));
                yield false;
            }
            // Wait for the connection's terminal notification to preserve its original cause.
            case CLOSED -> false;
        };
    }

    void failed(long connection, Throwable cause) {
        if (connection == generation && connectionActive()) {
            end(cause, EndReason.CONNECTION_FAILURE);
        }
    }

    void schedulerFailed(long connection, Throwable cause) {
        if ((connection == generation || connection == 0) && state != State.CLOSED) {
            end(cause, EndReason.SCHEDULER_FAILURE);
        }
    }

    void close(@Nullable Throwable cause) {
        end(cause, EndReason.CLIENT_CLOSE);
    }

    private void end(@Nullable Throwable cause, EndReason reason) {
        if (state == State.CLOSED) {
            return;
        }
        // A failure during a pending disconnect still belongs to that shutdown request.
        // Decide before replacing the cause with a session refusal below.
        boolean shutdownRequested = switch (reason) {
            case CLIENT_CLOSE -> cause == null || state == State.DISCONNECTING;
            // Only a pending disconnect() makes an ending requested; the DISCONNECT with which an
            // attempt abandons a connection is the client's own.
            case DISCONNECT_WRITTEN, CONNECTION_FAILURE, HANDLER_FAILURE, SCHEDULER_FAILURE ->
                    state == State.DISCONNECTING;
        };
        boolean permanent = reason == EndReason.CLIENT_CLOSE;
        // The ended connection keeps only what this end still settles; the core sees fresh
        // connection state from here on, and the parts replace theirs as they settle it below.
        Connection ended = current;
        current = new Connection(responseTimeout, Step.FIRST);
        if (cause instanceof PacketDecodeException failure) {
            // Both listener notifications and pending operations expose the same public cause.
            cause = new MqttProtocolException(
                    requireNonNull(failure.getMessage()),
                    failure.reasonCode(),
                    failure
            );
        }
        MqttConnectException refusal = ended.sessionRefusal;
        // A clock or handler failure stops the request with its own cause, while the policy
        // would retry the refusal; the refusal then only accompanies the attempt's failure.
        boolean stop = reason == EndReason.SCHEDULER_FAILURE || reason == EndReason.HANDLER_FAILURE;
        if (refusal != null && !stop) {
            // A failed DISCONNECT must not replace the reason this session was refused.
            if (cause != null && cause != refusal) {
                refusal.addSuppressed(cause);
            }
            cause = refusal;
        }
        // Pending connect() calls that this end gives up fail with this exception, and a failed
        // attempt reports the same instance to the listener.
        MqttConnectException attemptFailure = switch (cause) {
            // Only a requested disconnect or close ends without a cause.
            case null -> new MqttConnectException("Connection request withdrawn");
            case MqttConnectException failure -> failure;
            default -> new MqttConnectException("Connection failed", cause);
        };
        if (refusal != null && stop) {
            attemptFailure.addSuppressed(refusal);
        }
        try {
            boolean retry = endConnection(ended, cause, attemptFailure, reason, shutdownRequested);
            finishOperations(ended, cause, attemptFailure, permanent, retry);
        } finally {
            // Shell teardown or a logging backend must not retain a closed client's payloads.
            if (permanent || cleanStart) {
                discardSession();
            }
        }
    }

    private boolean endConnection(
            Connection ended,
            @Nullable Throwable cause,
            MqttConnectException attemptFailure,
            EndReason reason,
            boolean shutdownRequested
    ) {
        boolean established = ended.ack != null;
        boolean waiting = state == State.RECONNECT_WAIT;
        // A wait ends silently only when the application withdraws the request. A stop, a clock
        // failure, or an internal failure that closes the client reports the cancelled attempt.
        boolean notifyEnd = switch (state) {
            case CONNECTING, CONNECTED, DISCONNECTING -> true;
            case RECONNECT_WAIT -> reason == EndReason.HANDLER_FAILURE
                    || reason == EndReason.SCHEDULER_FAILURE
                    || reason == EndReason.CLIENT_CLOSE && cause != null;
            case DISCONNECTED, CLOSED -> false;
        };
        for (var timer : Timer.values()) {
            port.cancel(timer);
        }
        boolean retry = reason == EndReason.CONNECTION_FAILURE
                && !shutdownRequested
                && request.retries(cause);
        boolean permanent = reason == EndReason.CLIENT_CLOSE;
        Duration delay = null;
        if (retry) {
            delay = request.nextDelay();
            try {
                // Reserve before announcing the end, so the reported delay is a scheduled attempt.
                port.schedule(0, Timer.RECONNECT, delay);
            } catch (SpiException failure) {
                // A TimerScheduler failure stops the request. Unwinding here would leave this end
                // half done, so stop now and keep the connection's own cause primary.
                requireNonNull(cause).addSuppressed(requireNonNull(failure.getCause()));
                retry = false;
                delay = null;
            }
        }
        if (retry) {
            state = State.RECONNECT_WAIT;
        } else {
            state = permanent ? State.CLOSED : State.DISCONNECTED;
            request.withdraw();
            port.connectionRequested(false);
        }
        if (notifyEnd) {
            if (waiting) {
                // The stopped wait reports and consumes the reserved next attempt number.
                request.nextAttempt();
            }
            // A connection reports its own cause, and so does a stop by a handler failure. Other
            // failed attempts report the exception that fails their connect() calls.
            boolean stopped = reason == EndReason.HANDLER_FAILURE;
            @Nullable Throwable reported = established || stopped ? cause : attemptFailure;
            Disconnection disconnection = (delay == null)
                    ? Disconnection.of(reported, shutdownRequested, request.attempt())
                    : Disconnection.of(reported, shutdownRequested, request.attempt(), delay);
            // Retire queued recipients before removing subscriptions, without acknowledging
            // work that never reached the application. Persistent handed work keeps its state.
            Map<Long, Boolean> handedReadiness = port.end(
                    generation,
                    established,
                    disconnection,
                    !permanent && !cleanStart,
                    stopped
            );
            receptions.reconcile(handedReadiness, true);
        }
        if (permanent || cleanStart) {
            receptions.clear();
        }
        return retry;
    }

    private void finishOperations(
            Connection ended,
            @Nullable Throwable cause,
            MqttConnectException attemptFailure,
            boolean permanent,
            boolean retry
    ) {
        var pendingFailure = (cause == null)
                ? new MqttConnectionLostException()
                : new MqttConnectionLostException(cause);
        // Each part settles its state of the ended connection: registrations first, then
        // publications, which a scheduled reconnection of a persistent session holds instead of
        // failing.
        subscriptions.connectionEnded(permanent, cause);
        publications.connectionEnded(retry && !cleanStart, pendingFailure);
        receptions.connectionEnded();
        if (connecting != null) {
            if (!credentialsReady) {
                port.cancelCredentials(connecting);
            }
            connecting = null;
            attemptCredentials = null;
        }
        if (!retry) {
            completeConnections(null, attemptFailure);
        }
        // A disconnect requested during an attempt that established no connection is satisfied,
        // unless the broker accepted the connection and its DISCONNECT was not written: the
        // broker may then publish the connection's Will.
        @Nullable Throwable disconnectFailure =
                ended.ack != null || ended.abandoned || permanent ? pendingFailure : null;
        for (long operation : request.takeDisconnections()) {
            port.complete(operation, null, disconnectFailure);
        }
    }

    private void completeConnections(@Nullable ConnAck ack, @Nullable Throwable failure) {
        for (long operation : request.takeConnections()) {
            port.complete(operation, ack, failure);
        }
    }

    private void discardSession() {
        publications.sessionDiscarded();
        receptions.sessionDiscarded();
        boolean established = session.established;
        session = new Session();
        port.sessionChanged(++sessionNumber);
        // Every clean connection starts and discards a session; only persistent ones are news.
        if (established && !cleanStart) {
            log.info("Discarded session for {}", clientId);
        }
    }

    static PacketDecodeException protocolError(String message) {
        return new PacketDecodeException(ReasonCode.PROTOCOL_ERROR, message);
    }

    /// Whether an acknowledgement's `code` reports a failure: MQTT 5.0 gives failures values of
    /// 0x80 or greater, and MQTT 3.1.1's only failure, a refused SUBACK, maps to 0x80.
    static boolean isFailure(ReasonCode code) {
        return code.value() >= 0x80;
    }

    private final class PartContext implements Context {

        @Override
        public State state() {
            return state;
        }

        @Override
        public long generation() {
            return generation;
        }

        @Override
        public long attempt() {
            return request.attempt();
        }

        @Override
        public long sessionNumber() {
            return sessionNumber;
        }

        @Override
        public long nextTicket() {
            return ++nextTicket;
        }

        @Override
        public void fail(Throwable cause) {
            failed(generation, cause);
        }

        @Override
        public ResponseDeadlines responses() {
            return current.responses;
        }

        @Override
        public OptionalInt allocatePacketId() {
            return SessionCore.this.allocatePacketId();
        }

        @Override
        public void releasePacketId(int packetId) {
            session.packetIds.release(packetId);
        }

        @Override
        public void drain() {
            drainDeferred();
        }

        @Override
        public void sendCapacityAvailable() {
            SessionCore.this.sendCapacityAvailable(generation);
        }

        @Override
        public void acknowledgementAdmitted() {
            releasesFirst = true;
        }

        @Override
        public void releaseAdmitted() {
            releasesFirst = false;
        }

        @Override
        public List<Long> matching(Topic topic) {
            return subscriptions.matching(topic);
        }

        @Override
        public ConnAck limits() {
            return requireNonNull(current.ack);
        }

        @Override
        public int receiveMaximum() {
            return current.receiveMaximum;
        }
    }

    // State owned by one connection. Its attempt, the session, and the parts' own state can
    // outlive it.
    private static final class Connection {

        // Present once a successful CONNACK has established this connection.
        @Nullable ConnAck ack;
        // Set with the CONNACK: the configured interval, or the broker's Server Keep Alive,
        // which lasts for this connection only.
        Duration keepAlive = Duration.ZERO;
        // Set with the CONNACK: how many QoS 1 and QoS 2 publications the broker takes at once.
        int receiveMaximum;
        long stableDeadline;
        long pingTicket;
        long disconnectTicket;
        long lastSent;
        boolean transportConnected;
        boolean connectSent;
        boolean awaitingPing;
        // Set when the attempt gives this connection up after its CONNACK. Once DISCONNECT is
        // written, the refusal, if any, fails the attempt; otherwise the attempt continues on
        // its next connection unless a disconnect() is pending. Only an abandoned connection
        // has a refusal.
        boolean abandoned;
        @Nullable MqttConnectException sessionRefusal;
        final ResponseDeadlines responses;
        // This connection's place in its attempt. Ending a connection replaces it with a first
        // connection, so every attempt starts with one.
        final Step step;

        Connection(Duration responseTimeout, Step step) {
            responses = new ResponseDeadlines(responseTimeout);
            this.step = step;
        }
    }

    // State owned by one session. A CONNACK that starts a session without Session Present,
    // permanent closure, or the end of a nonpersistent connection replaces it. SUBSCRIBE and
    // UNSUBSCRIBE borrow its identifiers only until their connection ends.
    private static final class Session {

        final PacketIds packetIds = new PacketIds();
        // A CONNACK without Session Present started this session, except the one that answers
        // the connection discarding a broker session; only then can a later Session Present
        // continue it.
        boolean established;
    }
}
