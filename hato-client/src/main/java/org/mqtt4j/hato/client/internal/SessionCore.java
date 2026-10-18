package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.SequencedMap;
import java.util.SequencedSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttCredentials;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttNotConnectedException;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttSubscribeException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.client.internal.common.protocol.PacketIds;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A single-owner state machine. Time is input; outputs are nonblocking commands.
/// Operation tokens belong to the shell, which alone owns and completes futures.
final class SessionCore {

    /// DISCONNECTING follows an accepted disconnect(), both while its attempt still waits for
    /// CONNACK and while the established connection writes DISCONNECT.
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

        void resubscribeFailed(Subscription handle, MqttSubscribeException failure);

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

        /// Sends queued work in the core's order: replays, PUBREC responses, then registrations.
        void drain();

        /// Lets queued work use returned send capacity, in the core's order.
        void sendCapacityAvailable();

        /// An acknowledgement took the reserved send quota, so PUBREL goes first next time.
        void acknowledgementAdmitted();

        /// The registrations whose filter matches `topic`, except those being removed.
        List<Long> matching(Topic topic);
    }

    private static final Log log = Log.of(SessionCore.class);

    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    private static final int INFLIGHT_LIMIT = 64;

    private static final Duration STABLE_CONNECTION = Duration.ofSeconds(30);

    private static final long INFLIGHT_BYTE_LIMIT = 32L * 1024 * 1024;

    // A broker can repeat PUBREC at will, so PUBREC responses bound their completion events in
    // the same way. Replayed PUBRELs are already bounded by the session's exchanges.
    private static final int RELEASE_WRITE_LIMIT = 16;

    private final Port port;

    private final String clientId;

    private final Duration timeout;

    private final Duration keepAlive;

    private final Duration responseTimeout;

    private final boolean cleanStart;

    private final Packet.@Nullable Will will;

    private final boolean credentialsRequired;

    private final ConnectionRequest request;

    private final Receptions receptions;

    private final Subscriptions subscriptions;

    private State state = State.DISCONNECTED;

    private long generation;

    // State of the current connection, including one that its attempt is still opening. Ending
    // the connection replaces it, so the next one starts from fresh values. The attempt's
    // credentials and deadline stay outside, because an attempt can outlive a connection.
    private Connection current;

    // The attempt waiting for its CONNACK, and null once a CONNACK establishes its connection
    // or the attempt ends.
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

    private boolean unknownPubAckLogged;

    private boolean downgradedPubAckLogged;

    private boolean unknownPubRecLogged;

    private boolean unknownPubCompLogged;

    // Incoming acknowledgements and outgoing PUBREL share the reserved send quota. Returned
    // room goes first to the side that did not take the last slot, so neither can starve.
    private boolean releasesFirst;

    SessionCore(
            Port port,
            String clientId,
            Duration timeout,
            Duration keepAlive,
            boolean cleanStart,
            Packet.@Nullable Will will,
            boolean credentialsRequired,
            ReconnectBackoff backoff,
            boolean initialRetry,
            Duration responseTimeout
    ) {
        this.port = port;
        this.clientId = clientId;
        this.timeout = timeout;
        this.keepAlive = keepAlive;
        this.responseTimeout = responseTimeout;
        this.cleanStart = cleanStart;
        this.will = will;
        this.credentialsRequired = credentialsRequired;
        request = new ConnectionRequest(backoff, initialRetry);
        current = new Connection(responseTimeout);
        var context = new PartContext();
        receptions = new Receptions(port, context, clientId);
        subscriptions = new Subscriptions(port, context, cleanStart);
    }

    State state() {
        return state;
    }

    /// Whether an attempt waits for its CONNACK, possibly with a requested disconnect pending.
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
            MqttCredentials credentials = attemptCredentials;
            sendRequired(
                    ++nextTicket,
                    new Packet.Connect(
                            clientId,
                            MqttVersion.V3_1_1,
                            cleanStart,
                            (int) keepAlive.toSeconds(),
                            will,
                            credentials == null ? null : credentials.username(),
                            credentials == null ? null : credentials.password().orElse(null),
                            EMPTY
                    )
            );
        }
    }

    void received(long connection, Packet packet, long now, int bytes) {
        if (connection != generation || !connectionActive()) {
            return;
        }
        if (current.sessionRefusal != null) {
            // This broker session has no local counterpart; only its DISCONNECT may progress.
            return;
        }
        if (attempting()) {
            if (!credentialsReady) {
                failed(connection, protocolError("Received packet before credentials were ready"));
            } else if (!(packet instanceof Packet.ConnAck ack)) {
                failed(connection, protocolError("Expected CONNACK"));
            } else if (ack.reasonCode() != ReasonCode.SUCCESS) {
                failed(connection, new MqttConnectException(
                        "Broker refused connection",
                        ack.reasonCode()
                ));
            } else if (ack.sessionPresent() && cleanStart) {
                failed(connection, protocolError("Session Present with Clean Session"));
            } else if (ack.sessionPresent() && !session.established) {
                current.sessionRefusal = new MqttConnectException(
                        "Broker session has no local state"
                );
                port.cancel(Timer.CONNECT);
                port.schedule(generation, Timer.DISCONNECT, timeout);
                current.disconnectTicket = ++nextTicket;
                sendRequired(
                        current.disconnectTicket,
                        new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY)
                );
            } else {
                if (!ack.sessionPresent()) {
                    discardSession();
                    session.established = true;
                    if (!cleanStart) {
                        log.info("Started session for {}", clientId);
                    }
                } else {
                    receptions.reconcile(port.receiveState(), false);
                    current.replays.addAll(session.inflight.values());
                }
                if (state == State.CONNECTING) {
                    state = State.CONNECTED;
                }
                var result = ConnAck.of(ack.sessionPresent(), request.attempt());
                current.ack = result;
                request.establish();
                port.admit(generation);
                subscriptions.connectionEstablished(ack.sessionPresent());
                port.connected(generation, result);
                port.cancel(Timer.CONNECT);
                connecting = null;
                attemptCredentials = null;
                if (state == State.DISCONNECTING) {
                    beginDisconnect();
                } else {
                    completeConnections(result, null);
                    if (!keepAlive.isZero()) {
                        scheduleKeepAlive(now);
                    }
                    if (request.reconnects()) {
                        current.stableDeadline = now + STABLE_CONNECTION.toNanos();
                        port.schedule(generation, Timer.STABLE, STABLE_CONNECTION);
                    }
                }
            }
        } else if (packet instanceof Packet.PubAck ack) {
            // Publication responses neither send nor close while disconnecting, so they can
            // progress without disturbing a pending shutdown.
            acknowledged(ack);
        } else if (packet instanceof Packet.PubRec ack) {
            release(ack.packetId());
        } else if (packet instanceof Packet.PubComp ack) {
            completed(ack.packetId());
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
        } else if (packet instanceof Packet.Publish publish) {
            receptions.deliver(publish, bytes);
        } else if (packet instanceof Packet.PubRel release) {
            receptions.released(release.packetId());
        } else {
            failed(connection, protocolError("Unexpected " + packet.getClass().getSimpleName()));
        }
        // Acknowledgements can return packet identifiers or the close exchange window.
        subscriptions.drainClosures();
        drainDeferred();
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
        Long publication = current.publications.remove(ticket);
        if (publication != null) {
            port.complete(publication, PublishResult.of(), null);
        }
        if (current.releaseWrites.remove(ticket)) {
            drainDeferred();
        }
        if (ticket == current.disconnectTicket) {
            if (current.sessionRefusal != null) {
                end(current.sessionRefusal, EndReason.DISCONNECT_WRITTEN);
                return;
            }
            List<Long> operations = request.takeDisconnections();
            end(null, EndReason.DISCONNECT_WRITTEN);
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
        if (!ready(operation)) {
            return;
        }
        // Replays precede subscription restoration; SUBACK waiting does not block publication.
        if (!current.replays.isEmpty()) {
            capacityFailure(operation, "Publication replay pending");
            return;
        }
        long ticket = ++nextTicket;
        int packetId = 0;
        SendReceipt receipt = null;
        if (message.qos() != Qos.AT_MOST_ONCE) {
            if (session.inflight.size() >= INFLIGHT_LIMIT) {
                capacityFailure(operation, "Publication capacity");
                return;
            }
            int byteCount = 0;
            if (!cleanStart) {
                long remaining = INFLIGHT_BYTE_LIMIT - session.bytes;
                if (message.payloadSize() > remaining) {
                    capacityFailure(operation, "Publication byte capacity");
                    return;
                }
                byteCount = codec.encodePublishHeader(
                        message.topic(),
                        message.qos(),
                        message.retain(),
                        false,
                        1,
                        EMPTY,
                        message.payloadSize()
                )
                        .length + message.payloadSize();
                if (byteCount > remaining) {
                    capacityFailure(operation, "Publication byte capacity");
                    return;
                }
            }
            OptionalInt allocated = allocatePacketId();
            if (allocated.isEmpty()) {
                capacityFailure(operation, "Packet identifier capacity");
                return;
            }
            packetId = allocated.getAsInt();
            var publication = new Publication(
                    packetId,
                    operation,
                    message.qos(),
                    cleanStart ? null : message,
                    byteCount
            );
            session.inflight.put(packetId, publication);
            session.bytes += byteCount;
            receipt = publication.receipt;
        } else {
            current.publications.put(ticket, operation);
        }
        SendQueue.OfferResult result = port.publish(
                generation,
                ticket,
                message,
                packetId,
                receipt
        );
        if (result == SendQueue.OfferResult.ACCEPTED) {
            awaitPublication(ticket, message.qos(), packetId);
        } else if (result == SendQueue.OfferResult.FULL) {
            if (packetId == 0) {
                current.publications.remove(ticket);
            } else {
                removeInflight(packetId);
            }
            capacityFailure(operation, "Send queue capacity");
        }
        // CLOSED waits for the connection's terminal notification, as do SUBSCRIBE/UNSUBSCRIBE.
    }

    private void awaitPublication(long ticket, Qos qos, int packetId) {
        ResponseDeadlines responses = current.responses;
        switch (qos) {
            case AT_MOST_ONCE -> {
            }
            case AT_LEAST_ONCE -> responses.add(ticket, ResponseDeadlines.Kind.PUBACK, packetId);
            case EXACTLY_ONCE -> responses.add(ticket, ResponseDeadlines.Kind.PUBREC, packetId);
        }
    }

    void subscribe(long operation, Subscribe request) {
        subscriptions.subscribe(operation, request);
    }

    void unsubscribe(long operation, long subscriptionId) {
        subscriptions.unsubscribe(operation, subscriptionId);
    }

    /// Sends queued work in order: the session's replays, then PUBREC responses, then
    /// registrations, which use no application operation slots and resume when capacity returns.
    private void drainDeferred() {
        if (state != State.CONNECTED) {
            return;
        }
        // Replay the single session order before fresh PUBREC responses. A duplicate PUBREC
        // shares a PUBREL replay still waiting here instead of adding another response.
        Inflight inflight;
        while ((inflight = current.replays.peekFirst()) != null) {
            if (session.inflight.get(inflight.packetId) != inflight) {
                current.replays.removeFirst();
                continue;
            }
            long ticket = ++nextTicket;
            SendQueue.OfferResult result = switch (inflight) {
                case Publication publication -> port.publish(
                        generation,
                        ticket,
                        requireNonNull(publication.message),
                        publication.packetId,
                        requireNonNull(publication.receipt)
                );
                case Release release -> sendRelease(ticket, release.packetId);
            };
            if (result != SendQueue.OfferResult.ACCEPTED) {
                return;
            }
            if (inflight instanceof Publication publication) {
                awaitPublication(ticket, publication.qos, publication.packetId);
            }
            current.replays.removeFirst();
        }
        while (!current.releases.isEmpty() && current.releaseWrites.size() < RELEASE_WRITE_LIMIT) {
            int id = current.releases.getFirst();
            long ticket = ++nextTicket;
            if (sendRelease(ticket, id) != SendQueue.OfferResult.ACCEPTED) {
                // Re-creation uses the regular quota; it must not wait for acknowledgement room.
                break;
            }
            current.releases.removeFirst();
            current.releaseWrites.add(ticket);
            current.unknownReleases.computeIfPresent(id, (_, admitted) -> admitted + 1);
        }
        subscriptions.sendRegistrations();
    }

    private SendQueue.OfferResult sendRelease(long ticket, int packetId) {
        SendQueue.OfferResult result = port.send(
                generation,
                ticket,
                new Packet.PubRel(packetId, ReasonCode.SUCCESS, EMPTY),
                null
        );
        if (result == SendQueue.OfferResult.ACCEPTED) {
            releasesFirst = false;
            current.responses.add(ticket, ResponseDeadlines.Kind.PUBCOMP, packetId);
        }
        return result;
    }

    private boolean ready(long operation) {
        if (state == State.CLOSED) {
            port.complete(operation, null, new IllegalStateException("Client is closed"));
        } else if (state != State.CONNECTED) {
            port.complete(operation, null, new MqttNotConnectedException());
        } else {
            return true;
        }
        return false;
    }

    private void capacityFailure(long operation, String message) {
        port.complete(operation, null, new MqttCapacityException(message));
    }

    private OptionalInt allocatePacketId() {
        // A PUBREL answering an unknown PUBREC has no session entry. Its id stays unavailable
        // until PUBCOMP, including the time after I/O admitted the response.
        // The cursor advances on every attempt, so the 65,535-id space bounds the scan; move
        // exclusions into PacketIds if that adversarial bound becomes a throughput concern.
        for (int remaining = 65_535; remaining > 0; remaining--) {
            OptionalInt allocated = session.packetIds.allocate();
            if (allocated.isEmpty() || !current.unknownReleases.containsKey(allocated.getAsInt())) {
                return allocated;
            }
            session.packetIds.release(allocated.getAsInt());
        }
        return OptionalInt.empty();
    }

    private void acknowledged(Packet.PubAck ack) {
        Inflight inflight = session.inflight.get(ack.packetId());
        if (inflight == null) {
            if (!unknownPubAckLogged) {
                unknownPubAckLogged = true;
                log.warn("Ignoring PUBACK for unknown packet identifier {} on {}",
                        ack.packetId(), clientId);
            }
            return;
        }
        switch (inflight) {
            case Release _ -> stageMismatch("PUBACK does not match the outgoing QoS stage");
            case Publication publication when publication.qos == Qos.EXACTLY_ONCE ->
                    downgraded(publication);
            case Publication publication -> completePublication(publication);
        }
    }

    /// Brokers without QoS 2, such as RabbitMQ with MQTT 3.1.1, accept a QoS 2 PUBLISH as QoS 1
    /// and answer PUBACK. That ends the broker's exchange, possibly after delivering the message.
    /// Only this publication fails: the connection stays, even while disconnecting, and a
    /// persistent session does not resume the exchange or deliver the message again.
    private void downgraded(Publication publication) {
        if (!downgradedPubAckLogged) {
            downgradedPubAckLogged = true;
            log.warn("Broker accepted QoS 2 packet identifier {} at QoS 1 with PUBACK on {}",
                    publication.packetId, clientId);
        }
        removeInflight(publication.packetId);
        if (publication.operation != 0) {
            port.complete(
                    publication.operation,
                    null,
                    new MqttProtocolException(
                            "PUBACK accepted a QoS 2 publication at QoS 1",
                            ReasonCode.PROTOCOL_ERROR
                    )
            );
        }
    }

    private void release(int packetId) {
        Inflight inflight = session.inflight.get(packetId);
        switch (inflight) {
            case Publication publication -> {
                if (publication.qos != Qos.EXACTLY_ONCE) {
                    stageMismatch("PUBREC does not match the outgoing QoS stage");
                    return;
                }
                // The receiver owns the body after PUBREC, but the id and count stay reserved
                // until PUBCOMP. Replacing the entry drops the body and retains PUBREC order.
                session.bytes -= publication.bytes;
                current.replays.remove(publication);
                current.responses.answered(ResponseDeadlines.Kind.PUBREC, packetId);
                session.inflight.putLast(packetId, new Release(packetId, publication.operation));
            }
            case Release pending -> {
                // A PUBREL replay still waiting for capacity also answers this duplicate. The
                // scan is bounded by the 64 exchanges; index it only if that limit grows.
                if (current.replays.contains(pending)) {
                    return;
                }
            }
            case null -> {
                if (!unknownPubRecLogged) {
                    unknownPubRecLogged = true;
                    log.warn("Received PUBREC for unknown packet identifier {} on {}",
                            packetId, clientId);
                }
                current.unknownReleases.putIfAbsent(packetId, 0);
            }
        }
        // DISCONNECT remains last. A persistent session still records the new stage so the
        // next connection continues with PUBREL, even though this one cannot send it.
        if (state == State.DISCONNECTING) {
            return;
        }
        current.releases.add(packetId);
    }

    private void completed(int packetId) {
        Inflight inflight = session.inflight.get(packetId);
        if (inflight == null) {
            Integer admitted = current.unknownReleases.get(packetId);
            if (admitted != null && admitted > 1) {
                // The answer to one of several PUBRELs for an unknown PUBREC.
                current.unknownReleases.put(packetId, admitted - 1);
            } else if (admitted != null) {
                // The last answer to a PUBREL for an unknown PUBREC makes its id available.
                current.unknownReleases.remove(packetId);
                current.releases.remove(packetId);
                current.responses.answered(ResponseDeadlines.Kind.PUBCOMP, packetId);
            } else if (!unknownPubCompLogged) {
                unknownPubCompLogged = true;
                log.warn("Ignoring PUBCOMP for unknown packet identifier {} on {}",
                        packetId, clientId);
            }
            return;
        }
        if (!(inflight instanceof Release)) {
            stageMismatch("PUBCOMP does not match the outgoing QoS stage");
            return;
        }
        completePublication(inflight);
    }

    private void stageMismatch(String message) {
        // A pending DISCONNECT keeps its write and close sequence. The entry keeps its stage,
        // and the connection end fails its future.
        if (state != State.DISCONNECTING) {
            failed(generation, protocolError(message));
        }
    }

    private void completePublication(Inflight inflight) {
        removeInflight(inflight.packetId);
        if (inflight.operation != 0) {
            port.complete(inflight.operation, PublishResult.of(), null);
        }
    }

    private void removeInflight(int packetId) {
        Inflight inflight = session.inflight.remove(packetId);
        if (inflight != null) {
            session.packetIds.release(packetId);
            if (inflight instanceof Publication publication) {
                session.bytes -= publication.bytes;
                current.responses.answered(
                        publication.qos == Qos.AT_LEAST_ONCE
                                ? ResponseDeadlines.Kind.PUBACK
                                : ResponseDeadlines.Kind.PUBREC,
                        packetId
                );
            } else {
                current.responses.answered(ResponseDeadlines.Kind.PUBCOMP, packetId);
            }
            current.replays.remove(inflight);
            current.releases.remove(packetId);
        }
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
                    long remaining = keepAlive.toNanos() - (now - current.lastSent);
                    if (current.awaitingPing) {
                        port.schedule(generation, Timer.KEEP_ALIVE, keepAlive);
                    } else if (remaining > 0) {
                        port.schedule(generation, Timer.KEEP_ALIVE, Duration.ofNanos(remaining));
                    } else {
                        current.awaitingPing = true;
                        current.pingTicket = ++nextTicket;
                        // Include a stalled write in the deadline; otherwise it can wait forever.
                        port.schedule(generation, Timer.PING, keepAlive);
                        if (sendRequired(current.pingTicket, new Packet.PingReq())) {
                            port.schedule(generation, Timer.KEEP_ALIVE, keepAlive);
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
                if (state == State.DISCONNECTING || current.sessionRefusal != null) {
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
        port.schedule(
                generation,
                Timer.KEEP_ALIVE,
                Duration.ofNanos(Math.max(0, keepAlive.toNanos() - (now - current.lastSent)))
        );
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
                failPublications(new MqttConnectionLostException());
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
            // The DISCONNECT that refuses a broker session without local state is not requested.
            case DISCONNECT_WRITTEN, CONNECTION_FAILURE, HANDLER_FAILURE, SCHEDULER_FAILURE ->
                    state == State.DISCONNECTING;
        };
        boolean permanent = reason == EndReason.CLIENT_CLOSE;
        // The ended connection keeps only what this end still settles; anything that runs from
        // here on sees fresh connection state.
        Connection ended = current;
        current = new Connection(responseTimeout);
        if (cause instanceof PacketDecodeException failure) {
            // Both listener notifications and pending operations expose the same public cause.
            cause = new MqttProtocolException(
                    requireNonNull(failure.getMessage()),
                    failure.reasonCode(),
                    failure
            );
        }
        MqttConnectException refusal = ended.sessionRefusal;
        if (refusal != null) {
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
        boolean retry = shouldReconnect(ended, cause, reason, shutdownRequested);
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

    private boolean shouldReconnect(
            Connection ended,
            @Nullable Throwable cause,
            EndReason reason,
            boolean shutdownRequested
    ) {
        if (reason != EndReason.CONNECTION_FAILURE || shutdownRequested) {
            return false;
        }
        // A retry would meet the same broker session, which the client cannot discard yet.
        if (ended.sessionRefusal != null) {
            return false;
        }
        return request.retries(cause);
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
        subscriptions.connectionEnded(permanent, cause);
        // QoS 0 publications belong to the ended connection and are never replayed.
        for (long operation : ended.publications.values()) {
            port.complete(operation, null, pendingFailure);
        }
        // A scheduled reconnection keeps a persistent session's publications pending, and the
        // next connection decides them: continuing completes their exchanges, and losing the
        // session fails them. Otherwise their outcome stays unknown now.
        if (!retry || cleanStart) {
            failPublications(pendingFailure);
        }
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
        // A disconnect requested during an attempt that established no connection is satisfied.
        @Nullable Throwable disconnectFailure =
                ended.ack != null || permanent ? pendingFailure : null;
        for (long operation : request.takeDisconnections()) {
            port.complete(operation, null, disconnectFailure);
        }
    }

    private void completeConnections(@Nullable ConnAck ack, @Nullable Throwable failure) {
        for (long operation : request.takeConnections()) {
            port.complete(operation, ack, failure);
        }
    }

    /// Fails the futures of the session's QoS 1 and QoS 2 publications with an unknown outcome.
    /// Their exchanges stay for a later connection unless the session goes too.
    private void failPublications(Throwable failure) {
        for (Inflight inflight : session.inflight.values()) {
            if (inflight.operation != 0) {
                port.complete(inflight.operation, null, failure);
                inflight.operation = 0;
            }
        }
    }

    private void discardSession() {
        // Publications still waiting for the discarded exchanges have an unknown outcome.
        failPublications(new MqttConnectionLostException());
        boolean established = session.established;
        session = new Session();
        receptions.sessionReplaced();
        port.sessionChanged(++sessionNumber);
        // Every clean connection starts and discards a session; only persistent ones are news.
        if (established && !cleanStart) {
            log.info("Discarded session for {}", clientId);
        }
    }

    static PacketDecodeException protocolError(String message) {
        return new PacketDecodeException(ReasonCode.PROTOCOL_ERROR, message);
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
        public List<Long> matching(Topic topic) {
            return subscriptions.matching(topic);
        }
    }

    // State owned by one connection. Its attempt, the session, and the parts' own state can
    // outlive it.
    private static final class Connection {

        // Present once a successful CONNACK has established this connection.
        @Nullable ConnAck ack;
        long stableDeadline;
        long pingTicket;
        long disconnectTicket;
        long lastSent;
        boolean transportConnected;
        boolean connectSent;
        boolean awaitingPing;
        @Nullable MqttConnectException sessionRefusal;
        final ResponseDeadlines responses;
        // QoS 0 publications by ticket; they complete when their write completes.
        final Map<Long, Long> publications = new HashMap<>();
        // At most 64 retained publications await queue admission, in the session's order.
        final ArrayDeque<Inflight> replays = new ArrayDeque<>();
        // PUBREC responses have their own FIFO: an application's unready incoming
        // acknowledgement must not block its outgoing publication. Coalescing by id bounds even
        // unknown responses to the protocol's 65,535 identifiers.
        final SequencedSet<Integer> releases = new LinkedHashSet<>();
        // Tickets of PUBREC responses admitted to I/O whose write notification is still pending.
        final Set<Long> releaseWrites = new HashSet<>();
        // Unknown exchanges have no future or persistent state, but their PUBCOMP must not land
        // on a new publication using the same id. Each id stays aside until disconnect, or until
        // PUBCOMP has answered every PUBREL admitted for it; the value counts those PUBRELs.
        // A repeated PUBREC can leave two answers in flight, and the first must not free the id.
        final Map<Integer, Integer> unknownReleases = new HashMap<>();

        Connection(Duration responseTimeout) {
            responses = new ResponseDeadlines(responseTimeout);
        }
    }

    // State owned by one session. A CONNACK without Session Present, permanent closure, or
    // the end of a nonpersistent connection replaces it. SUBSCRIBE and UNSUBSCRIBE borrow its
    // identifiers only until their connection ends.
    private static final class Session {

        final PacketIds packetIds = new PacketIds();
        // PUBLISH entries retain send order; replacing one with PUBREL moves it to the end in
        // PUBREC order. Both stages share the identifier and count window across connections.
        final SequencedMap<Integer, Inflight> inflight = new LinkedHashMap<>();
        // A CONNACK without Session Present started this session; only then can a later
        // Session Present continue it.
        boolean established;
        // Retained PUBLISH wire sizes, counted only for a persistent session until PUBACK/REC.
        long bytes;
    }

    private abstract static sealed class Inflight {

        final int packetId;
        long operation;

        Inflight(int packetId, long operation) {
            this.packetId = packetId;
            this.operation = operation;
        }
    }

    private static final class Publication extends Inflight {

        final Qos qos;
        final @Nullable MqttMessage message;
        final int bytes;
        final @Nullable SendReceipt receipt;

        Publication(
                int packetId,
                long operation,
                Qos qos,
                @Nullable MqttMessage message,
                int bytes
        ) {
            super(packetId, operation);
            this.qos = qos;
            this.message = message;
            this.bytes = bytes;
            receipt = message == null ? null : new SendReceipt();
        }
    }

    private static final class Release extends Inflight {

        Release(int packetId, long operation) {
            super(packetId, operation);
        }
    }
}
