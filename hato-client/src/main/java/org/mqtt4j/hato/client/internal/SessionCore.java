package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.client.internal.common.protocol.PacketIds;
import org.mqtt4j.hato.client.internal.common.protocol.SharedTopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A single-owner state machine. Time is input; outputs are nonblocking commands.
/// Operation tokens belong to the shell, which alone owns and completes futures.
final class SessionCore {

    enum State {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
        CLOSED
    }

    enum Timer {
        CONNECT,
        KEEP_ALIVE,
        PING,
        DISCONNECT
    }

    interface Port {

        void open(long generation, long operation);

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
        Map<Long, Boolean> end(
                long generation,
                boolean established,
                Disconnection disconnection,
                boolean preserveSession,
                boolean stopped
        );
    }

    private static final Log log = Log.of(SessionCore.class);

    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    private static final int INFLIGHT_LIMIT = 64;

    private static final long INFLIGHT_BYTE_LIMIT = 32L * 1024 * 1024;

    private static final int ACKNOWLEDGEMENT_LIMIT = 4096;

    // Broker registrations outlive locally closed handles until UNSUBACK or a CONNACK without
    // Session Present, so they are bounded apart from the delivery lanes that local closure
    // releases.
    private static final int REGISTRATION_LIMIT = 256;

    // Bound queued completion events even when I/O returns capacity during an actor drain.
    private static final int ACK_WRITE_LIMIT = 16;

    // A broker can repeat PUBREC at will, so PUBREC responses bound their completion events in
    // the same way. Replayed PUBRELs are already bounded by the session's exchanges.
    private static final int RELEASE_WRITE_LIMIT = 16;

    // Close has no public operation slot. Bound its in-flight exchanges separately, so a
    // batch of closures cannot flood the actor with send completions and UNSUBACKs.
    private static final int CLOSE_INFLIGHT_LIMIT = 16;

    // Re-creation after CONNACK has no public operation slot either. Bound its pending SUBACKs
    // apart from closures, so many retained subscriptions cannot flood the actor.
    private static final int RESTORE_INFLIGHT_LIMIT = 16;

    private final Port port;

    private final String clientId;

    private final Duration timeout;

    private final Duration keepAlive;

    private final boolean cleanStart;

    // Broker registrations can survive even when their last local operation was unconfirmed.
    private boolean sessionPresent;

    private State state = State.DISCONNECTED;

    private long generation;

    private long connectOperation;

    private long disconnectOperation;

    private long nextTicket;

    private long pingTicket;

    private long disconnectTicket;

    private long lastSent;

    private boolean transportConnected;

    private boolean awaitingPing;

    private @Nullable MqttConnectException sessionRefusal;

    private Session session = new Session();

    // Identifies the current session to delivery and mailbox code, which run outside the actor
    // and must not reach its state. Every replacement of the session advances it.
    private long sessionNumber = 1;

    private final Map<Long, Long> publications = new HashMap<>();

    private final AckSequencer acknowledgements = new AckSequencer();

    private long nextDelivery;

    private boolean unknownPubAckLogged;

    private boolean downgradedPubAckLogged;

    private boolean unknownPubRecLogged;

    private boolean unknownPubCompLogged;

    private boolean reusedIdentifierLogged;

    private final Map<Long, LocalSubscription> subscriptions = new LinkedHashMap<>();

    private final Map<Integer, LocalSubscription> subscribing = new HashMap<>();

    private final Map<Integer, UnsubscribeOperation> unsubscribing = new HashMap<>();

    // At most one removal per registered subscription; the registry supplies the total bound.
    private final ArrayDeque<UnsubscribeOperation> queuedClosures = new ArrayDeque<>();

    // At most 64 retained publications and 256 broker registrations await queue admission.
    private final ArrayDeque<Deferred> deferred = new ArrayDeque<>();

    // PUBREC responses have their own FIFO: an application's unready incoming acknowledgement
    // must not block its outgoing publication. Coalescing by id bounds even unknown responses
    // to the protocol's 65,535 identifiers.
    private final SequencedSet<Integer> releases = new LinkedHashSet<>();

    // Tickets of PUBREC responses admitted to I/O whose write notification is still pending.
    private final Set<Long> releaseWrites = new HashSet<>();

    // PUBACK and PUBREL share the acknowledgement room of the send queue. Returned room goes
    // first to the kind that did not take the last slot, so a steady supply of either kind
    // cannot starve the other.
    private boolean releasesFirst;

    // Unknown exchanges have no future or persistent state, but their PUBCOMP must not land
    // on a new publication using the same id. Each id stays aside until disconnect, or until
    // PUBCOMP has answered every PUBREL admitted for it; the value counts those PUBRELs.
    // A repeated PUBREC can leave two answers in flight, and the first must not free the id.
    private final Map<Integer, Integer> unknownReleases = new HashMap<>();

    private int restoringInFlight;

    private int closingInFlight;

    SessionCore(
            Port port,
            String clientId,
            Duration timeout,
            Duration keepAlive,
            boolean cleanStart
    ) {
        this.port = port;
        this.clientId = clientId;
        this.timeout = timeout;
        this.keepAlive = keepAlive;
        this.cleanStart = cleanStart;
    }

    State state() {
        return state;
    }

    long sessionNumber() {
        return sessionNumber;
    }

    void connect(long operation, long now) {
        if (state != State.DISCONNECTED) {
            port.complete(operation, null, new IllegalStateException("Client is " + state));
            return;
        }
        state = State.CONNECTING;
        generation++;
        connectOperation = operation;
        lastSent = now;
        transportConnected = false;
        port.open(generation, operation);
        port.schedule(generation, Timer.CONNECT, timeout);
    }

    void opened(long connection) {
        if (connection == generation && state == State.CONNECTING) {
            transportConnected = true;
            sendRequired(
                    ++nextTicket,
                    new Packet.Connect(
                            clientId,
                            MqttVersion.V3_1_1,
                            cleanStart,
                            (int) keepAlive.toSeconds(),
                            null,
                            null,
                            null,
                            EMPTY
                    )
            );
        }
    }

    void received(long connection, Packet packet, long now, int bytes) {
        if (connection != generation || state == State.DISCONNECTED || state == State.CLOSED) {
            return;
        }
        if (sessionRefusal != null) {
            // This broker session has no local counterpart; only its DISCONNECT may progress.
            return;
        }
        if (state == State.CONNECTING) {
            if (!(packet instanceof Packet.ConnAck ack)) {
                failed(connection, protocolError("Expected CONNACK"));
            } else if (ack.reasonCode() != ReasonCode.SUCCESS) {
                failed(connection, new MqttConnectException(
                        "Broker refused connection",
                        ack.reasonCode()
                ));
            } else if (ack.sessionPresent() && cleanStart) {
                failed(connection, protocolError("Session Present with Clean Session"));
            } else if (ack.sessionPresent() && !session.established) {
                sessionRefusal = new MqttConnectException("Broker session has no local state");
                port.cancel(Timer.CONNECT);
                port.schedule(generation, Timer.DISCONNECT, timeout);
                disconnectTicket = ++nextTicket;
                sendRequired(disconnectTicket, new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY));
            } else {
                sessionPresent = ack.sessionPresent();
                if (!ack.sessionPresent()) {
                    discardSession();
                    session.established = true;
                    if (!cleanStart) {
                        log.info("Started session for {}", clientId);
                    }
                } else {
                    reconcileReceptions(port.receiveState());
                    deferred.addAll(session.inflight.values());
                }
                state = State.CONNECTED;
                port.cancel(Timer.CONNECT);
                long operation = connectOperation;
                connectOperation = 0;
                port.complete(operation, ConnAck.of(ack.sessionPresent()), null);
                for (LocalSubscription local : List.copyOf(subscriptions.values())) {
                    if (local.closed) {
                        if (ack.sessionPresent()) {
                            local.established = true;
                            beginRemoval(local.closeOperation, local, true);
                        } else {
                            removeSubscription(local, null);
                            finishRemoval(local);
                        }
                    } else if (!ack.sessionPresent() || !local.established) {
                        local.established = false;
                        deferred.addLast(local);
                    }
                }
                if (!keepAlive.isZero()) {
                    scheduleKeepAlive(now);
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
        } else if (disconnectOperation != 0) {
            // Other input already in flight cannot change a local shutdown's write/close sequence.
            return;
        } else if (packet instanceof Packet.PingResp) {
            if (awaitingPing) {
                awaitingPing = false;
                port.cancel(Timer.PING);
            } else {
                port.ignoredPingResponse();
            }
        } else if (packet instanceof Packet.SubAck ack) {
            subscribed(ack);
        } else if (packet instanceof Packet.UnsubAck ack) {
            unsubscribed(ack);
        } else if (packet instanceof Packet.Publish publish) {
            deliver(publish, bytes);
        } else {
            failed(connection, protocolError("Unexpected " + packet.getClass().getSimpleName()));
        }
        // Acknowledgements can return packet identifiers or the close exchange window.
        drainClosures();
        drainDeferred();
    }

    void sent(long connection, long ticket, long now) {
        if (connection != generation || state == State.DISCONNECTED || state == State.CLOSED) {
            return;
        }
        lastSent = now;
        Long publication = publications.remove(ticket);
        if (publication != null) {
            port.complete(publication, PublishResult.of(), null);
        }
        if (releaseWrites.remove(ticket)) {
            drainDeferred();
        }
        if (ticket == disconnectTicket) {
            if (sessionRefusal != null) {
                end(sessionRefusal, true, false);
                return;
            }
            long operation = disconnectOperation;
            disconnectOperation = 0;
            end(null, true, false);
            port.complete(operation, null, null);
        }
    }

    /// Ends a PUBACK's retransmission window, independently of send-buffer release. Input may
    /// arrive before or after it; the DUP flag decides how a reused identifier is treated.
    void written(long connection, long ticket) {
        if (connection == generation && state == State.CONNECTED) {
            acknowledgements.written(ticket).ifPresent(entry -> {
                session.received.remove(entry);
                port.forgetDelivery(entry.token());
            });
            sendCapacityAvailable(connection);
        }
    }

    void publish(long operation, MqttMessage message) {
        if (!ready(operation)) {
            return;
        }
        // Replays precede subscription restoration; SUBACK waiting does not block publication.
        if (deferred.peekFirst() instanceof Inflight) {
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
            publications.put(ticket, operation);
        }
        if (port.publish(generation, ticket, message, packetId, receipt)
                == SendQueue.OfferResult.FULL) {
            if (packetId == 0) {
                publications.remove(ticket);
            } else {
                removeInflight(packetId);
            }
            capacityFailure(operation, "Send queue capacity");
        }
        // CLOSED waits for the connection's terminal notification, as do SUBSCRIBE/UNSUBSCRIBE.
    }

    void subscribe(long operation, Subscribe request) {
        if (!ready(operation) || !supportedQos(operation, request.qos())) {
            return;
        }
        if (subscriptions.values().stream()
                .anyMatch(local -> local.handle != null && local.filter.equals(request.filter()))) {
            port.complete(operation, null, new IllegalStateException(
                    "Subscription already exists: " + request.filter().pattern()
            ));
            return;
        }
        var local = new LocalSubscription(operation, request);
        Optional<LocalSubscription> conflict = subscriptions.values().stream()
                .filter(existing -> existing.handle != null && (local.shared || existing.shared)
                        && filtersOverlap(local.matchingFilter, existing.matchingFilter))
                .findFirst();
        if (conflict.isPresent()) {
            port.complete(operation, null, new IllegalStateException(
                    "Subscription " + request.filter().pattern() + " overlaps "
                            + conflict.orElseThrow().filter.pattern()
                            + "; a shared subscription cannot overlap another subscription"
            ));
            return;
        }
        if (subscriptions.size() >= REGISTRATION_LIMIT) {
            capacityFailure(operation, "Subscription capacity");
            return;
        }

        OptionalInt allocated = allocatePacketId();
        if (allocated.isEmpty()) {
            capacityFailure(operation, "Packet identifier capacity");
            return;
        }
        int id = allocated.getAsInt();
        // Registration precedes send: a broker may publish before sending SUBACK.
        Optional<Subscription> registered = port.registerSubscription(operation);
        if (registered.isEmpty()) {
            session.packetIds.release(id);
            capacityFailure(operation, "Subscription capacity");
            return;
        }
        local.handle = registered.orElseThrow();
        subscriptions.put(operation, local);
        subscribing.put(id, local);
        var packet = subscriptionPacket(local, id);
        if (port.send(generation, ++nextTicket, packet, null) == SendQueue.OfferResult.FULL) {
            subscribing.remove(id);
            subscriptions.remove(operation);
            session.packetIds.release(id);
            port.removeSubscription(operation, requireNonNull(local.handle), null);
            capacityFailure(operation, "Send queue capacity");
        } else {
            // Unsent cleanup of a failed registration must not remove its replacement.
            // Already admitted UNSUBSCRIBE packets precede this SUBSCRIBE in the send FIFO.
            for (LocalSubscription abandoned : List.copyOf(subscriptions.values())) {
                if (abandoned.handle == null && abandoned.filter.equals(local.filter)
                        && (abandoned.removal == null
                                || queuedClosures.contains(abandoned.removal))) {
                    removeSubscription(abandoned, null);
                    finishRemoval(abandoned);
                }
            }
        }
    }

    void unsubscribe(long operation, long subscriptionId) {
        if (!ready(operation)) {
            return;
        }
        LocalSubscription local = subscriptions.get(subscriptionId);
        if (local == null || local.closed) {
            port.complete(operation, null, null);
            return;
        }
        if (local.removing) {
            port.complete(operation, null, new IllegalStateException("Subscription is not active"));
            return;
        }
        local.removing = true;
        beginRemoval(operation, local, false);
    }

    /// Local close survives connection loss, including a close while disconnected.
    void closeSubscription(long operation, long subscriptionId) {
        LocalSubscription local = subscriptions.get(subscriptionId);
        if (local == null) {
            port.complete(operation, null, null);
            return;
        }
        local.closed = true;
        local.removing = true;
        local.closeOperation = operation;
        if (state != State.CONNECTED || disconnectOperation != 0) {
            return;
        }
        if (local.removal == null) {
            beginRemoval(operation, local, true);
        }
        drainClosures();
        drainDeferred();
    }

    private void beginRemoval(long operation, LocalSubscription local, boolean background) {
        deferred.remove(local);
        if (sessionPresent && !subscribing.containsValue(local)) {
            // An unsent restoration cannot prove that a persistent broker has no registration.
            local.established = true;
        }
        var removal = new UnsubscribeOperation(operation, local, background);
        local.removal = removal;
        if (!local.established && !subscribing.containsValue(local)) {
            // Clean Session has no broker registration yet when restoration is still queued.
            removeSubscription(local, null);
            finishRemoval(local);
        } else if (background || !local.established) {
            queuedClosures.addLast(removal);
        } else {
            sendUnsubscribe(removal).ifPresent(refusal -> {
                local.removing = false;
                local.removal = null;
                capacityFailure(operation, refusal);
            });
        }
    }

    /// Returns which capacity refused the packet, or empty once the send queue admits it. A
    /// refusal releases the packet identifier and leaves no pending UNSUBACK entry. CLOSED
    /// counts as admitted and waits for the connection's terminal cause, like other admitted
    /// protocol operations.
    private Optional<String> sendUnsubscribe(UnsubscribeOperation removal) {
        if (!removal.subscription.established) {
            return Optional.of("Subscription acknowledgement pending");
        }
        OptionalInt allocated = allocatePacketId();
        if (allocated.isEmpty()) {
            return Optional.of("Packet identifier capacity");
        }
        int id = allocated.getAsInt();
        unsubscribing.put(id, removal);
        var packet = new Packet.Unsubscribe(id, List.of(removal.subscription.filter), EMPTY);
        if (port.send(generation, ++nextTicket, packet, null) == SendQueue.OfferResult.FULL) {
            unsubscribing.remove(id);
            session.packetIds.release(id);
            return Optional.of("Send queue capacity");
        }
        return Optional.empty();
    }

    private void drainClosures() {
        // DISCONNECT stays last, even when a PUBACK arrives while it is pending.
        if (state != State.CONNECTED || disconnectOperation != 0) {
            return;
        }
        // A refused closure stays first, so removals keep their close order.
        while (closingInFlight < CLOSE_INFLIGHT_LIMIT && !queuedClosures.isEmpty()) {
            if (sendUnsubscribe(queuedClosures.getFirst()).isPresent()) {
                return;
            }
            UnsubscribeOperation removal = queuedClosures.removeFirst();
            if (removal.background) {
                closingInFlight++;
            }
        }
    }

    /// Restoration uses no application operation slots and resumes when capacity returns.
    private void drainDeferred() {
        if (state != State.CONNECTED || disconnectOperation != 0) {
            return;
        }
        // Replay the single session order before fresh PUBREC responses. A duplicate PUBREC
        // shares a PUBREL replay still waiting here instead of adding another response.
        while (deferred.peekFirst() instanceof Inflight inflight) {
            if (session.inflight.get(inflight.packetId) != inflight) {
                deferred.removeFirst();
                continue;
            }
            SendQueue.OfferResult result = switch (inflight) {
                case Publication publication -> port.publish(
                        generation,
                        ++nextTicket,
                        requireNonNull(publication.message),
                        publication.packetId,
                        requireNonNull(publication.receipt)
                );
                case Release release -> sendRelease(++nextTicket, release.packetId);
            };
            if (result != SendQueue.OfferResult.ACCEPTED) {
                return;
            }
            deferred.removeFirst();
        }
        while (!releases.isEmpty() && releaseWrites.size() < RELEASE_WRITE_LIMIT) {
            int id = releases.getFirst();
            long ticket = ++nextTicket;
            if (sendRelease(ticket, id) != SendQueue.OfferResult.ACCEPTED) {
                // Re-creation uses the regular quota; it must not wait for acknowledgement room.
                break;
            }
            releases.removeFirst();
            releaseWrites.add(ticket);
            unknownReleases.computeIfPresent(id, (_, admitted) -> admitted + 1);
        }
        while (!deferred.isEmpty()) {
            if (restoringInFlight >= RESTORE_INFLIGHT_LIMIT) {
                return;
            }
            var local = (LocalSubscription) deferred.getFirst();
            if (!subscriptions.containsKey(local.id)) {
                deferred.removeFirst();
                continue;
            }
            OptionalInt allocated = allocatePacketId();
            if (allocated.isEmpty()) {
                return;
            }
            int id = allocated.getAsInt();
            subscribing.put(id, local);
            if (port.send(generation, ++nextTicket, subscriptionPacket(local, id), null)
                    == SendQueue.OfferResult.FULL) {
                subscribing.remove(id);
                session.packetIds.release(id);
                return;
            }
            deferred.removeFirst();
            restoringInFlight++;
        }
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
        }
        return result;
    }

    private static Packet.Subscribe subscriptionPacket(LocalSubscription local, int id) {
        return new Packet.Subscribe(id, List.of(new Packet.Subscription(
                local.filter, local.requestedQos, false, false, 0
        )), EMPTY);
    }

    private void removeSubscription(LocalSubscription local, @Nullable Throwable cause) {
        subscriptions.remove(local.id);
        deferred.remove(local);
        if (local.handle != null) {
            port.removeSubscription(local.id, local.handle, cause);
        }
    }

    private void finishRemoval(LocalSubscription local) {
        UnsubscribeOperation removal = local.removal;
        if (removal != null) {
            queuedClosures.remove(removal);
            if (!removal.background) {
                port.complete(removal.operation, null, null);
            }
        }
        if (local.closeOperation != 0) {
            port.complete(local.closeOperation, null, null);
            local.closeOperation = 0;
        }
        local.removal = null;
    }

    private boolean ready(long operation) {
        if (state == State.CLOSED) {
            port.complete(operation, null, new IllegalStateException("Client is closed"));
        } else if (state != State.CONNECTED || disconnectOperation != 0) {
            port.complete(operation, null, new MqttNotConnectedException());
        } else {
            return true;
        }
        return false;
    }

    private boolean supportedQos(long operation, Qos qos) {
        if (qos != Qos.EXACTLY_ONCE) {
            return true;
        }
        port.complete(operation, null, new IllegalArgumentException("QoS 2 is not supported"));
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
            if (allocated.isEmpty() || !unknownReleases.containsKey(allocated.getAsInt())) {
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
                deferred.remove(publication);
                session.inflight.putLast(packetId, new Release(packetId, publication.operation));
            }
            case Release pending -> {
                // A PUBREL replay still waiting for capacity also answers this duplicate. The
                // scan is bounded by 64 exchanges and 256 subscriptions; index it only if those
                // limits grow.
                if (deferred.contains(pending)) {
                    return;
                }
            }
            case null -> {
                if (!unknownPubRecLogged) {
                    unknownPubRecLogged = true;
                    log.warn("Received PUBREC for unknown packet identifier {} on {}",
                            packetId, clientId);
                }
                unknownReleases.putIfAbsent(packetId, 0);
            }
        }
        // DISCONNECT remains last. A persistent session still records the new stage so the
        // next connection continues with PUBREL, even though this one cannot send it.
        if (disconnectOperation != 0) {
            return;
        }
        releases.add(packetId);
    }

    private void completed(int packetId) {
        Inflight inflight = session.inflight.get(packetId);
        if (inflight == null) {
            Integer admitted = unknownReleases.get(packetId);
            if (admitted != null && admitted > 1) {
                // The answer to one of several PUBRELs for an unknown PUBREC.
                unknownReleases.put(packetId, admitted - 1);
            } else if (admitted != null) {
                // The last answer to a PUBREL for an unknown PUBREC makes its id available.
                unknownReleases.remove(packetId);
                releases.remove(packetId);
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
        if (disconnectOperation == 0) {
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
            }
            deferred.remove(inflight);
            releases.remove(packetId);
        }
    }

    /// The earlier reception was a stale retransmission or a resend that omitted DUP. Either way
    /// the new reception's PUBACK covers the broker's exchange, so the earlier one only releases
    /// its local delivery and stops holding back later acknowledgements.
    private void withdrawAcknowledgement(int packetId, long token) {
        if (!reusedIdentifierLogged) {
            reusedIdentifierLogged = true;
            log.warn("Packet identifier {} was reused without DUP before its PUBACK on {}; "
                    + "the earlier reception is not acknowledged", packetId, clientId);
        }
        port.releaseDelivery(token);
        port.forgetDelivery(token);
        sendCapacityAvailable(generation);
    }

    private void subscribed(Packet.SubAck ack) {
        LocalSubscription local = subscribing.get(ack.packetId());
        if (local == null || ack.reasonCodes().size() != 1) {
            failed(generation, protocolError("Unexpected SUBACK"));
            return;
        }
        ReasonCode reason = ack.reasonCodes().getFirst();
        if (reason != ReasonCode.UNSPECIFIED_ERROR
                && (reason.value() > local.requestedQos.level()
                        || reason.value() > Qos.AT_LEAST_ONCE.level())) {
            failed(generation, protocolError("SUBACK grants an unsupported QoS or reason"));
            return;
        }
        subscribing.remove(ack.packetId());
        session.packetIds.release(ack.packetId());
        boolean restoring = local.confirmed;
        if (restoring) {
            restoringInFlight--;
        }
        if (reason == ReasonCode.UNSPECIFIED_ERROR) {
            var refusal = new MqttSubscribeException(reason);
            removeSubscription(local, refusal);
            if (!restoring) {
                port.complete(local.id, null, refusal);
            } else {
                try {
                    log.warn("Subscription restoration refused for {} (subscription {})",
                            clientId, local.id, refusal);
                } catch (Throwable _) {
                    // A logging backend failure must not turn local refusal into client closure.
                }
                finishRemoval(local);
            }
        } else {
            local.confirmed = true;
            local.established = true;
            Subscription handle = requireNonNull(local.handle);
            port.subscribed(handle, Qos.of(reason.value()));
            if (!restoring) {
                port.complete(local.id, handle, null);
            }
        }
    }

    private void unsubscribed(Packet.UnsubAck ack) {
        UnsubscribeOperation pending = unsubscribing.get(ack.packetId());
        if (pending == null || !ack.reasonCodes().isEmpty()) {
            failed(generation, protocolError("Unexpected UNSUBACK"));
            return;
        }
        unsubscribing.remove(ack.packetId());
        session.packetIds.release(ack.packetId());
        if (pending.background) {
            closingInFlight--;
        }
        removeSubscription(pending.subscription, null);
        finishRemoval(pending.subscription);
    }

    private void deliver(Packet.Publish publish, int bytes) {
        if (publish.qos() == Qos.EXACTLY_ONCE) {
            failed(generation, protocolError("QoS 2 delivery is not supported"));
            return;
        }
        long token = 0;
        boolean continued = false;
        if (publish.qos() == Qos.AT_LEAST_ONCE) {
            int packetId = publish.packetId();
            Optional<ReceiveTable.Entry> existing = session.received.find(packetId);
            if (existing.isPresent()) {
                ReceiveTable.Entry earlier = existing.orElseThrow();
                if (publish.dup()) {
                    if (acknowledgements.contains(earlier)) {
                        return;
                    }
                    // A continued reception joins this connection only when its resend arrives.
                    token = earlier.token();
                    continued = true;
                    acknowledgements.add(earlier);
                } else {
                    session.received.supersede(packetId);
                    if (acknowledgements.remove(earlier)
                            || !acknowledgements.contains(earlier)) {
                        session.received.remove(earlier);
                        withdrawAcknowledgement(packetId, earlier.token());
                    }
                }
            }
            if (token == 0) {
                token = ++nextDelivery;
                Optional<ReceiveTable.Entry> added = session.received.add(token, packetId);
                if (added.isEmpty()) {
                    failed(generation, new MqttCapacityException("Acknowledgement capacity"));
                    return;
                }
                acknowledgements.add(added.orElseThrow());
            }
        }
        var matches = new ArrayList<Long>();
        // Matching is O(subscriptions). Replace with a topic tree if 0.4 measurements show
        // this scan dominates, keeping TopicFilter.matches as the reference implementation.
        for (var local : subscriptions.values()) {
            if (!local.removing && local.matchingFilter.matches(publish.topic())) {
                matches.add(local.id);
            }
        }
        if (matches.isEmpty() && !continued) {
            if (token != 0) {
                deliveryReady(token);
            } else {
                log.debug("Discarding unmatched QoS 0 delivery for {} on {}",
                        publish.topic(), clientId);
            }
            return;
        }
        var message = MqttMessage.of(
                publish.topic(),
                publish.payload(),
                publish.qos(),
                publish.retain()
        );
        if (!port.deliver(
                new DeliveryToken(sessionNumber, generation, token),
                matches,
                message,
                publish.dup(),
                bytes
        )) {
            failed(generation, new MqttCapacityException("Delivery capacity"));
        } else if (continued) {
            session.received.ready(token, port.readyDelivery(token));
            sendCapacityAvailable(generation);
        }
    }

    void deliveryReady(long token) {
        deliveriesReady(List.of(token));
    }

    /// QoS 0 and already acknowledged deliveries have no entry in the receive table.
    void deliveryFailed(DeliveryToken token, Throwable cause) {
        if (token.session() == sessionNumber
                && state != State.DISCONNECTED
                && state != State.CLOSED) {
            end(cause, disconnectOperation != 0, false, true);
        }
    }

    /// Tokens belong to the client lifetime; only entries still in the receive table can advance.
    void deliveriesReady(List<Long> tokens) {
        if (state == State.CLOSED) {
            return;
        }
        tokens.forEach(session.received::ready);
        sendCapacityAvailable(generation);
    }

    void sendCapacityAvailable(long connection) {
        if (connection != generation || state != State.CONNECTED || disconnectOperation != 0) {
            return;
        }
        if (releasesFirst) {
            drainDeferred();
        }
        sendAcknowledgements();
        drainClosures();
        drainDeferred();
    }

    private void sendAcknowledgements() {
        Optional<ReceiveTable.Entry> ready;
        while (acknowledgements.writingCount() < ACK_WRITE_LIMIT
                && (ready = acknowledgements.nextReady()).isPresent()) {
            ReceiveTable.Entry entry = ready.orElseThrow();
            long ticket = ++nextTicket;
            var receipt = new SendReceipt();
            if (port.send(
                    generation,
                    ticket,
                    new Packet.PubAck(entry.packetId(), ReasonCode.SUCCESS, EMPTY),
                    receipt
            )
                    != SendQueue.OfferResult.ACCEPTED) {
                // FULL waits for actual budget release; CLOSED waits for the original cause.
                // Work in the regular quota, such as closures and re-creation, still runs.
                return;
            }
            releasesFirst = true;
            acknowledgements.submitted(ticket);
            entry.submitted(receipt);
            port.releaseDelivery(entry.token());
        }
    }

    void timer(long connection, Timer timer, long now) {
        if (connection != generation) {
            return;
        }
        switch (timer) {
            case CONNECT -> {
                if (state == State.CONNECTING) {
                    String phase = transportConnected ? "CONNACK" : "Transport connection";
                    failed(connection, new TimeoutException(phase + " deadline expired"));
                }
            }
            case KEEP_ALIVE -> {
                if (state == State.CONNECTED && disconnectOperation == 0) {
                    long remaining = keepAlive.toNanos() - (now - lastSent);
                    if (awaitingPing) {
                        port.schedule(generation, Timer.KEEP_ALIVE, keepAlive);
                    } else if (remaining > 0) {
                        port.schedule(generation, Timer.KEEP_ALIVE, Duration.ofNanos(remaining));
                    } else {
                        awaitingPing = true;
                        pingTicket = ++nextTicket;
                        // Include a stalled write in the deadline; otherwise it can wait forever.
                        port.schedule(generation, Timer.PING, keepAlive);
                        if (sendRequired(pingTicket, new Packet.PingReq())) {
                            port.schedule(generation, Timer.KEEP_ALIVE, keepAlive);
                        }
                    }
                }
            }
            case PING -> {
                if (state == State.CONNECTED && awaitingPing) {
                    failed(connection, new TimeoutException("PINGRESP deadline expired"));
                }
            }
            case DISCONNECT -> {
                if (disconnectOperation != 0 || sessionRefusal != null) {
                    failed(connection, new TimeoutException("DISCONNECT write deadline expired"));
                }
            }
        }
    }

    private void scheduleKeepAlive(long now) {
        port.schedule(
                generation,
                Timer.KEEP_ALIVE,
                Duration.ofNanos(Math.max(0, keepAlive.toNanos() - (now - lastSent)))
        );
    }

    void disconnect(long operation) {
        if (state == State.CLOSED) {
            port.complete(operation, null, new IllegalStateException("Client is closed"));
        } else if (state != State.CONNECTED || disconnectOperation != 0) {
            port.complete(operation, null, new MqttNotConnectedException());
        } else {
            disconnectOperation = operation;
            port.cancel(Timer.KEEP_ALIVE);
            port.cancel(Timer.PING);
            awaitingPing = false;
            disconnectTicket = ++nextTicket;
            port.schedule(generation, Timer.DISCONNECT, timeout);
            sendRequired(
                    disconnectTicket,
                    new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY)
            );
        }
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
        if (connection == generation && state != State.DISCONNECTED && state != State.CLOSED) {
            end(cause, disconnectOperation != 0, false);
        }
    }

    void close(@Nullable Throwable cause) {
        end(cause, cause == null || disconnectOperation != 0, true);
    }

    private void end(@Nullable Throwable cause, boolean requested, boolean permanent) {
        end(cause, requested, permanent, false);
    }

    private void end(
            @Nullable Throwable cause,
            boolean requested,
            boolean permanent,
            boolean stopped
    ) {
        if (state == State.CLOSED) {
            return;
        }
        if (cause instanceof PacketDecodeException failure) {
            // Both listener notifications and pending operations expose the same public cause.
            cause = new MqttProtocolException(
                    requireNonNull(failure.getMessage()),
                    failure.reasonCode(),
                    failure
            );
        }
        if (sessionRefusal != null) {
            // A failed DISCONNECT must not replace the reason this session was refused.
            if (cause != null && cause != sessionRefusal) {
                sessionRefusal.addSuppressed(cause);
            }
            cause = sessionRefusal;
        }
        try {
            endConnection(cause, requested, permanent, stopped);
            finishOperations(cause, permanent);
        } finally {
            // Shell teardown or a logging backend must not retain a closed client's payloads.
            if (permanent || cleanStart) {
                discardSession();
            }
        }
    }

    private void endConnection(
            @Nullable Throwable cause,
            boolean requested,
            boolean permanent,
            boolean stopped
    ) {
        boolean established = state == State.CONNECTED;
        boolean active = state != State.DISCONNECTED;
        state = permanent ? State.CLOSED : State.DISCONNECTED;
        for (var timer : Timer.values()) {
            port.cancel(timer);
        }
        awaitingPing = false;
        pingTicket = 0;
        disconnectTicket = 0;
        sessionRefusal = null;
        sessionPresent = false;
        if (active) {
            // Retire queued recipients before removing subscriptions, without acknowledging
            // work that never reached the application. Persistent handed work keeps its state.
            Map<Long, Boolean> handedReadiness = port.end(
                    generation,
                    established,
                    Disconnection.of(cause, requested),
                    !permanent && !cleanStart,
                    stopped
            );
            reconcileReceptions(handedReadiness);
        }
        acknowledgements.clear();
        if (permanent || cleanStart) {
            session.received.clear();
        }
    }

    /// `handedReadiness` follows [Port#receiveState()]: receptions it omits reached no recipient.
    private void reconcileReceptions(Map<Long, Boolean> handedReadiness) {
        for (ReceiveTable.Entry entry : session.received.entries()) {
            SendReceipt receipt = entry.receipt();
            Boolean ready = handedReadiness.get(entry.token());
            if ((receipt != null && receipt.accepted()) || ready == null
                    || session.received.find(entry.packetId()).orElse(null) != entry) {
                session.received.remove(entry);
                port.forgetDelivery(entry.token());
            } else {
                session.received.ready(entry.token(), ready);
            }
        }
    }

    private void finishOperations(@Nullable Throwable cause, boolean permanent) {
        var pendingFailure = (cause == null)
                ? new MqttConnectionLostException()
                : new MqttConnectionLostException(cause);
        for (LocalSubscription local : List.copyOf(subscriptions.values())) {
            if (permanent || (cleanStart && !local.confirmed)) {
                removeSubscription(local, permanent ? cause : null);
                if (permanent && local.closeOperation != 0) {
                    port.complete(local.closeOperation, null, pendingFailure);
                    local.closeOperation = 0;
                }
            } else if (!local.confirmed) {
                // A failed initial SUBSCRIBE can still have reached a persistent broker.
                // Keep only its bounded cleanup intent, without a handle or delivery lane.
                if (local.handle != null) {
                    port.removeSubscription(local.id, local.handle, null);
                    local.handle = null;
                }
                local.closed = true;
                local.removing = true;
            } else {
                if (cleanStart || (local.removing && !local.closed)) {
                    local.established = false;
                }
                local.removing = local.closed;
            }
            local.removal = null;
        }
        for (long operation : publications.values()) {
            port.complete(operation, null, pendingFailure);
        }
        publications.clear();
        for (Inflight inflight : session.inflight.values()) {
            if (inflight.operation != 0) {
                port.complete(inflight.operation, null, pendingFailure);
                inflight.operation = 0;
            }
        }
        for (var local : subscribing.values()) {
            if (!local.confirmed) {
                port.complete(local.id, null, pendingFailure);
            }
        }
        subscribing.keySet().forEach(session.packetIds::release);
        subscribing.clear();
        for (var pending : unsubscribing.values()) {
            if (!pending.background) {
                port.complete(pending.operation, null, pendingFailure);
            }
        }
        unsubscribing.keySet().forEach(session.packetIds::release);
        unsubscribing.clear();
        for (var pending : queuedClosures) {
            if (!pending.background) {
                port.complete(pending.operation, null, pendingFailure);
            }
        }
        queuedClosures.clear();
        deferred.clear();
        releases.clear();
        releaseWrites.clear();
        unknownReleases.clear();
        restoringInFlight = 0;
        closingInFlight = 0;
        if (connectOperation != 0) {
            Throwable failure = switch (cause) {
                case null -> new MqttConnectException("Connection failed");
                case MqttConnectException refusal -> refusal;
                default -> new MqttConnectException("Connection failed", cause);
            };
            long operation = connectOperation;
            connectOperation = 0;
            port.complete(operation, null, failure);
        }
        if (disconnectOperation != 0) {
            long operation = disconnectOperation;
            disconnectOperation = 0;
            port.complete(
                    operation,
                    null,
                    (cause == null)
                            ? new MqttConnectionLostException()
                            : new MqttConnectionLostException(cause)
            );
        }
    }

    private void discardSession() {
        boolean established = session.established;
        session = new Session();
        port.sessionChanged(++sessionNumber);
        // Every clean connection starts and discards a session; only persistent ones are news.
        if (established && !cleanStart) {
            log.info("Discarded session for {}", clientId);
        }
    }

    private static PacketDecodeException protocolError(String message) {
        return new PacketDecodeException(ReasonCode.PROTOCOL_ERROR, message);
    }

    /// Whether both filters can match at least one valid, nonempty MQTT topic.
    private static boolean filtersOverlap(TopicFilter first, TopicFilter second) {
        String[] left = first.pattern().split("/", -1);
        String[] right = second.pattern().split("/", -1);
        boolean leftTail = left[left.length - 1].equals("#");
        boolean rightTail = right[right.length - 1].equals("#");
        int leftMinimum = left.length - (leftTail ? 1 : 0);
        int rightMinimum = right.length - (rightTail ? 1 : 0);
        int levels = Math.max(1, Math.max(leftMinimum, rightMinimum));
        if ((!leftTail && leftMinimum != levels) || (!rightTail && rightMinimum != levels)) {
            return false;
        }

        // Build the shortest common topic's byte count. A trailing # supplies unrestricted
        // remaining levels; an unconstrained + level can be empty, including the first one.
        int bytes = levels - 1;
        boolean firstLevelFlexible = false;
        for (int i = 0; i < levels; i++) {
            String a = i < leftMinimum ? left[i] : "+";
            String b = i < rightMinimum ? right[i] : "+";
            if (i == 0 && ((a.startsWith("$") && b.equals("+"))
                    || (b.startsWith("$") && a.equals("+")))) {
                return false;
            }
            if (!a.equals("+") && !b.equals("+") && !a.equals(b)) {
                return false;
            }
            String literal = a.equals("+") ? b : a;
            if (literal.equals("+")) {
                firstLevelFlexible |= i == 0;
            } else {
                bytes += literal.getBytes(UTF_8).length;
            }
        }

        // An empty candidate is valid only if its first level can become nonempty, or both
        // trailing # wildcards allow adding another empty level to form the topic "/".
        return bytes <= 65535
                && (bytes > 0 || firstLevelFlexible || (leftTail && rightTail));
    }

    // State owned by one session. A CONNACK without Session Present, permanent closure, or
    // the end of a nonpersistent connection replaces it.
    // SUBSCRIBE and UNSUBSCRIBE borrow its identifiers only until their connection ends.
    private static final class Session {

        final PacketIds packetIds = new PacketIds();
        final ReceiveTable received = new ReceiveTable(ACKNOWLEDGEMENT_LIMIT);
        // PUBLISH entries retain send order; replacing one with PUBREL moves it to the end in
        // PUBREC order. Both stages share the identifier and count window across connections.
        final SequencedMap<Integer, Inflight> inflight = new LinkedHashMap<>();
        // A CONNACK without Session Present started this session; only then can a later
        // Session Present continue it.
        boolean established;
        // Retained PUBLISH wire sizes, counted only for a persistent session until PUBACK/REC.
        long bytes;
    }

    private sealed interface Deferred {
    }

    private abstract static sealed class Inflight implements Deferred {

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

    private static final class LocalSubscription implements Deferred {

        // The subscribe operation's token; the shell and its delivery lanes use the same id.
        final long id;
        final TopicFilter filter;
        final TopicFilter matchingFilter;
        final Qos requestedQos;
        final boolean shared;
        @Nullable Subscription handle;
        boolean confirmed;
        boolean established;
        boolean removing;
        boolean closed;
        long closeOperation;
        @Nullable UnsubscribeOperation removal;

        LocalSubscription(long id, Subscribe request) {
            this.id = id;
            filter = request.filter();
            requestedQos = request.qos();
            Optional<SharedTopicFilter> sharedFilter = SharedTopicFilter.parse(filter);
            shared = sharedFilter.isPresent();
            matchingFilter = sharedFilter.map(SharedTopicFilter::filter).orElse(filter);
        }
    }

    private record UnsubscribeOperation(
            long operation,
            LocalSubscription subscription,
            boolean background
    ) {
    }
}
