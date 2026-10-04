package org.mqtt4j.mqtj.client.internal;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.ConnAck;
import org.mqtt4j.mqtj.Disconnection;
import org.mqtt4j.mqtj.MqttCapacityException;
import org.mqtt4j.mqtj.MqttConnectException;
import org.mqtt4j.mqtj.MqttConnectionLostException;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.MqttNotConnectedException;
import org.mqtt4j.mqtj.MqttProtocolException;
import org.mqtt4j.mqtj.MqttSubscribeException;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.PublishResult;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.ReasonCode;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.Subscription;
import org.mqtt4j.mqtj.TopicFilter;
import org.mqtt4j.mqtj.client.internal.common.io.SendQueue;
import org.mqtt4j.mqtj.client.internal.common.log.Log;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketIds;
import org.mqtt4j.mqtj.client.internal.common.protocol.SharedTopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.mqtt4j.mqtj.client.internal.common.protocol.Properties.EMPTY;

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
        SendQueue.OfferResult send(long generation, long ticket, Packet packet);

        /// Reports admission at the exact wire size, not write completion. The payload is copied
        /// only after admission.
        SendQueue.OfferResult publish(
                long generation,
                long ticket,
                MqttMessage message,
                int packetId
        );

        void schedule(long generation, Timer timer, Duration delay);

        void cancel(Timer timer);

        void ignoredPingResponse();

        void complete(long operation, @Nullable Object result, @Nullable Throwable failure);

        boolean registerSubscription(long generation, long id);

        Subscription subscribed(long id, Qos granted);

        boolean deliver(
                long generation,
                long token,
                List<Long> ids,
                MqttMessage message,
                boolean dup,
                int bytes
        );

        void releaseDelivery(long token);

        void removeSubscription(long id);

        void end(long generation, boolean established, Disconnection disconnection);
    }

    private static final Log log = Log.of(SessionCore.class);

    private static final int ACKNOWLEDGEMENT_LIMIT = 4096;

    // Broker registrations outlive locally closed handles until UNSUBACK or connection end,
    // so they are bounded apart from the delivery lanes that local closure releases.
    private static final int REGISTRATION_LIMIT = 256;

    // Bound queued completion events even when I/O returns capacity during an actor drain.
    private static final int ACK_WRITE_LIMIT = 16;

    // Close has no public operation slot. Bound its in-flight exchanges separately, so a
    // batch of closures cannot flood the actor with send completions and UNSUBACKs.
    private static final int CLOSE_INFLIGHT_LIMIT = 16;

    private final Port port;

    private final String clientId;

    private final Duration timeout;

    private final Duration keepAlive;

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

    private PacketIds packetIds = new PacketIds();

    private final Map<Long, Long> publications = new HashMap<>();

    private final Map<Integer, Long> inflight = new HashMap<>();

    private final AckSequencer acknowledgements = new AckSequencer(ACKNOWLEDGEMENT_LIMIT);

    private long nextDelivery;

    private boolean unknownPubAckLogged;

    private boolean reusedIdentifierLogged;

    private final Map<Long, LocalSubscription> subscriptions = new LinkedHashMap<>();

    private final Map<Integer, LocalSubscription> subscribing = new HashMap<>();

    private final Map<Integer, UnsubscribeOperation> unsubscribing = new HashMap<>();

    // At most one removal per registered subscription; the registry supplies the total bound.
    private final ArrayDeque<UnsubscribeOperation> queuedClosures = new ArrayDeque<>();

    private int closingInFlight;

    SessionCore(Port port, String clientId, Duration timeout, Duration keepAlive) {
        this.port = port;
        this.clientId = clientId;
        this.timeout = timeout;
        this.keepAlive = keepAlive;
    }

    State state() {
        return state;
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
            sendRequired(++nextTicket, new Packet.Connect(
                    clientId,
                    MqttVersion.V3_1_1,
                    true,
                    (int) keepAlive.toSeconds(),
                    null,
                    null,
                    null,
                    EMPTY
            ));
        }
    }

    void received(long connection, Packet packet, long now, int bytes) {
        if (connection != generation || state == State.DISCONNECTED || state == State.CLOSED) {
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
            } else if (ack.sessionPresent()) {
                failed(connection, protocolError("Session Present with Clean Session"));
            } else {
                state = State.CONNECTED;
                port.cancel(Timer.CONNECT);
                long operation = connectOperation;
                connectOperation = 0;
                port.complete(operation, new ConnAck(false), null);
                if (!keepAlive.isZero()) {
                    scheduleKeepAlive(now);
                }
            }
        } else if (packet instanceof Packet.PubAck ack) {
            // Completing a publication sends nothing, so it cannot disturb a pending shutdown.
            acknowledged(ack);
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
        // SUBACK/PUBACK/UNSUBACK can return packet identifiers or the close exchange window.
        drainClosures();
    }

    void sent(long connection, long ticket, long now) {
        if (connection != generation || state == State.DISCONNECTED || state == State.CLOSED) {
            return;
        }
        lastSent = now;
        Long publication = publications.remove(ticket);
        if (publication != null) {
            port.complete(publication, new PublishResult(), null);
        }
        if (ticket == disconnectTicket) {
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
            acknowledgements.written(ticket);
            sendCapacityAvailable(connection);
        }
    }

    void publish(long operation, MqttMessage message) {
        if (!ready(operation) || !supportedQos(operation, message.qos())) {
            return;
        }
        long ticket = ++nextTicket;
        int packetId = 0;
        if (message.qos() == Qos.AT_LEAST_ONCE) {
            var allocated = packetIds.allocate();
            if (allocated.isEmpty()) {
                capacityFailure(operation, "Packet identifier capacity");
                return;
            }
            packetId = allocated.getAsInt();
            inflight.put(packetId, operation);
        } else {
            publications.put(ticket, operation);
        }
        if (port.publish(generation, ticket, message, packetId) == SendQueue.OfferResult.FULL) {
            if (packetId == 0) {
                publications.remove(ticket);
            } else {
                inflight.remove(packetId);
                packetIds.release(packetId);
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
                .anyMatch(local -> local.filter.equals(request.filter()))) {
            port.complete(operation, null, new IllegalStateException(
                    "Subscription already exists: " + request.filter().pattern()
            ));
            return;
        }
        var local = new LocalSubscription(operation, request);
        Optional<LocalSubscription> conflict = subscriptions.values().stream()
                .filter(existing -> (local.shared || existing.shared)
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

        var allocated = packetIds.allocate();
        if (allocated.isEmpty()) {
            capacityFailure(operation, "Packet identifier capacity");
            return;
        }
        int id = allocated.getAsInt();
        subscriptions.put(operation, local);
        subscribing.put(id, local);
        // Registration precedes send: a broker may publish before sending SUBACK.
        if (!port.registerSubscription(generation, operation)) {
            subscribing.remove(id);
            subscriptions.remove(operation);
            packetIds.release(id);
            capacityFailure(operation, "Subscription capacity");
            return;
        }
        var packet = new Packet.Subscribe(id, List.of(new Packet.Subscription(
                request.filter(), request.qos(), false, false, 0
        )), EMPTY);
        if (port.send(generation, ++nextTicket, packet) == SendQueue.OfferResult.FULL) {
            subscribing.remove(id);
            subscriptions.remove(operation);
            packetIds.release(id);
            port.removeSubscription(operation);
            capacityFailure(operation, "Send queue capacity");
        }
    }

    void unsubscribe(long operation, long subscriptionId) {
        beginUnsubscribe(operation, subscriptionId, false);
    }

    /// Records one permanent local closure. The shell drains send capacity after its batch.
    void closeSubscription(long operation, long subscriptionId) {
        beginUnsubscribe(operation, subscriptionId, true);
    }

    private void beginUnsubscribe(long operation, long subscriptionId, boolean background) {
        if (!ready(operation)) {
            return;
        }
        LocalSubscription local = subscriptions.get(subscriptionId);
        // The shell sends one removal per established handle of the current connection
        // and resumes its delivery lane when the removal fails, unless the handle was closed.
        if (local == null || !local.established || local.removing) {
            port.complete(operation, null, new IllegalStateException("Subscription is not active"));
            return;
        }
        local.removing = true;
        var removal = new UnsubscribeOperation(operation, local, background);
        if (background) {
            queuedClosures.addLast(removal);
        } else {
            sendUnsubscribe(removal).ifPresent(refusal -> {
                local.removing = false;
                capacityFailure(operation, refusal);
            });
        }
    }

    /// Returns which capacity refused the packet, or empty once the send queue admits it. A
    /// refusal releases the packet identifier and leaves no pending UNSUBACK entry. CLOSED
    /// counts as admitted and waits for the connection's terminal cause, like other admitted
    /// protocol operations.
    private Optional<String> sendUnsubscribe(UnsubscribeOperation removal) {
        var allocated = packetIds.allocate();
        if (allocated.isEmpty()) {
            return Optional.of("Packet identifier capacity");
        }
        int id = allocated.getAsInt();
        unsubscribing.put(id, removal);
        var packet = new Packet.Unsubscribe(id, List.of(removal.subscription.filter), EMPTY);
        if (port.send(generation, ++nextTicket, packet) == SendQueue.OfferResult.FULL) {
            unsubscribing.remove(id);
            packetIds.release(id);
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
            queuedClosures.removeFirst();
            closingInFlight++;
        }
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

    private void acknowledged(Packet.PubAck ack) {
        Long operation = inflight.remove(ack.packetId());
        if (operation == null) {
            if (!unknownPubAckLogged) {
                unknownPubAckLogged = true;
                log.warn("Ignoring PUBACK for unknown packet identifier {} on {}",
                        ack.packetId(), clientId);
            }
            return;
        }
        packetIds.release(ack.packetId());
        port.complete(operation, new PublishResult(), null);
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
        packetIds.release(ack.packetId());
        if (reason == ReasonCode.UNSPECIFIED_ERROR) {
            subscriptions.remove(local.id);
            port.removeSubscription(local.id);
            port.complete(local.id, null, new MqttSubscribeException(reason));
        } else {
            local.established = true;
            port.complete(local.id, port.subscribed(local.id, Qos.of(reason.value())), null);
        }
    }

    private void unsubscribed(Packet.UnsubAck ack) {
        UnsubscribeOperation pending = unsubscribing.get(ack.packetId());
        if (pending == null || !ack.reasonCodes().isEmpty()) {
            failed(generation, protocolError("Unexpected UNSUBACK"));
            return;
        }
        unsubscribing.remove(ack.packetId());
        packetIds.release(ack.packetId());
        if (pending.background) {
            closingInFlight--;
        }
        subscriptions.remove(pending.subscription.id);
        port.removeSubscription(pending.subscription.id);
        port.complete(pending.operation, null, null);
    }

    private void deliver(Packet.Publish publish, int bytes) {
        if (publish.qos() == Qos.EXACTLY_ONCE) {
            failed(generation, protocolError("QoS 2 delivery is not supported"));
            return;
        }
        long token = 0;
        if (publish.qos() == Qos.AT_LEAST_ONCE) {
            int packetId = publish.packetId();
            if (acknowledgements.contains(packetId)) {
                if (publish.dup()) {
                    // A retransmission joins the reception that holds this identifier until its
                    // PUBACK is written, without another delivery or acknowledgement.
                    return;
                }
                // Without DUP this is a new publication. Senders reuse an identifier only after
                // the earlier exchange is acknowledged, so that reception needs no PUBACK.
                acknowledgements.supersede(packetId)
                        .ifPresent(earlier -> withdrawAcknowledgement(packetId, earlier));
            }
            token = ++nextDelivery;
            if (!acknowledgements.add(token, packetId)) {
                failed(generation, new MqttCapacityException("Acknowledgement capacity"));
                return;
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
        if (matches.isEmpty()) {
            if (token != 0) {
                deliveryReady(generation, token);
            } else {
                log.debug("Discarding unmatched QoS 0 delivery for {} on {}",
                        publish.topic(), clientId);
            }
            return;
        }
        var message = new MqttMessage(
                publish.topic(), publish.payload(), publish.qos(), publish.retain()
        );
        if (!port.deliver(generation, token, matches, message, publish.dup(), bytes)) {
            failed(generation, new MqttCapacityException("Delivery capacity"));
        }
    }

    void deliveryReady(long connection, long token) {
        deliveriesReady(connection, List.of(token));
    }

    void deliveriesReady(long connection, List<Long> tokens) {
        if (connection != generation || state != State.CONNECTED || disconnectOperation != 0) {
            return;
        }
        tokens.forEach(acknowledgements::ready);
        sendCapacityAvailable(connection);
    }

    void sendCapacityAvailable(long connection) {
        if (connection != generation || state != State.CONNECTED || disconnectOperation != 0) {
            return;
        }
        Optional<AckSequencer.Entry> ready;
        while (acknowledgements.writingCount() < ACK_WRITE_LIMIT
                && (ready = acknowledgements.nextReady()).isPresent()) {
            AckSequencer.Entry entry = ready.orElseThrow();
            long ticket = ++nextTicket;
            if (port.send(generation, ticket,
                    new Packet.PubAck(entry.packetId(), ReasonCode.SUCCESS, EMPTY))
                    != SendQueue.OfferResult.ACCEPTED) {
                // FULL waits for actual budget release; CLOSED waits for the original cause.
                return;
            }
            acknowledgements.submitted(ticket);
            port.releaseDelivery(entry.token());
        }
        drainClosures();
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
                if (disconnectOperation != 0) {
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
        return switch (port.send(generation, ticket, packet)) {
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
        boolean established = state == State.CONNECTED;
        boolean active = state != State.DISCONNECTED;
        state = permanent ? State.CLOSED : State.DISCONNECTED;
        for (var timer : Timer.values()) {
            port.cancel(timer);
        }
        awaitingPing = false;
        pingTicket = 0;
        disconnectTicket = 0;
        if (active) {
            // Retire delivery conditions before removing subscriptions: termination must not
            // turn thousands of abandoned recipients into fresh readiness notifications.
            port.end(generation, established, new Disconnection(cause, requested));
        }
        for (long id : subscriptions.keySet()) {
            port.removeSubscription(id);
        }
        subscriptions.clear();
        var pendingFailure = (cause == null)
                ? new MqttConnectionLostException()
                : new MqttConnectionLostException(cause);
        for (long operation : publications.values()) {
            port.complete(operation, null, pendingFailure);
        }
        publications.clear();
        for (long operation : inflight.values()) {
            port.complete(operation, null, pendingFailure);
        }
        inflight.clear();
        acknowledgements.clear();
        for (var local : subscribing.values()) {
            port.complete(local.id, null, pendingFailure);
        }
        subscribing.clear();
        for (var pending : unsubscribing.values()) {
            port.complete(pending.operation, null, pendingFailure);
        }
        unsubscribing.clear();
        for (var pending : queuedClosures) {
            port.complete(pending.operation, null, pendingFailure);
        }
        queuedClosures.clear();
        closingInFlight = 0;
        packetIds = new PacketIds();
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

    private static final class LocalSubscription {

        // The subscribe operation's token; the shell and its delivery lanes use the same id.
        final long id;
        final TopicFilter filter;
        final TopicFilter matchingFilter;
        final Qos requestedQos;
        final boolean shared;
        boolean established;
        boolean removing;

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
