package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.concurrent.Timer;
import org.mqtt4j.hato.broker.internal.common.io.FrameReader;
import org.mqtt4j.hato.broker.internal.common.io.FrameWriter;
import org.mqtt4j.hato.broker.internal.common.io.SendQueue;
import org.mqtt4j.hato.broker.internal.common.log.Log;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.protocol.UnsupportedProtocolVersionException;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static java.util.Objects.requireNonNull;

// One network connection of the broker, with its own reader and writer.
final class Connection {

    private static final Log log = Log.of(Connection.class);

    private static final Set<MqttVersion> SUPPORTED_VERSIONS = EnumSet.of(
            MqttVersion.V3_1_1,
            MqttVersion.V5
    );

    static final int SEND_COUNT = 128;

    // Responses to a client's requests queue apart from deliveries and PUBRELs, up to the 128
    // the regular queue allowed them before sessions. While they are all waiting, the reader
    // stops reading requests, so a client that does not read its responses is held back by TCP.
    private static final int RESERVED_COUNT = 128;

    private static final long RESERVED_BYTES = 1024 * 1024;

    // The framing format bounds packet size; a configurable broker cap comes later.
    private static final int MAX_PACKET_SIZE = 268_435_460;

    private final Routing routing;

    private final BrokerSettings settings;

    private final Timer scheduler;

    private final Socket socket;

    private final FrameReader reader;

    private final FrameWriter writer;

    private final SendQueue<Outbound> outgoing;

    // The session reference belongs to routing; the identifier is assigned once under both
    // locks on acceptance and can then be read under either lock for timer/logging purposes.
    private @Nullable Session session;

    String clientId = "";

    // The Will of the accepted connection, until the connection's end publishes it or a
    // normal DISCONNECT discards it. Like the session reference, it belongs to routing.
    private Packet.@Nullable Will will;

    // The responses that a test holds back from this connection, in the order held. They
    // belong to routing too, and end with the connection.
    final ArrayDeque<Packet> held = new ArrayDeque<>();

    // Remaining state is guarded by this connection's monitor.

    // Responses that a test released, waiting for room in the reserved quota in order. Later
    // responses queue behind them, so that none overtakes a released one.
    private final ArrayDeque<Packet> released = new ArrayDeque<>();

    // The version of the connection's CONNECT. Until CONNECT selects one, a refusal of an
    // unsupported protocol level uses the MQTT 3.1.1 form, which every client can read.
    private MqttVersion version = MqttVersion.V3_1_1;

    private boolean ended;

    // Why the connection ended first; later failures are consequences of that end.
    private String endReason = "";

    private long ticket;

    // Set once the connection is to end with these bytes, such as a refusal CONNACK or a
    // DISCONNECT. The queue then admits nothing more; the writer writes what waits, except
    // deliveries, then these bytes, and closes.
    private byte @Nullable [] lastPacket;

    // A delivery or PUBREL found its queue full; the writer's next completion resumes it.
    private boolean awaitingSpace;

    private long lastReceived;

    private long receiveTimeout;

    private long timerGeneration;

    private Timer.@Nullable Cancellable cancellation;

    Connection(Socket socket, Routing routing) throws IOException {
        this.routing = routing;
        settings = routing.settings;
        scheduler = settings.scheduler();
        outgoing = new SendQueue<>(
                SEND_COUNT,
                settings.sendBytes(),
                RESERVED_COUNT,
                RESERVED_BYTES
        );
        lastReceived = scheduler.nanoTime();
        receiveTimeout = settings.connectTimeout();
        this.socket = socket;
        socket.setTcpNoDelay(true);
        reader = new FrameReader(socket.getInputStream(), MAX_PACKET_SIZE);
        writer = new FrameWriter(socket.getOutputStream());
    }

    synchronized void start() {
        armTimer(receiveTimeout - (scheduler.nanoTime() - lastReceived));
        String name = "hato-broker-" + routing.port + "-" + socket.getRemoteSocketAddress();
        Thread.ofVirtual().name(name + "-writer").start(this::writeLoop);
        Thread.ofVirtual().name(name + "-reader").start(this::readLoop);
    }

    private void readLoop() {
        String reason = "connection closed by the peer";
        try {
            Optional<byte[]> first = reader.read();
            if (first.isEmpty()) {
                return;
            }
            received();
            Packet.Connect request;
            try {
                request = PacketCodec.decodeConnect(first.get(), SUPPORTED_VERSIONS);
            } catch (UnsupportedProtocolVersionException unsupported) {
                refuse(
                        ReasonCode.UNSUPPORTED_PROTOCOL_VERSION,
                        "protocol level " + unsupported.protocolLevel()
                );
                return;
            }
            synchronized (this) {
                version = request.version();
            }
            settings.beforeProcess().accept(request);
            if (!connect(request)) {
                return;
            }
            PacketCodec codec = PacketCodec.of(request.version());
            while (true) {
                Optional<byte[]> bytes = reader.read();
                if (bytes.isEmpty()) {
                    return;
                }
                received();
                Packet packet = codec.decode(bytes.get());
                settings.beforeProcess().accept(packet);
                log.debug("Broker received {} from {}", packet.getClass().getSimpleName(),
                        socket.getRemoteSocketAddress());
                // Packets without a response, such as QoS 0 publications, never wait.
                if (answered(packet) && !awaitResponseRoom()) {
                    return;
                }
                synchronized (routing) {
                    Session owner = currentSession();
                    if (owner == null) {
                        return;
                    }
                    process(owner, packet);
                }
            }
        } catch (Throwable failure) {
            reason = failureReason("reader", failure);
            // A CONNECT that fails to decode leaves the version at MQTT 3.1.1, so DISCONNECT
            // only follows an accepted CONNECT.
            if (failure instanceof PacketDecodeException violation) {
                disconnect(violation.reasonCode(), reason);
            }
        } finally {
            // The writer closes a connection that ends with a last packet; closing here could
            // drop that packet.
            if (!endsWithLastPacket()) {
                close(reason);
            }
        }
    }

    private boolean connect(Packet.Connect request) {
        // MQTT 3.1.1 refuses an empty identifier for a persistent session [MQTT-3.1.3-8];
        // MQTT 5.0 assigns one below whatever the session.
        if (request.version() == MqttVersion.V3_1_1
                && !request.cleanStart()
                && request.clientId().isEmpty()) {
            refuse(ReasonCode.CLIENT_IDENTIFIER_NOT_VALID, "an empty persistent identifier");
            return false;
        }
        if (request.properties().find(PropertyId.AUTHENTICATION_METHOD).isPresent()) {
            refuse(
                    ReasonCode.BAD_AUTHENTICATION_METHOD,
                    "authentication for client '" + request.clientId() + "'"
            );
            return false;
        }
        // The broker publishes a Will as its connection ends, so it cannot keep one for a
        // delay that a new connection of the session would cancel [MQTT-3.1.3-9]. Only
        // whether the delay is positive matters.
        Packet.Will requestedWill = request.will();
        boolean delayed = requestedWill != null && requestedWill.properties()
                .find(PropertyId.WILL_DELAY_INTERVAL)
                .filter(delay -> delay instanceof Property.FourByteValue value
                        && value.value() > 0)
                .isPresent();
        if (delayed) {
            refuse(
                    ReasonCode.SERVER_UNAVAILABLE,
                    "a delayed Will for client '" + request.clientId() + "'"
            );
            return false;
        }
        boolean keep = switch (request.version()) {
            case V3_1_1 -> !request.cleanStart();
            // Only whether the interval is positive matters; sessions do not expire.
            case V5 -> request.properties().find(PropertyId.SESSION_EXPIRY_INTERVAL)
                    .filter(interval -> interval instanceof Property.FourByteValue value
                            && value.value() > 0)
                    .isPresent();
        };
        String id;
        synchronized (routing) {
            Connection previous;
            Session accepted;
            synchronized (this) {
                if (ended) {
                    return false;
                }
                // Inline expiry or a scheduling failure must happen before taking over a peer.
                keepAlive(request.keepAliveSeconds());
                if (ended) {
                    return false;
                }
                id = request.clientId();
                if (id.isEmpty()) {
                    do {
                        id = "hato" + UUID.randomUUID().toString().replace("-", "");
                    } while (routing.sessions.containsKey(id));
                }
                Session existing = routing.sessions.get(id);
                boolean present = !request.cleanStart()
                        && existing != null
                        && existing.kept
                        && existing.version == request.version();
                // Resuming or replacing a kept session reuses its place in the limit.
                if (keep && (existing == null || !existing.kept)
                        && routing.sessions.values().stream().filter(value -> value.kept).count()
                        >= settings.maxSessions()) {
                    refuse(
                            ReasonCode.SERVER_UNAVAILABLE,
                            "persistent session capacity for client '" + id + "'"
                    );
                    return false;
                }
                previous = existing == null ? null : existing.connection;
                if (present) {
                    accepted = requireNonNull(existing);
                    if (previous != null) {
                        accepted.detach(previous);
                    }
                    accepted.kept = keep;
                } else {
                    if (existing != null) {
                        existing.clear();
                    }
                    accepted = new Session(routing, id, request.version(), keep);
                    routing.sessions.put(id, accepted);
                }
                clientId = id;
                session = accepted;
                will = requestedWill;
                accepted.connection = this;
                Properties properties = switch (request.version()) {
                    case V3_1_1 -> Properties.EMPTY;
                    case V5 -> acceptanceV5(id, request.clientId().isEmpty());
                };
                enqueue(new Packet.ConnAck(present, ReasonCode.SUCCESS, properties));
            }
            // Do not hold the new peer's monitor while ending the old peer.
            if (previous != null) {
                previous.disconnect(
                        ReasonCode.SESSION_TAKEN_OVER,
                        "taken over by " + socket.getRemoteSocketAddress()
                );
            }
            // CONNACK, retransmissions, and queued deliveries stay ahead of later publications,
            // including the Will of the connection that this one took over.
            accepted.resume();
            if (previous != null) {
                previous.publishWill();
            }
        }
        log.info("Broker accepted client {} from {} on port {}",
                id, socket.getRemoteSocketAddress(), routing.port);
        return true;
    }

    // The properties of an MQTT 5.0 CONNACK that accepts the connection. The broker sets no
    // Subscription Identifiers on its deliveries, so it tells clients to send none; and only
    // MQTT 5.0 can tell a client the identifier assigned to it.
    private static Properties acceptanceV5(String id, boolean assigned) {
        var entries = new ArrayList<Property>();
        entries.add(new Property.ByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE, 0));
        if (assigned) {
            entries.add(new Property.StringValue(PropertyId.ASSIGNED_CLIENT_IDENTIFIER, id));
        }
        return new Properties(entries);
    }

    private @Nullable Session currentSession() {
        Session owner = session;
        return (owner != null && owner.connection == this && !isEnded())
                ? owner
                : null;
    }

    // Publishes the Will of this connection, which ended without a DISCONNECT that discards
    // it, as its client would have published the message. Called under routing; taking the
    // Will means that no later end publishes it again. A closing broker publishes none,
    // because every session goes with it.
    private void publishWill() {
        Packet.Will last = will;
        Session owner = session;
        will = null;
        if (last == null || owner == null || routing.closed) {
            return;
        }
        // The Will properties travel as the publication's, without the delay, which only
        // the Will has; User Properties keep their order [MQTT-3.1.3-10]. The packet
        // identifier is a placeholder, because every delivery takes its own.
        var properties = new Properties(last.properties().entries().stream()
                .filter(property -> property.id() != PropertyId.WILL_DELAY_INTERVAL)
                .toList());
        var publication = new Packet.Publish(
                last.topic(),
                last.payload(),
                last.qos(),
                last.retain(),
                false,
                (last.qos() == Qos.AT_MOST_ONCE) ? 0 : 1,
                properties
        );
        routing.route(publication, owner);
    }

    private void refuse(ReasonCode reason, String request) {
        log.info("Broker refused {} from {} on port {}: {}",
                request, socket.getRemoteSocketAddress(), routing.port, reason);
        synchronized (this) {
            closeAfter(new Packet.ConnAck(false, reason, Properties.EMPTY), "refusal sent");
        }
    }

    private void process(Session owner, Packet packet) {
        switch (packet) {
            case Packet.Subscribe request -> {
                // CONNACK announced that Subscription Identifiers are not supported, so one
                // is a Protocol Error that subscribes to nothing (MQTT 5.0 section
                // 3.2.2.3.12). MQTT 3.1.1 has no properties.
                if (request.properties().find(PropertyId.SUBSCRIPTION_IDENTIFIER).isPresent()) {
                    disconnect(
                            ReasonCode.SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED,
                            "Subscription Identifier although none are supported"
                    );
                    return;
                }
                var grants = new ArrayList<ReasonCode>();
                // The subscriptions that receive retained messages after SUBACK.
                var retaining = new ArrayList<Packet.Subscription>();
                for (Packet.Subscription requested : request.subscriptions()) {
                    // A test rule can refuse the filter, which then subscribes to nothing
                    // and leaves an earlier subscription to it as it was, or grant it less
                    // than the requested QoS.
                    Qos maximum = routing.allowedQos(requested);
                    if (maximum == null) {
                        grants.add(ReasonCode.UNSPECIFIED_ERROR);
                        continue;
                    }
                    boolean lowered = maximum.level() < requested.qos().level();
                    Packet.Subscription subscription = lowered
                            ? new Packet.Subscription(
                                    requested.filter(),
                                    maximum,
                                    requested.noLocal(),
                                    requested.retainAsPublished(),
                                    requested.retainHandling()
                            )
                            : requested;
                    Packet.Subscription replaced = owner.subscriptions.put(
                            subscription.filter(),
                            subscription
                    );
                    ReasonCode granted = switch (subscription.qos()) {
                        case AT_MOST_ONCE -> ReasonCode.SUCCESS;
                        case AT_LEAST_ONCE -> ReasonCode.GRANTED_QOS_1;
                        case EXACTLY_ONCE -> ReasonCode.GRANTED_QOS_2;
                    };
                    grants.add(granted);
                    // Retain Handling 0 sends retained messages for every SUBSCRIBE, 1 only
                    // for a new subscription, and 2, the only other value, never
                    // [MQTT-3.3.1-9] [MQTT-3.3.1-10] [MQTT-3.3.1-11]. MQTT 3.1.1 uses 0.
                    boolean sendsRetained = switch (subscription.retainHandling()) {
                        case 0 -> true;
                        case 1 -> replaced == null;
                        default -> false;
                    };
                    if (sendsRetained) {
                        retaining.add(subscription);
                    }
                }
                send(new Packet.SubAck(request.packetId(), grants, Properties.EMPTY));
                for (Packet.Subscription subscription : retaining) {
                    owner.deliverRetained(subscription);
                }
            }
            case Packet.Unsubscribe request -> {
                var results = new ArrayList<ReasonCode>();
                for (TopicFilter filter : request.filters()) {
                    boolean existed = owner.subscriptions.remove(filter) != null;
                    results.add(
                            existed ? ReasonCode.SUCCESS : ReasonCode.NO_SUBSCRIPTION_EXISTED
                    );
                }
                // MQTT 3.1.1 UNSUBACK carries no results.
                List<ReasonCode> reasonCodes = switch (owner.version) {
                    case V3_1_1 -> List.of();
                    case V5 -> results;
                };
                send(new Packet.UnsubAck(request.packetId(), reasonCodes, Properties.EMPTY));
            }
            case Packet.Publish publish -> {
                // The broker announces no Topic Alias Maximum, so no alias is valid, and only
                // a server sets Subscription Identifiers. MQTT 3.1.1 has no properties.
                if (publish.properties().find(PropertyId.TOPIC_ALIAS).isPresent()) {
                    throw new PacketDecodeException(
                            ReasonCode.TOPIC_ALIAS_INVALID,
                            "Topic Alias without an announced maximum"
                    );
                }
                if (publish.properties().find(PropertyId.SUBSCRIPTION_IDENTIFIER).isPresent()) {
                    throw new PacketDecodeException(
                            ReasonCode.PROTOCOL_ERROR,
                            "Subscription Identifier from a client"
                    );
                }
                if (publish.qos() == Qos.EXACTLY_ONCE) {
                    if (owner.received.get(publish.packetId())) {
                        send(new Packet.PubRec(
                                publish.packetId(),
                                ReasonCode.SUCCESS,
                                Properties.EMPTY
                        ));
                        return;
                    }
                    owner.received.set(publish.packetId());
                } else if (publish.qos() == Qos.AT_LEAST_ONCE) {
                    // Reusing the identifier abandons a QoS 2 exchange still waiting for
                    // PUBREL, as a client that lost its state does. Its message was already
                    // forwarded on PUBLISH, so nothing is lost.
                    owner.received.clear(publish.packetId());
                }
                routing.route(publish, owner);
                // Acknowledge only after every delivery was enqueued or its target closed.
                if (publish.qos() == Qos.AT_LEAST_ONCE) {
                    send(new Packet.PubAck(
                            publish.packetId(),
                            ReasonCode.SUCCESS,
                            Properties.EMPTY
                    ));
                } else if (publish.qos() == Qos.EXACTLY_ONCE) {
                    send(new Packet.PubRec(
                            publish.packetId(),
                            ReasonCode.SUCCESS,
                            Properties.EMPTY
                    ));
                }
            }
            case Packet.PubAck ack -> owner.acknowledge(ack.packetId(), false);
            case Packet.PubRec ack -> owner.release(ack.packetId(), ack.reasonCode());
            case Packet.PubRel ack -> {
                owner.received.clear(ack.packetId());
                send(new Packet.PubComp(ack.packetId(), ReasonCode.SUCCESS, Properties.EMPTY));
            }
            case Packet.PubComp ack -> owner.acknowledge(ack.packetId(), true);
            case Packet.PingReq _ -> send(new Packet.PingResp());
            case Packet.Disconnect disconnect -> {
                // Only a normal disconnection discards the Will [MQTT-3.14.4-3]; MQTT 5.0
                // publishes it for any other reason code, Disconnect with Will Message
                // among them (MQTT 5.0 section 3.1.2.5).
                boolean normal = disconnect.reasonCode() == ReasonCode.SUCCESS;
                if (normal) {
                    will = null;
                }
                close(
                        normal
                                ? "client sent DISCONNECT"
                                : "client sent DISCONNECT with " + disconnect.reasonCode()
                );
            }
            case Packet.AliasPublish _ -> throw new PacketDecodeException(
                    ReasonCode.TOPIC_ALIAS_INVALID,
                    "Topic Alias without an announced maximum"
            );
            // Name only the type; the INFO end log must not carry fields such as AUTH data.
            case Packet.Connect _,
                 Packet.ConnAck _,
                 Packet.SubAck _,
                 Packet.UnsubAck _,
                 Packet.PingResp _,
                 Packet.Auth _ -> throw new PacketDecodeException(
                         ReasonCode.PROTOCOL_ERROR,
                         "Unexpected " + packet.getClass().getSimpleName() + " packet"
                 );
        }
    }

    synchronized SendQueue.OfferResult enqueue(Outgoing message) {
        if (lastPacket != null) {
            return SendQueue.OfferResult.CLOSED;
        }
        Delivery delivery = message instanceof Delivery value ? value : null;
        // PUBLISH and PUBREL share the regular queue, whose window always fits a resumed
        // session, and leave the reserved queue to responses to the client's requests.
        // Only a PUBLISH is checked before encoding, so a full queue never copies its payload;
        // the offer decides with the encoded size.
        SendQueue.OfferResult result = delivery == null
                ? SendQueue.OfferResult.ACCEPTED
                : outgoing.checkCapacity(delivery.byteCount, false);
        if (result == SendQueue.OfferResult.ACCEPTED) {
            byte[] bytes = PacketCodec.of(version).encode(message.packet());
            result = outgoing.offer(
                    ++ticket,
                    new Outbound(bytes, delivery),
                    bytes.length,
                    false
            );
        }
        if (result == SendQueue.OfferResult.ACCEPTED) {
            notifyAll();
        } else {
            awaitingSpace = true;
        }
        return result;
    }

    /// Whether the broker answers `packet` with one response in the reserved quota.
    private static boolean answered(Packet packet) {
        return switch (packet) {
            case Packet.Publish publish -> publish.qos() != Qos.AT_MOST_ONCE;
            case Packet.Subscribe _,
                 Packet.Unsubscribe _,
                 Packet.PubRel _,
                 Packet.PingReq _ -> true;
            case Packet.Connect _,
                 Packet.ConnAck _,
                 Packet.AliasPublish _,
                 Packet.PubAck _,
                 Packet.PubRec _,
                 Packet.PubComp _,
                 Packet.SubAck _,
                 Packet.UnsubAck _,
                 Packet.PingResp _,
                 Packet.Disconnect _,
                 Packet.Auth _ -> false;
        };
    }

    /// Holds the reader, outside the routing lock, until the reserved quota has room for one
    /// more response, so that answering the next request never finds it full. Only this
    /// reader queues responses, so the room stays until the response takes it. A deliberate
    /// simplification: the wait counts responses, not their bytes. Only a backlog of SUBACKs
    /// for SUBSCRIBE packets with many thousands of filters reaches the byte limit, which
    /// still closes the connection; waiting for the size of each response would remove that
    /// limit. Returns false once the connection has ended. A connection that is to end with
    /// a last packet drops responses, so its reader does not wait.
    private synchronized boolean awaitResponseRoom() throws InterruptedException {
        while (lastPacket == null && responseRoomTaken()) {
            wait();
        }
        return !ended;
    }

    // Whether a response now would find no room: the reserved quota is full, or responses
    // that a test released wait for it first. Called under the connection monitor.
    private boolean responseRoomTaken() {
        return !released.isEmpty()
                || outgoing.checkCapacity(0, true) == SendQueue.OfferResult.FULL;
    }

    // Called under routing, which guards the test controls.
    private void send(Packet packet) {
        // A response that a test holds back waits outside the send queue, and so do the later
        // ones of its kind, until the test releases them.
        if (routing.holdsBack(packet.getClass(), clientId)) {
            if (held.size() >= settings.maxHeld()) {
                close("held responses over their limit");
            } else {
                held.addLast(packet);
            }
            return;
        }
        boolean accepted;
        synchronized (this) {
            // A connection that ends with a last packet drops later responses.
            if (lastPacket != null) {
                return;
            }
            if (!released.isEmpty()) {
                released.addLast(packet);
                return;
            }
            accepted = enqueue(packet);
        }
        if (!accepted) {
            close("send queue full");
        }
    }

    // Moves the held responses of `type` behind any released before, in the order held, and
    // sends what fits. Called under routing.
    void release(Class<? extends Packet> type) {
        synchronized (this) {
            for (Iterator<Packet> responses = held.iterator(); responses.hasNext(); ) {
                Packet response = responses.next();
                if (type.isInstance(response)) {
                    responses.remove();
                    released.addLast(response);
                }
            }
            sendReleased();
        }
    }

    // Sends released responses while the reserved quota has room; the writer calls this
    // again each time it frees room. A connection that is ending drops them. Called under
    // the connection monitor.
    private void sendReleased() {
        Packet next;
        while ((next = released.peekFirst()) != null) {
            if (ended || lastPacket != null) {
                released.clear();
                return;
            }
            byte[] bytes = PacketCodec.of(version).encode(next);
            if (outgoing.checkCapacity(bytes.length, true) != SendQueue.OfferResult.ACCEPTED) {
                return;
            }
            SendQueue.OfferResult result = outgoing.offer(
                    ++ticket,
                    new Outbound(bytes, null),
                    bytes.length,
                    true
            );
            if (result != SendQueue.OfferResult.ACCEPTED) {
                return;
            }
            released.removeFirst();
            notifyAll();
        }
    }

    // Called under the connection monitor; no socket I/O occurs on a routing thread.
    private boolean enqueue(Packet packet) {
        if (ended || lastPacket != null) {
            return false;
        }
        byte[] bytes = PacketCodec.of(version).encode(packet);
        boolean enqueueFailed = outgoing.offer(
                ++ticket,
                new Outbound(bytes, null),
                bytes.length,
                true
        ) != SendQueue.OfferResult.ACCEPTED;
        if (enqueueFailed) {
            return false;
        }
        notifyAll();
        return true;
    }

    private void writeLoop() {
        // Unless a write fails, the writer ends an open connection only after its last
        // packet, and the connection already holds the reason for that end.
        String reason = "last packet written";
        try {
            while (true) {
                Optional<SendQueue.Send<Outbound>> next;
                byte[] last;
                synchronized (this) {
                    next = outgoing.poll();
                    while (!ended && next.isEmpty() && lastPacket == null) {
                        wait();
                        next = outgoing.poll();
                    }
                    if (ended) {
                        return;
                    }
                    last = lastPacket;
                }
                if (next.isEmpty()) {
                    writer.write(requireNonNull(last));
                    return;
                }
                SendQueue.Send<Outbound> send = next.get();
                Delivery delivery = send.value().delivery();
                boolean skip = false;
                if (delivery != null) {
                    settings.beforeDeliveryWrite().run();
                    synchronized (routing) {
                        if (currentSession() != null) {
                            delivery.sent = true;
                        } else if (endsWithLastPacket()) {
                            // The delivery stays unsent for the session's next connection;
                            // the last packet still follows.
                            skip = true;
                        } else {
                            return;
                        }
                    }
                }
                if (!skip) {
                    writer.write(send.value().bytes());
                }
                boolean resume;
                synchronized (this) {
                    outgoing.completed(send.ticket());
                    sendReleased();
                    // Refusal and completion share this monitor, so no wakeup is lost.
                    resume = awaitingSpace;
                    awaitingSpace = false;
                    // The reader may wait for room for its next response.
                    notifyAll();
                }
                if (resume) {
                    synchronized (routing) {
                        Session owner = currentSession();
                        if (owner != null) {
                            owner.drain();
                        }
                    }
                }
            }
        } catch (Throwable failure) {
            reason = failureReason("writer", failure);
        } finally {
            close(reason);
            outgoing.ended();
            // Each started connection has one writer, so every end is logged exactly once.
            logEnd();
        }
    }

    // Peer I/O and protocol errors end only this connection. Anything else is a broker
    // defect, such as an OutOfMemoryError, and keeps its stack trace at WARNING.
    private String failureReason(String loop, Throwable failure) {
        if (failure instanceof IOException || failure instanceof PacketDecodeException) {
            if (!isEnded()) {
                log.debug("Broker {} for {} failed", loop, socket.getRemoteSocketAddress(),
                        failure);
            }
        } else {
            log.warn("Broker {} for {} failed", loop, socket.getRemoteSocketAddress(), failure);
        }
        return failure.getClass().getSimpleName() + ": " + failure.getMessage();
    }

    private void logEnd() {
        String id;
        String reason;
        synchronized (this) {
            id = clientId;
            reason = endReason;
        }
        if (id.isEmpty()) {
            log.debug("Broker closed {} on port {} before accepting a client: {}",
                    socket.getRemoteSocketAddress(), routing.port, reason);
        } else {
            log.info("Broker disconnected client {} from {} on port {}: {}",
                    id, socket.getRemoteSocketAddress(), routing.port, reason);
        }
    }

    // Whether routing must leave this connection: it has ended or ends with its last packet.
    synchronized boolean isEnded() {
        return ended || lastPacket != null;
    }

    private synchronized boolean endsWithLastPacket() {
        return lastPacket != null;
    }

    // Ends the connection, first telling an MQTT 5.0 client why with DISCONNECT. MQTT 3.1.1
    // has no DISCONNECT from a server.
    synchronized void disconnect(ReasonCode reasonCode, String reason) {
        switch (version) {
            case V3_1_1 -> terminate(reason);
            case V5 -> closeAfter(new Packet.Disconnect(reasonCode, Properties.EMPTY), reason);
        }
    }

    // Ends the connection with `packet` after the sends that wait. Called under the
    // connection monitor.
    private void closeAfter(Packet packet, String reason) {
        if (ended || lastPacket != null) {
            return;
        }
        byte[] bytes = PacketCodec.of(version).encode(packet);
        // The connect timeout bounds the wait for a writer that a slow peer holds up.
        cancelTimer();
        try {
            armTimer(settings.connectTimeout());
        } catch (RuntimeException failure) {
            log.warn("Broker timer for {} failed", socket.getRemoteSocketAddress(), failure);
            terminate(reason);
            return;
        }
        lastPacket = bytes;
        endReason = reason;
        notifyAll();
    }

    private synchronized void received() {
        lastReceived = scheduler.nanoTime();
    }

    private synchronized void keepAlive(int seconds) {
        cancelTimer();
        receiveTimeout = seconds * 1_500_000_000L;
        if (receiveTimeout != 0) {
            armTimer(receiveTimeout - (scheduler.nanoTime() - lastReceived));
        }
    }

    private void armTimer(long delay) {
        long generation = ++timerGeneration;
        Timer.Cancellable scheduled = scheduler.schedule(
                Duration.ofNanos(Math.max(0, delay)), () -> checkDeadline(generation)
        );
        // An already-due callback may close or rearm the connection before schedule returns.
        if (ended || generation != timerGeneration) {
            cancel(scheduled);
        } else {
            cancellation = scheduled;
        }
    }

    private void cancelTimer() {
        // Cancellation cannot stop a callback already waiting for this monitor.
        timerGeneration++;
        Timer.Cancellable previous = cancellation;
        cancellation = null;
        if (previous != null) {
            cancel(previous);
        }
    }

    private void cancel(Timer.Cancellable task) {
        try {
            task.cancel();
        } catch (Throwable failure) {
            // A failing injected timer must not prevent socket/queue cleanup.
            log.warn(
                    "Cancelling broker timer for {} failed",
                    socket.getRemoteSocketAddress(),
                    failure
            );
        }
    }

    private void checkDeadline(long generation) {
        try {
            synchronized (this) {
                if (ended || generation != timerGeneration) {
                    return;
                }
                if (lastPacket != null) {
                    // The writer did not get the last packet out within the connect timeout.
                    terminate(endReason);
                    return;
                }
                long remaining = receiveTimeout - (scheduler.nanoTime() - lastReceived);
                if (remaining > 0) {
                    armTimer(remaining);
                    return;
                }
                // Mark the deadline under the same lock as receipt; a late frame cannot
                // revive this connection while removal waits for an ongoing route.
                if (clientId.isEmpty()) {
                    terminate("CONNECT timeout");
                } else {
                    disconnect(ReasonCode.KEEP_ALIVE_TIMEOUT, "keep-alive timeout");
                }
            }
        } catch (Throwable failure) {
            log.warn("Broker timer for {} failed", socket.getRemoteSocketAddress(), failure);
            synchronized (this) {
                terminate("timer failure");
            }
        }
        // Ending the connection releases reader/writer, which remove the session. Do not
        // make the shared timer wait for the routing lock before checking another peer.
    }

    // Stops I/O without the routing lock; the exiting reader and writer remove the connection.
    private void terminate(String reason) {
        if (ended) {
            return;
        }
        ended = true;
        // A connection that ends with a last packet already holds its reason.
        if (endReason.isEmpty()) {
            endReason = reason;
        }
        cancelTimer();
        outgoing.close();
        notifyAll();
        try {
            socket.close();
        } catch (IOException failure) {
            log.warn("Closing broker peer {} failed", socket.getRemoteSocketAddress(), failure);
        }
    }

    void close(String reason) {
        synchronized (this) {
            terminate(reason);
        }
        synchronized (routing) {
            routing.connections.remove(this);
            Session owner = session;
            if (owner != null) {
                owner.detach(this);
            }
            // The Will follows the end of the network connection ([MQTT-3.1.2-8] in MQTT
            // 3.1.1), so a persistent session that subscribes to it receives it as an
            // absent client.
            publishWill();
        }
    }

    private record Outbound(byte[] bytes, @Nullable Delivery delivery) {
    }
}
