package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.concurrent.Timer;
import org.mqtt4j.hato.broker.internal.common.concurrent.TimerService;
import org.mqtt4j.hato.broker.internal.common.io.FrameReader;
import org.mqtt4j.hato.broker.internal.common.io.FrameWriter;
import org.mqtt4j.hato.broker.internal.common.io.SendQueue;
import org.mqtt4j.hato.broker.internal.common.log.Log;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketIds;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.protocol.UnsupportedProtocolVersionException;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static java.util.Objects.requireNonNull;

/// One listener with independent reader/writer pairs and bounded, in-memory sessions.
public final class BrokerServer implements AutoCloseable {

    private static final Log log = Log.of(BrokerServer.class);

    private static final Set<MqttVersion> SUPPORTED_VERSIONS = EnumSet.of(
            MqttVersion.V3_1_1,
            MqttVersion.V5
    );

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);

    private static final int SEND_COUNT = 128;

    private static final long SEND_BYTES = 16L * 1024 * 1024;

    private static final int MAX_INFLIGHT = 128;

    // Responses to a client's requests queue apart from deliveries and PUBRELs, up to the 128
    // the regular queue allowed them before sessions. Requests beyond that close the connection.
    private static final int RESERVED_COUNT = 128;

    private static final long RESERVED_BYTES = 1024 * 1024;

    private static final int MAX_QUEUED = 128;

    private static final int MAX_SESSIONS = 1024;

    // The framing format bounds packet size; a configurable broker cap comes later.
    private static final int MAX_PACKET_SIZE = 268_435_460;

    private final ServerSocket listener;

    private final long connectTimeout;

    private final long sendBytes;

    private final int maxInflight;

    private final long maxInflightBytes;

    private final int maxQueued;

    private final long maxQueuedBytes;

    private final int maxSessions;

    private final Timer scheduler;

    private final Consumer<Packet> beforeProcess;

    private final Runnable beforeDeliveryWrite;

    // This global lock and linear session/subscription scan bound routing throughput. Replace
    // them with an index and finer ownership if broker measurements justify productization.
    // Lock order is routing -> Connection. Readers/writers release their monitor before routing;
    // timers stop I/O without routing, and reader/writer cleanup detaches the connection later.
    private final Object routing = new Object();

    private final Set<Connection> connections = new HashSet<>();

    private final Map<String, Session> sessions = new HashMap<>();

    private boolean closed;

    private BrokerServer(ServerSocket listener, Builder settings) {
        this.listener = listener;
        this.connectTimeout = settings.connectTimeout.toNanos();
        this.sendBytes = settings.sendBytes;
        this.maxInflight = settings.maxInflight;
        this.maxInflightBytes = settings.maxInflightBytes;
        this.maxQueued = settings.maxQueued;
        this.maxQueuedBytes = settings.maxQueuedBytes;
        this.maxSessions = settings.maxSessions;
        this.scheduler = settings.scheduler;
        this.beforeProcess = settings.beforeProcess;
        this.beforeDeliveryWrite = settings.beforeDeliveryWrite;
    }

    public static BrokerServer start(InetSocketAddress address) throws IOException {
        return builder().start(address);
    }

    static Builder builder() {
        return new Builder();
    }

    /// Internal limits and test hooks, copied when the broker starts. Not thread safe.
    /// Values are checked together at [#start(InetSocketAddress)], so they can be set in any order.
    static final class Builder {

        private Duration connectTimeout = CONNECT_TIMEOUT;

        private long sendBytes = SEND_BYTES;

        private int maxInflight = MAX_INFLIGHT;

        private long maxInflightBytes = SEND_BYTES;

        private int maxQueued = MAX_QUEUED;

        private long maxQueuedBytes = SEND_BYTES;

        private int maxSessions = MAX_SESSIONS;

        private Timer scheduler = TimerService.instance;

        private Consumer<Packet> beforeProcess = _ -> { };

        private Runnable beforeDeliveryWrite = () -> { };

        private Builder() {
        }

        Builder connectTimeout(Duration value) {
            connectTimeout = requireNonNull(value, "connectTimeout must not be null");
            return this;
        }

        Builder sendBytes(long value) {
            sendBytes = value;
            return this;
        }

        Builder maxInflight(int value) {
            maxInflight = value;
            return this;
        }

        Builder maxInflightBytes(long value) {
            maxInflightBytes = value;
            return this;
        }

        Builder maxQueued(int value) {
            maxQueued = value;
            return this;
        }

        Builder maxQueuedBytes(long value) {
            maxQueuedBytes = value;
            return this;
        }

        Builder maxSessions(int value) {
            maxSessions = value;
            return this;
        }

        Builder scheduler(Timer value) {
            scheduler = requireNonNull(value, "scheduler must not be null");
            return this;
        }

        Builder beforeProcess(Consumer<Packet> value) {
            beforeProcess = requireNonNull(value, "beforeProcess must not be null");
            return this;
        }

        Builder beforeDeliveryWrite(Runnable value) {
            beforeDeliveryWrite = requireNonNull(value, "beforeDeliveryWrite must not be null");
            return this;
        }

        BrokerServer start(InetSocketAddress address) throws IOException {
            requireNonNull(address, "address must not be null");
            if (connectTimeout.isNegative()
                || connectTimeout.isZero()
                || sendBytes <= 0
                || maxInflight <= 0
                || maxInflight > SEND_COUNT
                || maxInflightBytes <= 0
                || maxInflightBytes > sendBytes
                || maxQueued <= 0
                || maxQueuedBytes <= 0
                || maxSessions <= 0
            ) {
                throw new IllegalArgumentException(
                        "Broker limits must be positive and window fit queue"
                );
            }
            var listener = new ServerSocket();
            try {
                listener.bind(address);
                var server = new BrokerServer(listener, this);
                Thread.ofVirtual()
                        .name("hato-broker-" + server.port() + "-accept")
                        .start(server::accept);
                return server;
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    listener.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }
    }

    public int port() {
        return listener.getLocalPort();
    }

    private void accept() {
        try {
            while (true) {
                Socket socket = listener.accept();
                Connection connection = null;
                try {
                    synchronized (routing) {
                        if (closed) {
                            socket.close();
                            return;
                        }
                        connection = new Connection(socket);
                        connections.add(connection);
                        connection.start();
                    }
                } catch (Throwable failure) {
                    if (connection != null) {
                        connection.close("start failed");
                    } else {
                        try {
                            socket.close();
                        } catch (IOException closeFailure) {
                            failure.addSuppressed(closeFailure);
                        }
                    }
                    if (failure instanceof Error error) {
                        throw error;
                    }
                    // A peer's I/O or injected timer failure does not end unrelated sessions.
                    // Peer I/O, such as a reset before acceptance, stays at DEBUG as in the loops.
                    if (failure instanceof IOException) {
                        log.debug("Starting broker peer {} failed",
                                socket.getRemoteSocketAddress(), failure);
                    } else {
                        log.warn("Starting broker peer {} failed",
                                socket.getRemoteSocketAddress(), failure);
                    }
                }
            }
        } catch (Throwable failure) {
            synchronized (routing) {
                if (!closed) {
                    log.error("Broker listener on port {} failed", port(), failure);
                }
            }
            close();
        }
    }

    @Override
    public void close() {
        synchronized (routing) {
            if (closed) {
                return;
            }
            closed = true;
            try {
                listener.close();
            } catch (IOException failure) {
                log.warn("Closing broker listener on port {} failed", port(), failure);
            }
            for (Connection connection : List.copyOf(connections)) {
                connection.close("broker closed");
            }
            sessions.values().forEach(Session::clear);
            sessions.clear();
        }
    }

    // Every session field, including the active connection, belongs to routing.
    private final class Session {

        private final String clientId;

        // A session continues only with the protocol version that created it, so its deliveries
        // are sized and encoded for one version.
        private final MqttVersion version;

        // Whether the session outlives its connection: Clean Session 0 in MQTT 3.1.1, or a
        // positive Session Expiry Interval in MQTT 5.0. A connection that resumes the session
        // sets it again from its own CONNECT.
        private boolean kept;

        // Each filter's subscription with the options that the client requested. Retain Handling
        // has no effect, because retained publications are not stored.
        private final Map<TopicFilter, Packet.Subscription> subscriptions = new HashMap<>();

        private final PacketIds packetIds = new PacketIds();

        private final LinkedHashMap<Integer, Outgoing> inflight = new LinkedHashMap<>();

        // Replays and PUBRELs waiting for space in the current connection's send queue, at most
        // one per inflight identifier. It lives for one connection: resume() rebuilds it from
        // inflight, and detaching the connection drops it.
        private final LinkedHashMap<Integer, Outgoing> deferred = new LinkedHashMap<>();

        // Only identifiers are retained for inbound QoS 2. The set grows by doubling on first use,
        // so the 16-bit identifier space bounds it to 16 KiB.
        private final BitSet received = new BitSet();

        private final ArrayDeque<Delivery> pending = new ArrayDeque<>();

        private @Nullable Connection connection;

        private long inflightBytes;

        private long pendingBytes;

        private Session(String clientId, MqttVersion version, boolean kept) {
            this.clientId = clientId;
            this.version = version;
            this.kept = kept;
        }

        // Delivers `publication` from `publisher` for the subscriptions that match it.
        // Overlapping subscriptions share one copy at their highest QoS [MQTT-3.3.4-2]. A retained
        // publication takes a second copy when only some of them keep its retain flag, so that
        // each subscription gets the flag it asked for [MQTT-3.3.1-12] [MQTT-3.3.1-13].
        private void deliverMatching(Packet.Publish publication, Session publisher) {
            // The highest QoS granted by the matching subscriptions with Retain As Published, and
            // by the other matching subscriptions; -1 while none matches.
            int asPublished = -1;
            int cleared = -1;
            for (Packet.Subscription subscription : subscriptions.values()) {
                // No Local withholds a client's own publications [MQTT-3.8.3-3].
                boolean withheld = subscription.noLocal() && publisher == this;
                if (withheld || !subscription.filter().matches(publication.topic())) {
                    continue;
                }
                int level = subscription.qos().level();
                if (subscription.retainAsPublished()) {
                    asPublished = Math.max(asPublished, level);
                } else {
                    cleared = Math.max(cleared, level);
                }
            }
            int published = publication.qos().level();
            if (publication.retain() && asPublished >= 0 && cleared >= 0) {
                deliver(publication, Qos.of(Math.min(asPublished, published)), true);
                deliver(publication, Qos.of(Math.min(cleared, published)), false);
            } else if (asPublished >= 0 || cleared >= 0) {
                Qos qos = Qos.of(Math.min(Math.max(asPublished, cleared), published));
                deliver(publication, qos, publication.retain() && asPublished >= 0);
            }
        }

        private void deliver(Packet.Publish publication, Qos qos, boolean retain) {
            Connection peer = connection;
            if (peer != null && peer.isEnded()) {
                detach(peer);
            }
            boolean online = connection != null;
            if (!online && (!kept || qos == Qos.AT_MOST_ONCE)) {
                return;
            }
            var delivery = new Delivery(publication, qos, retain, version);
            // A frame that can never fit must not block the head indefinitely.
            if (delivery.byteCount > sendBytes
                    || (qos != Qos.AT_MOST_ONCE && delivery.byteCount > maxInflightBytes)) {
                discard("delivery exceeds send window");
                return;
            }
            pending.addLast(delivery);
            pendingBytes += delivery.byteCount;
            drain();
            if (pending.size() > maxQueued || pendingBytes > maxQueuedBytes) {
                discard("delivery queue full");
            }
        }

        private void resume() {
            deferred.clear();
            deferred.putAll(inflight);
            drain();
        }

        private void drain() {
            Connection peer = connection;
            if (peer == null || peer.isEnded()) {
                return;
            }
            // Replays and PUBRELs precede new deliveries in the regular queue. When it is full,
            // draining pauses here and the writer resumes it after freeing space.
            while (!deferred.isEmpty()) {
                Outgoing outgoing = deferred.firstEntry().getValue();
                if (peer.enqueue(outgoing) != SendQueue.OfferResult.ACCEPTED) {
                    return;
                }
                deferred.pollFirstEntry();
            }
            Delivery delivery;
            while ((delivery = pending.peekFirst()) != null) {
                boolean acknowledged = delivery.qos != Qos.AT_MOST_ONCE;
                if (acknowledged && (inflight.size() == maxInflight
                        || delivery.byteCount > maxInflightBytes - inflightBytes)) {
                    return;
                }
                if (acknowledged) {
                    delivery.packetId = packetIds.allocate().orElseThrow();
                }
                SendQueue.OfferResult result = peer.enqueue(delivery);
                if (result != SendQueue.OfferResult.ACCEPTED) {
                    if (acknowledged) {
                        packetIds.release(delivery.packetId);
                        delivery.packetId = 0;
                    }
                    // An ACK can free the logical window before the writer frees physical space.
                    return;
                }
                pending.removeFirst();
                pendingBytes -= delivery.byteCount;
                if (acknowledged) {
                    inflight.put(delivery.packetId, delivery);
                    inflightBytes += delivery.byteCount;
                }
            }
        }

        private void acknowledge(int id, boolean complete) {
            Outgoing outgoing = inflight.get(id);
            if (outgoing == null) {
                return;
            }
            boolean expected = complete
                    ? outgoing instanceof Release
                    : outgoing instanceof Delivery delivery && delivery.qos == Qos.AT_LEAST_ONCE;
            if (!expected) {
                throw new PacketDecodeException(
                        ReasonCode.PROTOCOL_ERROR,
                        "Acknowledgement does not match the outgoing QoS stage"
                );
            }
            end(id, outgoing);
            drain();
        }

        private void release(int id, ReasonCode reason) {
            Outgoing outgoing = inflight.get(id);
            if (outgoing == null) {
                return;
            }
            // Drop the body on PUBREC, but retain the identifier and count until PUBCOMP.
            // Moving to the end preserves PUBREC order for both initial and replayed PUBREL.
            switch (outgoing) {
                case Delivery delivery -> {
                    if (delivery.qos != Qos.EXACTLY_ONCE) {
                        throw new PacketDecodeException(
                                ReasonCode.PROTOCOL_ERROR,
                                "PUBREC does not match the outgoing QoS stage"
                        );
                    }
                    if (reason.value() >= 0x80) {
                        // An MQTT 5.0 receiver that refuses the message ends the exchange here;
                        // only a successful PUBREC leads to PUBREL.
                        end(id, delivery);
                    } else {
                        inflightBytes -= delivery.byteCount;
                        var release = new Release(id);
                        inflight.putLast(id, release);
                        deferred.putLast(id, release);
                    }
                }
                // Duplicate PUBREC shares an unsent response without moving it behind later ids.
                case Release release -> deferred.putIfAbsent(id, release);
            }
            drain();
        }

        // Forgets an outgoing exchange and returns its identifier and window share.
        private void end(int id, Outgoing outgoing) {
            inflight.remove(id);
            deferred.remove(id);
            packetIds.release(id);
            if (outgoing instanceof Delivery delivery) {
                inflightBytes -= delivery.byteCount;
            }
        }

        private void detach(Connection peer) {
            if (connection != peer) {
                return;
            }
            connection = null;
            deferred.clear();
            if (!kept) {
                sessions.remove(clientId, this);
                clear();
                return;
            }
            // QoS 0 belongs to the connection; only acknowledged deliveries survive it.
            pending.removeIf(delivery -> {
                if (delivery.qos == Qos.AT_MOST_ONCE) {
                    pendingBytes -= delivery.byteCount;
                    return true;
                }
                return false;
            });
        }

        private void clear() {
            subscriptions.clear();
            received.clear();
            inflight.clear();
            deferred.clear();
            pending.clear();
            inflightBytes = 0;
            pendingBytes = 0;
            connection = null;
        }

        private void discard(String reason) {
            Connection peer = connection;
            sessions.remove(clientId, this);
            clear();
            if (peer != null) {
                peer.close(reason);
            }
        }
    }

    private sealed interface Outgoing {

        Packet packet();
    }

    private record Release(int packetId) implements Outgoing {

        @Override
        public Packet.PubRel packet() {
            return new Packet.PubRel(packetId, ReasonCode.SUCCESS, Properties.EMPTY);
        }
    }

    private static final class Delivery implements Outgoing {

        private final Packet.Publish publication;

        private final Qos qos;

        // The publisher's retain flag for a Retain As Published subscription, and false
        // otherwise, as for every live delivery in MQTT 3.1.1.
        private final boolean retain;

        // The publisher's properties reach MQTT 5.0 receivers unchanged; MQTT 3.1.1 has none.
        private final Properties properties;

        private final int byteCount;

        private int packetId;

        private boolean sent;

        private Delivery(Packet.Publish publication, Qos qos, boolean retain, MqttVersion version) {
            this.publication = publication;
            this.qos = qos;
            this.retain = retain;
            properties = switch (version) {
                case V3_1_1 -> Properties.EMPTY;
                case V5 -> publication.properties();
            };
            byte[] encodedHeader = PacketCodec.of(version).encodePublishHeader(
                    publication.topic(),
                    qos,
                    retain,
                    false,
                    (qos == Qos.AT_MOST_ONCE) ? 0 : 1,
                    properties,
                    publication.payload().length
            );
            byteCount = encodedHeader.length + publication.payload().length;
        }

        @Override
        public Packet.Publish packet() {
            return new Packet.Publish(
                    publication.topic(),
                    publication.payload(),
                    qos,
                    retain,
                    qos != Qos.AT_MOST_ONCE && sent,
                    packetId,
                    properties
            );
        }
    }

    private record Outbound(byte[] bytes, @Nullable Delivery delivery) {
    }

    private final class Connection {

        private final Socket socket;

        private final FrameReader reader;

        private final FrameWriter writer;

        private final SendQueue<Outbound> outgoing = new SendQueue<>(
                SEND_COUNT,
                sendBytes,
                RESERVED_COUNT,
                RESERVED_BYTES
        );

        // The session reference belongs to routing; the identifier is assigned once under both
        // locks on acceptance and can then be read under either lock for timer/logging purposes.
        private @Nullable Session session;

        private String clientId = "";

        // Remaining state is guarded by this connection's monitor.

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

        private long lastReceived = scheduler.nanoTime();

        private long receiveTimeout = connectTimeout;

        private long timerGeneration;

        private Timer.@Nullable Cancellable cancellation;

        private Connection(Socket socket) throws IOException {
            this.socket = socket;
            socket.setTcpNoDelay(true);
            reader = new FrameReader(socket.getInputStream(), MAX_PACKET_SIZE);
            writer = new FrameWriter(socket.getOutputStream());
        }

        private synchronized void start() {
            armTimer(receiveTimeout - (scheduler.nanoTime() - lastReceived));
            String name = "hato-broker-" + port() + "-" + socket.getRemoteSocketAddress();
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
                beforeProcess.accept(request);
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
                    beforeProcess.accept(packet);
                    log.debug("Broker received {} from {}", packet.getClass().getSimpleName(),
                            socket.getRemoteSocketAddress());
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
            if (request.will() != null) {
                refuse(
                        ReasonCode.SERVER_UNAVAILABLE,
                        "a Will for client '" + request.clientId() + "'"
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
                        } while (sessions.containsKey(id));
                    }
                    Session existing = sessions.get(id);
                    boolean present = !request.cleanStart()
                            && existing != null
                            && existing.kept
                            && existing.version == request.version();
                    // Resuming or replacing a kept session reuses its place in the limit.
                    if (keep && (existing == null || !existing.kept)
                            && sessions.values().stream().filter(value -> value.kept).count()
                            >= maxSessions) {
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
                        accepted = new Session(id, request.version(), keep);
                        sessions.put(id, accepted);
                    }
                    clientId = id;
                    session = accepted;
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
                // CONNACK, retransmissions, and queued deliveries stay ahead of later publications.
                accepted.resume();
            }
            log.info("Broker accepted client {} from {} on port {}",
                    id, socket.getRemoteSocketAddress(), port());
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

        private void refuse(ReasonCode reason, String request) {
            log.info("Broker refused {} from {} on port {}: {}",
                    request, socket.getRemoteSocketAddress(), port(), reason);
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
                    for (Packet.Subscription subscription : request.subscriptions()) {
                        owner.subscriptions.put(subscription.filter(), subscription);
                        ReasonCode granted = switch (subscription.qos()) {
                            case AT_MOST_ONCE -> ReasonCode.SUCCESS;
                            case AT_LEAST_ONCE -> ReasonCode.GRANTED_QOS_1;
                            case EXACTLY_ONCE -> ReasonCode.GRANTED_QOS_2;
                        };
                        grants.add(granted);
                    }
                    send(new Packet.SubAck(request.packetId(), grants, Properties.EMPTY));
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
                    // Snapshot permits an overloaded target to remove itself during delivery.
                    for (Session target : List.copyOf(sessions.values())) {
                        target.deliverMatching(publish, owner);
                    }
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
                case Packet.Disconnect disconnect -> close(
                        disconnect.reasonCode() == ReasonCode.SUCCESS
                                ? "client sent DISCONNECT"
                                : "client sent DISCONNECT with " + disconnect.reasonCode()
                );
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

        private synchronized SendQueue.OfferResult enqueue(Outgoing message) {
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

        private void send(Packet packet) {
            boolean accepted;
            synchronized (this) {
                // A connection that ends with a last packet drops later responses.
                accepted = lastPacket != null || enqueue(packet);
            }
            if (!accepted) {
                close("send queue full");
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
                        beforeDeliveryWrite.run();
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
                        // Refusal and completion share this monitor, so no wakeup is lost.
                        resume = awaitingSpace;
                        awaitingSpace = false;
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
                        socket.getRemoteSocketAddress(), port(), reason);
            } else {
                log.info("Broker disconnected client {} from {} on port {}: {}",
                        id, socket.getRemoteSocketAddress(), port(), reason);
            }
        }

        // Whether routing must leave this connection: it has ended or ends with its last packet.
        private synchronized boolean isEnded() {
            return ended || lastPacket != null;
        }

        private synchronized boolean endsWithLastPacket() {
            return lastPacket != null;
        }

        // Ends the connection, first telling an MQTT 5.0 client why with DISCONNECT. MQTT 3.1.1
        // has no DISCONNECT from a server.
        private synchronized void disconnect(ReasonCode reasonCode, String reason) {
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
                armTimer(connectTimeout);
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

        private void close(String reason) {
            synchronized (this) {
                terminate(reason);
            }
            synchronized (routing) {
                connections.remove(this);
                Session owner = session;
                if (owner != null) {
                    owner.detach(this);
                }
            }
        }
    }
}
