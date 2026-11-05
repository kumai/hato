package org.mqtt4j.hato.broker.internal;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.concurrent.Timer;
import org.mqtt4j.hato.broker.internal.common.io.FrameReader;
import org.mqtt4j.hato.broker.internal.common.io.FrameWriter;
import org.mqtt4j.hato.broker.internal.common.io.SendQueue;
import org.mqtt4j.hato.broker.internal.common.io.WriteParts;
import org.mqtt4j.hato.broker.internal.common.log.Log;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.UnsupportedProtocolVersionException;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;

import static java.util.Objects.requireNonNull;

// One network connection of the broker, with its own reader and writer. The reader hands the
// packets that it decodes to the session that the CONNECT attached, whose actor handles them.
final class Connection {

    private static final Log log = Log.of(Connection.class);

    private static final Set<MqttVersion> SUPPORTED_VERSIONS = EnumSet.of(
            MqttVersion.V3_1_1,
            MqttVersion.V5
    );

    static final int SEND_COUNT = 128;

    // Sends that wait together go out in one write, up to this many bytes, rather than one write
    // each: every write is a call into the operating system, which costs more than a small packet
    // itself. A larger send goes alone.
    static final int BATCH_BYTES = 64 * 1024;

    // Responses to a client's requests queue apart from deliveries and PUBRELs, up to the 128
    // the regular queue allowed them before sessions. While they are all waiting, the reader
    // stops reading requests, so a client that does not read its responses is held back by TCP.
    private static final int RESERVED_COUNT = 128;

    private static final long RESERVED_BYTES = 1024 * 1024;

    // The framing format bounds packet size, until a connection's announced Maximum Packet Size
    // lowers the bound.
    static final int MAX_PACKET_SIZE = 268_435_460;

    private final Routing routing;

    private final BrokerSettings settings;

    private final Timer scheduler;

    private final Socket socket;

    private final FrameReader reader;

    private final FrameWriter writer;

    private final SendQueue<Outbound> outgoing;

    final Client client;

    // The identifier is assigned once on acceptance, before the session attaches the connection,
    // and read by timers, logs, and the test controls.
    volatile String clientId = "";

    // Remaining state is guarded by this connection's monitor.

    // Responses that a test released, waiting for room in the reserved quota in order. Later
    // responses queue behind them, so that none overtakes a released one.
    private final ArrayDeque<Packet> released = new ArrayDeque<>();

    // The version of the connection's CONNECT. Until CONNECT selects one, a refusal of an
    // unsupported protocol level uses the MQTT 3.1.1 form, which every client can read.
    private MqttVersion version = MqttVersion.V3_1_1;

    boolean ended;

    // Whether the end of the connection has gone to its session, or found none attached.
    private boolean endHandled;

    // The responses that the reader has reserved room for and the session has yet to queue: the
    // reader hands the requests to the session, which answers them on its actor.
    private int owed;

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
        // The frame reader takes a packet's fixed header a byte at a time, and every read of the
        // socket is a call into the operating system. Packets still in this buffer when the
        // connection ends can reach the session, which handles none from an ended connection.
        reader = new FrameReader(new BufferedInputStream(socket.getInputStream()), MAX_PACKET_SIZE);
        writer = new FrameWriter(socket.getOutputStream(), socket.getChannel());
        client = new Client(this, routing);
    }

    SocketAddress remoteAddress() {
        return socket.getRemoteSocketAddress();
    }

    // Refuses the packets after this point that are larger than `maximumPacketSize`, which
    // CONNACK announced. Called by the reader, which alone uses it.
    void limitPacketSize(int maximumPacketSize) {
        reader.limit(maximumPacketSize);
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
            if (!client.connect(request)) {
                return;
            }
            Session owner = requireNonNull(client.session);
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
                // Packets without a response, such as QoS 0 publications, never wait here.
                boolean answered = answered(packet);
                if (answered && !reserveResponse()) {
                    return;
                }
                owner.receive(this, packet, answered, bytes.get().length);
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

    void refuse(ReasonCode reason, String request) {
        refuse(new Packet.ConnAck(false, reason, Properties.EMPTY), request);
    }

    // Answers the CONNECT with `refusal` and closes the connection. MQTT 3.1.1 has return codes
    // for five reasons only, and a broker with none of them to give closes the connection
    // without CONNACK (MQTT 3.1.1 [MQTT-3.2.2-6]).
    void refuse(Packet.ConnAck refusal, String request) {
        ReasonCode reason = refusal.reasonCode();
        log.info("Broker refused {} from {} on port {}: {}",
                request, socket.getRemoteSocketAddress(), routing.port, reason);
        synchronized (this) {
            boolean answered = switch (version) {
                case V3_1_1 -> switch (reason) {
                    case UNSUPPORTED_PROTOCOL_VERSION,
                         CLIENT_IDENTIFIER_NOT_VALID,
                         SERVER_UNAVAILABLE,
                         BAD_USER_NAME_OR_PASSWORD,
                         NOT_AUTHORIZED -> true;
                    default -> false;
                };
                case V5 -> true;
            };
            if (answered) {
                closeAfter(refusal, "refusal sent");
            } else {
                terminate("refusal without an MQTT 3.1.1 return code");
            }
        }
    }

    synchronized SendQueue.OfferResult enqueue(Outgoing message) {
        if (lastPacket != null) {
            return SendQueue.OfferResult.CLOSED;
        }
        // PUBLISH and PUBREL share the regular queue, whose window always fits a resumed
        // session, and leave the reserved queue to responses to the client's requests.
        SendQueue.OfferResult result = switch (message) {
            // The writer prepares a delivery as it writes it. A session queues a delivery here
            // only once the connection that had it before has ended, so whether that connection
            // started to send it no longer changes.
            case Delivery delivery -> outgoing.offer(
                    ++ticket,
                    new Outbound.Publish(delivery, delivery.sent),
                    delivery.byteCount,
                    false
            );
            case Release release -> {
                byte[] bytes = PacketCodec.of(version).encode(release.packet());
                yield outgoing.offer(
                        ++ticket,
                        new Outbound.Encoded(bytes),
                        bytes.length,
                        false
                );
            }
        };
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
            case Packet.AliasPublish publish -> publish.qos() != Qos.AT_MOST_ONCE;
            case Packet.Subscribe _,
                 Packet.Unsubscribe _,
                 Packet.PubRel _,
                 Packet.PingReq _ -> true;
            case Packet.Connect _,
                 Packet.ConnAck _,
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

    /// Holds the reader until the reserved quota has room for one more response besides those
    /// that the session still owes, and reserves it, so that answering the next request never
    /// finds the quota full. Only responses to this reader's requests take the reserved quota,
    /// so the room stays until the response takes it. A deliberate simplification: the wait
    /// counts responses, not their bytes. Only a backlog of SUBACKs for SUBSCRIBE packets with
    /// many thousands of filters reaches the byte limit, which still closes the connection;
    /// waiting for the size of each response would remove that limit. Returns false once the
    /// connection has ended. A connection that is to end with a last packet drops responses, so
    /// its reader does not wait.
    private synchronized boolean reserveResponse() throws InterruptedException {
        while (lastPacket == null && responseRoomTaken()) {
            wait();
        }
        if (ended) {
            return false;
        }
        owed++;
        return true;
    }

    // Whether a response now would find no room: the reserved quota is full with the responses
    // owed, or responses that a test released wait for it first. Called under the connection
    // monitor.
    private boolean responseRoomTaken() {
        return !released.isEmpty()
                || outgoing.checkCapacity(0, true, owed) == SendQueue.OfferResult.FULL;
    }

    // Returns the room that the reader reserved for the response to a request, which the
    // session has answered or passed over.
    synchronized void responseSettled() {
        owed--;
        notifyAll();
    }

    // Queues a response to the client's request in the reserved quota, behind the responses
    // that a test released, and ends the connection when the quota is full. Called on the
    // session's actor, which holds no monitor, so that closing can take the session's inbox.
    void respond(Packet packet) {
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

    // Queues `responses`, which a test held back and now releases, behind any released before,
    // and sends what fits. Called by the test controls.
    synchronized void release(List<Packet> responses) {
        released.addAll(responses);
        sendReleased();
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
                    new Outbound.Encoded(bytes),
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

    // Called under the connection monitor; no socket I/O occurs on a session's actor.
    boolean enqueue(Packet packet) {
        if (ended || lastPacket != null) {
            return false;
        }
        byte[] bytes = PacketCodec.of(version).encode(packet);
        boolean enqueueFailed = outgoing.offer(
                ++ticket,
                new Outbound.Encoded(bytes),
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
                List<SendQueue.Send<Outbound>> sends;
                byte[] last;
                synchronized (this) {
                    sends = outgoing.pollBatch(BATCH_BYTES);
                    while (!ended && sends.isEmpty() && lastPacket == null) {
                        wait();
                        sends = outgoing.pollBatch(BATCH_BYTES);
                    }
                    if (ended) {
                        return;
                    }
                    last = lastPacket;
                }
                if (sends.isEmpty()) {
                    writer.write(requireNonNull(last));
                    return;
                }
                boolean delivers = false;
                for (SendQueue.Send<Outbound> send : sends) {
                    delivers |= send.value() instanceof Outbound.Publish;
                }
                boolean skip = false;
                if (delivers) {
                    settings.beforeDeliveryWrite().run();
                    // This monitor decides. A session ends the connection that it leaves, under
                    // this monitor, before it resends the connection's deliveries elsewhere, so
                    // a delivery is marked as sent before the resend reads the mark, or is never
                    // written here.
                    synchronized (this) {
                        if (!ended && lastPacket == null) {
                            for (SendQueue.Send<Outbound> send : sends) {
                                if (send.value() instanceof Outbound.Publish publish) {
                                    publish.delivery().sent = true;
                                }
                            }
                        } else if (lastPacket != null) {
                            // The deliveries stay unsent for the session's next connection;
                            // the last packet still follows.
                            skip = true;
                        } else {
                            return;
                        }
                    }
                }
                ByteBuffer[] parts = parts(sends, skip);
                if (parts.length > 0) {
                    writer.writeParts(parts);
                }
                boolean resume;
                Session owner;
                synchronized (this) {
                    outgoing.completed(sends.getLast().ticket());
                    sendReleased();
                    // Refusal and completion share this monitor, so no wakeup is lost.
                    resume = awaitingSpace;
                    awaitingSpace = false;
                    owner = client.session;
                    // The reader may wait for room for its next response.
                    notifyAll();
                }
                if (resume && owner != null) {
                    owner.requestDrain(settings.inPlace());
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

    // The parts of the one write that carries `sends`: the bytes of each packet that was queued
    // encoded, and the headers of each delivery, prepared now, before the payload that it shares
    // with the publication's deliveries to other sessions. The deliveries that `skip` leaves
    // unsent have none.
    private ByteBuffer[] parts(List<SendQueue.Send<Outbound>> sends, boolean skip) {
        var parts = new WriteParts();
        long now = scheduler.nanoTime();
        for (SendQueue.Send<Outbound> send : sends) {
            switch (send.value()) {
                case Outbound.Encoded(var bytes) -> parts.add(bytes);
                case Outbound.Publish(var delivery, var sentBefore) -> {
                    if (!skip) {
                        parts.add(delivery.header(sentBefore, now));
                        parts.add(delivery.payload());
                    }
                }
            }
        }
        return parts.toArray();
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

    // Whether sessions must leave this connection: it has ended or ends with its last packet.
    synchronized boolean isEnded() {
        return ended || lastPacket != null;
    }

    private synchronized boolean endsWithLastPacket() {
        return lastPacket != null;
    }

    // Ends the connection, first telling an MQTT 5.0 client why with DISCONNECT. MQTT 3.1.1
    // has no DISCONNECT from a server.
    synchronized void disconnect(ReasonCode reasonCode, String reason) {
        disconnect(new Packet.Disconnect(reasonCode, Properties.EMPTY), reason);
    }

    synchronized void disconnect(Packet.Disconnect disconnect, String reason) {
        switch (version) {
            case V3_1_1 -> terminate(reason);
            case V5 -> closeAfter(disconnect, reason);
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

    synchronized void keepAlive(int seconds) {
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
                // revive this connection while its end goes to the session.
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
        // Ending the connection releases reader/writer, which hand its end to the session. The
        // shared timer waits for no session before checking another peer.
    }

    // Stops I/O; the exiting reader and writer hand the connection's end to the session.
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

    // Ends the connection and hands its end, once, to the session that it is attached to.
    void close(String reason) {
        Session owner;
        synchronized (this) {
            terminate(reason);
            if (endHandled) {
                return;
            }
            endHandled = true;
            owner = client.session;
        }
        routing.connections.remove(this);
        if (owner != null) {
            owner.post(client::connectionEnded, settings.inPlace());
        }
    }

    // Has the connection's client belong to `session`, whose actor accepted the CONNECT, and
    // queues `connAck`. Returns false if the connection's end has come already and found no
    // session. Called under the session's inbox.
    synchronized boolean attach(Session session, Packet.ConnAck connAck) {
        client.session = session;
        enqueue(connAck);
        return !endHandled;
    }

    // Ends the connection for a failure in handling one of its packets, as the reader ends it for
    // one in reading: a packet that breaks the protocol with DISCONNECT, after which the writer
    // closes it.
    void failed(Throwable failure) {
        String reason = failureReason("reader", failure);
        if (failure instanceof PacketDecodeException violation) {
            disconnect(violation.reasonCode(), reason);
        }
        if (!endsWithLastPacket()) {
            close(reason);
        }
    }

    // What waits in the send queue: a packet that was encoded as it was queued, or a delivery
    // with whether a connection before this one started to send it.
    private sealed interface Outbound {

        record Encoded(byte[] bytes) implements Outbound {}

        record Publish(Delivery delivery, boolean sentBefore) implements Outbound {}
    }
}
