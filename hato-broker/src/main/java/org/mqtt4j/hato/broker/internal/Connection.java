package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
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

// One network connection of the broker, with its own reader and writer. Its client handles the
// packets that the reader decodes, under routing.
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

    // The identifier is assigned once under both locks on acceptance and can then be read
    // under either lock for timer/logging purposes.
    String clientId = "";

    // Remaining state is guarded by this connection's monitor.

    // Responses that a test released, waiting for room in the reserved quota in order. Later
    // responses queue behind them, so that none overtakes a released one.
    private final ArrayDeque<Packet> released = new ArrayDeque<>();

    // The version of the connection's CONNECT. Until CONNECT selects one, a refusal of an
    // unsupported protocol level uses the MQTT 3.1.1 form, which every client can read.
    private MqttVersion version = MqttVersion.V3_1_1;

    boolean ended;

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
                    Session owner = client.currentSession();
                    if (owner == null) {
                        return;
                    }
                    client.process(owner, packet);
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

    void refuse(ReasonCode reason, String request) {
        log.info("Broker refused {} from {} on port {}: {}",
                request, socket.getRemoteSocketAddress(), routing.port, reason);
        synchronized (this) {
            closeAfter(new Packet.ConnAck(false, reason, Properties.EMPTY), "refusal sent");
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
            byte[] bytes = PacketCodec.of(version).encode(message.packet(scheduler.nanoTime()));
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

    // Queues a response to the client's request in the reserved quota, behind the responses
    // that a test released, and ends the connection when the quota is full. Called under
    // routing.
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
    // and sends what fits. Called under routing.
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
    boolean enqueue(Packet packet) {
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
                        if (client.currentSession() != null) {
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
                        Session owner = client.currentSession();
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
            client.connectionEnded();
        }
    }

    private record Outbound(byte[] bytes, @Nullable Delivery delivery) {
    }
}
