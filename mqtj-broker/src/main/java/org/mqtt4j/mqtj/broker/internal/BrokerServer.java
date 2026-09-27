package org.mqtt4j.mqtj.broker.internal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.broker.internal.common.concurrent.Timer;
import org.mqtt4j.mqtj.broker.internal.common.concurrent.TimerService;
import org.mqtt4j.mqtj.broker.internal.common.io.FrameReader;
import org.mqtt4j.mqtj.broker.internal.common.io.FrameWriter;
import org.mqtt4j.mqtj.broker.internal.common.io.SendQueue;
import org.mqtt4j.mqtj.broker.internal.common.log.Log;
import org.mqtt4j.mqtj.broker.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.mqtj.broker.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.mqtj.broker.internal.common.protocol.PacketIds;
import org.mqtt4j.mqtj.broker.internal.common.protocol.Properties;
import org.mqtt4j.mqtj.broker.internal.common.protocol.UnsupportedProtocolVersionException;
import org.mqtt4j.mqtj.broker.internal.common.value.MqttVersion;
import org.mqtt4j.mqtj.broker.internal.common.value.Qos;
import org.mqtt4j.mqtj.broker.internal.common.value.ReasonCode;
import org.mqtt4j.mqtj.broker.internal.common.value.TopicFilter;

import static java.util.Objects.requireNonNull;

/// One listener with independent reader/writer pairs and clean, in-memory sessions.
public final class BrokerServer implements AutoCloseable {

    private static final Log log = Log.of(BrokerServer.class);

    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);

    private static final int SEND_COUNT = 128;

    private static final long SEND_BYTES = 16L * 1024 * 1024;

    private static final int MAX_INFLIGHT = 128;

    // The framing format bounds packet size; a configurable broker cap comes later.
    private static final int MAX_PACKET_SIZE = 268_435_460;

    private final ServerSocket listener;

    private final long connectTimeout;

    private final long sendBytes;

    private final int maxInflight;

    private final Timer scheduler;

    // This global lock and linear subscription scan bound routing throughput. Replace them
    // with a topic index and finer ownership if broker measurements justify productization.
    // Lock order is routing -> Connection; a takeover locks the previous Connection while
    // holding the new one. A Connection releases its lock before removal.
    private final Object routing = new Object();

    private final Set<Connection> connections = new HashSet<>();

    private final Map<String, Connection> sessions = new HashMap<>();

    private boolean closed;

    private BrokerServer(
            ServerSocket listener,
            long connectTimeout,
            long sendBytes,
            int maxInflight,
            Timer scheduler
    ) {
        this.listener = listener;
        this.connectTimeout = connectTimeout;
        this.sendBytes = sendBytes;
        this.maxInflight = maxInflight;
        this.scheduler = scheduler;
    }

    public static BrokerServer start(InetSocketAddress address) throws IOException {
        return start(address, CONNECT_TIMEOUT, SEND_BYTES, MAX_INFLIGHT, TimerService.instance);
    }

    static BrokerServer start(
            InetSocketAddress address,
            Duration connectTimeout,
            long sendBytes,
            int maxInflight,
            Timer scheduler
    ) throws IOException {
        requireNonNull(address, "address must not be null");
        requireNonNull(scheduler, "scheduler must not be null");
        if (connectTimeout.isNegative()
            || connectTimeout.isZero()
            || sendBytes <= 0
            || maxInflight <= 0
            || maxInflight > 65535
        ) {
            throw new IllegalArgumentException("Broker limits and timeout must be positive");
        }
        long connectTimeoutNanos = connectTimeout.toNanos();
        var listener = new ServerSocket();
        try {
            listener.bind(address);
            var server = new BrokerServer(
                    listener, connectTimeoutNanos, sendBytes, maxInflight, scheduler
            );
            Thread.ofVirtual()
                    .name("mqtj-broker-" + server.port() + "-accept")
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
        }
    }

    private final class Connection {

        private final Socket socket;

        private final FrameReader reader;

        private final FrameWriter writer;

        private final SendQueue<byte[]> outgoing = new SendQueue<>(SEND_COUNT, sendBytes, 0, 0);

        // Only routing reads/writes the subscriptions. The identifier is assigned once, on
        // acceptance, while holding both routing and this connection's monitor; either lock can
        // read it, and it stays empty for a connection that was never accepted.
        private final Map<TopicFilter, Qos> subscriptions = new HashMap<>();

        private String clientId = "";

        // Remaining state is guarded by this connection's monitor.
        private final PacketIds packetIds = new PacketIds();

        private boolean ended;

        // Why the connection ended first; later failures are consequences of that end.
        private String endReason = "";

        private int inflight;

        private long ticket;

        private long closeAfterWrite;

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
            String name = "mqtj-broker-" + port() + "-" + socket.getRemoteSocketAddress();
            Thread.ofVirtual().name(name + "-writer").start(this::writeLoop);
            Thread.ofVirtual().name(name + "-reader").start(this::readLoop);
        }

        private void readLoop() {
            // The writer closes a refused or already terminated connection after writing any
            // refusal CONNACK; closing here could drop that CONNACK.
            boolean writerCloses = false;
            String reason = "connection closed by the peer";
            try {
                Optional<byte[]> first = reader.read();
                if (first.isEmpty()) {
                    return;
                }
                received();
                try {
                    writerCloses = !connect(PacketCodec.decodeConnect(first.get()));
                } catch (UnsupportedProtocolVersionException unsupported) {
                    refuse(
                            ReasonCode.UNSUPPORTED_PROTOCOL_VERSION,
                            "protocol level " + unsupported.protocolLevel()
                    );
                    writerCloses = true;
                }
                if (writerCloses) {
                    return;
                }
                while (true) {
                    Optional<byte[]> bytes = reader.read();
                    if (bytes.isEmpty()) {
                        return;
                    }
                    received();
                    Packet packet = codec.decode(bytes.get());
                    log.debug("Broker received {} from {}", packet.getClass().getSimpleName(),
                            socket.getRemoteSocketAddress());
                    synchronized (routing) {
                        if (isEnded()) {
                            return;
                        }
                        process(packet);
                    }
                }
            } catch (Throwable failure) {
                reason = failureReason("reader", failure);
            } finally {
                if (!writerCloses) {
                    close(reason);
                }
            }
        }

        private boolean connect(Packet.Connect request) {
            // Refusal logs quote the identifier, which a refused client may have left empty.
            if (!request.cleanStart()) {
                refuse(
                        request.clientId().isEmpty()
                                ? ReasonCode.CLIENT_IDENTIFIER_NOT_VALID
                                : ReasonCode.SERVER_UNAVAILABLE,
                        "a persistent session for client '" + request.clientId() + "'"
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
            String id;
            synchronized (routing) {
                // Acceptance and its timer transition are atomic with deadline termination.
                // Otherwise an expired connection could take over a live peer and rearm a timer.
                synchronized (this) {
                    if (ended) {
                        return false;
                    }
                    id = request.clientId();
                    if (id.isEmpty()) {
                        do {
                            id = "mqtj" + UUID.randomUUID().toString().replace("-", "");
                        } while (sessions.containsKey(id));
                    }
                    clientId = id;
                    Connection previous = sessions.put(id, this);
                    if (previous != null) {
                        previous.close("taken over by " + socket.getRemoteSocketAddress());
                    }
                    keepAlive(request.keepAliveSeconds());
                    send(new Packet.ConnAck(false, ReasonCode.SUCCESS, Properties.EMPTY));
                }
            }
            log.info("Broker accepted client {} from {} on port {}",
                    id, socket.getRemoteSocketAddress(), port());
            return true;
        }

        private void refuse(ReasonCode reason, String request) {
            log.info("Broker refused {} from {} on port {}: {}",
                    request, socket.getRemoteSocketAddress(), port(), reason);
            synchronized (this) {
                if (enqueue(new Packet.ConnAck(false, reason, Properties.EMPTY))) {
                    // The writer closes the socket once it has written this CONNACK.
                    closeAfterWrite = ticket;
                    return;
                }
            }
            close("send queue full");
        }

        private void process(Packet packet) throws IOException {
            switch (packet) {
                case Packet.Subscribe request -> {
                    var grants = new ArrayList<ReasonCode>();
                    for (Packet.Subscription subscription : request.subscriptions()) {
                        Qos qos = Qos.of(Math.min(subscription.qos().level(), 1));
                        subscriptions.put(subscription.filter(), qos);
                        grants.add(
                                (qos == Qos.AT_MOST_ONCE)
                                        ? ReasonCode.SUCCESS
                                        : ReasonCode.GRANTED_QOS_1
                        );
                    }
                    send(new Packet.SubAck(request.packetId(), grants, Properties.EMPTY));
                }
                case Packet.Unsubscribe request -> {
                    request.filters().forEach(subscriptions::remove);
                    send(new Packet.UnsubAck(request.packetId(), List.of(), Properties.EMPTY));
                }
                case Packet.Publish publish -> {
                    if (publish.qos() == Qos.EXACTLY_ONCE) {
                        throw new IOException("QoS 2 publications are not supported");
                    }
                    // Snapshot permits an overloaded target to remove itself during delivery.
                    for (Connection target : List.copyOf(sessions.values())) {
                        int granted = -1;
                        for (var subscription : target.subscriptions.entrySet()) {
                            if (subscription.getKey().matches(publish.topic())) {
                                granted = Math.max(granted, subscription.getValue().level());
                            }
                        }
                        if (granted >= 0) {
                            Qos qos = Qos.of(Math.min(granted, publish.qos().level()));
                            target.deliver(publish, qos);
                        }
                    }
                    // Acknowledge only after every delivery was enqueued or its target closed.
                    if (publish.qos() == Qos.AT_LEAST_ONCE) {
                        send(new Packet.PubAck(
                                publish.packetId(),
                                ReasonCode.SUCCESS,
                                Properties.EMPTY
                        ));
                    }
                }
                case Packet.PubAck ack -> acknowledge(ack.packetId());
                case Packet.PingReq _ -> send(new Packet.PingResp());
                case Packet.Disconnect _ -> close("client sent DISCONNECT");
                // Name only the type; a CONNECT's text would put its username in the INFO end log.
                case Packet.Connect _,
                     Packet.ConnAck _,
                     Packet.SubAck _,
                     Packet.UnsubAck _,
                     Packet.PingResp _,
                     Packet.PubRec _,
                     Packet.PubRel _,
                     Packet.PubComp _,
                     Packet.Auth _ -> throw new IOException(
                             "Unexpected " + packet.getClass().getSimpleName() + " packet"
                     );
            }
        }

        private void deliver(Packet.Publish publish, Qos qos) {
            String overload = "unacknowledged delivery limit reached";
            synchronized (this) {
                if (ended) {
                    return;
                }
                if (qos == Qos.AT_MOST_ONCE || inflight < maxInflight) {
                    int id = 0;
                    if (qos == Qos.AT_LEAST_ONCE) {
                        id = packetIds.allocate().orElseThrow();
                        inflight++;
                    }
                    if (enqueue(new Packet.Publish(
                            publish.topic(),
                            publish.payload(),
                            qos,
                            false,
                            false,
                            id,
                            Properties.EMPTY
                    ))) {
                        return;
                    }
                    overload = "send queue full";
                }
            }
            close(overload);
        }

        private synchronized void acknowledge(int id) {
            if (packetIds.isInUse(id)) {
                packetIds.release(id);
                inflight--;
            }
        }

        private void send(Packet packet) {
            boolean accepted;
            synchronized (this) {
                accepted = enqueue(packet);
            }
            if (!accepted) {
                close("send queue full");
            }
        }

        // Called under the connection monitor; no socket I/O occurs on a routing thread.
        private boolean enqueue(Packet packet) {
            if (ended) {
                return false;
            }
            byte[] bytes = codec.encode(packet);
            boolean enqueueFailed = outgoing.offer(
                    ++ticket,
                    bytes,
                    bytes.length,
                    false
            ) != SendQueue.OfferResult.ACCEPTED;
            if (enqueueFailed) {
                return false;
            }
            notifyAll();
            return true;
        }

        private void writeLoop() {
            // Unless a write fails, the writer ends an open connection only after a refusal.
            String reason = "refusal sent";
            try {
                while (true) {
                    SendQueue.Send<byte[]> send;
                    synchronized (this) {
                        Optional<SendQueue.Send<byte[]>> next = outgoing.poll();
                        while (!ended && next.isEmpty()) {
                            wait();
                            next = outgoing.poll();
                        }
                        if (ended) {
                            return;
                        }
                        send = next.orElseThrow();
                    }
                    writer.write(send.value());
                    synchronized (this) {
                        outgoing.completed(send.ticket());
                        if (send.ticket() == closeAfterWrite) {
                            return;
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

        private synchronized boolean isEnded() {
            return ended;
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
                    long remaining = receiveTimeout - (scheduler.nanoTime() - lastReceived);
                    if (remaining > 0) {
                        armTimer(remaining);
                        return;
                    }
                    // Mark the deadline under the same lock as receipt; a late frame cannot
                    // revive this connection while removal waits for an ongoing route.
                    terminate(clientId.isEmpty() ? "CONNECT timeout" : "keep-alive timeout");
                }
            } catch (Throwable failure) {
                log.warn("Broker timer for {} failed", socket.getRemoteSocketAddress(), failure);
                synchronized (this) {
                    terminate("timer failure");
                }
            }
            // Closing the socket releases reader/writer, which remove the session. Do not
            // make the shared timer wait for the routing lock before checking another peer.
        }

        // Stops I/O without the routing lock; the exiting reader and writer remove the connection.
        private void terminate(String reason) {
            if (ended) {
                return;
            }
            ended = true;
            endReason = reason;
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
                sessions.remove(clientId, this);
                subscriptions.clear();
            }
        }
    }
}
