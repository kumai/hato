package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.common.concurrent.Timer;
import org.mqtt4j.hato.broker.internal.common.concurrent.TimerService;
import org.mqtt4j.hato.broker.internal.common.log.Log;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketType;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;

import static java.util.Objects.requireNonNull;

/// One listener whose connections share the bounded, in-memory sessions of one broker.
public final class BrokerServer implements AutoCloseable {

    private static final Log log = Log.of(BrokerServer.class);

    // How the test controls find the server of a broker. MqttBroker, which alone can reach its
    // server, sets it as it starts each broker, before the broker exists.
    private static volatile @Nullable Function<MqttBroker, BrokerServer> servers;

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);

    private static final long SEND_BYTES = 16L * 1024 * 1024;

    private static final int MAX_INFLIGHT = 128;

    private static final int MAX_QUEUED = 128;

    private static final int MAX_SESSIONS = 1024;

    // Retained messages are bounded like the other state that the broker keeps: by count, as
    // sessions are, and by size, as each session's waiting deliveries are.
    private static final int MAX_RETAINED = 1024;

    // Every response that a test holds takes one of the client's packet identifiers, so a client
    // that follows the protocol never holds more.
    private static final int MAX_HELD = 65_535;

    // How many selector threads do the socket I/O, and how many threads the actor pool has. The
    // properties are for trials, until the numbers follow from the processors at hand.
    private static final int SELECTORS = Integer.getInteger("hato.broker.selectors", 4);

    private static final int ACTORS = Integer.getInteger("hato.broker.actors", 2);

    private final ServerSocketChannel listener;

    // Tells the accepting thread that a connection waits at the listener, which is all that it
    // watches. The thread waits here and never inside accept: when a listener is closed just as
    // a thread enters a blocking accept, the closing thread can wait in the kernel until
    // something else in the process uses a file descriptor (seen on macOS), which a quiet
    // process never does.
    private final Selector acceptor;

    private final Routing routing;

    // The thread that accepts the connections.
    private final Thread accepting;

    private BrokerServer(ServerSocketChannel listener, BrokerSettings settings) throws IOException {
        this.listener = listener;
        acceptor = Selector.open();
        try {
            listener.configureBlocking(false);
            listener.register(acceptor, SelectionKey.OP_ACCEPT);
            routing = new Routing(settings, listener.socket().getLocalPort(), this::close);
            accepting = Thread.ofPlatform().daemon()
                    .name("hato-broker-" + routing.port + "-accept")
                    .unstarted(this::accept);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                acceptor.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    public static BrokerServer start(InetSocketAddress address) throws IOException {
        return builder().start(address);
    }

    /// Lets [#of(MqttBroker)] find the server of each broker. MqttBroker calls this as it starts
    /// each broker, with the same lookup every time.
    public static void serversOf(Function<MqttBroker, BrokerServer> lookup) {
        servers = requireNonNull(lookup, "lookup must not be null");
    }

    /// The server of `broker`, for the test controls.
    public static BrokerServer of(MqttBroker broker) {
        // A broker exists, so MqttBroker has set the lookup.
        return requireNonNull(servers, "MqttBroker sets the lookup").apply(broker);
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

        private int maxRetained = MAX_RETAINED;

        private long maxRetainedBytes = SEND_BYTES;

        private int maxHeld = MAX_HELD;

        private int selectors = SELECTORS;

        private int actors = ACTORS;

        private Capabilities capabilities = Capabilities.DEFAULT;

        private Timer scheduler = TimerService.instance;

        private Consumer<Packet> beforeProcess = _ -> { };

        private Consumer<Packet> beforeHandle = _ -> { };

        private Predicate<Connection> deliveryWriteHeld = _ -> false;

        private Runnable beforeSelect = () -> { };

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

        Builder maxRetained(int value) {
            maxRetained = value;
            return this;
        }

        Builder maxRetainedBytes(long value) {
            maxRetainedBytes = value;
            return this;
        }

        Builder maxHeld(int value) {
            maxHeld = value;
            return this;
        }

        Builder selectors(int value) {
            selectors = value;
            return this;
        }

        Builder actors(int value) {
            actors = value;
            return this;
        }

        Builder capabilities(Capabilities value) {
            capabilities = requireNonNull(value, "capabilities must not be null");
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

        Builder beforeHandle(Consumer<Packet> value) {
            beforeHandle = requireNonNull(value, "beforeHandle must not be null");
            return this;
        }

        // Runs `value` on the selector before each write that carries a delivery. A hook that
        // waits holds the selector, and with it the input of the same connection.
        Builder beforeDeliveryWrite(Runnable value) {
            requireNonNull(value, "beforeDeliveryWrite must not be null");
            deliveryWriteHeld = _ -> {
                value.run();
                return false;
            };
            return this;
        }

        // Keeps a write that carries a delivery back while `value` says so for its connection,
        // without holding the selector. It replaces a hook set with beforeDeliveryWrite.
        Builder holdDeliveryWrite(Predicate<Connection> value) {
            deliveryWriteHeld = requireNonNull(value, "holdDeliveryWrite must not be null");
            return this;
        }

        Builder beforeSelect(Runnable value) {
            beforeSelect = requireNonNull(value, "beforeSelect must not be null");
            return this;
        }

        BrokerServer start(InetSocketAddress address) throws IOException {
            requireNonNull(address, "address must not be null");
            if (connectTimeout.isNegative()
                || connectTimeout.isZero()
                || sendBytes <= 0
                || maxInflight <= 0
                || maxInflight > Connection.SEND_COUNT
                || maxInflightBytes <= 0
                || maxInflightBytes > sendBytes
                || maxQueued <= 0
                || maxQueuedBytes <= 0
                || maxSessions <= 0
                || maxRetained <= 0
                || maxRetainedBytes <= 0
                || maxHeld <= 0
                || selectors <= 0
                || actors <= 0
            ) {
                throw new IllegalArgumentException(
                        "Broker limits must be positive and window fit queue"
                );
            }
            var settings = new BrokerSettings(
                    connectTimeout.toNanos(),
                    sendBytes,
                    maxInflight,
                    maxInflightBytes,
                    maxQueued,
                    maxQueuedBytes,
                    maxSessions,
                    maxRetained,
                    maxRetainedBytes,
                    maxHeld,
                    selectors,
                    actors,
                    capabilities,
                    scheduler,
                    beforeProcess,
                    beforeHandle,
                    deliveryWriteHeld,
                    beforeSelect
            );
            ServerSocketChannel listener = ServerSocketChannel.open();
            try {
                listener.bind(address);
                var server = new BrokerServer(listener, settings);
                try {
                    for (Reactor reactor : server.routing.reactors) {
                        reactor.start();
                    }
                    server.accepting.start();
                } catch (RuntimeException | Error failure) {
                    server.close();
                    throw failure;
                }
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
        return routing.port;
    }

    private void accept() {
        try {
            while (true) {
                Socket socket = accepted();
                Connection connection = null;
                try {
                    connection = new Connection(socket, routing);
                    if (!connection.start()) {
                        socket.close();
                        return;
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
            if (!routing.closed) {
                log.error("Broker listener on port {} failed", port(), failure);
            }
            close();
        }
    }

    // The next connection that waits at the listener, once there is one. close() ends the wait
    // with an exception, by closing the listener and the selector.
    private Socket accepted() throws IOException {
        while (true) {
            SocketChannel channel = listener.accept();
            if (channel != null) {
                return channel.socket();
            }
            // The key tells nothing more than the return does: the loop asks the listener.
            acceptor.select(_ -> { });
            if (Thread.interrupted()) {
                // An interrupted thread no longer waits in a selector. It ends the listener, as
                // an interrupt of a blocking accept does.
                throw new InterruptedIOException("Interrupted while waiting for a connection");
            }
        }
    }

    // Closes the broker, and returns once the listening port is free and every connection is
    // closed. The first call ends the listener, the connections, and the sessions. Each
    // selector closes the sockets of its connections on its own thread, as it ends, and the
    // accepting thread closes a socket that it took from the listener too late; every call
    // waits for those threads, so that a caller that closes the broker again meanwhile finds
    // the same. Only a thread of the broker itself does not wait: one that closes the broker
    // after its failure goes on to close its own sockets.
    @Override
    public void close() {
        boolean first;
        synchronized (this) {
            first = !routing.closed;
            routing.closed = true;
        }
        if (first) {
            end();
        }
        Thread current = Thread.currentThread();
        boolean own = current == accepting;
        for (Reactor reactor : routing.reactors) {
            own |= current == reactor.thread;
        }
        if (own) {
            return;
        }
        awaitEnd(accepting);
        for (Reactor reactor : routing.reactors) {
            awaitEnd(reactor.thread);
        }
    }

    // Ends the listener, the connections, and the sessions, and has the selectors end.
    private void end() {
        try {
            try {
                listener.close();
            } catch (IOException failure) {
                log.warn("Closing broker listener on port {} failed", port(), failure);
            }
            // A channel that a selector watches keeps its socket until the selector lets go of
            // it. Closing the selector does that here, so the port is free from now on, and it
            // wakes the accepting thread.
            try {
                acceptor.close();
            } catch (IOException failure) {
                log.warn("Closing the accept selector of the broker on port {} failed",
                        port(), failure);
            }
            routing.clear();
        } finally {
            routing.actors.shutdown();
            for (Reactor reactor : routing.reactors) {
                reactor.close();
            }
        }
    }

    // Waits until `thread` has ended; one that never started has. An interrupt does not end
    // the wait, and stays with the calling thread.
    private static void awaitEnd(Thread thread) {
        boolean interrupted = false;
        while (true) {
            try {
                thread.join();
                break;
            } catch (InterruptedException _) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// Refuses later subscriptions to exactly `filter`: SUBACK answers them with failure 0x80,
    /// and they subscribe to nothing.
    ///
    /// @throws IllegalArgumentException if `filter` is not a valid topic filter
    public void refuseSubscriptions(String filter) {
        routing.refuseSubscriptions(filter, new Reason(ReasonCode.UNSPECIFIED_ERROR, null, null));
    }

    /// Refuses later subscriptions to exactly `filter` as [#refuseSubscriptions(String)] does,
    /// with `reasonCode`, and `reasonString` if not null, for MQTT 5.0 clients. A SUBACK carries
    /// the Reason String of the first filter refused with one.
    ///
    /// @throws IllegalArgumentException if `filter` is not a valid topic filter, `reasonCode`
    ///         is not a SUBACK failure, or `reasonString` is not an MQTT UTF-8 string
    public void refuseSubscriptions(String filter, int reasonCode, @Nullable String reasonString) {
        Reason reason = Reason.checked(PacketType.SUBACK, reasonCode, reasonString, null);
        routing.refuseSubscriptions(filter, reason);
    }

    /// Refuses later publications to the topics that `filter` matches; a topic that several
    /// rules match takes the one set last. A refused publication reaches no subscriber and is
    /// not retained. MQTT 5.0 PUBACK and PUBREC answer it with `reasonCode`, and `reasonString`
    /// if not null; MQTT 3.1.1 acknowledges it as usual, and QoS 0 has no answer.
    ///
    /// @throws IllegalArgumentException if `filter` is not a valid topic filter, `reasonCode`
    ///         is not a PUBACK and PUBREC failure, or `reasonString` is not an MQTT UTF-8 string
    public void refusePublications(
            String filter,
            int reasonCode,
            @Nullable String reasonString
    ) {
        Reason reason = Reason.checked(PacketType.PUBACK, reasonCode, reasonString, null);
        routing.refusePublications(filter, reason);
    }

    /// Lets later publications to the topics that `filter` matches go through again, unless
    /// another rule refuses them.
    ///
    /// @throws IllegalArgumentException if `filter` is not a valid topic filter
    public void clearPublicationRule(String filter) {
        routing.clearPublicationRule(filter);
    }

    /// Refuses later CONNECT requests with exactly `clientId`, before they touch a session or a
    /// Will. MQTT 5.0 CONNACK answers them with `reasonCode`, `reasonString`, and
    /// `serverReference`, the last two if not null. MQTT 3.1.1 answers with the return code of
    /// `reasonCode`, if it has one, and otherwise closes the connection without CONNACK.
    ///
    /// @throws IllegalArgumentException if `reasonCode` is not a CONNACK failure, a Server
    ///         Reference goes with another reason than Use another server or Server moved, or a
    ///         string is not an MQTT UTF-8 string
    public void refuseConnections(
            String clientId,
            int reasonCode,
            @Nullable String reasonString,
            @Nullable String serverReference
    ) {
        Reason reason = Reason.checked(
                PacketType.CONNACK,
                reasonCode,
                reasonString,
                serverReference
        );
        routing.refuseConnections(clientId, reason);
    }

    /// Lets later CONNECT requests with exactly `clientId` through again.
    public void clearConnectionRule(String clientId) {
        routing.clearConnectionRule(clientId);
    }

    /// Grants later subscriptions to exactly `filter` at most the QoS of `maximumLevel`.
    ///
    /// @throws IllegalArgumentException if `filter` is not a valid topic filter or the level is
    ///         not 0, 1, or 2
    public void grantAtMost(String filter, int maximumLevel) {
        routing.grantAtMost(filter, maximumLevel);
    }

    /// Lets later subscriptions to exactly `filter` follow no rule again.
    ///
    /// @throws IllegalArgumentException if `filter` is not a valid topic filter
    public void clearSubscriptionRule(String filter) {
        routing.clearSubscriptionRule(filter);
    }

    /// Holds back the responses of `type` to every connection of `clientId`, from now until
    /// [#releaseResponses(Class, String)].
    public void holdResponses(Class<? extends Packet> type, String clientId) {
        routing.holdResponses(type, clientId);
    }

    /// Stops holding back the responses of `type` to `clientId`, and sends those held in the
    /// order they were held, ahead of later responses.
    public void releaseResponses(Class<? extends Packet> type, String clientId) {
        routing.releaseResponses(type, clientId);
    }

    /// The responses that connections hold back now, each connection's in the order held.
    public List<Held> heldResponses() {
        return routing.heldResponses();
    }

    /// The identifiers of the clients whose connections are open.
    public Set<String> connectedClients() {
        return routing.connectedClients();
    }

    /// Ends the open connection of `clientId` for an administrative reason, which an MQTT 5.0
    /// client learns from DISCONNECT. The client did not disconnect, so its Will goes out as for
    /// a lost connection. Does nothing when the client has no open connection.
    public void disconnect(String clientId) {
        routing.disconnect(clientId, new Reason(ReasonCode.ADMINISTRATIVE_ACTION, null, null));
    }

    /// Ends the open connection of `clientId` as [#disconnect(String)] does, but tells an MQTT
    /// 5.0 client `reasonCode`, `reasonString`, and `serverReference`, the last two if not
    /// null. Even Normal disconnection leaves the Will to go out, because the client did not
    /// disconnect.
    ///
    /// @throws IllegalArgumentException if a server cannot send `reasonCode` in DISCONNECT, a
    ///         Server Reference goes with another reason than Use another server or Server
    ///         moved, or a string is not an MQTT UTF-8 string
    public void disconnect(
            String clientId,
            int reasonCode,
            @Nullable String reasonString,
            @Nullable String serverReference
    ) {
        Reason reason = Reason.checked(
                PacketType.DISCONNECT,
                reasonCode,
                reasonString,
                serverReference
        );
        routing.disconnect(clientId, reason);
    }

    /// Makes the broker announce these capabilities to the MQTT 5.0 connections that it accepts
    /// from now on, and hold those connections to them; connections accepted before keep theirs.
    /// The maximum QoS is a level. A Receive Maximum of 65,535 and a Maximum Packet Size of
    /// 268,435,460 bytes are what a client assumes when CONNACK leaves them out, so the broker
    /// leaves them out. The Server Keep Alive stays as the broker was built with.
    ///
    /// @throws IllegalArgumentException if the level is not 0, 1, or 2, or a value is outside
    ///         what MQTT 5.0 can announce
    public void announce(
            int maximumQosLevel,
            boolean retainAvailable,
            boolean wildcardSubscriptionAvailable,
            boolean subscriptionIdentifiersAvailable,
            int receiveMaximum,
            int maximumPacketSize,
            int topicAliasMaximum
    ) {
        var capabilities = new Capabilities(
                Qos.of(maximumQosLevel),
                retainAvailable,
                wildcardSubscriptionAvailable,
                subscriptionIdentifiersAvailable,
                (receiveMaximum == 65_535) ? null : receiveMaximum,
                (maximumPacketSize == Connection.MAX_PACKET_SIZE) ? null : maximumPacketSize,
                topicAliasMaximum,
                routing.settings.capabilities().serverKeepAlive()
        );
        routing.announce(capabilities);
    }

    /// A response that a connection of `clientId` holds back.
    public record Held(String clientId, Packet response) {
    }
}
