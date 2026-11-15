package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
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

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.internal.concurrent.Timer;
import org.mqtt4j.hato.internal.concurrent.TimerService;
import org.mqtt4j.hato.internal.log.Log;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.PacketType;

import static java.util.Objects.requireNonNull;

/// One listener whose connections share the bounded, in-memory sessions of one broker.
public final class BrokerServer implements AutoCloseable {

    private static final Log log = Log.of(BrokerServer.class);

    // How the test controls find the server of a broker. MqttBroker, which alone can reach its
    // server, sets it as it starts each broker, before the broker exists.
    private static volatile @Nullable Function<MqttBroker, BrokerServer> servers;

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);

    // How many clients the broker serves at once.
    private static final int MAX_CLIENTS = 10_000;

    // How many connections may wait for the broker to accept a CONNECT on them. Clients that
    // all connect at once wait here for a moment: on two processors, 10,000 and 20,000 such
    // clients left a few hundred waiting at the most, and the broker closed none of them.
    // Ceiling: the listen backlog, which is the platform's default, let those clients in
    // slowly. A longer backlog lets more wait here at once, and needs the measurement again.
    private static final int MAX_AWAITING_CONNECT = 1024;

    // The largest packet that the broker takes from a client. A smaller one than the framing
    // format allows keeps what one client can have the broker hold at a time small.
    private static final int MAX_PACKET_SIZE = 1024 * 1024;

    // The largest CONNECT that the broker takes, from a peer that it knows nothing of yet. A
    // CONNECT whose strings all have their greatest length is about a third of this, so only
    // one that carries large properties besides is too large.
    private static final int MAX_CONNECT_SIZE = 1024 * 1024;

    private static final long SEND_BYTES = 16L * 1024 * 1024;

    private static final int MAX_INFLIGHT = 128;

    // How many QoS 1 and 2 deliveries may wait for a session, also while its client is away.
    private static final int MAX_QUEUED = 1000;

    // The QoS 0 deliveries that may wait for a session have a number of their own, which stays
    // small whatever the other is: a deeper queue would only have a client that falls behind
    // get older ones, and none of them waits for a client that is away.
    private static final int MAX_QUEUED_AT_MOST_ONCE = 128;

    // How many sessions may outlive their connections: as many as there may be clients.
    private static final int MAX_SESSIONS = 10_000;

    // How long a session may outlive its connection at the most. The sessions of clients that
    // never come back give their places up after it, where nobody is there to remove them.
    private static final Duration MAX_SESSION_EXPIRY = Duration.ofHours(24);

    // One session's subscriptions are bounded by number, because every publication is matched
    // against each of them, and by the length of their filters in all, because a single filter
    // can have 65,535 bytes.
    private static final int MAX_SUBSCRIPTIONS = 10_000;

    private static final long MAX_SUBSCRIPTION_LENGTH = 1024 * 1024;

    // Retained messages are bounded like the other state that the broker keeps: by count, as
    // sessions are, and by size, as each session's waiting deliveries are. The size is what
    // they hold on the heap.
    private static final int MAX_RETAINED = 10_000;

    private static final long MAX_RETAINED_BYTES = 64L * 1024 * 1024;

    // The share of the largest heap that the retained messages hold at most unless a setting
    // says otherwise: an eighth, so that a small heap does not fill with what a large one may
    // retain.
    private static final int RETAINED_MEMORY_SHARE = 8;

    // Every response that a test holds takes one of the client's packet identifiers, so a client
    // that follows the protocol never holds more.
    private static final int MAX_HELD = 65_535;

    // The share of the largest heap that the deliveries that wait for all the sessions may take
    // together, whatever the limits of each session allow in sum: a quarter. The broker shares
    // the heap with the application that embeds it.
    private static final int DELIVERY_MEMORY_SHARE = 4;

    // How much of that memory a thread of the actor pool fetches for itself at a time.
    private static final long DELIVERY_MEMORY_UNIT = 256 * 1024;

    // How many selector threads do the socket I/O, and how many threads the actor pool has. On
    // two processors these numbers made a publication to many subscribers wait the shortest;
    // fewer selectors moved more QoS 0 publications at full speed, but such a publication and
    // QoS 1 exchanges waited longer. On four processors the numbers met the targets that the
    // broker has at that size, and a third or a fourth pool thread moved less, not more.
    // Ceiling: more than four processors are unmeasured, and so are other numbers of
    // selectors on four.
    private static final int SELECTORS = 4;

    private static final int ACTORS = 2;

    // The numbers for a broker with one processor, where more threads only take the processor
    // from each other. With these, QoS 1 exchanges went a tenth faster there, and the slowest
    // of them, and of the deliveries to a thousand subscribers, waited a quarter to two fifths
    // less.
    private static final int SELECTORS_ON_ONE_PROCESSOR = 2;

    private static final int ACTORS_ON_ONE_PROCESSOR = 1;

    // How long the accepting thread waits before it tries again after a connection could not
    // be accepted. The cause, such as a process without a free file descriptor, lasts until
    // something else in the process gives way, so trying at once would only spin.
    private static final long ACCEPT_RETRY_MILLIS = 100;

    private final ServerSocketChannel listener;

    // Tells the accepting thread that a connection waits at the listener, which is all that it
    // watches. The thread waits here and never inside accept: when a listener is closed just as
    // a thread enters a blocking accept, the closing thread can wait in the kernel until
    // something else in the process uses a file descriptor (seen on macOS), which a quiet
    // process never does.
    private final Selector acceptor;

    // The listener's registration with that selector.
    private final SelectionKey acceptKey;

    private final Routing routing;

    // The thread that accepts the connections.
    private final Thread accepting;

    // Whether the accepting thread has warned yet that it closed a connection at once, for the
    // limit on those that wait for their CONNECT. Only that thread uses it.
    private boolean turnedAway;

    private BrokerServer(ServerSocketChannel listener, BrokerSettings settings) throws IOException {
        this.listener = listener;
        acceptor = Selector.open();
        try {
            listener.configureBlocking(false);
            acceptKey = listener.register(acceptor, SelectionKey.OP_ACCEPT);
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

    // Starts a broker with the default settings and the threads that suit this machine.
    static BrokerServer start(InetSocketAddress address) throws IOException {
        return start(address, Runtime.getRuntime().availableProcessors());
    }

    // Starts a broker with the default settings and the threads that suit a machine of
    // `processors` processors.
    static BrokerServer start(InetSocketAddress address, int processors) throws IOException {
        return builder().start(address, processors);
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

    /// A builder with the broker's default settings, for MqttBrokerBuilder and the tests.
    public static Builder builder() {
        return new Builder();
    }

    /// Internal limits and test hooks, copied when the broker starts. Not thread safe.
    /// Values are checked together at [#start(InetSocketAddress)], so they can be set in any order.
    /// MqttBrokerBuilder checks the limits that it sets before it passes them on.
    ///
    /// A builder starts with the numbers of threads for two processors or more, whatever the
    /// machine has: a test that stops one thread of the broker counts on the others.
    /// [#start(InetSocketAddress, int)] picks the numbers for the machine instead.
    public static final class Builder {

        private Duration connectTimeout = CONNECT_TIMEOUT;

        private int maxAwaitingConnect = MAX_AWAITING_CONNECT;

        private int maxClients = MAX_CLIENTS;

        private int maxPacketSize = MAX_PACKET_SIZE;

        private int maxConnectSize = MAX_CONNECT_SIZE;

        private long sendBytes = SEND_BYTES;

        private int maxInflight = MAX_INFLIGHT;

        private long maxInflightBytes = SEND_BYTES;

        private int maxQueued = MAX_QUEUED;

        private long maxQueuedBytes = SEND_BYTES;

        private int maxQueuedAtMostOnce = MAX_QUEUED_AT_MOST_ONCE;

        private long maxQueuedAtMostOnceBytes = SEND_BYTES;

        private int maxSessions = MAX_SESSIONS;

        private Duration maxSessionExpiry = MAX_SESSION_EXPIRY;

        private int maxSubscriptions = MAX_SUBSCRIPTIONS;

        private long maxSubscriptionLength = MAX_SUBSCRIPTION_LENGTH;

        private int maxRetained = MAX_RETAINED;

        private long maxRetainedBytes = Math.min(
                MAX_RETAINED_BYTES,
                Runtime.getRuntime().maxMemory() / RETAINED_MEMORY_SHARE
        );

        private int maxHeld = MAX_HELD;

        private long maxDeliveryMemory =
                Runtime.getRuntime().maxMemory() / DELIVERY_MEMORY_SHARE;

        private long deliveryMemoryUnit = DELIVERY_MEMORY_UNIT;

        private int selectors = SELECTORS;

        private int actors = ACTORS;

        private Capabilities capabilities = Capabilities.DEFAULT;

        private BrokerSettings.@Nullable DropListener dropListener;

        private Timer scheduler = TimerService.broker;

        private Consumer<Packet> beforeProcess = _ -> { };

        private Consumer<Packet> beforeHandle = _ -> { };

        private Predicate<Connection> deliveryWriteHeld = _ -> false;

        private Runnable beforeSelect = () -> { };

        private BrokerSettings.AcceptHook beforeAccept = () -> { };

        private Consumer<String> beforeHandOver = _ -> { };

        private Builder() {
        }

        Builder connectTimeout(Duration value) {
            connectTimeout = requireNonNull(value, "connectTimeout must not be null");
            return this;
        }

        Builder maxAwaitingConnect(int value) {
            maxAwaitingConnect = value;
            return this;
        }

        public Builder maxClients(int value) {
            maxClients = value;
            return this;
        }

        public Builder maxPacketSize(int value) {
            maxPacketSize = value;
            return this;
        }

        Builder maxConnectSize(int value) {
            maxConnectSize = value;
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

        public Builder maxQueued(int value) {
            maxQueued = value;
            return this;
        }

        public Builder maxQueuedBytes(long value) {
            maxQueuedBytes = value;
            return this;
        }

        Builder maxQueuedAtMostOnce(int value) {
            maxQueuedAtMostOnce = value;
            return this;
        }

        Builder maxQueuedAtMostOnceBytes(long value) {
            maxQueuedAtMostOnceBytes = value;
            return this;
        }

        public Builder maxSessions(int value) {
            maxSessions = value;
            return this;
        }

        public Builder maxSessionExpiry(Duration value) {
            maxSessionExpiry = requireNonNull(value, "maxSessionExpiry must not be null");
            return this;
        }

        Builder maxSubscriptions(int value) {
            maxSubscriptions = value;
            return this;
        }

        Builder maxSubscriptionLength(long value) {
            maxSubscriptionLength = value;
            return this;
        }

        public Builder maxRetained(int value) {
            maxRetained = value;
            return this;
        }

        public Builder maxRetainedBytes(long value) {
            maxRetainedBytes = value;
            return this;
        }

        Builder maxHeld(int value) {
            maxHeld = value;
            return this;
        }

        Builder maxDeliveryMemory(long value) {
            maxDeliveryMemory = value;
            return this;
        }

        Builder deliveryMemoryUnit(long value) {
            deliveryMemoryUnit = value;
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

        Builder dropListener(BrokerSettings.DropListener value) {
            dropListener = requireNonNull(value, "dropListener must not be null");
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

        Builder beforeAccept(BrokerSettings.AcceptHook value) {
            beforeAccept = requireNonNull(value, "beforeAccept must not be null");
            return this;
        }

        Builder beforeHandOver(Consumer<String> value) {
            beforeHandOver = requireNonNull(value, "beforeHandOver must not be null");
            return this;
        }

        /// Starts a broker at `address` with the numbers of threads that suit a machine of
        /// `processors` processors, in place of those that the builder holds.
        public BrokerServer start(InetSocketAddress address, int processors) throws IOException {
            if (processors == 1) {
                selectors(SELECTORS_ON_ONE_PROCESSOR).actors(ACTORS_ON_ONE_PROCESSOR);
            } else {
                selectors(SELECTORS).actors(ACTORS);
            }
            return start(address);
        }

        BrokerServer start(InetSocketAddress address) throws IOException {
            requireNonNull(address, "address must not be null");
            if (connectTimeout.isNegative()
                || connectTimeout.isZero()
                || maxAwaitingConnect <= 0
                || maxClients <= 0
                || maxPacketSize <= 0
                || maxPacketSize > Connection.MAX_PACKET_SIZE
                || maxConnectSize <= 0
                || sendBytes <= 0
                || maxInflight <= 0
                || maxInflight > Connection.SEND_COUNT
                || maxInflightBytes <= 0
                || maxInflightBytes > sendBytes
                || maxQueued <= 0
                || maxQueuedBytes <= 0
                || maxQueuedAtMostOnce <= 0
                || maxQueuedAtMostOnceBytes <= 0
                || maxSessions <= 0
                || maxSessionExpiry.toSeconds() <= 0
                || maxSubscriptions <= 0
                || maxSubscriptionLength <= 0
                || maxRetained <= 0
                || maxRetainedBytes <= 0
                || maxHeld <= 0
                || maxDeliveryMemory <= 0
                || deliveryMemoryUnit < 0
                || selectors <= 0
                || actors <= 0
            ) {
                throw new IllegalArgumentException(
                        "Broker limits must be positive and window fit queue"
                );
            }
            var settings = new BrokerSettings(
                    connectTimeout.toNanos(),
                    maxAwaitingConnect,
                    maxClients,
                    maxPacketSize,
                    maxConnectSize,
                    sendBytes,
                    maxInflight,
                    maxInflightBytes,
                    maxQueued,
                    maxQueuedBytes,
                    maxQueuedAtMostOnce,
                    maxQueuedAtMostOnceBytes,
                    maxSessions,
                    // No session lasts longer than the longest interval that a client can ask
                    // for, which keeps it without end.
                    Math.min(maxSessionExpiry.toSeconds(), Session.NEVER),
                    maxSubscriptions,
                    maxSubscriptionLength,
                    maxRetained,
                    maxRetainedBytes,
                    maxHeld,
                    maxDeliveryMemory,
                    deliveryMemoryUnit,
                    selectors,
                    actors,
                    capabilities,
                    dropListener,
                    scheduler,
                    beforeProcess,
                    beforeHandle,
                    deliveryWriteHeld,
                    beforeSelect,
                    beforeAccept,
                    beforeHandOver
            );
            ServerSocketChannel listener = ServerSocketChannel.open();
            try {
                listener.bind(address);
                var server = new BrokerServer(listener, settings);
                try {
                    // The pool would start a thread for each of its first tasks. Starting them
                    // now gives the broker all its threads from the start, whatever comes.
                    server.routing.actors.prestartAllCoreThreads();
                    DropReports reports = server.routing.dropReports;
                    if (reports != null) {
                        reports.start();
                    }
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
                // Only this thread adds to the connections that wait for their CONNECT, so the
                // limit holds exactly.
                if (routing.awaitingConnect.get() >= routing.settings.maxAwaitingConnect()) {
                    turnAway(socket);
                    continue;
                }
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

    // Closes a connection that came while as many as the broker allows waited for their
    // CONNECT. It has sent nothing that the broker has read, and learns nothing but the close:
    // the broker does not know which protocol version it speaks.
    private void turnAway(Socket socket) {
        if (turnedAway) {
            log.debug("Broker on port {} closed the connection from {} at once",
                    port(), socket.getRemoteSocketAddress());
        } else {
            turnedAway = true;
            log.warn(
                    "Broker on port {} closed the connection from {} at once, because {}"
                            + " connections wait for their CONNECT, its limit; later ones are"
                            + " logged at debug level",
                    port(),
                    socket.getRemoteSocketAddress(),
                    routing.settings.maxAwaitingConnect()
            );
        }
        try {
            socket.close();
        } catch (IOException failure) {
            log.debug("Closing the connection from {} failed",
                    socket.getRemoteSocketAddress(), failure);
        }
    }

    // The next connection that waits at the listener, once there is one. close() ends the wait
    // with an exception, by closing the listener and the selector.
    //
    // An attempt can fail while the listener is sound: the process may lack what a connection
    // takes, as when it has no file descriptor left. That passes, and the connection waits at
    // the listener meanwhile. So the thread tries again after a pause, rather than end the
    // listener and with it the broker and the connections that it serves.
    private Socket accepted() throws IOException {
        boolean failing = false;
        while (true) {
            try {
                routing.settings.beforeAccept().run();
                SocketChannel channel = listener.accept();
                if (channel != null) {
                    if (failing) {
                        log.info("Broker on port {} accepts connections again", port());
                    }
                    return channel.socket();
                }
            } catch (ClosedChannelException closed) {
                throw closed;
            } catch (IOException failure) {
                if (failing) {
                    log.debug("Broker on port {} cannot accept a connection", port(), failure);
                } else {
                    failing = true;
                    log.warn("Broker on port {} cannot accept a connection and keeps trying;"
                            + " later failures are logged at debug level", port(), failure);
                }
                // The connection that could not be accepted keeps the key ready, so the pause
                // looks away from it. Closing the selector still ends the wait.
                acceptKey.interestOps(0);
                acceptor.select(_ -> { }, ACCEPT_RETRY_MILLIS);
                acceptKey.interestOps(SelectionKey.OP_ACCEPT);
                endIfInterrupted();
                continue;
            }
            // The key tells nothing more than the return does: the loop asks the listener.
            acceptor.select(_ -> { });
            endIfInterrupted();
        }
    }

    // An interrupted thread no longer waits in a selector. It ends the listener, as an interrupt
    // of a blocking accept does.
    private static void endIfInterrupted() throws InterruptedIOException {
        if (Thread.interrupted()) {
            throw new InterruptedIOException("Interrupted while waiting for a connection");
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
        // Only a broker that runs with assertions, as in tests, looks and waits here.
        assert routing.deliveryMemoryAddsUp();
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
            DropReports reports = routing.dropReports;
            if (reports != null) {
                reports.close();
            }
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
