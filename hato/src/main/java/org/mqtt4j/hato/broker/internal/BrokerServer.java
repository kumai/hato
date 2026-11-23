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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.broker.BrokerListener;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.internal.concurrent.Timer;
import org.mqtt4j.hato.internal.concurrent.TimerService;
import org.mqtt4j.hato.internal.inproc.InprocBrokers;
import org.mqtt4j.hato.internal.log.Log;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.PacketType;

import static java.util.Objects.requireNonNull;

/// The listeners of one broker, plain, TLS, or both, whose connections share its bounded,
/// in-memory sessions.
public final class BrokerServer implements AutoCloseable {

    private static final Log log = Log.of(BrokerServer.class);

    // How the test controls find the server of a broker. MqttBroker, which alone can reach its
    // server, sets it as it starts each broker, before the broker exists.
    private static volatile @Nullable Function<MqttBroker, BrokerServer> servers;

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);

    // How many clients the broker serves at once.
    private static final int MAX_CLIENTS = 10_000;

    // How many connections may wait for the broker to accept a CONNECT on them. While that many
    // wait, the accepting thread takes no further connection, which waits in the listener's
    // backlog until one of them is accepted or ends.
    private static final int MAX_AWAITING_CONNECT = 1024;

    // How many connections the listener holds before the broker takes them, as when thousands
    // of clients connect again at once after an outage. With the platform's default of 50,
    // 10,000 clients that connected at once to a broker on two processors took 31 seconds to
    // all have their CONNACK, as many had to send their SYN or their CONNECT again; with this
    // backlog and the wait at the limit above, they took under a second. Linux holds no more
    // than its somaxconn, 4,096 by default.
    private static final int BACKLOG = 4096;

    // The largest packet that the broker takes from a client. A smaller one than the framing
    // format allows keeps what one client can have the broker hold at a time small.
    private static final int MAX_PACKET_SIZE = 1024 * 1024;

    // The largest CONNECT that the broker takes, from a peer that it knows nothing of yet. As
    // many such peers as may wait for their CONNECT can each have the broker hold this much
    // before the broker knows any of them, and a decoded CONNECT more than that, so it is kept
    // small. Credentials and Wills of the usual sizes fit many times over; a CONNECT whose
    // strings all have their greatest length, about 320 KiB, does not.
    private static final int MAX_CONNECT_SIZE = 64 * 1024;

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

    // One session's subscriptions are bounded by number, because every publication that reaches
    // the session is matched against each of them, and by what they hold on the heap in all (see
    // Session.sizeOf), because a single filter can have 65,535 bytes and each of its levels
    // takes room in the index of subscriptions. The bytes hold as many filters of six levels and
    // some sixty characters as the number allows.
    private static final int MAX_SUBSCRIPTIONS = 10_000;

    private static final long MAX_SUBSCRIPTION_BYTES = 24L * 1024 * 1024;

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

    // How long the application's authenticator may take over one decision.
    private static final Duration AUTHENTICATION_TIMEOUT = Duration.ofSeconds(10);

    // How many decisions of the authenticator may run at once. With decisions of 50 ms each,
    // these take about 300 clients a second, as hundreds that reconnect together after an
    // outage do.
    // Ceiling: an application whose decisions take long refuses more of a crowd that connects
    // at once; a public setting, or an executor of the application's, would lift that.
    private static final int MAX_AUTHENTICATIONS = 16;

    // How many threads run the delegated tasks of TLS handshakes at once: the costly part of a
    // handshake, about two milliseconds of a processor each with an RSA key of 2048 bits and a
    // client certificate, and the calls into the application's key and trust managers. One
    // leaves the other processors to the clients already connected while a crowd reconnects
    // with TLS. With 10,000 clients reconnecting beside 200 that publish, on two processors,
    // more threads let the slowest waits of those 200 pass a second near the end of the crowd,
    // for a crowd that got in a fifth sooner at most; on one processor, every number tried left
    // them waiting for seconds, and one the least.
    // Ceiling: the handshakes of a crowd take turns on this thread, about 500 a second on any
    // machine, and behind a key or trust manager that waits, as for a revocation check; more
    // threads, or a setting for them, would lift that. More than two processors are unmeasured.
    private static final int MAX_TLS_TASKS = 1;

    // How long the accepting thread waits before it tries again after a connection could not
    // be accepted. The cause, such as a process without a free file descriptor, lasts until
    // something else in the process gives way, so trying at once would only spin.
    private static final long ACCEPT_RETRY_MILLIS = 100;

    // How long a shutdown waits between its looks at whether the clients have what waits for
    // them: briefly at first, as a broker with little to deliver soon has, then longer.
    private static final long FIRST_LOOK_NANOS = 1_000_000;

    private static final long LAST_LOOK_NANOS = 16_000_000;

    // How long a shutdown gives the clients to close their connections once they have learned
    // of its end, whether or not its deadline has passed. A client closes as it reads the end,
    // so only one that has stopped reading takes this long.
    private static final long CLOSING_NANOS = 1_000_000_000;

    // The sockets that the broker listens on: the plain one, the TLS one, or both, in that order.
    private final List<Listening> listenings;

    // The ports of the plain listener and of the TLS listener, or -1 for one that the broker does
    // not have.
    private final int plainPort;

    private final int tlsPort;

    // What the connections of the TLS listener share, if the broker has one.
    private final @Nullable TlsListener tls;

    // The name that the broker holds while it runs.
    private final InprocBrokers.Registration registration;

    // Tells the accepting thread that a connection waits at a listener, which is all that it
    // watches, and wakes it as a place frees among the connections that wait for their CONNECT
    // while it waits for one. The thread waits here and never inside accept: when a listener is
    // closed just as a thread enters a blocking accept, the closing thread can wait in the
    // kernel until something else in the process uses a file descriptor (seen on macOS), which
    // a quiet process never does.
    private final Selector acceptor;

    private final Routing routing;

    // The thread that accepts the connections.
    private final Thread accepting;

    // Whether the accepting thread has told yet that it waits for a place among the connections
    // that wait for their CONNECT. Only that thread uses it.
    private boolean waitedForPlace;

    // The result of the shutdown, once one has begun: whether the clients had what waited for
    // them. Guarded by this.
    private @Nullable CompletableFuture<Boolean> shutdown;

    // Set by close(), and by an interrupt of a caller that waits for a shutdown, to have a
    // shutdown end the broker at once.
    private volatile boolean cutShort;

    // The listener that the accepting thread asks first for the next connection, so that one
    // whose connections keep coming leaves the other its turn. Only that thread uses it.
    private int nextListening;

    // `plain` and `secure` are the bound sockets of the plain and the TLS listener, or null for
    // one that the broker does not have, and `tlsSettings` what the TLS listener's connections
    // take.
    private BrokerServer(
            @Nullable ServerSocketChannel plain,
            @Nullable ServerSocketChannel secure,
            @Nullable TlsSettings tlsSettings,
            InprocBrokers.Registration registration,
            BrokerSettings settings
    ) throws IOException {
        this.registration = registration;
        acceptor = Selector.open();
        try {
            plainPort = (plain == null) ? -1 : plain.socket().getLocalPort();
            tlsPort = (secure == null) ? -1 : secure.socket().getLocalPort();
            // Threads and logs know the broker by the port of its plain listener, or of its TLS
            // listener when it has no plain one.
            int port = (plain == null) ? tlsPort : plainPort;
            // The parts whose ends end the broker: the accepting thread, the selectors, the loop
            // of the in-process connections, the actor pool, the two threads that call the
            // listener, if there is one, the pool that calls the authenticator, if there is
            // one, and the threads of the TLS handshakes, if there is a TLS listener.
            int parts = 3
                    + settings.selectors()
                    + ((settings.listener() == null) ? 0 : 2)
                    + ((settings.authenticator() == null) ? 0 : 1)
                    + ((secure == null) ? 0 : 1);
            var termination = new Termination(parts, port);
            routing = new Routing(
                    settings,
                    port,
                    this::fail,
                    acceptor::wakeup,
                    termination
            );
            tls = (secure == null || tlsSettings == null) ? null : new TlsListener(
                    tlsSettings.context(),
                    tlsSettings.parameters(),
                    tlsPort,
                    port,
                    settings.maxTlsTasks(),
                    settings.maxAwaitingConnect(),
                    termination
            );
            var listening = new ArrayList<Listening>(2);
            if (plain != null) {
                listening.add(listen(plain, null));
            }
            if (secure != null) {
                listening.add(listen(secure, tls));
            }
            listenings = List.copyOf(listening);
            accepting = Thread.ofPlatform().daemon()
                    .inheritInheritableThreadLocals(false)
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

    // Has the accepting thread watch `channel` for connections, which come through TLS with `tls`
    // or plain for null.
    private Listening listen(
            ServerSocketChannel channel,
            @Nullable TlsListener tls
    ) throws IOException {
        channel.configureBlocking(false);
        return new Listening(channel, channel.register(acceptor, SelectionKey.OP_ACCEPT), tls);
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

        // The broker's name, or null for an automatic one.
        private @Nullable String name;

        // The TLS listener, or null for none.
        private @Nullable TlsSettings tls;

        private int maxTlsTasks = MAX_TLS_TASKS;

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

        private long maxSubscriptionBytes = MAX_SUBSCRIPTION_BYTES;

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

        private @Nullable BrokerListener listener;

        private boolean recordActivity;

        private @Nullable Authenticator authenticator;

        private Duration authenticationTimeout = AUTHENTICATION_TIMEOUT;

        private int maxAuthentications = MAX_AUTHENTICATIONS;

        private Timer scheduler = TimerService.broker;

        private Consumer<Packet> beforeProcess = _ -> { };

        private Consumer<Packet> beforeHandle = _ -> { };

        private Predicate<Connection> deliveryWriteHeld = _ -> false;

        private Runnable beforeSelect = () -> { };

        private BrokerSettings.AcceptHook beforeAccept = () -> { };

        private Consumer<String> beforeHandOver = _ -> { };

        private Consumer<String> beforeLastShareStep = _ -> { };

        private Consumer<String> beforeTakeOver = _ -> { };

        private Consumer<String> beforeAttach = _ -> { };

        private Consumer<String> afterLeave = _ -> { };

        private Consumer<String> whileListing = _ -> { };

        private Runnable beforeAcceptingStarts = () -> { };

        private Builder() {
        }

        public Builder name(String value) {
            name = requireNonNull(value, "name must not be null");
            return this;
        }

        /// Listens with TLS at `address` too, or alone: every connection there takes an engine of
        /// `context` that `parameters` set, which the caller has checked and does not change any
        /// more.
        public Builder listenTls(
                InetSocketAddress address,
                SSLContext context,
                SSLParameters parameters
        ) {
            tls = new TlsSettings(
                    requireNonNull(address, "address must not be null"),
                    requireNonNull(context, "context must not be null"),
                    requireNonNull(parameters, "parameters must not be null")
            );
            return this;
        }

        Builder maxTlsTasks(int value) {
            maxTlsTasks = value;
            return this;
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

        Builder maxSubscriptionBytes(long value) {
            maxSubscriptionBytes = value;
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

        public Builder authenticator(Authenticator value) {
            authenticator = requireNonNull(value, "authenticator must not be null");
            return this;
        }

        public Builder authenticationTimeout(Duration value) {
            authenticationTimeout = requireNonNull(value, "authenticationTimeout must not be null");
            return this;
        }

        Builder maxAuthentications(int value) {
            maxAuthentications = value;
            return this;
        }

        public Builder listener(BrokerListener value) {
            listener = requireNonNull(value, "listener must not be null");
            return this;
        }

        /// Has the connections record what their clients do, for the list of clients.
        public Builder recordActivity() {
            recordActivity = true;
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

        Builder beforeLastShareStep(Consumer<String> value) {
            beforeLastShareStep = requireNonNull(value, "beforeLastShareStep must not be null");
            return this;
        }

        Builder beforeTakeOver(Consumer<String> value) {
            beforeTakeOver = requireNonNull(value, "beforeTakeOver must not be null");
            return this;
        }

        Builder beforeAttach(Consumer<String> value) {
            beforeAttach = requireNonNull(value, "beforeAttach must not be null");
            return this;
        }

        Builder afterLeave(Consumer<String> value) {
            afterLeave = requireNonNull(value, "afterLeave must not be null");
            return this;
        }

        Builder whileListing(Consumer<String> value) {
            whileListing = requireNonNull(value, "whileListing must not be null");
            return this;
        }

        Builder beforeAcceptingStarts(Runnable value) {
            beforeAcceptingStarts = requireNonNull(value, "beforeAcceptingStarts must not be null");
            return this;
        }

        /// Starts a broker that listens plain at `address`, or not at all for null, and with TLS
        /// as [#listenTls] set, with the numbers of threads that suit a machine of `processors`
        /// processors, in place of those that the builder holds.
        public BrokerServer start(
                @Nullable InetSocketAddress address,
                int processors
        ) throws IOException {
            if (processors == 1) {
                selectors(SELECTORS_ON_ONE_PROCESSOR).actors(ACTORS_ON_ONE_PROCESSOR);
            } else {
                selectors(SELECTORS).actors(ACTORS);
            }
            return start(address);
        }

        BrokerServer start(@Nullable InetSocketAddress address) throws IOException {
            if (address == null && tls == null) {
                throw new IllegalArgumentException("A broker needs a listener");
            }
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
                || maxSubscriptionBytes <= 0
                || maxRetained <= 0
                || maxRetainedBytes <= 0
                || maxHeld <= 0
                || maxDeliveryMemory <= 0
                || deliveryMemoryUnit < 0
                || selectors <= 0
                || actors <= 0
                || authenticationTimeout.isNegative()
                || authenticationTimeout.isZero()
                || maxAuthentications <= 0
                || maxTlsTasks <= 0
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
                    maxSubscriptionBytes,
                    maxRetained,
                    maxRetainedBytes,
                    maxHeld,
                    maxDeliveryMemory,
                    deliveryMemoryUnit,
                    selectors,
                    actors,
                    capabilities,
                    listener,
                    recordActivity,
                    authenticator,
                    authenticationTimeout.toNanos(),
                    maxAuthentications,
                    maxTlsTasks,
                    scheduler,
                    beforeProcess,
                    beforeHandle,
                    deliveryWriteHeld,
                    beforeSelect,
                    beforeAccept,
                    beforeHandOver,
                    beforeLastShareStep,
                    beforeTakeOver,
                    beforeAttach,
                    afterLeave,
                    whileListing,
                    beforeAcceptingStarts
            );
            // The name first, so that a broker whose name another one holds binds no port.
            String given = name;
            InprocBrokers.Registration registration = given == null
                    ? InprocBrokers.takeNext()
                    : InprocBrokers.take(given);
            try {
                return open(address, tls, registration, settings);
            } catch (IOException | RuntimeException | Error failure) {
                registration.release();
                throw failure;
            }
        }

        private static BrokerServer open(
                @Nullable InetSocketAddress address,
                @Nullable TlsSettings tls,
                InprocBrokers.Registration registration,
                BrokerSettings settings
        ) throws IOException {
            ServerSocketChannel plain = null;
            ServerSocketChannel secure = null;
            try {
                if (address != null) {
                    plain = ServerSocketChannel.open();
                    plain.bind(address, BACKLOG);
                }
                if (tls != null) {
                    secure = ServerSocketChannel.open();
                    secure.bind(tls.address(), BACKLOG);
                }
                var server = new BrokerServer(plain, secure, tls, registration, settings);
                try {
                    // The pool would start a thread for each of its first tasks. Starting them
                    // now gives the broker all its threads from the start, whatever comes.
                    server.routing.actors.prestartAllCoreThreads();
                    Notices notices = server.routing.notices;
                    if (notices != null) {
                        notices.start();
                    }
                    FailureNotice notice = server.routing.failureNotice;
                    if (notice != null) {
                        notice.start();
                    }
                    for (Reactor reactor : server.routing.reactors) {
                        reactor.start();
                    }
                    server.routing.inproc.start();
                    settings.beforeAcceptingStarts().run();
                    server.accepting.start();
                    // Clients in the same JVM reach the broker from now on, as TCP clients do.
                    registration.open(server.routing.inproc);
                    // Last, once every part is there to end: a start that fails before this
                    // leaves no thread that waits for parts that never started.
                    server.routing.termination.start();
                } catch (RuntimeException | Error failure) {
                    server.close();
                    throw failure;
                }
                return server;
            } catch (IOException | RuntimeException | Error failure) {
                closeAfter(plain, failure);
                closeAfter(secure, failure);
                throw failure;
            }
        }

        // Closes `channel`, if there is one, after `failure`, which keeps a failure to close.
        private static void closeAfter(@Nullable ServerSocketChannel channel, Throwable failure) {
            if (channel == null) {
                return;
            }
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    // The TLS listener of a broker: where it listens, and what every connection there takes.
    private record TlsSettings(
            InetSocketAddress address,
            SSLContext context,
            SSLParameters parameters
    ) {
    }

    // One socket that the broker listens on, its registration with the accepting thread's
    // selector, and the TLS that its connections come through, or null for plain ones.
    private record Listening(
            ServerSocketChannel channel,
            SelectionKey key,
            @Nullable TlsListener tls
    ) {
    }

    // A connection that a listener gave, and the TLS that it comes through, or null.
    private record Accepted(Socket socket, @Nullable TlsListener tls) {
    }

    /// The port that the broker goes by in its threads' names and its logs: that of its plain
    /// listener, or of its TLS listener when it has no plain one.
    public int port() {
        return routing.port;
    }

    /// The port of the plain listener, including after close.
    ///
    /// @throws IllegalStateException if the broker listens with TLS alone
    public int plainPort() {
        if (plainPort < 0) {
            throw new IllegalStateException("The broker listens with TLS alone");
        }
        return plainPort;
    }

    /// The port of the TLS listener, including after close.
    ///
    /// @throws IllegalStateException if the broker has no TLS listener
    public int tlsPort() {
        if (tlsPort < 0) {
            throw new IllegalStateException("The broker has no TLS listener");
        }
        return tlsPort;
    }

    public String name() {
        return registration.name();
    }

    private void accept() {
        try {
            while (true) {
                awaitPlace();
                Accepted next = accepted();
                Socket socket = next.socket();
                Connection connection = null;
                try {
                    connection = new Connection(
                            new SocketWire(socket, routing, next.tls()),
                            routing
                    );
                    if (!connection.start()) {
                        socket.close();
                        return;
                    }
                } catch (Throwable failure) {
                    if (connection != null) {
                        connection.close("start failed", null);
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
            // A shutdown closes the listener as close() does, which ends this thread without a
            // failure of the broker's.
            if (!routing.stopping) {
                if (!routing.closed) {
                    log.error("Broker listener on port {} failed", port(), failure);
                }
                fail(failure);
            }
        } finally {
            routing.termination.partEnded();
        }
    }

    // Waits while as many network connections as the broker allows wait for their CONNECT, until
    // one of them is accepted or ends; the connections that come meanwhile wait in the listener's
    // backlog, rather than be closed. The connection that frees a place wakes this thread
    // through the selector (see Routing.leftAwaitingConnect). Only this thread adds to the
    // network connections that wait, after this check, so no more than the limit wait at a
    // time.
    private void awaitPlace() throws IOException {
        int limit = routing.settings.maxAwaitingConnect();
        if (routing.awaitingConnect.get() < limit) {
            return;
        }
        if (waitedForPlace) {
            log.debug("Broker on port {} waits for a place among the {} connections that wait"
                    + " for their CONNECT", port(), limit);
        } else {
            waitedForPlace = true;
            log.info(
                    "Broker on port {} accepts no further connection while {} connections wait"
                            + " for their CONNECT, its limit, and goes on as they leave; later"
                            + " waits are logged at debug level",
                    port(),
                    limit
            );
        }
        // The connections that wait at the listeners keep their keys ready, so the wait looks
        // away from them. Closing the selector still ends the wait.
        watchListeners(false);
        while (routing.awaitingConnect.get() >= limit) {
            acceptor.select(_ -> { });
            endIfInterrupted();
        }
        watchListeners(true);
    }

    // Has the accepting thread's selector watch the listeners for connections, or look away.
    private void watchListeners(boolean watch) {
        for (Listening listening : listenings) {
            listening.key().interestOps(watch ? SelectionKey.OP_ACCEPT : 0);
        }
    }

    // The next connection that waits at a listener, once there is one. Each attempt asks the
    // listeners in turn, starting after the one that gave the last connection. close() ends the
    // wait with an exception, by closing the listeners and the selector.
    //
    // An attempt can fail while the listeners are sound: the process may lack what a connection
    // takes, as when it has no file descriptor left. That passes, and the connection waits at
    // its listener meanwhile. So the thread tries again after a pause, rather than end the
    // listeners and with them the broker and the connections that it serves.
    private Accepted accepted() throws IOException {
        boolean failing = false;
        while (true) {
            try {
                routing.settings.beforeAccept().run();
                for (int i = 0; i < listenings.size(); i++) {
                    int index = (nextListening + i) % listenings.size();
                    Listening listening = listenings.get(index);
                    SocketChannel channel = listening.channel().accept();
                    if (channel != null) {
                        if (failing) {
                            log.info("Broker on port {} accepts connections again", port());
                        }
                        nextListening = (index + 1) % listenings.size();
                        return new Accepted(channel.socket(), listening.tls());
                    }
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
                // The connection that could not be accepted keeps its key ready, so the pause
                // looks away from the listeners. Closing the selector still ends the wait.
                watchListeners(false);
                acceptor.select(_ -> { }, ACCEPT_RETRY_MILLIS);
                watchListeners(true);
                endIfInterrupted();
                continue;
            }
            // The keys tell nothing more than the returns do: the loop asks the listeners.
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
    // the same. Only a thread of the broker itself does not wait: one that ends the broker
    // after its failure goes on to close its own sockets.
    @Override
    public void close() {
        Boolean _ = offVirtualThread("close", () -> {
            closeHere();
            return true;
        });
    }

    private void closeHere() {
        boolean first;
        synchronized (this) {
            first = !routing.closed;
            routing.closed = true;
        }
        // A shutdown that waits for its clients to close ends the broker at once.
        cutShort = true;
        if (first) {
            end();
        }
        awaitThreads();
    }

    /// Shuts the broker down, as MqttBroker.shutdown(Duration) describes: returns once the
    /// broker has closed, and whether the clients that were connected had what waited for them
    /// within `timeout`.
    public boolean shutdown(Duration timeout) {
        return offVirtualThread("shutdown", () -> shutdownHere(timeout));
    }

    private boolean shutdownHere(Duration timeout) {
        CompletableFuture<Boolean> result;
        boolean first;
        synchronized (this) {
            if (shutdown == null && routing.closed) {
                return false;
            }
            first = shutdown == null;
            if (first) {
                shutdown = new CompletableFuture<>();
                routing.stopping = true;
            }
            result = requireNonNull(shutdown);
        }
        if (!first) {
            return awaitShutdown(result);
        }
        boolean delivered = false;
        try {
            delivered = shutDown(System.nanoTime() + timeout.toNanos());
        } catch (RuntimeException | Error failure) {
            // A broker that takes no further client must not stay so: it ends as close() ends it.
            close();
            throw failure;
        } finally {
            result.complete(delivered);
        }
        return delivered;
    }

    // Shuts the broker down by `deadline`, and returns whether the clients that were connected
    // had what waited for them by then. The broker takes no further connection: it frees its
    // name and its port at once, and its clients refuse a CONNECT that comes now (see Client).
    // It goes on serving the clients that it has, and has each connection read what has arrived
    // by now, which they sent before the shutdown and is theirs to have delivered. Once they
    // have it all, or at the deadline, the broker ends: it tells them so, and closes.
    private boolean shutDown(long deadline) {
        log.info("Broker on port {} shuts down: it takes no further connection, and delivers"
                + " what waits for its clients first", port());
        stopAccepting();
        for (Connection connection : routing.connections) {
            connection.loop.execute(connection, connection::catchUp);
        }
        boolean delivered = false;
        boolean interrupted = false;
        long pause = FIRST_LOOK_NANOS;
        while (!routing.closed && !cutShort) {
            if (delivered()) {
                delivered = true;
                break;
            }
            long left = deadline - System.nanoTime();
            if (left <= 0) {
                break;
            }
            LockSupport.parkNanos(this, Math.min(pause, left));
            if (Thread.interrupted()) {
                interrupted = true;
                break;
            }
            pause = Math.min(2 * pause, LAST_LOOK_NANOS);
        }
        if (delivered) {
            log.info("Broker on port {} delivered what waited for its clients, and closes",
                    port());
        } else if (!routing.closed) {
            log.info("Broker on port {} closes with {} deliveries waiting for its clients",
                    port(), routing.queuedDeliveries());
        }
        interrupted |= closeAfterShutdown();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return delivered;
    }

    // Whether the clients that are connected have what waits for them, judged by two looks at
    // the sessions in a row. Each look finds that every connected client has read what had
    // arrived as the shutdown began, that its session is idle and has nothing left for it, and
    // that its connection has written everything; and that every other session is idle. What a
    // client sent before the shutdown has then been routed by the end of the first look, as its
    // session has handled it, and a delivery that its routing made for a session that the first
    // look had passed by shows in the second one.
    // Ceiling: each look visits every session, so a shutdown of a broker with many sessions
    // looks less often; telling the shutdown as sessions settle would lift that.
    private boolean delivered() {
        return settled() && settled();
    }

    private boolean settled() {
        for (Session session : routing.sessions.values()) {
            Connection peer = session.connection;
            boolean done = (peer != null && peer.takesDeliveries())
                    ? peer.caughtUp() && peer.sentAll() && session.settled()
                    : session.idle();
            if (!done) {
                return false;
            }
        }
        return true;
    }

    // Ends the broker once a shutdown has delivered what it could, unless close() or a failure
    // has ended it already: the clients learn that the broker shuts down, and have a moment to
    // close their connections before the broker closes as close() does. Returns whether the
    // calling thread was interrupted meanwhile, which ends the moment at once.
    private boolean closeAfterShutdown() {
        boolean first;
        synchronized (this) {
            first = !routing.closed;
            routing.closed = true;
        }
        boolean interrupted = false;
        if (first) {
            try {
                for (Connection connection : routing.connections) {
                    connection.shutDown();
                }
                long until = System.nanoTime() + CLOSING_NANOS;
                while (!routing.connections.isEmpty()
                        && !cutShort
                        && System.nanoTime() - until < 0) {
                    LockSupport.parkNanos(this, FIRST_LOOK_NANOS);
                    if (Thread.interrupted()) {
                        interrupted = true;
                        break;
                    }
                }
            } finally {
                // The broker has closed, as close() finds it, so the end is this call's to make.
                end();
            }
        }
        awaitThreads();
        return interrupted;
    }

    // Waits for the shutdown that another call began, and returns its result. An interrupt has
    // that shutdown end the broker at once, as an interrupt of its own caller does, and stays
    // with the calling thread.
    private boolean awaitShutdown(CompletableFuture<Boolean> result) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return result.get();
                } catch (InterruptedException _) {
                    interrupted = true;
                    cutShort = true;
                } catch (ExecutionException _) {
                    // The shutdown completes its result normally, also when it fails.
                    return false;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// An observation of the broker's end; see MqttBroker.terminated().
    public CompletableFuture<Void> terminated() {
        return routing.termination.observe();
    }

    // Ends the broker for `failure`, which stopped its accepting thread or a selector, as close()
    // ends it. The listener hears of the failure once, unless the broker had ended already: then
    // the application asked for the end, or another failure came first. The notice goes out on a
    // thread of its own (see FailureNotice), while this one ends the broker without waiting for
    // it.
    private void fail(Throwable failure) {
        boolean first;
        synchronized (this) {
            first = !routing.closed;
            routing.closed = true;
        }
        if (first) {
            FailureNotice notice = routing.failureNotice;
            if (notice != null) {
                notice.fail(failure);
            }
            end();
        }
        awaitThreads();
    }

    // Returns once the accepting thread and the selectors have ended, unless the calling thread
    // is one of them (see close()).
    private void awaitThreads() {
        Thread current = Thread.currentThread();
        boolean own = current == accepting || current == routing.inproc.thread;
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
        awaitEnd(routing.inproc.thread);
        // Only a broker that runs with assertions, as in tests, looks and waits here.
        assert routing.deliveryMemoryAddsUp();
    }

    // Stops taking connections: frees the name and the ports, which another broker can take
    // from now on, and has the accepting thread end. Doing so again does nothing.
    private void stopAccepting() {
        registration.release();
        for (Listening listening : listenings) {
            try {
                listening.channel().close();
            } catch (IOException failure) {
                log.warn("Closing broker listener on port {} failed",
                        listening.channel().socket().getLocalPort(), failure);
            }
        }
        // A channel that a selector watches keeps its socket until the selector lets go of it.
        // Closing the selector does that here, so the ports are free from now on, and it wakes
        // the accepting thread.
        try {
            acceptor.close();
        } catch (IOException failure) {
            log.warn("Closing the accept selector of the broker on port {} failed",
                    port(), failure);
        }
    }

    // Ends the listeners, the connections, and the sessions, and has the selectors end.
    private void end() {
        try {
            stopAccepting();
            routing.clear();
        } finally {
            routing.actors.shutdown();
            ThreadPoolExecutor authentications = routing.authentications;
            if (authentications != null) {
                // The decisions that run are the application's code. An interrupt is all that
                // asks them to return; the broker's end waits until they have.
                authentications.shutdownNow();
            }
            TlsListener listener = tls;
            if (listener != null) {
                listener.close();
            }
            Notices notices = routing.notices;
            if (notices != null) {
                notices.close();
            }
            FailureNotice notice = routing.failureNotice;
            if (notice != null) {
                notice.end();
            }
            for (Reactor reactor : routing.reactors) {
                reactor.close();
            }
            routing.inproc.close();
        }
    }

    // Waits until `thread` has ended; one that never started has. An interrupt does not end
    // the wait, and stays with the calling thread.
    // Runs `work`, the operation `name` of the application's, here, or on a platform thread of its
    // own that this thread waits for when this thread is virtual. Ending connections, sessions,
    // and the broker takes locks that the broker's threads take too, and a platform thread that
    // waits for a lock behind a virtual thread sleeps while every carrier is busy, though the
    // lock is free (JDK 25; see Log): the broker's threads would stop with it. An interrupt of
    // the waiting thread goes on to the one that does the work, and stays with the waiting
    // thread as well. What `work` throws, this throws.
    private <T> T offVirtualThread(String name, Supplier<T> work) {
        if (!Thread.currentThread().isVirtual()) {
            return work.get();
        }
        var task = new FutureTask<T>(work::get);
        Thread worker = Thread.ofPlatform()
                .daemon()
                .inheritInheritableThreadLocals(false)
                .name("hato-broker-" + port() + "-" + name)
                .start(task);
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return task.get();
                } catch (InterruptedException _) {
                    interrupted = true;
                    worker.interrupt();
                } catch (ExecutionException failure) {
                    Throwable cause = failure.getCause();
                    if (cause instanceof RuntimeException unchecked) {
                        throw unchecked;
                    }
                    if (cause instanceof Error error) {
                        throw error;
                    }
                    throw new IllegalStateException(cause);
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

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

    // The identifiers of the sessions that the index of subscriptions finds for `topic`, for
    // tests.
    Set<String> indexedFor(Topic topic) {
        return routing.indexedFor(topic);
    }

    // Runs `whileHeld` holding the index of subscriptions, which a session that changes its
    // subscriptions, as one that is cleared does, waits for, for tests.
    void holdingIndex(Runnable whileHeld) {
        routing.holdingIndex(whileHeld);
    }

    /// What the broker counts, for MqttBroker.stats(). Once the broker has ended, it holds
    /// nothing, whatever its parts have yet to let go of, and its lifetime counts stay.
    public Counts counts() {
        var received = new long[3];
        var delivered = new long[3];
        for (Reactor reactor : routing.reactors) {
            reactor.traffic().addTo(received, delivered);
        }
        routing.inproc.traffic().addTo(received, delivered);
        var dropped = new long[3];
        for (int level = 0; level < dropped.length; level++) {
            dropped[level] = routing.dropped(level);
        }
        long retained = routing.retained.count();
        Notices notices = routing.notices;
        boolean ended = routing.closed;
        return new Counts(
                ended ? 0 : routing.clients(),
                ended ? 0 : routing.persistentSessions(),
                ended ? 0 : retained,
                ended ? 0 : routing.queuedDeliveries(),
                received,
                delivered,
                dropped,
                routing.refusedPublications(),
                (notices == null) ? 0 : notices.droppedNotices()
        );
    }

    /// The clients that the broker serves now, for MqttBroker.clients(), in no particular order.
    /// Once the broker has ended, it serves none.
    public List<ClientValues> clients() {
        if (routing.closed) {
            return List.of();
        }
        return routing.clients(routing.settings.scheduler().nanoTime());
    }

    /// Ends the open connection of `clientId` for an administrative reason, which an MQTT 5.0
    /// client learns from DISCONNECT. The client did not disconnect, so its Will goes out as for
    /// a lost connection. Does nothing when the client has no open connection. Returns whether
    /// it ended one: false also for a connection that was ending already.
    public boolean disconnect(String clientId) {
        var reason = new Reason(ReasonCode.ADMINISTRATIVE_ACTION, null, null);
        return offVirtualThread("disconnect", () -> routing.disconnect(clientId, reason));
    }

    /// Ends the connection that the listener heard of as `connection` as [#disconnect(String)]
    /// does, while it is the open connection of its client; a later connection of the same
    /// client stays open. Returns whether it ended it: false for a connection that has ended or
    /// was ending already, and for one that this broker did not report.
    public boolean disconnect(BrokerListener.Connection connection) {
        var reason = new Reason(ReasonCode.ADMINISTRATIVE_ACTION, null, null);
        return offVirtualThread("disconnect", () -> routing.disconnect(connection, reason));
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
        boolean _ = offVirtualThread("disconnect", () -> routing.disconnect(clientId, reason));
    }

    /// Makes the broker announce these capabilities to the MQTT 5.0 connections that it accepts
    /// from now on, and hold those connections to them; connections accepted before keep theirs,
    /// and MQTT 3.1.1 connections, which learn of none, are held to none. The maximum QoS is a
    /// level. A Receive Maximum of 65,535 and a Maximum Packet Size of 268,435,460 bytes are what
    /// a client assumes when CONNACK leaves them out, so the broker leaves them out. The Server
    /// Keep Alive stays as the broker was built with.
    ///
    /// @throws IllegalArgumentException if the level is not 0, 1, or 2, or a value is outside
    ///         what MQTT 5.0 can announce
    public void announce(
            int maximumQosLevel,
            boolean retainAvailable,
            boolean wildcardSubscriptionAvailable,
            boolean subscriptionIdentifiersAvailable,
            boolean sharedSubscriptionAvailable,
            int receiveMaximum,
            int maximumPacketSize,
            int topicAliasMaximum
    ) {
        var capabilities = new Capabilities(
                Qos.of(maximumQosLevel),
                retainAvailable,
                wildcardSubscriptionAvailable,
                subscriptionIdentifiersAvailable,
                sharedSubscriptionAvailable,
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

    /// What a broker counts: what it holds now, and lifetime counts, the arrays of which are by
    /// QoS level. See MqttBroker.stats().
    public record Counts(
            long connections,
            long persistentSessions,
            long retainedMessages,
            long queuedDeliveries,
            long[] received,
            long[] delivered,
            long[] dropped,
            long refusedPublications,
            long droppedNotices
    ) {
    }

    /// What the broker holds of one client that it serves, for MqttBroker.clients(): its
    /// connection as the broker reports it, how long ago the broker accepted the connection in
    /// nanoseconds, the subscriptions of its session in the order made, the deliveries that wait
    /// in its session, and what it has done on the connection, or null for a broker that does not
    /// record the activity of its clients.
    public record ClientValues(
            BrokerListener.Connection connection,
            long connectedNanos,
            List<Packet.Subscription> subscriptions,
            long queuedDeliveries,
            @Nullable ActivityValues activity
    ) {
    }

    /// What the client of one connection has done there: the PUBLISH packets that the broker
    /// took from it and the topic of the last one, which is null before the first, and how long
    /// ago that came in nanoseconds, which means nothing before the first; the PUBLISH packets
    /// that the broker wrote to it; and the publications and subscriptions that its scope
    /// refused, with the topic and the filter of the last ones. Each topic or filter is followed
    /// by whether ClientActivity cut it, in which case it is the beginning of a longer one.
    public record ActivityValues(
            long received,
            @Nullable String lastTopic,
            boolean lastTopicTruncated,
            long sinceLastReceivedNanos,
            long delivered,
            long refusedPublications,
            @Nullable String lastRefusedTopic,
            boolean lastRefusedTopicTruncated,
            long refusedSubscriptions,
            @Nullable String lastRefusedFilter,
            boolean lastRefusedFilterTruncated
    ) {
    }
}
