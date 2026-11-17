package org.mqtt4j.hato.broker;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.BrokerServer;
import org.mqtt4j.hato.internal.inproc.InprocBrokers;

import static java.util.Objects.requireNonNull;

/// Configures and starts brokers. Not thread safe.
/// Every setting has a default, so [#start()] can follow [MqttBroker#builder()] at once. Each
/// setter rejects a value that is invalid on its own with [IllegalArgumentException] at once, and
/// [#start()] rejects settings that do not work together in the same way: a listener at an
/// address other than a loopback one without an [Authenticator] or [#allowAnonymous()], TLS
/// parameters without a TLS listener, and TLS settings that it cannot use.
///
/// The limits here are the ones that an application may need to change. The broker keeps its
/// other limits internal, and they may change. The limits on retained messages and on the
/// deliveries that wait count bytes as the memory that these take, which is more than the bytes
/// of their packets.
public final class MqttBrokerBuilder {

    // The smallest packet that MQTT's framing can carry, and the largest that it allows.
    private static final int MIN_PACKET_SIZE = 2;

    private static final int MAX_PACKET_SIZE = 268_435_460;

    // The longest wait that a setting may give, as for the client's timeouts, and that
    // MqttBroker.shutdown takes.
    static final Duration MAX_TIMEOUT = Duration.ofMillis(Integer.MAX_VALUE);

    // The internal settings that this builder fills in. They hold the defaults of every limit,
    // so a setting that the application leaves alone keeps the broker's own value.
    private final BrokerServer.Builder settings = BrokerServer.builder();

    // Where the broker listens plain, and where with TLS; null for a listener that the
    // application has not set.
    private @Nullable InetSocketAddress address;

    private @Nullable InetSocketAddress tlsAddress;

    private @Nullable SSLContext sslContext;

    private @Nullable Consumer<SSLParameters> sslParameters;

    // Whether an authenticator decides on the clients, or the application has allowed clients
    // without one; either lets the broker listen beyond the loopback address.
    private boolean authenticated;

    private boolean anonymousAllowed;

    MqttBrokerBuilder() {
    }

    /// Listens plain at `value`, which must be resolved; port zero selects a port that the system
    /// assigns. Without this or [#listenTls(InetSocketAddress, SSLContext)], the broker listens
    /// plain at the loopback address with a port that the system assigns; with
    /// [#listenTls(InetSocketAddress, SSLContext)] alone, it listens with TLS alone. An address
    /// other than a loopback one, the wildcard address included, needs an [Authenticator] or
    /// [#allowAnonymous()].
    ///
    /// @param value
    ///   the address to listen at
    /// @return
    ///   this builder
    public MqttBrokerBuilder listen(InetSocketAddress value) {
        address = resolved(value);
        return this;
    }

    /// Listens with TLS at `address`, which must be resolved; port zero selects a port that the
    /// system assigns. Every connection there takes an engine of `context`, which holds the
    /// broker's key and the trust in client certificates, with the parameters that
    /// [#sslParameters(Consumer)] sets. The TLS listener comes besides a plain one that
    /// [#listen(InetSocketAddress)] sets, on another port, or alone. An address other than a
    /// loopback one, the wildcard address included, needs an [Authenticator] or
    /// [#allowAnonymous()], as for a plain listener: TLS authenticates the broker, and the
    /// clients only through the certificates that the application has the broker ask for and
    /// check.
    ///
    /// Initialize `context` before [#start()] and do not initialize it again while the broker
    /// runs. The broker creates an engine of it for each connection, so key and trust managers
    /// that change what they return, as for renewed certificates, take effect for the
    /// connections that come afterwards.
    ///
    /// @param address
    ///   the address to listen at with TLS
    /// @param context
    ///   the TLS context
    /// @return
    ///   this builder
    public MqttBrokerBuilder listenTls(InetSocketAddress address, SSLContext context) {
        tlsAddress = resolved(address);
        sslContext = requireNonNull(context, "context");
        return this;
    }

    /// Customizes the TLS parameters of the TLS listener once per [#start()], starting with the
    /// context's defaults for a server. For example, `setNeedClientAuth(true)` requires a client
    /// certificate that the context's trust manager accepts, which
    /// [Authenticator.Request#clientCertificates()] then shows, and `setWantClientAuth(true)`
    /// asks for one without requiring it. [#start()] checks the parameters on an engine of the
    /// context, and a detached snapshot is used for every connection; custom algorithm
    /// constraints and SNI matchers remain shared, so do not change those objects while the
    /// broker runs. A later call replaces the customizer. Requires
    /// [#listenTls(InetSocketAddress, SSLContext)].
    ///
    /// @param value
    ///   the customizer of the TLS parameters
    /// @return
    ///   this builder
    public MqttBrokerBuilder sslParameters(Consumer<SSLParameters> value) {
        sslParameters = requireNonNull(value, "sslParameters");
        return this;
    }

    private static InetSocketAddress resolved(InetSocketAddress value) {
        requireNonNull(value, "address");
        if (value.isUnresolved()) {
            throw new IllegalArgumentException("Address must be resolved: " + value);
        }
        return value;
    }

    /// Names the broker; see [MqttBroker#name()]. A name is a host name in lower case: labels of
    /// 1 to 63 letters, digits, and hyphens, separated by periods, at most 253 characters in all,
    /// with no label that starts or ends with a hyphen, and, if it has several labels, a last one
    /// that starts with a letter, such as `fleet` or `site-a.fleet`. Without a name, the broker
    /// takes `hato-` followed by a number that no running broker's name has.
    ///
    /// @param value
    ///   the name of the broker
    /// @return
    ///   this builder
    public MqttBrokerBuilder name(String value) {
        requireNonNull(value, "name");
        InprocBrokers.check(value);
        settings.name(value);
        return this;
    }

    /// Limits the packets that the broker takes from a client, in bytes including the fixed
    /// header. Defaults to 1 MiB. Must be between 2 and 268435460 bytes. The broker tells MQTT
    /// 5.0 clients the limit when they connect. A client that sends a larger packet loses its
    /// connection: an MQTT 5.0 client receives DISCONNECT with Packet too large first, and an
    /// MQTT 3.1.1 client, which cannot learn the limit, receives nothing. Whatever this limit,
    /// the broker takes no larger CONNECT than an internal limit allows from a peer that it
    /// knows nothing of yet.
    ///
    /// @param value
    ///   the largest packet in bytes, including the fixed header
    /// @return
    ///   this builder
    public MqttBrokerBuilder maxPacketSize(int value) {
        if (value < MIN_PACKET_SIZE || value > MAX_PACKET_SIZE) {
            throw new IllegalArgumentException(
                    "Packet size must be " + MIN_PACKET_SIZE + ".." + MAX_PACKET_SIZE + " bytes"
            );
        }
        settings.maxPacketSize(value);
        return this;
    }

    /// Limits the clients that the broker serves at once. Defaults to 10000. Must be positive.
    /// A CONNECT beyond the limit is refused, in MQTT 5.0 with Quota exceeded and in MQTT 3.1.1
    /// with Server unavailable, unless it comes for the session of a client that is connected,
    /// whose connection it takes over.
    ///
    /// @param value
    ///   the most clients at once
    /// @return
    ///   this builder
    public MqttBrokerBuilder maxConnections(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Connection limit must be positive");
        }
        settings.maxClients(value);
        return this;
    }

    /// Limits the sessions that outlive their connections: those of MQTT 3.1.1 clients that
    /// connect with Clean Session false, and those of MQTT 5.0 clients with a positive Session
    /// Expiry Interval. Defaults to 10000. Must be positive. A CONNECT that would start one more
    /// is refused, in MQTT 5.0 with Quota exceeded and in MQTT 3.1.1 with Server unavailable.
    ///
    /// @param value
    ///   the most persistent sessions
    /// @return
    ///   this builder
    public MqttBrokerBuilder maxPersistentSessions(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Persistent session limit must be positive");
        }
        settings.maxSessions(value);
        return this;
    }

    /// Limits how long a session outlives its connection. Defaults to 24 hours. Must be at least
    /// one second; a fraction of a second is dropped. An MQTT 5.0 client that asks for longer
    /// gets the limit, and learns it from the Session Expiry Interval in CONNACK. A Clean Session
    /// false session of MQTT 3.1.1 ends once the limit has passed since its connection ended. A
    /// limit of 4294967295 seconds or more lets a session last without end: an MQTT 5.0 session
    /// whose client asks for that, and every Clean Session false session of MQTT 3.1.1.
    ///
    /// @param value
    ///   the longest that a session outlives its connection
    /// @return
    ///   this builder
    public MqttBrokerBuilder maxSessionExpiry(Duration value) {
        requireNonNull(value, "maxSessionExpiry");
        if (value.toSeconds() < 1) {
            throw new IllegalArgumentException("Session expiry limit must be at least one second");
        }
        settings.maxSessionExpiry(value);
        return this;
    }

    /// Limits how many retained messages the broker keeps. Defaults to 10000. Must be positive.
    /// A retained publication beyond the limit reaches the current subscribers but is not kept,
    /// which leaves its topic without a retained message.
    ///
    /// @param value
    ///   the most retained messages
    /// @return
    ///   this builder
    public MqttBrokerBuilder maxRetainedMessages(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Retained message limit must be positive");
        }
        settings.maxRetained(value);
        return this;
    }

    /// Limits the memory that the retained messages take together, in bytes. Defaults to 64 MiB,
    /// or to an eighth of the largest heap if that is less. Must be positive. A retained
    /// publication beyond the limit reaches the current subscribers but is not kept, which leaves
    /// its topic without a retained message.
    ///
    /// @param value
    ///   the most memory for retained messages, in bytes
    /// @return
    ///   this builder
    public MqttBrokerBuilder maxRetainedBytes(long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Retained memory limit must be positive");
        }
        settings.maxRetainedBytes(value);
        return this;
    }

    /// Limits the QoS 1 and QoS 2 deliveries that wait for one session, while its client is away
    /// too. Defaults to 1000. Must be positive. A session beyond the limit misses deliveries,
    /// while its connection and session go on.
    ///
    /// @param value
    ///   the most deliveries that wait for one session
    /// @return
    ///   this builder
    public MqttBrokerBuilder maxQueuedDeliveries(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Queued delivery limit must be positive");
        }
        settings.maxQueued(value);
        return this;
    }

    /// Limits the memory that the QoS 1 and QoS 2 deliveries that wait for one session take
    /// together, in bytes. Defaults to 16 MiB. Must be positive. A session beyond the limit
    /// misses deliveries, while its connection and session go on.
    ///
    /// @param value
    ///   the most memory for the deliveries that wait for one session, in bytes
    /// @return
    ///   this builder
    public MqttBrokerBuilder maxQueuedDeliveryBytes(long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Queued delivery memory limit must be positive");
        }
        settings.maxQueuedBytes(value);
        return this;
    }

    /// Has `value` decide on every client that connects; see [Authenticator]. Without one, which
    /// is the default, the broker accepts every client and lets it publish and subscribe to every
    /// topic.
    ///
    /// @param value
    ///   the authenticator
    /// @return
    ///   this builder
    public MqttBrokerBuilder authenticator(Authenticator value) {
        settings.authenticator(requireNonNull(value, "authenticator"));
        authenticated = true;
        return this;
    }

    /// Lets the broker accept clients without an [Authenticator] when it listens at an address
    /// other than a loopback one. Without this or an authenticator, [#start()] refuses such an
    /// address, so that a broker never opens to a network without a check on its clients unless
    /// the application asks for it.
    ///
    /// @return
    ///   this builder
    public MqttBrokerBuilder allowAnonymous() {
        anonymousAllowed = true;
        return this;
    }

    /// Limits how long the broker waits for the [Authenticator]'s decision on a client, counted
    /// from its CONNECT. Defaults to 10 seconds. Must be positive and at most 2147483647
    /// milliseconds. A client whose decision takes longer is refused with Server unavailable,
    /// return code 3 in MQTT 3.1.1, and the decision is ignored when it comes.
    ///
    /// @param value
    ///   the longest wait for a decision
    /// @return
    ///   this builder
    public MqttBrokerBuilder authenticationTimeout(Duration value) {
        requireNonNull(value, "authenticationTimeout");
        if (value.isNegative() || value.isZero() || value.compareTo(MAX_TIMEOUT) > 0) {
            throw new IllegalArgumentException(
                    "Authentication timeout must be positive and at most "
                            + MAX_TIMEOUT.toMillis() + " ms"
            );
        }
        settings.authenticationTimeout(value);
        return this;
    }

    /// Registers the single listener for the broker's notices; defaults to none.
    ///
    /// @param value
    ///   the listener
    /// @return
    ///   this builder
    public MqttBrokerBuilder listener(BrokerListener value) {
        settings.listener(requireNonNull(value, "listener"));
        return this;
    }

    /// Starts a broker with these settings. Each call starts another broker, independent of the
    /// others, with the settings at that time.
    ///
    /// @return
    ///   the started broker
    /// @throws IOException
    ///   if a listening socket cannot be opened
    /// @throws IllegalArgumentException
    ///   if a listener's address is not a loopback one and neither an [Authenticator] nor
    ///   [#allowAnonymous()] is set, if TLS parameters are set without a TLS listener, or if the
    ///   TLS context or parameters cannot be used
    /// @throws IllegalStateException
    ///   if a running broker has the name that this builder gives
    public MqttBroker start() throws IOException {
        InetSocketAddress secure = tlsAddress;
        InetSocketAddress plain = address;
        if (plain == null && secure == null) {
            plain = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        }
        boolean beyondLoopback = beyondLoopback(plain) || beyondLoopback(secure);
        if (beyondLoopback && !authenticated && !anonymousAllowed) {
            throw new IllegalArgumentException(
                    "A broker that listens beyond the loopback address needs an authenticator"
                            + " or allowAnonymous()"
            );
        }
        SSLContext context = sslContext;
        if (secure == null || context == null) {
            if (sslParameters != null) {
                throw new IllegalArgumentException("TLS parameters need listenTls");
            }
        } else {
            settings.listenTls(secure, context, serverParameters(context));
        }
        return MqttBroker.start(settings, plain);
    }

    private static boolean beyondLoopback(@Nullable InetSocketAddress address) {
        if (address == null) {
            return false;
        }
        InetAddress host = address.getAddress();
        return host == null || !host.isLoopbackAddress();
    }

    // The parameters of every connection of the TLS listener: the context's defaults for a
    // server, as the application's customizer changes them, checked on an engine of the context
    // and read back, which detaches them from the customizer's object.
    private SSLParameters serverParameters(SSLContext context) {
        SSLEngine engine;
        try {
            engine = context.createSSLEngine();
        } catch (IllegalStateException failure) {
            // JSSE reports a context without init as an illegal state; here it is a setting to
            // fix.
            throw new IllegalArgumentException("Cannot initialize TLS configuration", failure);
        }
        engine.setUseClientMode(false);
        SSLParameters parameters = engine.getSSLParameters();
        Consumer<SSLParameters> customizer = sslParameters;
        if (customizer != null) {
            customizer.accept(parameters);
        }
        engine.setSSLParameters(parameters);
        return engine.getSSLParameters();
    }
}
