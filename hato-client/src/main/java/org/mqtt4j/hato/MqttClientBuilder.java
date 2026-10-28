package org.mqtt4j.hato;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.ClientActor;
import org.mqtt4j.hato.client.internal.ClientLimits;
import org.mqtt4j.hato.client.internal.DefaultTimerScheduler;
import org.mqtt4j.hato.client.internal.MessageProperties;
import org.mqtt4j.hato.client.internal.TcpTransport;
import org.mqtt4j.hato.client.internal.WebSocketTransports;
import org.mqtt4j.hato.client.internal.common.protocol.MqttString;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// Configures a client without opening a connection. Not thread safe.
/// Set [#server(URI)] before calling [#build()]; the other settings have defaults.
/// Invalid or unsupported configuration is rejected with [IllegalArgumentException].
public final class MqttClientBuilder {

    private static final Duration DEFAULT_KEEP_ALIVE = Duration.ofSeconds(60);

    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(30);

    private static final Duration MAX_TIMEOUT = Duration.ofMillis(Integer.MAX_VALUE);

    private static final Duration DEFAULT_RESPONSE_TIMEOUT = Duration.ofSeconds(60);

    private static final ReconnectPolicy DEFAULT_RECONNECT_POLICY =
            ReconnectPolicy.exponentialBackoff(Duration.ofSeconds(1), Duration.ofSeconds(120));

    private static final Duration MAX_SESSION_EXPIRY = Duration.ofSeconds(0xFFFF_FFFFL);

    // A Will Delay Interval is a four-byte count of seconds, like the Session Expiry Interval.
    private static final Duration MAX_WILL_DELAY = Duration.ofSeconds(0xFFFF_FFFFL);

    // The CONNECT that carries the Will must fit the send capacity that the client reserves for
    // connection control (1 MiB), or no attempt could send it. Every other CONNECT field holds
    // at most 65535 bytes, so Will Properties of at most 65535 bytes keep CONNECT well inside.
    // A deliberate simplification: a larger bound needs that reserved capacity to grow with it.
    private static final int MAX_WILL_PROPERTY_BYTES = 65_535;

    private static final int DEFAULT_MAX_PACKET_SIZE = 16 * 1024 * 1024;

    private static final int MAX_PORT = 65_535;

    private static final int MAX_KEEP_ALIVE_SECONDS = 65_535;

    private static final int MIN_PACKET_SIZE = 2;

    private static final int MAX_PACKET_SIZE = 268_435_460;

    // QoS 1 and QoS 2 exchanges need distinct packet identifiers.
    private static final int MAX_INFLIGHT_PUBLISHES = 65_535;

    // Each subscription takes an MQTT 5.0 Subscription Identifier of its own.
    private static final int MAX_SUBSCRIPTIONS = 268_435_455;

    private static final int GENERATED_CLIENT_ID_LENGTH = 23;

    private static final String CLIENT_ID_PREFIX = "hato";

    private @Nullable URI server;

    private @Nullable String clientId;

    private MqttVersion version = MqttVersion.V5;

    private Duration keepAlive = DEFAULT_KEEP_ALIVE;

    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    private Duration responseTimeout = DEFAULT_RESPONSE_TIMEOUT;

    private ReconnectPolicy reconnectPolicy = DEFAULT_RECONNECT_POLICY;

    private Duration sessionExpiry = Duration.ZERO;

    private int maxPacketSize = DEFAULT_MAX_PACKET_SIZE;

    private int maxInflightPublishes = ClientLimits.DEFAULT.inflightPublishes();

    private int maxSubscriptions = ClientLimits.DEFAULT.subscriptions();

    private TimerScheduler scheduler = DefaultTimerScheduler.instance;

    private ConnectionListener listener = _ -> { };

    private @Nullable MetricsListener metrics;

    private Transport.@Nullable Factory transportFactory;

    private @Nullable SSLContext sslContext;

    private @Nullable Consumer<SSLParameters> sslParameters;

    private @Nullable Supplier<MqttCredentials> credentials;

    private @Nullable MqttMessage will;

    private Duration willDelay = Duration.ZERO;

    MqttClientBuilder() {
    }

    /// A required broker URI using `mqtt` for TCP, `mqtts` for TLS over TCP, `ws` for WebSocket,
    /// or `wss` for WebSocket over TLS. The default ports are 1883, 8883, 80, and 443,
    /// respectively; a broker's WebSocket listener often has its own port and path, such as
    /// `ws://broker.example.com:8080/mqtt`. A `ws` or `wss` URI keeps its path and query for the
    /// opening handshake, and connects directly, without a proxy that the JVM configures for
    /// HTTP. Hosts must satisfy [URI#parseServerAuthority()]; names containing underscores are
    /// rejected. Schemes must be in lower case. User information and fragments are not accepted,
    /// nor are paths and queries for `mqtt` and `mqtts`. Rejection messages hide any user
    /// information in the given URI.
    ///
    /// @param value
    ///     the broker URI
    /// @return
    ///     this builder
    public MqttClientBuilder server(URI value) {
        requireNonNull(value, "server");
        try {
            value.parseServerAuthority();
        } catch (URISyntaxException failure) {
            // The exception's message and input repeat the whole URI, so only the reason is kept.
            throw new IllegalArgumentException(failure.getReason() + ": " + redactUserInfo(value));
        }
        boolean tcp = "mqtt".equals(value.getScheme()) || "mqtts".equals(value.getScheme());
        boolean webSocket = "ws".equals(value.getScheme()) || "wss".equals(value.getScheme());
        if ((!tcp && !webSocket)
            || value.getHost() == null
            || value.getRawUserInfo() != null
            || value.getRawFragment() != null
            || (tcp && value.getRawQuery() != null)
            || (tcp && value.getRawPath() != null && !value.getRawPath().isEmpty())
            || value.getPort() == 0
            || value.getPort() > MAX_PORT
        ) {
            throw new IllegalArgumentException(
                    "Expected mqtt://host[:port], mqtts://host[:port],"
                            + " ws://host[:port][/path][?query], or"
                            + " wss://host[:port][/path][?query]: " + redactUserInfo(value)
            );
        }
        server = value;
        return this;
    }

    /// Renders a rejected URI without credentials by hiding everything before the last `@` of
    /// the authority. Without an authority, the whole part after the scheme is treated the same
    /// way, which also covers a missing `//`, as in `mqtt:user:password@host`.
    private static String redactUserInfo(URI value) {
        String text = value.toString();
        String authority = value.getRawAuthority();
        String part = authority != null ? authority : value.getRawSchemeSpecificPart();
        int at = part.lastIndexOf('@');
        return at < 0 ? text : text.replace(part, "***" + part.substring(at));
    }

    /// Uses this nonempty MQTT UTF-8 identifier of at most 65535 bytes, validated immediately.
    /// Persistent sessions require an explicit value. Otherwise, omission generates 23 ASCII
    /// letters/digits. Give each running client its own identifier: a broker closes the older
    /// connection of two that share one, so two clients that reconnect automatically keep
    /// taking over each other's connection. Reconnection delays slow this down but do not
    /// stop it. With a persistent session, a client that connects while another one runs with
    /// the same identifier also discards that client's session and the messages stored for it;
    /// see [MqttClient#connect()].
    ///
    /// @param value
    ///     the client identifier
    /// @return
    ///     this builder
    public MqttClientBuilder clientId(String value) {
        requireNonNull(value, "clientId");
        MqttString.validate(value);
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Client identifier must not be empty");
        }
        clientId = value;
        return this;
    }

    /// Selects the wire protocol; default [MqttVersion#V5]. Choose [MqttVersion#V3_1_1] for a
    /// broker without MQTT 5.0. An MQTT 5.0 broker announces limits when it accepts a
    /// connection, and the client keeps within them; see [ConnAck] and
    /// [MqttPublisher#publish(MqttMessage)].
    ///
    /// @param value
    ///     the MQTT version
    /// @return
    ///     this builder
    public MqttClientBuilder version(MqttVersion value) {
        version = requireNonNull(value, "version");
        return this;
    }

    /// Configures how long the session outlasts a connection; default zero, which starts a new
    /// session for each connection. A positive duration selects a persistent session, which
    /// requires an explicit [#clientId(String)] and lasts across this client's connections: the
    /// client gains local state when a connection starts a session, and its later connections
    /// ask the broker to resume it. A session that the broker kept for the identifier without
    /// local state in this client object, for example from before the application restarted,
    /// is not resumed: the attempt that finds it discards it and starts a new session, so
    /// messages that the broker stored for it are not received; see [MqttClient#connect()].
    /// [#build()] accepts zero, or durations from 1 through 4294967295 seconds inclusive.
    /// Fractional seconds are accepted within that range, such as 1.5 seconds. The client does
    /// not enforce the duration locally. MQTT 5.0 sends it as the Session Expiry Interval,
    /// rounded up to whole seconds so that the broker keeps the session at least that long;
    /// 4294967295 seconds means that the session does not expire. MQTT 3.1.1 does not send it:
    /// zero selects Clean Session 1 and a positive value Clean Session 0, apart from the
    /// connection that discards a broker session, and the broker determines how long the
    /// session lasts.
    ///
    /// @param value
    ///     how long the session outlasts a connection
    /// @return
    ///     this builder
    public MqttClientBuilder sessionExpiry(Duration value) {
        sessionExpiry = requireNonNull(value, "sessionExpiry");
        return this;
    }

    /// Supplies credentials once per connection attempt, on a thread of its own before
    /// opening the attempt's first transport. Evaluation consumes the same
    /// [#connectionTimeout(Duration)] deadline as that transport's establishment and the
    /// broker's CONNACK response. Every connection of the attempt, including those with which
    /// an MQTT 3.1.1 attempt replaces a broker session (see [MqttClient#connect()]), sends the
    /// same credentials, so the value must stay valid for several connections.
    /// The default is no credentials. At most one invocation runs at a time for this client;
    /// the supplier should respond to interruption when an attempt ends or the client closes.
    /// An invocation that ignores interruption delays evaluation for a later attempt until it
    /// returns; the abandoned result is discarded. Closing the client does not wait for it, but
    /// [MqttClient#terminated()] completes only after it returns, so such an invocation should
    /// bound its own wait.
    /// An exception, a null result, or exceeding [#connectionTimeout(Duration)] fails only
    /// that attempt with [MqttConnectException]. A later attempt evaluates the supplier again.
    /// The supplier is not evaluated during [#build()].
    ///
    /// The client authenticates only with these credentials and TLS: it does not implement MQTT
    /// 5.0 enhanced authentication and sends no Authentication Method. A broker that requires
    /// enhanced authentication refuses the connection. When it refuses with
    /// [ReasonCode#BAD_AUTHENTICATION_METHOD], the client does not reconnect; when it refuses
    /// with [ReasonCode#NOT_AUTHORIZED], the client reconnects and evaluates the supplier again.
    /// A broker that still sends an AUTH packet, or an Authentication Method in CONNACK, breaks
    /// the protocol: the attempt or the connection ends with [MqttProtocolException].
    ///
    /// @param value
    ///     the credentials supplier
    /// @return
    ///     this builder
    public MqttClientBuilder credentials(Supplier<MqttCredentials> value) {
        credentials = requireNonNull(value, "credentials");
        return this;
    }

    /// Configures the message the broker publishes if the connection ends without receiving
    /// MQTT DISCONNECT, including [MqttClient#close()] alone. No Will is configured by default.
    /// Any QoS and retention flag are accepted. With MQTT 5.0, the message's properties, such as
    /// [MqttMessage#contentType()], go to the broker as the Will Properties. [#build()] rejects
    /// payloads over 65535 bytes, properties that take more than 65535 bytes in all, and any
    /// properties for MQTT 3.1.1, which cannot carry them; the immutable message already
    /// validates its topic and owns a copy of its payload.
    /// Wait for [MqttClient#disconnect()] to complete successfully before [MqttClient#close()]
    /// so closing cannot discard the pending DISCONNECT. Completion confirms the write;
    /// the broker does not acknowledge receipt.
    ///
    /// @param value
    ///     the Will message
    /// @return
    ///     this builder
    public MqttClientBuilder will(MqttMessage value) {
        will = requireNonNull(value, "will");
        return this;
    }

    /// Delays the Will (the MQTT 5.0 Will Delay Interval); default zero. [#build()] accepts zero
    /// through 4294967295 seconds, and a fraction of a second is rounded up when sent. MQTT 5.0
    /// has the broker publish the Will once the delay has passed after the connection ended, or
    /// when the session ends, whichever comes first. With a positive
    /// [#sessionExpiry(Duration)], a reconnection of this client within the delay resumes the
    /// session, and the broker does not publish the Will. Brokers differ when the session ends
    /// sooner. With the default zero session expiry, EMQX and HiveMQ CE, for example, publish
    /// the Will as the connection ends, while Mosquitto waits for the delay. A new client object,
    /// for example after the application restarts, starts a new session instead; EMQX and
    /// HiveMQ CE then publish the old session's Will at once, while Mosquitto discards it.
    /// [#build()] rejects a positive delay without a [#will(MqttMessage)], and any positive
    /// delay for MQTT 3.1.1, which has no Will Delay.
    ///
    /// @param value
    ///     the delay of the Will
    /// @return
    ///     this builder
    public MqttClientBuilder willDelay(Duration value) {
        willDelay = requireNonNull(value, "willDelay");
        return this;
    }

    /// Sets the keep-alive interval in whole seconds from 0 (disabled) to 65535; default 60.
    /// Idle time is measured from completed writes. The interval also bounds a PINGREQ write
    /// and the wait for its PINGRESP, together. An MQTT 5.0 broker can set another interval for
    /// a connection with Server Keep Alive, which the client then follows on that connection.
    ///
    /// @param value
    ///     the keep-alive interval
    /// @return
    ///     this builder
    public MqttClientBuilder keepAlive(Duration value) {
        requireNonNull(value, "keepAlive");
        if (value.isNegative() || value.getNano() != 0
                || value.getSeconds() > MAX_KEEP_ALIVE_SECONDS) {
            throw new IllegalArgumentException(
                    "Keep alive must be 0.." + MAX_KEEP_ALIVE_SECONDS + " whole seconds"
            );
        }
        keepAlive = value;
        return this;
    }

    /// Deadline for establishing each connection: credentials, which an attempt evaluates before
    /// its first connection, transport establishment (including any TLS handshake), and
    /// CONNACK. Also bounds each DISCONNECT write. An MQTT 3.1.1 attempt that replaces a broker
    /// session, as [MqttClient#connect()] describes, applies it to each of its connections and
    /// DISCONNECT writes, so it can take several times this duration.
    /// Defaults to 30 seconds. Must be positive and no greater than 2147483647 milliseconds.
    ///
    /// @param value
    ///     the deadline for establishing each connection
    /// @return
    ///     this builder
    public MqttClientBuilder connectionTimeout(Duration value) {
        requireNonNull(value, "connectionTimeout");
        connectionTimeout = requireTimeout(value, "Connection timeout");
        return this;
    }

    /// How long the broker may take to answer SUBSCRIBE, UNSUBSCRIBE, and each stage of a QoS 1
    /// or QoS 2 publication, measured from the completed write of the packet that needs the
    /// answer. Expiry ends the connection with [java.util.concurrent.TimeoutException] as its
    /// cause, and the reconnect policy applies as after other connection failures. The next
    /// connection repeats an unanswered publication stage when it resumes a persistent session,
    /// and an unanswered SUBSCRIBE in either kind of session. Other responses and PINGRESP do
    /// not extend the deadline, and keep-alive does not change it, even when disabled.
    /// Defaults to 60 seconds. Must be positive and no greater than 2147483647 milliseconds.
    ///
    /// @param value
    ///     how long the broker may take to answer
    /// @return
    ///     this builder
    public MqttClientBuilder responseTimeout(Duration value) {
        requireNonNull(value, "responseTimeout");
        responseTimeout = requireTimeout(value, "Response timeout");
        return this;
    }

    /// Decides whether the client reconnects after an established connection ends without a
    /// request to stop, and how long it waits between attempts. Defaults to
    /// [ReconnectPolicy#exponentialBackoff(Duration, Duration)] from 1 second up to
    /// 120 seconds, which does not retry the first failure of a connection request and keeps the
    /// default classification of broker reason codes that [ReconnectPolicy] describes.
    /// [ReconnectPolicy#disabled()] leaves a client disconnected until the next
    /// [MqttClient#connect()].
    ///
    /// @param value
    ///     the reconnect policy
    /// @return
    ///     this builder
    public MqttClientBuilder reconnectPolicy(ReconnectPolicy value) {
        reconnectPolicy = requireNonNull(value, "reconnectPolicy");
        return this;
    }

    /// Inclusive incoming packet size, including the fixed header. Checked before allocation.
    /// Defaults to 16 MiB. Must be between 2 and 268435460 bytes.
    /// Increasing this limit can also increase memory consumption. MQTT 5.0 announces the limit
    /// to the broker, which then discards a message too large for this client instead of
    /// sending it. An MQTT 3.1.1 broker does not learn the limit, and a larger packet ends the
    /// connection with [MqttProtocolException].
    ///
    /// Whatever its size, an MQTT 5.0 packet with more properties than the client takes, such
    /// as a very large number of User Properties, also ends the connection with
    /// [MqttProtocolException] and [ReasonCode#PACKET_TOO_LARGE]. Brokers do not learn this
    /// limit.
    ///
    /// @param value
    ///     the largest incoming packet size in bytes, including the fixed header
    /// @return
    ///     this builder
    public MqttClientBuilder maxPacketSize(int value) {
        if (value < MIN_PACKET_SIZE || value > MAX_PACKET_SIZE) {
            throw new IllegalArgumentException(
                    "Packet size must be " + MIN_PACKET_SIZE + ".." + MAX_PACKET_SIZE + " bytes"
            );
        }
        maxPacketSize = value;
        return this;
    }

    /// Limits the QoS 1 and QoS 2 publications whose exchanges are unfinished, from 1 to 65535;
    /// defaults to 64. A publication counts from [MqttPublisher#publish(MqttMessage)] until its
    /// PUBACK for QoS 1 or its PUBCOMP for QoS 2, including while it waits for the broker's
    /// Receive Maximum and, in a persistent session, while its exchange waits for the next
    /// connection. A publication beyond the limit fails with [MqttCapacityException]; QoS 0
    /// publications do not count. Publications share packet identifiers with subscription
    /// requests, so one can also fail below the limit when no identifier is free. With MQTT 5.0,
    /// at most the smaller of this limit and the broker's Receive Maximum are in progress on one
    /// connection.
    ///
    /// The client keeps its other limits internal, and they may change: the bytes that unfinished
    /// publications keep, the operation and send capacity that publications use, which grow with
    /// this limit, and the capacity for incoming QoS 1 and QoS 2 exchanges.
    ///
    /// @param value
    ///     the limit on unfinished QoS 1 and QoS 2 publications
    /// @return
    ///     this builder
    public MqttClientBuilder maxInflightPublishes(int value) {
        if (value < 1 || value > MAX_INFLIGHT_PUBLISHES) {
            throw new IllegalArgumentException(
                    "Inflight publishes must be 1.." + MAX_INFLIGHT_PUBLISHES
            );
        }
        maxInflightPublishes = value;
        return this;
    }

    /// Limits the subscriptions that the client holds, from 1 to 268,435,455, the number of MQTT
    /// 5.0 Subscription Identifiers; defaults to 4,096. The limit guards against an application
    /// that subscribes without end: each subscription keeps a delivery lane, and a reconnection
    /// to a new session sends a SUBSCRIBE for each one, a few at a time, so that restoring many
    /// subscriptions takes many round trips. A subscription with a handler holds about 3.5 KB of
    /// heap, its delivery thread included, and a hand-off or pull subscription about 1.6 KB, so
    /// subscriptions at the default limit hold about 14 MB at most, besides the messages that
    /// wait for them. Routing a received publication takes time that grows with its topic's
    /// levels and with the subscriptions that it matches, not with the subscriptions that the
    /// client holds. A subscription counts from its request
    /// through [MqttSubscriber], including while it awaits establishment, until the broker has
    /// removed it, and its delivery lane counts until its handlers have returned. A subscription
    /// beyond the limit ends its handle, and [Subscription#established()] fails with
    /// [MqttCapacityException]. The client keeps the deliveries that it holds across all
    /// subscriptions within an internal limit.
    ///
    /// @param value
    ///     the limit on the subscriptions that the client holds
    /// @return
    ///     this builder
    public MqttClientBuilder maxSubscriptions(int value) {
        if (value < 1 || value > MAX_SUBSCRIPTIONS) {
            throw new IllegalArgumentException("Subscriptions must be 1.." + MAX_SUBSCRIPTIONS);
        }
        maxSubscriptions = value;
        return this;
    }

    /// Supplies monotonic time and delayed callbacks. Defaults to a shared daemon timer with
    /// [System#nanoTime()]. The scheduler is borrowed and is not closed with the client.
    /// The default transports use it for connection deadlines, including the TLS handshake and
    /// the WebSocket opening handshake, and for the grace periods of closing TCP and TLS
    /// connections and the WebSocket closing handshake. JDK socket connection and name resolution
    /// waits, and the JDK's own deadline for the WebSocket opening handshake, still use real time.
    /// Custom transports manage their own clocks.
    ///
    /// @param value
    ///     the timer scheduler
    /// @return
    ///     this builder
    public MqttClientBuilder timerScheduler(TimerScheduler value) {
        scheduler = requireNonNull(value, "timerScheduler");
        return this;
    }

    /// Registers the single listener for connection and subscription notifications; defaults
    /// to no action.
    ///
    /// @param value
    ///     the connection listener
    /// @return
    ///     this builder
    public MqttClientBuilder connectionListener(ConnectionListener value) {
        listener = requireNonNull(value, "connectionListener");
        return this;
    }

    /// Registers the single listener for the timings of publications and deliveries; see
    /// [MetricsListener]. Without one, which is the default, the client reads no time and
    /// creates no event for them.
    ///
    /// @param value
    ///     the metrics listener
    /// @return
    ///     this builder
    public MqttClientBuilder metricsListener(MetricsListener value) {
        metrics = requireNonNull(value, "metricsListener");
        return this;
    }

    /// Supplies the TLS context for an `mqtts` or `wss` connection, including trust and client
    /// keys. If omitted, [#build()] obtains [SSLContext#getDefault()] once. The same context is
    /// reused across connection attempts. Initialize it before building and do not reinitialize
    /// it while the client is in use. Explicit TLS settings require `mqtts` or `wss` and cannot
    /// be combined with [#transportFactory(Transport.Factory)].
    ///
    /// @param value
    ///     the TLS context
    /// @return
    ///     this builder
    public MqttClientBuilder sslContext(SSLContext value) {
        sslContext = requireNonNull(value, "sslContext");
        return this;
    }

    /// Customizes TLS parameters once per [#build()], starting with the selected context's
    /// defaults and endpoint identification set to `"HTTPS"` for hostname verification.
    /// For `mqtts`, setting the endpoint identification algorithm to `null` explicitly disables
    /// verification. The parameters are validated during [#build()], and a detached snapshot is
    /// used for every connection. Custom algorithm constraints and SNI matchers remain shared;
    /// do not mutate those objects while the client is in use.
    /// A later call replaces the customizer.
    /// Explicit TLS settings require `mqtts` or `wss` and cannot be combined with
    /// [#transportFactory(Transport.Factory)].
    ///
    /// A `wss` connection goes through the JDK's HTTP client, which applies only part of the
    /// parameters: it always verifies the hostname, uses no ALPN, and keeps the context's named
    /// groups and signature schemes. Its SNI is the URI's host name; only for an IP address host
    /// does it use the server names of the parameters, for both SNI and hostname verification.
    /// For `wss`, [#build()] therefore rejects parameters that change the endpoint
    /// identification algorithm, set application protocols, change the named groups or
    /// signature schemes, or set server names for a host name. The context, the protocols, and
    /// the cipher suites apply to both `mqtts` and `wss`.
    ///
    /// For AWS IoT Core with X.509 client authentication on port 443, use `mqtts`, configure the
    /// client keys in [#sslContext(SSLContext)], and set ALPN with:
    /// ```java
    /// builder.sslParameters(parameters ->
    ///         parameters.setApplicationProtocols(new String[]{"x-amzn-mqtt-ca"}));
    /// ```
    ///
    /// @param value
    ///     the customizer of the TLS parameters
    /// @return
    ///     this builder
    public MqttClientBuilder sslParameters(Consumer<SSLParameters> value) {
        sslParameters = requireNonNull(value, "sslParameters");
        return this;
    }

    /// Supplies a fresh transport for each connection; an MQTT 3.1.1 attempt that replaces a
    /// broker session, as [MqttClient#connect()] describes, opens several. Defaults to TCP for
    /// `mqtt`, TLS over TCP for `mqtts`, and the JDK's WebSocket client for `ws` and `wss`. The
    /// client owns and closes the returned transports.
    /// An explicit factory cannot be combined with [#sslContext(SSLContext)] or
    /// [#sslParameters(Consumer)]. The factory's transports must reject unsupported URI schemes.
    ///
    /// @param value
    ///     the transport factory
    /// @return
    ///     this builder
    public MqttClientBuilder transportFactory(Transport.Factory value) {
        transportFactory = requireNonNull(value, "transportFactory");
        return this;
    }

    /// Creates a disconnected client. Call [MqttClient#connect()] to open the connection.
    ///
    /// @return
    ///     the new client
    /// @throws IllegalStateException
    ///     if the server has not been specified
    /// @throws IllegalArgumentException
    ///     if the session expiry is out of range, TLS settings are invalid or incompatible, a
    ///     persistent session has no explicit client id, the Will payload exceeds 65535 bytes,
    ///     the Will's properties exceed 65535 bytes, the Will delay is out of range or positive
    ///     without a Will, or MQTT 3.1.1 is chosen with Will properties or a positive Will delay
    public MqttClient build() {
        URI endpoint = server;
        if (endpoint == null) {
            throw new IllegalStateException("Server must be specified");
        }
        boolean tls = "mqtts".equals(endpoint.getScheme()) || "wss".equals(endpoint.getScheme());
        Transport.Factory factory = transportFactory;
        if ((sslContext != null || sslParameters != null) && (!tls || factory != null)) {
            throw new IllegalArgumentException(
                    "TLS settings require mqtts or wss and the default transport factory"
            );
        }
        if (!sessionExpiry.isZero()
                && (sessionExpiry.compareTo(Duration.ofSeconds(1)) < 0
                || sessionExpiry.compareTo(MAX_SESSION_EXPIRY) > 0)) {
            throw new IllegalArgumentException(
                    "Session expiry must be zero or 1..4294967295 seconds"
            );
        }
        if (willDelay.isNegative() || willDelay.compareTo(MAX_WILL_DELAY) > 0) {
            throw new IllegalArgumentException("Will delay must be 0..4294967295 seconds");
        }
        if (!willDelay.isZero() && will == null) {
            throw new IllegalArgumentException("A Will delay needs a Will");
        }
        // The CONNECT form checks the payload limit here, before the client starts any thread.
        Packet.Will connectWill = (will == null) ? null : connectWill(will);
        String id = clientId;
        if (id == null) {
            if (!sessionExpiry.isZero()) {
                throw new IllegalArgumentException(
                        "Persistent sessions require an explicit client id"
                );
            }
            id = CLIENT_ID_PREFIX + UUID.randomUUID().toString().replace("-", "")
                    .substring(0, GENERATED_CLIENT_ID_LENGTH - CLIENT_ID_PREFIX.length());
        }
        TimerScheduler timer = scheduler;
        if (factory == null) {
            factory = switch (requireNonNull(endpoint.getScheme())) {
                case "mqtts" -> {
                    SSLContext context = tlsContext();
                    SSLParameters parameters = tlsParameters(context);
                    yield value -> new TcpTransport(value, context, parameters, timer);
                }
                case "ws" -> new WebSocketTransports(id, timer);
                case "wss" -> {
                    SSLContext context = tlsContext();
                    SSLParameters parameters = tlsParameters(context);
                    requireEffectiveOverWebSocket(
                            parameters,
                            defaultParameters(context),
                            endpoint.getHost()
                    );
                    yield new WebSocketTransports(id, timer, context, parameters);
                }
                default -> value -> new TcpTransport(value, timer);
            };
        }
        return new ClientActor(
                endpoint,
                id,
                keepAlive,
                connectionTimeout,
                maxPacketSize,
                timer,
                listener,
                metrics,
                factory,
                version,
                sessionExpiry,
                credentials,
                connectWill,
                reconnectPolicy.backoff(),
                reconnectPolicy.rules(),
                responseTimeout,
                DeliveryOptions::execution,
                MqttMessage::received,
                MqttMessage::payloadArray,
                ClientStats::of,
                new ClientLimits(maxInflightPublishes, maxSubscriptions)
        );
    }

    /// The Will in the form that CONNECT carries, checked against what the protocol version can
    /// carry and against the room that CONNECT has for its properties.
    private Packet.Will connectWill(MqttMessage message) {
        Properties properties = switch (version) {
            case V3_1_1 -> {
                if (MessageProperties.present(message) || !willDelay.isZero()) {
                    throw new IllegalArgumentException(
                            "MQTT 3.1.1 cannot carry Will properties or a Will delay"
                    );
                }
                yield EMPTY;
            }
            case V5 -> {
                // A Will Delay Interval takes five bytes: its identifier and its value.
                long bytes = MessageProperties.size(message) + (willDelay.isZero() ? 0L : 5L);
                if (bytes > MAX_WILL_PROPERTY_BYTES) {
                    throw new IllegalArgumentException("Will properties exceed 65535 bytes");
                }
                yield MessageProperties.will(message, willDelay);
            }
        };
        return new Packet.Will(
                message.topic(),
                message.payloadArray(),
                message.qos(),
                message.retain(),
                properties
        );
    }

    // The context of every TLS connection of the client.
    private SSLContext tlsContext() {
        try {
            return sslContext != null ? sslContext : SSLContext.getDefault();
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalArgumentException("Cannot initialize TLS configuration", failure);
        }
    }

    // The customized parameters of every TLS connection of the client, checked on a socket.
    private SSLParameters tlsParameters(SSLContext context) {
        SSLParameters parameters = defaultParameters(context);
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        if (sslParameters != null) {
            sslParameters.accept(parameters);
        }
        // A fresh socket preserves an explicit null endpoint algorithm. Reading back its
        // parameters also detaches the configuration from the customizer's mutable object.
        try (var socket = (SSLSocket) context.getSocketFactory().createSocket()) {
            socket.setSSLParameters(parameters);
            return socket.getSSLParameters();
        } catch (IOException failure) {
            throw new IllegalArgumentException("Cannot initialize TLS configuration", failure);
        }
    }

    /// HttpClient, which carries `wss`, copies only part of the parameters to each connection:
    /// it always verifies the hostname, sets no ALPN, keeps the context's named groups and
    /// signature schemes, and sends a host name for SNI rather than the server names of the
    /// parameters. Parameters that it would ignore are rejected rather than dropped.
    private static void requireEffectiveOverWebSocket(
            SSLParameters parameters,
            SSLParameters defaults,
            String host
    ) {
        if (!"HTTPS".equalsIgnoreCase(parameters.getEndpointIdentificationAlgorithm())) {
            throw new IllegalArgumentException(
                    "wss always verifies the hostname, so endpoint identification must stay HTTPS"
            );
        }
        if (parameters.getApplicationProtocols().length > 0) {
            throw new IllegalArgumentException("wss cannot set application protocols (ALPN)");
        }
        if (!Arrays.equals(parameters.getNamedGroups(), defaults.getNamedGroups())) {
            throw new IllegalArgumentException("wss cannot change the context's named groups");
        }
        if (!Arrays.equals(parameters.getSignatureSchemes(), defaults.getSignatureSchemes())) {
            throw new IllegalArgumentException("wss cannot change the context's signature schemes");
        }
        if (parameters.getServerNames() != null && !ipAddress(host)) {
            throw new IllegalArgumentException(
                    "wss sends the host name for SNI, so server names need an IP address host"
            );
        }
    }

    // Whether HttpClient treats `host` as an IP address, without resolving a name.
    private static boolean ipAddress(String host) {
        try {
            InetAddress _ = InetAddress.ofLiteral(host);
            return true;
        } catch (IllegalArgumentException _) {
            return false;
        }
    }

    /// JSSE reports a context without `init` as an illegal state; here it is a setting to fix.
    /// Only this call is translated, so a customizer's own exceptions still pass through.
    private static SSLParameters defaultParameters(SSLContext context) {
        try {
            return context.getDefaultSSLParameters();
        } catch (IllegalStateException failure) {
            throw new IllegalArgumentException("SSLContext is not initialized", failure);
        }
    }

    private static Duration requireTimeout(Duration value, String description) {
        if (!value.isPositive() || value.compareTo(MAX_TIMEOUT) > 0) {
            throw new IllegalArgumentException(
                    description + " must be positive and <= "
                            + MAX_TIMEOUT.toMillis() + " milliseconds"
            );
        }
        return value;
    }
}
