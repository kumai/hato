package org.mqtt4j.hato;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.ClientActor;
import org.mqtt4j.hato.client.internal.DefaultTimerScheduler;
import org.mqtt4j.hato.client.internal.TcpTransport;
import org.mqtt4j.hato.client.internal.common.protocol.MqttString;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.spi.TimerScheduler;
import org.mqtt4j.hato.spi.Transport;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// Configures a client without opening a connection. Not thread safe.
/// Set [#server(URI)] and [#version(MqttVersion)] before calling [#build].
/// Invalid or unsupported configuration is rejected with [IllegalArgumentException].
public final class MqttClientBuilder {

    private static final Duration DEFAULT_KEEP_ALIVE = Duration.ofSeconds(60);

    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(30);

    private static final Duration MAX_TIMEOUT = Duration.ofMillis(Integer.MAX_VALUE);

    private static final Duration DEFAULT_RESPONSE_TIMEOUT = Duration.ofSeconds(60);

    private static final ReconnectPolicy DEFAULT_RECONNECT_POLICY =
            ReconnectPolicy.exponentialBackoff(Duration.ofSeconds(1), Duration.ofSeconds(120));

    private static final Duration MAX_SESSION_EXPIRY = Duration.ofSeconds(0xFFFF_FFFFL);

    private static final int DEFAULT_MAX_PACKET_SIZE = 16 * 1024 * 1024;

    private static final int MAX_PORT = 65_535;

    private static final int MAX_KEEP_ALIVE_SECONDS = 65_535;

    private static final int MIN_PACKET_SIZE = 2;

    private static final int MAX_PACKET_SIZE = 268_435_460;

    private static final int GENERATED_CLIENT_ID_LENGTH = 23;

    private static final String CLIENT_ID_PREFIX = "hato";

    private @Nullable URI server;

    private @Nullable String clientId;

    private @Nullable MqttVersion version;

    private Duration keepAlive = DEFAULT_KEEP_ALIVE;

    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    private Duration responseTimeout = DEFAULT_RESPONSE_TIMEOUT;

    private ReconnectPolicy reconnectPolicy = DEFAULT_RECONNECT_POLICY;

    private Duration sessionExpiry = Duration.ZERO;

    private int maxPacketSize = DEFAULT_MAX_PACKET_SIZE;

    private TimerScheduler scheduler = DefaultTimerScheduler.instance;

    private ConnectionListener listener = _ -> { };

    private Transport.@Nullable Factory transportFactory;

    private @Nullable SSLContext sslContext;

    private @Nullable Consumer<SSLParameters> sslParameters;

    private @Nullable Supplier<MqttCredentials> credentials;

    private @Nullable MqttMessage will;

    MqttClientBuilder() {
    }

    /// A required broker URI using `mqtt` for TCP or `mqtts` for TLS over TCP.
    /// The default ports are 1883 and 8883, respectively.
    /// Hosts must satisfy [URI#parseServerAuthority]; names containing underscores are rejected.
    /// User information, paths, queries, and fragments are not accepted. Rejection messages
    /// hide any user information in the given URI.
    public MqttClientBuilder server(URI value) {
        requireNonNull(value, "server");
        try {
            value.parseServerAuthority();
        } catch (URISyntaxException failure) {
            // The exception's message and input repeat the whole URI, so only the reason is kept.
            throw new IllegalArgumentException(failure.getReason() + ": " + redactUserInfo(value));
        }
        if ((!"mqtt".equals(value.getScheme()) && !"mqtts".equals(value.getScheme()))
            || value.getHost() == null
            || value.getRawUserInfo() != null
            || value.getRawQuery() != null
            || value.getRawFragment() != null
            || (value.getRawPath() != null && !value.getRawPath().isEmpty())
            || value.getPort() == 0
            || value.getPort() > MAX_PORT
        ) {
            throw new IllegalArgumentException(
                    "Expected mqtt://host[:port] or mqtts://host[:port]: " + redactUserInfo(value)
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
    public MqttClientBuilder clientId(String value) {
        requireNonNull(value, "clientId");
        MqttString.write(new ByteArrayOutputStream(), value);
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Client identifier must not be empty");
        }
        clientId = value;
        return this;
    }

    /// Selects the required wire protocol explicitly. Supports [MqttVersion#V3_1_1].
    public MqttClientBuilder version(MqttVersion value) {
        if (requireNonNull(value, "version") != MqttVersion.V3_1_1) {
            throw new IllegalArgumentException("Unsupported MQTT version: " + value);
        }
        version = value;
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
    /// Fractional seconds are accepted within that range, such as 1.5 seconds.
    /// MQTT 3.1.1 does not send the duration, and the client does not enforce it locally: zero
    /// selects Clean Session 1 and a positive value Clean Session 0, apart from the connection
    /// that discards a broker session, and the broker determines how long the session lasts.
    public MqttClientBuilder sessionExpiry(Duration value) {
        sessionExpiry = requireNonNull(value, "sessionExpiry");
        return this;
    }

    /// Supplies credentials once per connection attempt, on a dedicated virtual thread before
    /// opening the attempt's first transport. Evaluation consumes the same
    /// [#connectionTimeout(Duration)] deadline as that transport's establishment and the
    /// broker's CONNACK response. Every connection of the attempt, including those with which
    /// it replaces a broker session (see [MqttClient#connect()]), sends the same credentials,
    /// so the value must stay valid for several connections.
    /// The default is no credentials. At most one invocation runs at a time for this client;
    /// the supplier should respond to interruption when an attempt ends or the client closes.
    /// An invocation that ignores interruption delays evaluation for a later attempt until it
    /// returns; the abandoned result is discarded. Closing the client does not wait for it, but
    /// [MqttClient#terminated()] completes only after it returns, so such an invocation should
    /// bound its own wait.
    /// An exception, a null result, or exceeding [#connectionTimeout(Duration)] fails only
    /// that attempt with [MqttConnectException]. A later attempt evaluates the supplier again.
    /// The supplier is not evaluated during [#build()].
    public MqttClientBuilder credentials(Supplier<MqttCredentials> value) {
        credentials = requireNonNull(value, "credentials");
        return this;
    }

    /// Configures the message the broker publishes if the connection ends without receiving
    /// MQTT DISCONNECT, including [MqttClient#close()] alone. No Will is configured by default.
    /// Any QoS and retention flag are accepted. [#build()] rejects payloads over 65535 bytes;
    /// the immutable message already validates its topic and owns a copy of its payload.
    /// Wait for [MqttClient#disconnect()] to complete successfully before [MqttClient#close()]
    /// so closing cannot discard the pending DISCONNECT. Completion confirms the write;
    /// the broker does not acknowledge receipt.
    public MqttClientBuilder will(MqttMessage value) {
        will = requireNonNull(value, "will");
        return this;
    }

    /// Sets the keep-alive interval in whole seconds from 0 (disabled) to 65535; default 60.
    /// Idle time is measured from completed writes. The interval also bounds a PINGREQ write
    /// and the wait for its PINGRESP, together.
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
    /// CONNACK. Also bounds each DISCONNECT write. An attempt that replaces a broker session,
    /// as [MqttClient#connect()] describes, applies it to each of its connections and
    /// DISCONNECT writes, so it can take several times this duration.
    /// Defaults to 30 seconds. Must be positive and no greater than 2147483647 milliseconds.
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
    public MqttClientBuilder responseTimeout(Duration value) {
        requireNonNull(value, "responseTimeout");
        responseTimeout = requireTimeout(value, "Response timeout");
        return this;
    }

    /// Decides whether the client reconnects after an established connection ends without a
    /// request to stop, and how long it waits between attempts. Defaults to
    /// [ReconnectPolicy#exponentialBackoff(Duration, Duration)] from 1 second up to
    /// 120 seconds, which does not retry the first failure of a connection request.
    /// [ReconnectPolicy#disabled()] leaves a client disconnected until the next
    /// [MqttClient#connect()].
    public MqttClientBuilder reconnectPolicy(ReconnectPolicy value) {
        reconnectPolicy = requireNonNull(value, "reconnectPolicy");
        return this;
    }

    /// Inclusive incoming packet size, including the fixed header. Checked before allocation.
    /// Defaults to 16 MiB. Must be between 2 and 268435460 bytes.
    /// Increasing this limit can also increase memory consumption.
    public MqttClientBuilder maxPacketSize(int value) {
        if (value < MIN_PACKET_SIZE || value > MAX_PACKET_SIZE) {
            throw new IllegalArgumentException(
                    "Packet size must be " + MIN_PACKET_SIZE + ".." + MAX_PACKET_SIZE + " bytes"
            );
        }
        maxPacketSize = value;
        return this;
    }

    /// Supplies monotonic time and delayed callbacks. Defaults to a shared daemon timer with
    /// [System#nanoTime]. The scheduler is borrowed and is not closed with the client.
    /// The default TCP and TLS transports use it for the connection and TLS handshake
    /// deadlines and TLS close-notify grace period. JDK socket connection and name resolution
    /// waits still use real time. Custom transports manage their own clocks.
    public MqttClientBuilder timerScheduler(TimerScheduler value) {
        scheduler = requireNonNull(value, "timerScheduler");
        return this;
    }

    /// Registers the single listener for connection and subscription notifications; defaults
    /// to no action.
    public MqttClientBuilder connectionListener(ConnectionListener value) {
        listener = requireNonNull(value, "connectionListener");
        return this;
    }

    /// Supplies the TLS context for an `mqtts` connection, including trust and client keys.
    /// If omitted, [#build()] obtains [SSLContext#getDefault()] once. The same context is
    /// reused across connection attempts. Initialize it before building and do not reinitialize
    /// it while the client is in use. Explicit TLS settings require `mqtts` and cannot be
    /// combined with [#transportFactory(Transport.Factory)].
    public MqttClientBuilder sslContext(SSLContext value) {
        sslContext = requireNonNull(value, "sslContext");
        return this;
    }

    /// Customizes TLS parameters once per [#build()], starting with the selected context's
    /// defaults and endpoint identification set to `"HTTPS"` for hostname verification.
    /// Setting the endpoint identification algorithm to `null` explicitly disables verification.
    /// The parameters are validated during [#build()], and a detached snapshot is used
    /// for every connection. Custom algorithm constraints and SNI matchers remain shared;
    /// do not mutate those objects while the client is in use.
    /// A later call replaces the customizer.
    /// Explicit TLS settings require `mqtts` and cannot be combined with
    /// [#transportFactory(Transport.Factory)].
    ///
    /// For AWS IoT Core with X.509 client authentication on port 443, configure the client keys
    /// in [#sslContext(SSLContext)] and set ALPN with:
    /// ```java
    /// builder.sslParameters(parameters ->
    ///         parameters.setApplicationProtocols(new String[]{"x-amzn-mqtt-ca"}));
    /// ```
    public MqttClientBuilder sslParameters(Consumer<SSLParameters> value) {
        sslParameters = requireNonNull(value, "sslParameters");
        return this;
    }

    /// Supplies a fresh transport for each connection; an attempt that replaces a broker
    /// session, as [MqttClient#connect()] describes, opens several. Defaults to TCP for `mqtt`
    /// and TLS over TCP for `mqtts`. The client owns and closes the returned transports.
    /// An explicit factory cannot be combined with [#sslContext(SSLContext)] or
    /// [#sslParameters(Consumer)]. The factory's transports must reject unsupported URI schemes.
    public MqttClientBuilder transportFactory(Transport.Factory value) {
        transportFactory = requireNonNull(value, "transportFactory");
        return this;
    }

    /// Creates a disconnected client. Call [MqttClient#connect] to open the connection.
    ///
    /// @throws IllegalStateException if the server or protocol version has not been specified
    /// @throws IllegalArgumentException if the session expiry is out of range, TLS settings are
    ///     invalid or incompatible, a persistent session has no explicit client id, or the Will
    ///     payload exceeds 65535 bytes
    public MqttClient build() {
        URI endpoint = server;
        if (endpoint == null || version == null) {
            throw new IllegalStateException("Server and MQTT version must be specified");
        }
        boolean tls = "mqtts".equals(endpoint.getScheme());
        Transport.Factory factory = transportFactory;
        if ((sslContext != null || sslParameters != null) && (!tls || factory != null)) {
            throw new IllegalArgumentException(
                    "TLS settings require mqtts and the default transport factory"
            );
        }
        if (!sessionExpiry.isZero()
                && (sessionExpiry.compareTo(Duration.ofSeconds(1)) < 0
                || sessionExpiry.compareTo(MAX_SESSION_EXPIRY) > 0)) {
            throw new IllegalArgumentException(
                    "Session expiry must be zero or 1..4294967295 seconds"
            );
        }
        // The CONNECT form checks the payload limit here, before the client starts any thread.
        Packet.Will connectWill = (will == null) ? null : new Packet.Will(
                will.topic(),
                will.payload(),
                will.qos(),
                will.retain(),
                EMPTY
        );
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
            factory = tls ? tlsTransportFactory(timer) : value -> new TcpTransport(value, timer);
        }
        return new ClientActor(
                endpoint,
                id,
                keepAlive,
                connectionTimeout,
                maxPacketSize,
                timer,
                listener,
                factory,
                sessionExpiry.isZero(),
                credentials,
                connectWill,
                reconnectPolicy.backoff(),
                reconnectPolicy.initialRetry(),
                responseTimeout,
                DeliveryOptions::execution
        );
    }

    private Transport.Factory tlsTransportFactory(TimerScheduler timer) {
        try {
            SSLContext context = sslContext != null ? sslContext : SSLContext.getDefault();
            SSLParameters parameters = defaultParameters(context);
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            if (sslParameters != null) {
                sslParameters.accept(parameters);
            }
            // A fresh socket preserves an explicit null endpoint algorithm. Reading back its
            // parameters also detaches the configuration from the customizer's mutable object.
            try (var socket = (SSLSocket) context.getSocketFactory().createSocket()) {
                socket.setSSLParameters(parameters);
                SSLParameters snapshot = socket.getSSLParameters();
                return id -> new TcpTransport(id, context, snapshot, timer);
            }
        } catch (IOException | NoSuchAlgorithmException failure) {
            throw new IllegalArgumentException("Cannot initialize TLS configuration", failure);
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
