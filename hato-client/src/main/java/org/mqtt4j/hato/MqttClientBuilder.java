package org.mqtt4j.hato;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.ClientActor;
import org.mqtt4j.hato.client.internal.DefaultTimerScheduler;
import org.mqtt4j.hato.client.internal.TcpTransport;
import org.mqtt4j.hato.client.internal.common.protocol.MqttString;
import org.mqtt4j.hato.spi.TimerScheduler;
import org.mqtt4j.hato.spi.Transport;

import static java.util.Objects.requireNonNull;

/// Configures a client without opening a connection. Not thread safe.
/// Set [#server(URI)] and [#version(MqttVersion)] before calling [#build].
/// Invalid or unsupported configuration is rejected with [IllegalArgumentException].
public final class MqttClientBuilder {

    private static final Duration DEFAULT_KEEP_ALIVE = Duration.ofSeconds(60);

    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(30);

    private static final Duration MAX_CONNECTION_TIMEOUT = Duration.ofMillis(Integer.MAX_VALUE);

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

    private boolean cleanStart = true;

    private Duration sessionExpiry = Duration.ZERO;

    private int maxPacketSize = DEFAULT_MAX_PACKET_SIZE;

    private TimerScheduler scheduler = DefaultTimerScheduler.instance;

    private ConnectionListener listener = _ -> { };

    private Transport.@Nullable Factory transportFactory;

    private @Nullable SSLContext sslContext;

    private @Nullable Consumer<SSLParameters> sslParameters;

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

    /// Uses this nonempty MQTT UTF-8 identifier. Persistent sessions require an explicit value.
    /// Otherwise, omission generates 23 ASCII letters/digits.
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

    /// Selects whether a connection without local session state starts a new session; default
    /// true. With false, even the first connection asks the broker to resume an existing session,
    /// and [MqttClient#connect()] fails when the client has no state for it. A client gains local
    /// state when one of its persistent connections succeeds, and its later connections request
    /// resumption of that state.
    /// MQTT 3.1.1 supports only true with zero [#sessionExpiry(Duration)], or false with a
    /// positive expiry. [#build()] rejects other combinations. These map to Clean Session 1
    /// and Clean Session 0, respectively. The broker determines the persistent session's lifetime.
    public MqttClientBuilder cleanStart(boolean value) {
        cleanStart = value;
        return this;
    }

    /// Configures session retention after disconnection; default zero.
    /// [#build()] accepts zero, or durations from 1 through 4294967295 seconds inclusive.
    /// Fractional seconds are accepted within that range, such as 1.5 seconds.
    /// MQTT 3.1.1 requires zero with [#cleanStart(boolean)] true, or a positive value with it
    /// false and an explicit [#clientId(String)]. The positive duration is not sent to the
    /// broker or enforced locally; every positive value selects Clean Session 0, and the
    /// broker determines the session's lifetime.
    public MqttClientBuilder sessionExpiry(Duration value) {
        sessionExpiry = requireNonNull(value, "sessionExpiry");
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

    /// Total deadline for transport establishment, including any TLS handshake, and CONNACK;
    /// also bounds the DISCONNECT write.
    /// Defaults to 30 seconds. Must be positive and no greater than 2147483647 milliseconds.
    public MqttClientBuilder connectionTimeout(Duration value) {
        requireNonNull(value, "connectionTimeout");
        if (value.isNegative() || value.isZero()
                || value.compareTo(MAX_CONNECTION_TIMEOUT) > 0) {
            throw new IllegalArgumentException(
                    "Connection timeout must be positive and <= "
                            + MAX_CONNECTION_TIMEOUT.toMillis() + " milliseconds"
            );
        }
        connectionTimeout = value;
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
    /// for every connection attempt. Custom algorithm constraints and SNI matchers remain shared;
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

    /// Supplies a fresh transport for each connection attempt. Defaults to TCP for `mqtt`
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
    /// @throws IllegalArgumentException if session or TLS settings are invalid or incompatible,
    ///     or a persistent session has no explicit client id
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
        if (cleanStart != sessionExpiry.isZero()) {
            throw new IllegalArgumentException(
                    "MQTT 3.1.1 requires cleanStart true with zero session expiry, "
                            + "or false with positive session expiry"
            );
        }
        String id = clientId;
        if (id == null) {
            if (!cleanStart) {
                throw new IllegalArgumentException(
                        "Persistent sessions require an explicit client id"
                );
            }
            id = CLIENT_ID_PREFIX + UUID.randomUUID().toString().replace("-", "")
                    .substring(0, GENERATED_CLIENT_ID_LENGTH - CLIENT_ID_PREFIX.length());
        }
        if (factory == null) {
            factory = tls ? tlsTransportFactory() : Transport::tcp;
        }
        return new ClientActor(
                endpoint,
                id,
                keepAlive,
                connectionTimeout,
                maxPacketSize,
                scheduler,
                listener,
                factory,
                cleanStart
        );
    }

    private Transport.Factory tlsTransportFactory() {
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
                return id -> new TcpTransport(id, context, snapshot);
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
}
