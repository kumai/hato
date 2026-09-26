package org.mqtt4j.mqtj;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.client.internal.ClientActor;
import org.mqtt4j.mqtj.client.internal.DefaultTimerScheduler;
import org.mqtt4j.mqtj.client.internal.common.protocol.MqttString;
import org.mqtt4j.mqtj.spi.TimerScheduler;
import org.mqtt4j.mqtj.spi.Transport;

import static java.util.Objects.requireNonNull;

/// Configures a client without opening a connection. Not thread safe.
/// Set [#server(URI)] and [#version(MqttVersion)] before calling [#build].
/// Invalid or unsupported configuration is rejected with [IllegalArgumentException].
public final class MqttClientBuilder {

    private static final Duration DEFAULT_KEEP_ALIVE = Duration.ofSeconds(60);

    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(30);

    private static final Duration MAX_CONNECTION_TIMEOUT = Duration.ofMillis(Integer.MAX_VALUE);

    private static final int DEFAULT_MAX_PACKET_SIZE = 16 * 1024 * 1024;

    private static final int MAX_PORT = 65_535;

    private static final int MAX_KEEP_ALIVE_SECONDS = 65_535;

    private static final int MIN_PACKET_SIZE = 2;

    private static final int MAX_PACKET_SIZE = 268_435_460;

    private static final int GENERATED_CLIENT_ID_LENGTH = 23;

    private static final String CLIENT_ID_PREFIX = "mqtj";

    private @Nullable URI server;

    private @Nullable String clientId;

    private @Nullable MqttVersion version;

    private Duration keepAlive = DEFAULT_KEEP_ALIVE;

    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    private int maxPacketSize = DEFAULT_MAX_PACKET_SIZE;

    private TimerScheduler scheduler = DefaultTimerScheduler.instance;

    private ConnectionListener listener = _ -> { };

    private Transport.Factory transportFactory = Transport::tcp;

    MqttClientBuilder() {
    }

    /// A required TCP broker URI, such as `mqtt://localhost:1883`. The default port is 1883.
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
        if (!"mqtt".equals(value.getScheme())
            || value.getHost() == null
            || value.getRawUserInfo() != null
            || value.getRawQuery() != null
            || value.getRawFragment() != null
            || (value.getRawPath() != null && !value.getRawPath().isEmpty())
            || value.getPort() == 0
            || value.getPort() > MAX_PORT
        ) {
            throw new IllegalArgumentException(
                    "Expected mqtt://host[:port]: " + redactUserInfo(value)
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

    /// Uses this nonempty MQTT UTF-8 identifier. Omission generates 23 ASCII letters/digits.
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

    /// Total deadline for transport establishment and CONNACK; also bounds the DISCONNECT write.
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
    /// Increasing this limit can also increase internal buffering budgets.
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

    /// Registers the single listener for established connection endings; defaults to no action.
    public MqttClientBuilder connectionListener(ConnectionListener value) {
        listener = requireNonNull(value, "connectionListener");
        return this;
    }

    /// Supplies a fresh transport for each connection attempt; defaults to [Transport#tcp].
    /// The client owns and closes the returned transports.
    public MqttClientBuilder transportFactory(Transport.Factory value) {
        transportFactory = requireNonNull(value, "transportFactory");
        return this;
    }

    /// Creates a disconnected client. Call [MqttClient#connect] to open the connection.
    ///
    /// @throws IllegalStateException if the server or protocol version has not been specified
    public MqttClient build() {
        URI endpoint = server;
        if (endpoint == null || version == null) {
            throw new IllegalStateException("Server and MQTT version must be specified");
        }
        String id = clientId;
        if (id == null) {
            id = CLIENT_ID_PREFIX + UUID.randomUUID().toString().replace("-", "")
                    .substring(0, GENERATED_CLIENT_ID_LENGTH - CLIENT_ID_PREFIX.length());
        }
        return new ClientActor(
                endpoint,
                id,
                keepAlive,
                connectionTimeout,
                maxPacketSize,
                scheduler,
                listener,
                transportFactory
        );
    }
}
