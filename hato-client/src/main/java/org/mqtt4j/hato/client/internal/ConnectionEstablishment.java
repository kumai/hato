package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.OptionalLong;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttCredentials;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static org.mqtt4j.hato.client.internal.SessionCore.protocolError;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// The session core's part for establishing connections: what each connection of an attempt
/// sends in CONNECT, and what its CONNACK means for the attempt. The core keeps the attempt's
/// lifecycle. It opens and closes the connections, sends the CONNECT built here, and acts on
/// the answer. This part keeps no state: the core passes each connection's step, the attempt's
/// credentials, and whether this client object has started a session.
final class ConnectionEstablishment {

    /// The connections of one MQTT 3.1.1 attempt. A broker session without local state cannot
    /// continue, so an attempt that finds one replaces it, using up to three connections. As
    /// MQTT 3.1.1 describes for discarding session state on both sides, it disconnects,
    /// connects with Clean Session 1, and disconnects again; then it starts a new persistent
    /// session. MQTT 5.0 needs one connection, whose Clean Start replaces the broker's session,
    /// so its attempts stay at the first step.
    enum Step {
        /// The attempt's first connection, which may find a broker session without local state.
        FIRST,
        /// A Clean Session 1 connection without a Will, which discards that session.
        DISCARD,
        /// The connection that starts a new persistent session once the old one is discarded.
        NEW_SESSION;

        /// The step of the connection that follows an abandoned one.
        Step next() {
            return this == FIRST ? DISCARD : NEW_SESSION;
        }
    }

    /// What a CONNACK means for the attempt.
    sealed interface Outcome {

        /// The broker refused the connection, or its CONNACK breaks the protocol.
        record Failed(RuntimeException cause) implements Outcome {}

        /// The attempt gives the connection up, closing it with DISCONNECT so that the broker
        /// discards its Will. Without `refusal`, the attempt continues on its next connection
        /// once DISCONNECT is written; with it, the attempt fails then.
        record Abandoned(@Nullable MqttConnectException refusal) implements Outcome {}

        /// The connection is established with `ack`, which continues the session when it has
        /// Session Present and otherwise starts a new one. The connection keeps alive at
        /// `keepAlive`: the configured interval, or the one that the broker set instead. The
        /// broker takes up to `receiveMaximum` QoS 1 and QoS 2 publications at once from this
        /// client, and accepts Subscription Identifiers when `subscriptionIdentifiers` is true:
        /// on MQTT 5.0, unless it announced them unavailable. The client may set up to
        /// `topicAliasMaximum` Topic Aliases, which is 0 for MQTT 3.1.1 and for a broker that
        /// announced none.
        record Established(
                ConnAck ack,
                Duration keepAlive,
                int receiveMaximum,
                boolean subscriptionIdentifiers,
                int topicAliasMaximum
        ) implements Outcome {}
    }

    private static final Log log = Log.of(ConnectionEstablishment.class);

    // The largest packet that MQTT can carry, including the fixed header. A broker that
    // announces no Maximum Packet Size, or a larger one, accepts packets up to this size.
    static final int LARGEST_PACKET_SIZE = 268_435_460;

    // The Receive Maximum when the broker announces none, as no MQTT 3.1.1 broker can.
    private static final int DEFAULT_RECEIVE_MAXIMUM = 65_535;

    // The Topic Alias Maximum that the client announces in CONNECT. A topic takes up to 65,535
    // bytes, so the mapping holds at most about 1 MiB. The value is provisional: it covers
    // Mosquitto's default of 10 aliases, and measurements may change it.
    static final int TOPIC_ALIAS_MAXIMUM = 16;

    private final String clientId;

    private final MqttVersion version;

    private final Duration keepAlive;

    // Zero session expiry starts a new session with every connection.
    private final boolean cleanStart;

    // MQTT 5.0 counts the expiry in whole seconds. Rounding up never shortens the requested
    // expiry; the largest value means that the session never expires.
    private final long sessionExpirySeconds;

    private final int maxPacketSize;

    private final Packet.@Nullable Will will;

    ConnectionEstablishment(
            String clientId,
            MqttVersion version,
            Duration keepAlive,
            Duration sessionExpiry,
            int maxPacketSize,
            Packet.@Nullable Will will
    ) {
        this.clientId = clientId;
        this.version = version;
        this.keepAlive = keepAlive;
        cleanStart = sessionExpiry.isZero();
        sessionExpirySeconds = Math.ceilDiv(sessionExpiry.toNanos(), 1_000_000_000L);
        this.maxPacketSize = maxPacketSize;
        this.will = will;
    }

    /// The CONNECT of a connection at `step` that sends the attempt's `credentials`, if any.
    /// `sessionEstablished` tells whether this client object has started a session, and an
    /// MQTT 5.0 CONNECT announces `receiveMaximum`.
    Packet.Connect connect(
            Step step,
            @Nullable MqttCredentials credentials,
            boolean sessionEstablished,
            int receiveMaximum
    ) {
        return switch (version) {
            case V3_1_1 -> {
                // The connection that discards a broker session carries no Will, so losing it
                // before its DISCONNECT publishes nothing.
                boolean discarding = step == Step.DISCARD;
                yield connectPacket(
                        cleanStart || discarding,
                        discarding ? null : will,
                        credentials,
                        EMPTY
                );
            }
            // Without local state, Clean Start replaces any session that the broker kept, so
            // one connection starts the new session.
            case V5 -> connectPacket(
                    cleanStart || !sessionEstablished,
                    will,
                    credentials,
                    connectProperties(receiveMaximum)
            );
        };
    }

    private Packet.Connect connectPacket(
            boolean cleanStartFlag,
            Packet.@Nullable Will connectWill,
            @Nullable MqttCredentials credentials,
            Properties properties
    ) {
        return new Packet.Connect(
                clientId,
                version,
                cleanStartFlag,
                (int) keepAlive.toSeconds(),
                connectWill,
                credentials == null ? null : credentials.username(),
                credentials == null ? null : credentials.password().orElse(null),
                properties
        );
    }

    /// The MQTT 5.0 CONNECT properties: the expiry of a persistent session, how many QoS 1 and
    /// QoS 2 publications the broker may send before their acknowledgements, the largest packet
    /// that the client accepts, and how many Topic Aliases the broker may set.
    private Properties connectProperties(int receiveMaximum) {
        var properties = new ArrayList<Property>();
        if (!cleanStart) {
            properties.add(new Property.FourByteValue(
                    PropertyId.SESSION_EXPIRY_INTERVAL,
                    sessionExpirySeconds
            ));
        }
        properties.add(new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, receiveMaximum));
        properties.add(new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, maxPacketSize));
        properties.add(new Property.TwoByteValue(
                PropertyId.TOPIC_ALIAS_MAXIMUM,
                TOPIC_ALIAS_MAXIMUM
        ));
        return new Properties(properties);
    }

    /// What `ack` means for the connection at `step` of attempt number `attempt`.
    /// `sessionEstablished` tells whether this client object has started a session that a
    /// Session Present can continue.
    Outcome accept(Packet.ConnAck ack, Step step, boolean sessionEstablished, long attempt) {
        if (ack.reasonCode() != ReasonCode.SUCCESS) {
            // An MQTT 5.0 broker can explain its refusal and name another server; an MQTT 3.1.1
            // CONNACK carries no properties.
            Optional<String> explanation = text(ack.properties(), PropertyId.REASON_STRING);
            return new Outcome.Failed(new MqttConnectException(
                    "Broker refused connection: " + ack.reasonCode()
                            + explanation.map(reason -> " (" + reason + ")").orElse(""),
                    ack.reasonCode(),
                    explanation.orElse(null),
                    text(ack.properties(), PropertyId.SERVER_REFERENCE).orElse(null)
            ));
        }
        return switch (version) {
            case V3_1_1 -> acceptMqtt311(ack, step, sessionEstablished, attempt);
            case V5 -> acceptMqtt5(ack, sessionEstablished, attempt);
        };
    }

    private Outcome acceptMqtt311(
            Packet.ConnAck ack,
            Step step,
            boolean sessionEstablished,
            long attempt
    ) {
        if (ack.sessionPresent() && (cleanStart || step == Step.DISCARD)) {
            return new Outcome.Failed(protocolError("Session Present with Clean Session"));
        }
        if (step == Step.DISCARD) {
            log.info("Discarded broker session without local state for {}", clientId);
            return new Outcome.Abandoned(null);
        }
        if (ack.sessionPresent() && !sessionEstablished && step == Step.FIRST) {
            // The attempt goes on to discard the broker's session.
            return new Outcome.Abandoned(null);
        }
        if (ack.sessionPresent() && !sessionEstablished) {
            // Only one discard per attempt: a session found again fails the attempt.
            return new Outcome.Abandoned(
                    new MqttConnectException("Broker session present after discarding it")
            );
        }
        return new Outcome.Established(
                ConnAck.of(ack.sessionPresent(), attempt),
                keepAlive,
                DEFAULT_RECEIVE_MAXIMUM,
                false,
                0
        );
    }

    private Outcome acceptMqtt5(Packet.ConnAck ack, boolean sessionEstablished, long attempt) {
        // A connection without local state sent Clean Start, which the broker answers without
        // Session Present [MQTT-3.2.2-2].
        if (ack.sessionPresent() && (cleanStart || !sessionEstablished)) {
            return new Outcome.Failed(protocolError("Session Present with Clean Start"));
        }
        Properties properties = ack.properties();
        // The client announces no Authentication Method, so the broker may send none back
        // [MQTT-4.12.0-6].
        if (properties.find(PropertyId.AUTHENTICATION_METHOD).isPresent()) {
            return new Outcome.Failed(protocolError("Authentication Method in CONNACK"));
        }
        text(properties, PropertyId.REASON_STRING).ifPresent(reason ->
                log.debug("CONNACK reason for {}: {}", clientId, reason));
        // An Assigned Client Identifier answers only an empty client identifier, which this
        // client never sends, so it is not read.
        ConnAck connAck = ConnAck.of(ack.sessionPresent(), attempt)
                .withMaximumQos(Qos.of((int) number(properties, PropertyId.MAXIMUM_QOS).orElse(2)))
                .withRetainAvailable(number(properties, PropertyId.RETAIN_AVAILABLE).orElse(1) == 1)
                .withWildcardSubscriptionAvailable(
                        number(properties, PropertyId.WILDCARD_SUBSCRIPTION_AVAILABLE)
                                .orElse(1) == 1
                )
                .withSharedSubscriptionAvailable(
                        number(properties, PropertyId.SHARED_SUBSCRIPTION_AVAILABLE)
                                .orElse(1) == 1
                )
                .withMaximumPacketSize((int) Math.min(
                        number(properties, PropertyId.MAXIMUM_PACKET_SIZE)
                                .orElse(LARGEST_PACKET_SIZE),
                        LARGEST_PACKET_SIZE
                ));
        // Server Keep Alive replaces the configured interval for this connection only.
        OptionalLong serverKeepAlive = number(properties, PropertyId.SERVER_KEEP_ALIVE);
        Duration connectionKeepAlive = serverKeepAlive.isPresent()
                ? Duration.ofSeconds(serverKeepAlive.getAsLong())
                : keepAlive;
        int receiveMaximum = (int) number(properties, PropertyId.RECEIVE_MAXIMUM)
                .orElse(DEFAULT_RECEIVE_MAXIMUM);
        boolean subscriptionIdentifiers =
                number(properties, PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE).orElse(1) == 1;
        int topicAliasMaximum = (int) number(properties, PropertyId.TOPIC_ALIAS_MAXIMUM)
                .orElse(0);
        return new Outcome.Established(
                connAck,
                connectionKeepAlive,
                receiveMaximum,
                subscriptionIdentifiers,
                topicAliasMaximum
        );
    }

    /// The value of the string property `id`, or empty when `properties` has none.
    static Optional<String> text(Properties properties, PropertyId id) {
        return properties.find(id).orElse(null) instanceof Property.StringValue(var _, var value)
                ? Optional.of(value)
                : Optional.empty();
    }

    /// The value of the integer property `id`, or empty when `properties` has none.
    static OptionalLong number(Properties properties, PropertyId id) {
        return switch (properties.find(id).orElse(null)) {
            case Property.ByteValue(var _, var value) -> OptionalLong.of(value);
            case Property.TwoByteValue(var _, var value) -> OptionalLong.of(value);
            case Property.FourByteValue(var _, var value) -> OptionalLong.of(value);
            case null, default -> OptionalLong.empty();
        };
    }
}
