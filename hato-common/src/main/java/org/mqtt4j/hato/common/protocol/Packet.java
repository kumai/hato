package org.mqtt4j.hato.common.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.ReasonCode;
import org.mqtt4j.hato.common.value.Topic;
import org.mqtt4j.hato.common.value.TopicFilter;

import static java.util.Objects.requireNonNull;

/// Control packets shared by the MQTT 3.1.1 and 5.0 codecs.
///
/// The model uses MQTT 5.0 reason codes and property fields. Constructors check structural
/// invariants; codecs check string encoding, allowed reason codes, and version-specific rules.
/// Lists are copied. Byte arrays are transferred without copying: callers must not modify
/// them after construction, including through the accessors. Equality compares array contents.
///
/// - [MQTT 3.1.1 Plus Errata 01, section 3: Control Packets](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html)
/// - [MQTT 5.0, section 3: Control Packets](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)
public sealed interface Packet {

    /// CONNECT — connection parameters.
    ///
    /// Null will, username, or password means the field is absent. String representations omit
    /// username and password.
    ///
    /// `keepAliveSeconds` is 0 (disabled) to 65535. An empty client identifier is representable;
    /// the codec checks its version-specific restrictions. Password bytes transfer ownership.
    record Connect(
            String clientId,
            MqttVersion version,
            boolean cleanStart,
            int keepAliveSeconds,
            @Nullable Will will,
            @Nullable String username,
            byte @Nullable [] password,
            Properties properties
    ) implements Packet {

        // With the current javac, a compact constructor loses the nullable array parameter
        // annotation in its class file. Keep this explicit until the toolchain preserves it.
        public Connect(
                String clientId,
                MqttVersion version,
                boolean cleanStart,
                int keepAliveSeconds,
                @Nullable Will will,
                @Nullable String username,
                byte @Nullable [] password,
                Properties properties
        ) {
            requireNonNull(clientId, "Client identifier must not be null");
            requireNonNull(version, "MQTT version must not be null");
            requireNonNull(properties, "Properties must not be null");
            // Section 3.1.2.10 in both specifications: Keep Alive is a Two Byte Integer.
            if (keepAliveSeconds < 0 || keepAliveSeconds > 65535) {
                throw new IllegalArgumentException(
                        "Keep Alive must be between 0 and 65535 seconds"
                );
            }
            if (password != null) {
                checkBinaryLength(password, "Password");
            }
            this.clientId = clientId;
            this.version = version;
            this.cleanStart = cleanStart;
            this.keepAliveSeconds = keepAliveSeconds;
            this.will = will;
            this.username = username;
            this.password = password;
            this.properties = properties;
        }

        @Override
        public boolean equals(@Nullable Object other) {
            return other instanceof Connect that
                    && clientId.equals(that.clientId)
                    && version == that.version
                    && cleanStart == that.cleanStart
                    && keepAliveSeconds == that.keepAliveSeconds
                    && Objects.equals(will, that.will)
                    && Objects.equals(username, that.username)
                    && Arrays.equals(password, that.password)
                    && properties.equals(that.properties);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    clientId,
                    version,
                    cleanStart,
                    keepAliveSeconds,
                    will,
                    username,
                    Arrays.hashCode(password),
                    properties
            );
        }

        @Override
        public String toString() {
            return "Connect[clientId=" + clientId + ", version=" + version
                    + ", cleanStart=" + cleanStart + ", keepAliveSeconds=" + keepAliveSeconds
                    + ", will=" + will + ", properties=" + properties + "]";
        }
    }

    /// The Will carried by CONNECT, with transferred ownership of its binary payload.
    record Will(Topic topic, byte[] payload, Qos qos, boolean retain, Properties properties) {

        public Will {
            requireNonNull(topic, "Will topic must not be null");
            requireNonNull(payload, "Will payload must not be null");
            requireNonNull(qos, "Will QoS must not be null");
            requireNonNull(properties, "Properties must not be null");
            checkBinaryLength(payload, "Will payload");
        }

        @Override
        public boolean equals(@Nullable Object other) {
            return other instanceof Will that
                    && topic.equals(that.topic)
                    && Arrays.equals(payload, that.payload)
                    && qos == that.qos
                    && retain == that.retain
                    && properties.equals(that.properties);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, Arrays.hashCode(payload), qos, retain, properties);
        }

        /// Reports the payload only by size, like the properties' application data.
        @Override
        public String toString() {
            return "Will[topic=" + topic + ", payloadBytes=" + payload.length + ", qos=" + qos
                    + ", retain=" + retain + ", properties=" + properties + "]";
        }
    }

    /// CONNACK — the connection result.
    ///
    /// Session Present may be true only when the connection succeeds.
    record ConnAck(boolean sessionPresent, ReasonCode reasonCode, Properties properties)
            implements Packet {

        public ConnAck {
            requireNonNull(reasonCode, "Reason code must not be null");
            requireNonNull(properties, "Properties must not be null");
            // MQTT 3.1.1 [MQTT-3.2.2-4] and MQTT 5.0 [MQTT-3.2.2-6].
            if (sessionPresent && reasonCode != ReasonCode.SUCCESS) {
                throw new IllegalArgumentException("A refused connection cannot resume a session");
            }
        }
    }

    /// PUBLISH — an application message with transferred ownership of its payload.
    ///
    /// `packetId` is 0 for QoS 0 and 1 to 65535 for QoS 1 or 2. QoS 0 cannot set DUP.
    /// The payload may be empty and, unlike a Will payload, has no two-byte length limit.
    /// The codec checks the total packet size.
    ///
    /// This variant always has a full topic name. [AliasPublish] represents an MQTT 5.0
    /// PUBLISH whose topic name is omitted on the wire.
    record Publish(
            Topic topic,
            byte[] payload,
            Qos qos,
            boolean retain,
            boolean dup,
            int packetId,
            Properties properties
    ) implements Packet {

        public Publish {
            requireNonNull(topic, "Topic must not be null");
            checkPublishFields(payload, qos, dup, packetId, properties);
        }

        @Override
        public boolean equals(@Nullable Object other) {
            return other instanceof Publish that
                    && topic.equals(that.topic)
                    && Arrays.equals(payload, that.payload)
                    && qos == that.qos
                    && retain == that.retain
                    && dup == that.dup
                    && packetId == that.packetId
                    && properties.equals(that.properties);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    topic,
                    Arrays.hashCode(payload),
                    qos,
                    retain,
                    dup,
                    packetId,
                    properties
            );
        }
    }

    /// PUBLISH with an omitted wire topic name, available only in MQTT 5.0.
    ///
    /// The codec requires a Topic Alias property. The connection owner must resolve that alias
    /// to a [Publish] before delivery, using its mapping for the receiving direction. The codec
    /// does not retain mappings; each connection discards them when it closes.
    ///
    /// Payload ownership and packet identifier rules are the same as for [Publish].
    record AliasPublish(
            byte[] payload,
            Qos qos,
            boolean retain,
            boolean dup,
            int packetId,
            Properties properties
    ) implements Packet {

        public AliasPublish {
            checkPublishFields(payload, qos, dup, packetId, properties);
        }

        @Override
        public boolean equals(@Nullable Object other) {
            return other instanceof AliasPublish that
                    && Arrays.equals(payload, that.payload)
                    && qos == that.qos
                    && retain == that.retain
                    && dup == that.dup
                    && packetId == that.packetId
                    && properties.equals(that.properties);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    Arrays.hashCode(payload),
                    qos,
                    retain,
                    dup,
                    packetId,
                    properties
            );
        }
    }

    /// PUBACK — acknowledges a QoS 1 publication.
    ///
    /// MQTT 3.1.1 uses SUCCESS and empty properties.
    record PubAck(int packetId, ReasonCode reasonCode, Properties properties) implements Packet {

        public PubAck {
            checkPacketId(packetId);
            requireNonNull(reasonCode, "Reason code must not be null");
            requireNonNull(properties, "Properties must not be null");
        }
    }

    /// PUBREC — acknowledges receipt of a QoS 2 publication.
    record PubRec(int packetId, ReasonCode reasonCode, Properties properties) implements Packet {

        public PubRec {
            checkPacketId(packetId);
            requireNonNull(reasonCode, "Reason code must not be null");
            requireNonNull(properties, "Properties must not be null");
        }
    }

    /// PUBREL — releases a QoS 2 publication after PUBREC.
    record PubRel(int packetId, ReasonCode reasonCode, Properties properties) implements Packet {

        public PubRel {
            checkPacketId(packetId);
            requireNonNull(reasonCode, "Reason code must not be null");
            requireNonNull(properties, "Properties must not be null");
        }
    }

    /// PUBCOMP — completes the QoS 2 exchange after PUBREL.
    record PubComp(int packetId, ReasonCode reasonCode, Properties properties) implements Packet {

        public PubComp {
            checkPacketId(packetId);
            requireNonNull(reasonCode, "Reason code must not be null");
            requireNonNull(properties, "Properties must not be null");
        }
    }

    /// SUBSCRIBE — requests one or more subscriptions, preserving their order and duplicates.
    record Subscribe(int packetId, List<Subscription> subscriptions, Properties properties)
            implements Packet {

        public Subscribe {
            checkPacketId(packetId);
            // [MQTT-3.8.3-3] in MQTT 3.1.1.
            subscriptions = nonEmptyCopy(subscriptions, "Subscriptions");
            requireNonNull(properties, "Properties must not be null");
        }
    }

    /// A filter and its MQTT 5.0 subscription options.
    ///
    /// Retain Handling is 0 (send retained messages), 1 (send only for a new subscription),
    /// or 2 (do not send). MQTT 3.1.1 uses false, false, and 0 for the three options.
    /// The filter is kept as written, including a `$share/` prefix; [SharedTopicFilter]
    /// interprets shared subscriptions and states which distribution applies it. The MQTT 5.0
    /// codec rejects No Local on a shared subscription.
    record Subscription(
            TopicFilter filter,
            Qos qos,
            boolean noLocal,
            boolean retainAsPublished,
            int retainHandling
    ) {

        public Subscription {
            requireNonNull(filter, "Topic filter must not be null");
            requireNonNull(qos, "QoS must not be null");
            // MQTT 5.0 section 3.8.3.1.
            if (retainHandling < 0 || retainHandling > 2) {
                throw new IllegalArgumentException("Retain Handling must be 0, 1, or 2");
            }
        }
    }

    /// SUBACK — one result for each requested subscription, in request order.
    record SubAck(int packetId, List<ReasonCode> reasonCodes, Properties properties)
            implements Packet {

        public SubAck {
            checkPacketId(packetId);
            // MQTT 3.1.1 section 3.9.3: the payload contains at least one result.
            reasonCodes = nonEmptyCopy(reasonCodes, "Reason codes");
            requireNonNull(properties, "Properties must not be null");
        }
    }

    /// UNSUBSCRIBE — removes one or more filters, preserving their order and duplicates.
    record Unsubscribe(int packetId, List<TopicFilter> filters, Properties properties)
            implements Packet {

        public Unsubscribe {
            checkPacketId(packetId);
            // [MQTT-3.10.3-2] in MQTT 3.1.1.
            filters = nonEmptyCopy(filters, "Topic filters");
            requireNonNull(properties, "Properties must not be null");
        }
    }

    /// UNSUBACK — unsubscription results in request order; the list is empty for MQTT 3.1.1.
    record UnsubAck(int packetId, List<ReasonCode> reasonCodes, Properties properties)
            implements Packet {

        public UnsubAck {
            checkPacketId(packetId);
            reasonCodes = List.copyOf(reasonCodes);
            requireNonNull(properties, "Properties must not be null");
        }
    }

    /// PINGREQ — a keep-alive request with no variable header or payload.
    record PingReq() implements Packet {}

    /// PINGRESP — the response to PINGREQ, with no variable header or payload.
    record PingResp() implements Packet {}

    /// DISCONNECT — closes the connection.
    ///
    /// MQTT 3.1.1 uses SUCCESS and empty properties.
    record Disconnect(ReasonCode reasonCode, Properties properties) implements Packet {

        public Disconnect {
            requireNonNull(reasonCode, "Reason code must not be null");
            requireNonNull(properties, "Properties must not be null");
        }
    }

    /// AUTH — an authentication step available only in MQTT 5.0.
    record Auth(ReasonCode reasonCode, Properties properties) implements Packet {

        public Auth {
            requireNonNull(reasonCode, "Reason code must not be null");
            requireNonNull(properties, "Properties must not be null");
        }
    }

    private static void checkPublishFields(
            byte[] payload,
            Qos qos,
            boolean dup,
            int packetId,
            Properties properties
    ) {
        requireNonNull(payload, "Payload must not be null");
        requireNonNull(qos, "QoS must not be null");
        requireNonNull(properties, "Properties must not be null");
        if (qos == Qos.AT_MOST_ONCE) {
            // MQTT 3.1.1 [MQTT-2.3.1-5] and [MQTT-3.3.1-2] in both specifications.
            if (packetId != 0 || dup) {
                throw new IllegalArgumentException("QoS 0 requires packet id 0 and DUP false");
            }
        } else {
            checkPacketId(packetId);
        }
    }

    private static void checkPacketId(int packetId) {
        // MQTT 3.1.1 [MQTT-2.3.1-1]: zero is never transmitted as a Packet Identifier.
        if (packetId < 1 || packetId > 65535) {
            throw new IllegalArgumentException("Packet identifier must be between 1 and 65535");
        }
    }

    private static void checkBinaryLength(byte[] value, String label) {
        // MQTT 5.0 section 1.5.6; MQTT 3.1.1 sections 3.1.3.3 and 3.1.3.5.
        if (value.length > 65535) {
            throw new IllegalArgumentException(label + " exceeds 65535 bytes");
        }
    }

    private static <T> List<T> nonEmptyCopy(List<T> values, String label) {
        List<T> copy = List.copyOf(values);
        if (copy.isEmpty()) {
            throw new IllegalArgumentException(label + " must not be empty");
        }
        return copy;
    }
}
