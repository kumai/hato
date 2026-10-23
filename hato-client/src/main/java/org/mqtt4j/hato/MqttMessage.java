package org.mqtt4j.hato;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.common.protocol.MqttString;

import static java.util.Objects.requireNonNull;

/// An immutable application message: a topic, a payload, a QoS, a retain flag, and the MQTT 5.0
/// message properties. The payload and the Correlation Data are copied on construction and on
/// access. Equality compares every component, including the payload contents and the order of
/// the User Properties. [#toString()] reports the payload, the Correlation Data, and the User
/// Properties only by size, because they carry application data, and leaves out properties
/// that are not set. Each `with` method returns a copy with one setting changed.
///
/// All QoS values can be represented and published. The properties need MQTT 5.0: an MQTT 3.1.1
/// client refuses to publish a message that has any of them, and receives messages without
/// them. A received message carries the properties that the broker forwarded.
public final class MqttMessage {

    // A Message Expiry Interval is a four-byte count of seconds.
    private static final Duration MAX_MESSAGE_EXPIRY = Duration.ofSeconds(4_294_967_295L);

    private final Topic topic;

    private final byte[] payload;

    private final Qos qos;

    private final boolean retain;

    private final boolean payloadUtf8;

    private final @Nullable Duration messageExpiry;

    private final @Nullable String contentType;

    private final @Nullable Topic responseTopic;

    private final byte @Nullable [] correlationData;

    private final List<UserProperty> userProperties;

    // Owns payload and correlationData. Only arrays supplied from outside are copied first, so
    // the other with methods share the immutable arrays instead of copying them again.
    private MqttMessage(
            Topic topic,
            byte[] payload,
            Qos qos,
            boolean retain,
            boolean payloadUtf8,
            @Nullable Duration messageExpiry,
            @Nullable String contentType,
            @Nullable Topic responseTopic,
            byte @Nullable [] correlationData,
            List<UserProperty> userProperties
    ) {
        requireNonNull(topic, "Topic must not be null");
        requireNonNull(qos, "QoS must not be null");
        this.topic = topic;
        this.payload = payload;
        this.qos = qos;
        this.retain = retain;
        this.payloadUtf8 = payloadUtf8;
        this.messageExpiry = messageExpiry;
        this.contentType = contentType;
        this.responseTopic = responseTopic;
        this.correlationData = correlationData;
        this.userProperties = userProperties;
    }

    /// Creates a message without properties, with a copy of the application bytes.
    ///
    /// @param topic the published topic name
    /// @param payload the application bytes, possibly empty
    /// @param qos the requested or received quality of service
    /// @param retain whether publication requests retention, or reception is a retained delivery
    public static MqttMessage of(Topic topic, byte[] payload, Qos qos, boolean retain) {
        return new MqttMessage(
                topic,
                copy(payload),
                qos,
                retain,
                false,
                null,
                null,
                null,
                null,
                List.of()
        );
    }

    /// Returns a copy with a different published topic.
    public MqttMessage withTopic(Topic topic) {
        return new MqttMessage(
                topic,
                payload,
                qos,
                retain,
                payloadUtf8,
                messageExpiry,
                contentType,
                responseTopic,
                correlationData,
                userProperties
        );
    }

    /// Returns a copy with a different payload, copying the supplied bytes.
    public MqttMessage withPayload(byte[] payload) {
        return new MqttMessage(
                topic,
                copy(payload),
                qos,
                retain,
                payloadUtf8,
                messageExpiry,
                contentType,
                responseTopic,
                correlationData,
                userProperties
        );
    }

    /// Returns a copy with a different requested or received quality of service.
    public MqttMessage withQos(Qos qos) {
        return new MqttMessage(
                topic,
                payload,
                qos,
                retain,
                payloadUtf8,
                messageExpiry,
                contentType,
                responseTopic,
                correlationData,
                userProperties
        );
    }

    /// Returns a copy with a different retention request or retained-delivery flag.
    public MqttMessage withRetain(boolean retain) {
        return new MqttMessage(
                topic,
                payload,
                qos,
                retain,
                payloadUtf8,
                messageExpiry,
                contentType,
                responseTopic,
                correlationData,
                userProperties
        );
    }

    /// Returns a copy that declares the payload as UTF-8 encoded character data (true, the MQTT
    /// 5.0 Payload Format Indicator) or as unspecified bytes (false). Neither publication nor
    /// reception checks the payload against the declaration.
    public MqttMessage withPayloadUtf8(boolean payloadUtf8) {
        return new MqttMessage(
                topic,
                payload,
                qos,
                retain,
                payloadUtf8,
                messageExpiry,
                contentType,
                responseTopic,
                correlationData,
                userProperties
        );
    }

    /// Returns a copy with a lifetime (the MQTT 5.0 Message Expiry Interval) from zero to
    /// 4294967295 seconds; a fraction of a second is rounded up when sent. When the lifetime
    /// passes before the broker starts delivering the message to a subscriber, the broker
    /// discards it for that subscriber. The client sends the same lifetime however long the
    /// message waits to be sent, including a resend in a later connection.
    ///
    /// @throws IllegalArgumentException if `messageExpiry` is negative or exceeds 4294967295
    ///     seconds
    public MqttMessage withMessageExpiry(Duration messageExpiry) {
        requireNonNull(messageExpiry, "Message expiry must not be null");
        if (messageExpiry.isNegative() || messageExpiry.compareTo(MAX_MESSAGE_EXPIRY) > 0) {
            throw new IllegalArgumentException("Message expiry must be 0..4294967295 seconds");
        }
        return new MqttMessage(
                topic,
                payload,
                qos,
                retain,
                payloadUtf8,
                messageExpiry,
                contentType,
                responseTopic,
                correlationData,
                userProperties
        );
    }

    /// Returns a copy with a content type (the MQTT 5.0 Content Type) that describes the
    /// payload, such as a MIME type. It is an MQTT UTF-8 string of at most 65535 bytes and may
    /// be empty.
    ///
    /// @throws IllegalArgumentException if `contentType` contains U+0000 or an unpaired
    ///     surrogate, or exceeds 65535 bytes in UTF-8
    public MqttMessage withContentType(String contentType) {
        requireNonNull(contentType, "Content type must not be null");
        MqttString.validate(contentType);
        return new MqttMessage(
                topic,
                payload,
                qos,
                retain,
                payloadUtf8,
                messageExpiry,
                contentType,
                responseTopic,
                correlationData,
                userProperties
        );
    }

    /// Returns a copy that names the topic for a reply (the MQTT 5.0 Response Topic).
    public MqttMessage withResponseTopic(Topic responseTopic) {
        requireNonNull(responseTopic, "Response topic must not be null");
        return new MqttMessage(
                topic,
                payload,
                qos,
                retain,
                payloadUtf8,
                messageExpiry,
                contentType,
                responseTopic,
                correlationData,
                userProperties
        );
    }

    /// Returns a copy with a copy of `correlationData` (the MQTT 5.0 Correlation Data), at most
    /// 65535 bytes and possibly empty, which a reply can return to identify its request.
    ///
    /// @throws IllegalArgumentException if `correlationData` exceeds 65535 bytes
    public MqttMessage withCorrelationData(byte[] correlationData) {
        requireNonNull(correlationData, "Correlation data must not be null");
        if (correlationData.length > 65_535) {
            throw new IllegalArgumentException("Correlation data exceeds 65535 bytes");
        }
        return new MqttMessage(
                topic,
                payload,
                qos,
                retain,
                payloadUtf8,
                messageExpiry,
                contentType,
                responseTopic,
                correlationData.clone(),
                userProperties
        );
    }

    /// Returns a copy whose User Properties are a copy of `userProperties`, in the same order.
    /// An empty list leaves the copy without User Properties.
    ///
    /// @throws NullPointerException if `userProperties` or one of its elements is null
    public MqttMessage withUserProperties(List<UserProperty> userProperties) {
        requireNonNull(userProperties, "User Properties must not be null");
        return new MqttMessage(
                topic,
                payload,
                qos,
                retain,
                payloadUtf8,
                messageExpiry,
                contentType,
                responseTopic,
                correlationData,
                List.copyOf(userProperties)
        );
    }

    /// The published topic name.
    public Topic topic() {
        return topic;
    }

    /// A copy of the application bytes.
    public byte[] payload() {
        return payload.clone();
    }

    /// The requested or received quality of service.
    public Qos qos() {
        return qos;
    }

    /// Whether publication requests retention, or reception is a retained delivery: one that the
    /// broker sent from its retained messages as it made the subscription. A subscription with
    /// [Subscribe#retainAsPublished()] instead receives the retain flag that the publisher set.
    public boolean retain() {
        return retain;
    }

    /// Returns the payload byte count without copying its contents.
    public int payloadSize() {
        return payload.length;
    }

    /// Whether the payload is declared as UTF-8 encoded character data. False means unspecified
    /// bytes, which is also what MQTT 5.0 assumes without a Payload Format Indicator.
    public boolean payloadUtf8() {
        return payloadUtf8;
    }

    /// The lifetime of the message, or empty when it does not expire. A received message
    /// reports the lifetime that remained when the broker forwarded it.
    public Optional<Duration> messageExpiry() {
        return Optional.ofNullable(messageExpiry);
    }

    /// The content type that describes the payload, or empty when absent.
    public Optional<String> contentType() {
        return Optional.ofNullable(contentType);
    }

    /// The topic for a reply, or empty when absent.
    public Optional<Topic> responseTopic() {
        return Optional.ofNullable(responseTopic);
    }

    /// A copy of the Correlation Data, or empty when absent. Present Correlation Data can be an
    /// empty array.
    public Optional<byte[]> correlationData() {
        return correlationData == null ? Optional.empty() : Optional.of(correlationData.clone());
    }

    /// The User Properties in order, possibly repeating a name. The list cannot be modified.
    public List<UserProperty> userProperties() {
        return userProperties;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof MqttMessage that
                && topic.equals(that.topic)
                && Arrays.equals(payload, that.payload)
                && qos == that.qos
                && retain == that.retain
                && payloadUtf8 == that.payloadUtf8
                && Objects.equals(messageExpiry, that.messageExpiry)
                && Objects.equals(contentType, that.contentType)
                && Objects.equals(responseTopic, that.responseTopic)
                && Arrays.equals(correlationData, that.correlationData)
                && userProperties.equals(that.userProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                topic,
                Arrays.hashCode(payload),
                qos,
                retain,
                payloadUtf8,
                messageExpiry,
                contentType,
                responseTopic,
                Arrays.hashCode(correlationData),
                userProperties
        );
    }

    @Override
    public String toString() {
        var text = new StringBuilder("MqttMessage[topic=");
        text.append(topic)
                .append(", payloadBytes=").append(payload.length)
                .append(", qos=").append(qos)
                .append(", retain=").append(retain);
        if (payloadUtf8) {
            text.append(", payloadUtf8=true");
        }
        if (messageExpiry != null) {
            text.append(", messageExpiry=").append(messageExpiry);
        }
        if (contentType != null) {
            text.append(", contentType=").append(contentType);
        }
        if (responseTopic != null) {
            text.append(", responseTopic=").append(responseTopic);
        }
        if (correlationData != null) {
            text.append(", correlationDataBytes=").append(correlationData.length);
        }
        if (!userProperties.isEmpty()) {
            text.append(", userProperties=").append(userProperties.size());
        }
        return text.append(']').toString();
    }

    private static byte[] copy(byte[] payload) {
        return requireNonNull(payload, "Payload must not be null").clone();
    }
}
