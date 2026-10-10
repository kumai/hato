package org.mqtt4j.mqtj;

import java.util.Arrays;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// An immutable application message. The payload is copied on construction and on access.
/// Equality compares payload contents, and [#toString()] reports only its byte count.
/// Each `with` method returns a copy with one setting changed.
///
/// All QoS values can be represented; the current client accepts QoS 0 and QoS 1 operations.
public final class MqttMessage {

    private final Topic topic;

    private final byte[] payload;

    private final Qos qos;

    private final boolean retain;

    // Owns payload. Only bytes supplied from outside are copied first, so the other with
    // methods share the immutable array instead of copying a large payload again.
    private MqttMessage(Topic topic, byte[] payload, Qos qos, boolean retain) {
        requireNonNull(topic, "Topic must not be null");
        requireNonNull(qos, "QoS must not be null");
        this.topic = topic;
        this.payload = payload;
        this.qos = qos;
        this.retain = retain;
    }

    /// Creates a message with a copy of the application bytes.
    ///
    /// @param topic the published topic name
    /// @param payload the application bytes, possibly empty
    /// @param qos the requested or received quality of service
    /// @param retain whether publication requests retention, or reception is a retained delivery
    public static MqttMessage of(Topic topic, byte[] payload, Qos qos, boolean retain) {
        return new MqttMessage(topic, copy(payload), qos, retain);
    }

    /// Returns a copy with a different published topic.
    public MqttMessage withTopic(Topic topic) {
        return new MqttMessage(topic, payload, qos, retain);
    }

    /// Returns a copy with a different payload, copying the supplied bytes.
    public MqttMessage withPayload(byte[] payload) {
        return new MqttMessage(topic, copy(payload), qos, retain);
    }

    /// Returns a copy with a different requested or received quality of service.
    public MqttMessage withQos(Qos qos) {
        return new MqttMessage(topic, payload, qos, retain);
    }

    /// Returns a copy with a different retention request or retained-delivery flag.
    public MqttMessage withRetain(boolean retain) {
        return new MqttMessage(topic, payload, qos, retain);
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

    /// Whether publication requests retention, or reception is a retained delivery.
    public boolean retain() {
        return retain;
    }

    /// Returns the payload byte count without copying its contents.
    public int payloadSize() {
        return payload.length;
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof MqttMessage that
                && topic.equals(that.topic)
                && Arrays.equals(payload, that.payload)
                && qos == that.qos
                && retain == that.retain;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, Arrays.hashCode(payload), qos, retain);
    }

    @Override
    public String toString() {
        return "MqttMessage[topic=" + topic + ", payloadBytes=" + payload.length
                + ", qos=" + qos + ", retain=" + retain + "]";
    }

    private static byte[] copy(byte[] payload) {
        return requireNonNull(payload, "Payload must not be null").clone();
    }
}
