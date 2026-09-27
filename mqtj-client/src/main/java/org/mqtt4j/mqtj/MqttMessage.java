package org.mqtt4j.mqtj;

import java.util.Arrays;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// An immutable application message. The payload is copied on construction and on access.
/// Equality compares payload contents, and [#toString()] reports only its byte count.
///
/// All QoS values can be represented; the current client accepts only QoS 0 operations.
///
/// @param topic the published topic name
/// @param payload the application bytes, possibly empty
/// @param qos the requested or received quality of service
/// @param retain whether publication requests retention, or reception is a retained delivery
@SuppressWarnings("exports") // Nullness annotations are compile-time only.
public record MqttMessage(Topic topic, byte[] payload, Qos qos, boolean retain) {

    public MqttMessage {
        requireNonNull(topic, "Topic must not be null");
        requireNonNull(payload, "Payload must not be null");
        requireNonNull(qos, "QoS must not be null");
        payload = payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    /// Returns the payload byte count without copying its contents.
    public int payloadSize() {
        return payload.length;
    }

    @Override
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
}
