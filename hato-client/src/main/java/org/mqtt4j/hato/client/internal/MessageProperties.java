package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.ArrayList;

import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.UserProperty;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static java.nio.charset.StandardCharsets.UTF_8;

/// Maps the properties of an application message to MQTT 5.0 PUBLISH properties and back.
/// Topic Alias and Subscription Identifier steer how the client handles a connection, so a
/// message never carries them.
public final class MessageProperties {

    private MessageProperties() {}

    /// Whether `message` has a property that only MQTT 5.0 can carry.
    public static boolean present(MqttMessage message) {
        return message.payloadUtf8()
                || message.messageExpiry().isPresent()
                || message.contentType().isPresent()
                || message.responseTopic().isPresent()
                || !message.userProperties().isEmpty()
                // Last, because the accessor copies the bytes.
                || message.correlationData().isPresent();
    }

    /// The PUBLISH properties of `message`, or [Properties#EMPTY] when it has none.
    static Properties of(MqttMessage message) {
        var entries = new ArrayList<Property>();
        if (message.payloadUtf8()) {
            entries.add(new Property.ByteValue(PropertyId.PAYLOAD_FORMAT_INDICATOR, 1));
        }
        message.messageExpiry().ifPresent(expiry -> entries.add(new Property.FourByteValue(
                PropertyId.MESSAGE_EXPIRY_INTERVAL,
                // Rounding up never shortens the lifetime that the application asked for.
                Math.ceilDiv(expiry.toNanos(), 1_000_000_000L)
        )));
        message.contentType().ifPresent(type -> entries.add(new Property.StringValue(
                PropertyId.CONTENT_TYPE,
                type
        )));
        message.responseTopic().ifPresent(topic -> entries.add(new Property.StringValue(
                PropertyId.RESPONSE_TOPIC,
                topic.name()
        )));
        message.correlationData().ifPresent(data -> entries.add(new Property.BinaryValue(
                PropertyId.CORRELATION_DATA,
                data
        )));
        for (UserProperty property : message.userProperties()) {
            entries.add(new Property.UserProperty(property.name(), property.value()));
        }
        return entries.isEmpty() ? Properties.EMPTY : new Properties(entries);
    }

    /// The bytes that the payload and the properties of `message` take in a PUBLISH, which bound
    /// the memory that a waiting publication holds. Saturates at [Integer#MAX_VALUE], which no
    /// limit admits.
    static int bodySize(MqttMessage message) {
        long bytes = message.payloadSize();
        // Each property is a one-byte identifier and its value; strings and binary data have
        // a two-byte length.
        if (message.payloadUtf8()) {
            bytes += 2;
        }
        if (message.messageExpiry().isPresent()) {
            bytes += 5;
        }
        bytes += message.contentType().map(type -> 3L + utf8Length(type)).orElse(0L);
        bytes += message.responseTopic().map(topic -> 3L + utf8Length(topic.name())).orElse(0L);
        bytes += message.correlationData().map(data -> 3L + data.length).orElse(0L);
        for (UserProperty property : message.userProperties()) {
            bytes += 5L + utf8Length(property.name()) + utf8Length(property.value());
        }
        return (int) Math.min(bytes, Integer.MAX_VALUE);
    }

    /// `message` with the application properties of a received PUBLISH.
    static MqttMessage received(MqttMessage message, Properties properties) {
        MqttMessage result = message;
        var userProperties = new ArrayList<UserProperty>();
        for (Property property : properties.entries()) {
            switch (property) {
                case Property.ByteValue p when p.id() == PropertyId.PAYLOAD_FORMAT_INDICATOR ->
                        result = result.withPayloadUtf8(p.value() == 1);
                case Property.FourByteValue p when p.id() == PropertyId.MESSAGE_EXPIRY_INTERVAL ->
                        result = result.withMessageExpiry(Duration.ofSeconds(p.value()));
                case Property.StringValue p when p.id() == PropertyId.CONTENT_TYPE ->
                        result = result.withContentType(p.value());
                case Property.StringValue p when p.id() == PropertyId.RESPONSE_TOPIC ->
                        result = result.withResponseTopic(new Topic(p.value()));
                case Property.BinaryValue p when p.id() == PropertyId.CORRELATION_DATA ->
                        result = result.withCorrelationData(p.value());
                case Property.UserProperty(String name, String value) ->
                        userProperties.add(new UserProperty(name, value));
                // Topic Alias and Subscription Identifier steer the client; the codec admits no
                // other property in PUBLISH.
                default -> { }
            }
        }
        return userProperties.isEmpty() ? result : result.withUserProperties(userProperties);
    }

    private static int utf8Length(String value) {
        return value.getBytes(UTF_8).length;
    }
}
