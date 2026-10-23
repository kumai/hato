package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.function.Function;

import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.UserProperty;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
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
                seconds(expiry)
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

    /// The Will Properties of the Will message `will`: its message properties and, unless `delay`
    /// is zero, a Will Delay Interval rounded up to whole seconds.
    public static Properties will(MqttMessage will, Duration delay) {
        Properties properties = of(will);
        if (delay.isZero()) {
            return properties;
        }
        var entries = new ArrayList<>(properties.entries());
        entries.add(new Property.FourByteValue(PropertyId.WILL_DELAY_INTERVAL, seconds(delay)));
        return new Properties(entries);
    }

    /// The bytes that the payload and the properties of `message` take in a PUBLISH, which bound
    /// the memory that a waiting publication holds. Saturates at [Integer#MAX_VALUE], which no
    /// limit admits.
    static int bodySize(MqttMessage message) {
        return (int) Math.min((long) message.payloadSize() + size(message), Integer.MAX_VALUE);
    }

    /// The bytes that the properties of `message` take in a PUBLISH or a Will, without their
    /// length prefix. Saturates at [Integer#MAX_VALUE], which no limit admits.
    public static int size(MqttMessage message) {
        long bytes = 0;
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

    /// The application message that a received PUBLISH carries. `userPropertyOf` wraps each User
    /// Property, whose strings the codec validated while decoding them.
    static MqttMessage received(
            Packet.Publish publish,
            Function<Property.UserProperty, UserProperty> userPropertyOf
    ) {
        MqttMessage result = MqttMessage.of(
                publish.topic(),
                publish.payload(),
                publish.qos(),
                publish.retain()
        );
        var userProperties = new ArrayList<UserProperty>();
        for (Property property : publish.properties().entries()) {
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
                case Property.UserProperty p -> userProperties.add(userPropertyOf.apply(p));
                // Topic Alias and Subscription Identifier steer the client; the codec admits no
                // other property in PUBLISH.
                default -> { }
            }
        }
        return userProperties.isEmpty() ? result : result.withUserProperties(userProperties);
    }

    // Rounding up never shortens the lifetime or the delay that the application asked for.
    private static long seconds(Duration duration) {
        return Math.ceilDiv(duration.toNanos(), 1_000_000_000L);
    }

    private static int utf8Length(String value) {
        return value.getBytes(UTF_8).length;
    }
}
