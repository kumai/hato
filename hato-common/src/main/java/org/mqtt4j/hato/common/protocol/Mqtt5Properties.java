package org.mqtt4j.hato.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.mqtt4j.hato.common.value.ReasonCode.MALFORMED_PACKET;
import static org.mqtt4j.hato.common.value.ReasonCode.PACKET_TOO_LARGE;
import static org.mqtt4j.hato.common.value.ReasonCode.PROTOCOL_ERROR;
import static org.mqtt4j.hato.common.value.ReasonCode.TOPIC_ALIAS_INVALID;

final class Mqtt5Properties {

    private Mqtt5Properties() {}

    static Properties read(ByteBuffer input, PacketType type) {
        return read(input, type, false);
    }

    static Properties readWill(ByteBuffer input) {
        return read(input, PacketType.CONNECT, true);
    }

    static void write(ByteArrayOutputStream output, Properties properties, PacketType type) {
        write(output, properties, type, false);
    }

    static void writeWill(ByteArrayOutputStream output, Properties properties) {
        write(output, properties, PacketType.CONNECT, true);
    }

    /// The bytes that `properties` take in a packet of `type`, without their length prefix,
    /// after checking them as [#write(ByteArrayOutputStream, Properties, PacketType)] does.
    static int size(Properties properties, PacketType type) {
        return size(properties, type, false);
    }

    /// Writes `properties`, which [#size(Properties, PacketType)] has checked, into `output` from
    /// `offset` without their length prefix, and returns the offset after them.
    static int write(byte[] output, int offset, Properties properties) {
        int at = offset;
        // An index rather than an iterator, which each PUBLISH written could otherwise allocate.
        List<Property> entries = properties.entries();
        for (int i = 0; i < entries.size(); i++) {
            Property property = entries.get(i);
            at = RemainingLength.write(output, at, property.id().value());
            switch (property) {
                case Property.ByteValue p -> output[at++] = (byte) p.value();
                case Property.TwoByteValue p -> at = writeUnsigned(output, at, p.value(), 2);
                case Property.FourByteValue p -> at = writeUnsigned(output, at, p.value(), 4);
                case Property.VariableByteValue p ->
                        at = RemainingLength.write(output, at, p.value());
                case Property.BinaryValue p -> at = BinaryData.write(output, at, p.value());
                case Property.StringValue p -> at = MqttString.write(output, at, p.value());
                case Property.UserProperty p -> {
                    at = MqttString.write(output, at, p.name());
                    at = MqttString.write(output, at, p.value());
                }
            }
        }
        return at;
    }

    private static Properties read(ByteBuffer input, PacketType type, boolean will) {
        int length = RemainingLength.readMinimal(input);
        if (length > input.remaining()) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Truncated MQTT properties");
        }
        ByteBuffer values = input.slice(input.position(), length);
        input.position(input.position() + length);
        var entries = new ArrayList<Property>();
        while (values.hasRemaining()) {
            if (entries.size() == PacketCodec.MAX_PROPERTIES) {
                throw new PacketDecodeException(
                        PACKET_TOO_LARGE,
                        "More than " + PacketCodec.MAX_PROPERTIES + " MQTT properties"
                );
            }
            PropertyId id;
            try {
                id = PropertyId.of(RemainingLength.readMinimal(values));
                checkAllowed(id, type, will);
            } catch (IllegalArgumentException e) {
                // Section 2.2.2.2 classifies unknown or misplaced identifiers as malformed.
                throw new PacketDecodeException(
                        MALFORMED_PACKET,
                        "Unknown or misplaced property",
                        e
                );
            }
            try {
                entries.add(readValue(values, id));
            } catch (BufferUnderflowException e) {
                throw new PacketDecodeException(MALFORMED_PACKET, "Truncated property value", e);
            } catch (IllegalArgumentException e) {
                // Section 3.3.4 assigns a specific disconnect reason to the forbidden alias zero.
                throw new PacketDecodeException(
                        id == PropertyId.TOPIC_ALIAS ? TOPIC_ALIAS_INVALID : PROTOCOL_ERROR,
                        "Invalid MQTT property",
                        e
                );
            }
        }
        var properties = new Properties(entries);
        try {
            checkProperties(properties, type, will);
        } catch (IllegalArgumentException e) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "Invalid MQTT property", e);
        }
        return properties;
    }

    private static Property readValue(ByteBuffer input, PropertyId id) {
        return switch (id.kind()) {
            case BYTE -> new Property.ByteValue(id, input.get() & 0xFF);
            case TWO_BYTE -> new Property.TwoByteValue(id, readUnsigned(input, 2));
            case FOUR_BYTE -> new Property.FourByteValue(
                    id,
                    Integer.toUnsignedLong(readUnsigned(input, 4))
            );
            case VARIABLE_BYTE -> new Property.VariableByteValue(
                    id,
                    RemainingLength.readMinimal(input)
            );
            case BINARY -> new Property.BinaryValue(id, BinaryData.read(input));
            case STRING -> new Property.StringValue(id, MqttString.read(input));
            case STRING_PAIR -> new Property.UserProperty(
                    MqttString.read(input),
                    MqttString.read(input)
            );
        };
    }

    private static void write(
            ByteArrayOutputStream output,
            Properties properties,
            PacketType type,
            boolean will
    ) {
        var values = new byte[size(properties, type, will)];
        write(values, 0, properties);
        RemainingLength.write(output, values.length);
        output.writeBytes(values);
    }

    private static int size(Properties properties, PacketType type, boolean will) {
        checkProperties(properties, type, will);
        // Each property is its identifier, a variable byte integer, and its value; strings and
        // binary data have a two-byte length. Sizing the strings checks them, so that nothing
        // is written for properties that cannot be. A sum that no length can state fails here,
        // before an array is allocated for it.
        long size = 0;
        List<Property> entries = properties.entries();
        for (int i = 0; i < entries.size(); i++) {
            Property property = entries.get(i);
            size += RemainingLength.encodedSize(property.id().value()) + switch (property) {
                case Property.ByteValue _ -> 1;
                case Property.TwoByteValue _ -> 2;
                case Property.FourByteValue _ -> 4;
                case Property.VariableByteValue p -> RemainingLength.encodedSize(p.value());
                case Property.BinaryValue p -> 2 + p.value().length;
                case Property.StringValue p -> MqttString.encodedSize(p.value());
                case Property.UserProperty p ->
                        MqttString.encodedSize(p.name()) + MqttString.encodedSize(p.value());
            };
        }
        if (size > RemainingLength.MAX_VALUE) {
            throw new IllegalArgumentException("MQTT properties exceed the Remaining Length limit");
        }
        return (int) size;
    }

    private static void checkProperties(Properties properties, PacketType type, boolean will) {
        var seen = EnumSet.noneOf(PropertyId.class);
        for (Property property : properties.entries()) {
            PropertyId id = property.id();
            checkAllowed(id, type, will);
            boolean repeated = id == PropertyId.USER_PROPERTY
                    || id == PropertyId.SUBSCRIPTION_IDENTIFIER && type == PacketType.PUBLISH;
            if (!seen.add(id) && !repeated) {
                throw new IllegalArgumentException("Duplicate property: " + id);
            }
        }
        // Later packets depend on the method agreed on in CONNECT, which the codec cannot know.
        if (!will && type == PacketType.CONNECT
                && seen.contains(PropertyId.AUTHENTICATION_DATA)
                && !seen.contains(PropertyId.AUTHENTICATION_METHOD)) {
            throw new IllegalArgumentException(
                    "Authentication Data requires Authentication Method"
            );
        }
    }

    private static void checkAllowed(PropertyId id, PacketType type, boolean will) {
        if (!(will ? id.allowedInWill() : id.allowedIn(type))) {
            throw new IllegalArgumentException(id + " is not allowed in " + (will ? "Will" : type));
        }
    }

    private static int readUnsigned(ByteBuffer input, int bytes) {
        int value = 0;
        for (int i = 0; i < bytes; i++) {
            value = (value << 8) | (input.get() & 0xFF);
        }
        return value;
    }

    private static int writeUnsigned(byte[] output, int offset, long value, int bytes) {
        int at = offset;
        for (int shift = (bytes - 1) * 8; shift >= 0; shift -= 8) {
            output[at++] = (byte) (value >>> shift);
        }
        return at;
    }
}
