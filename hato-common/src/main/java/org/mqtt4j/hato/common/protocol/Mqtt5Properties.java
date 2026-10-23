package org.mqtt4j.hato.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;

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
        checkProperties(properties, type, will);
        var values = new ByteArrayOutputStream();
        for (Property property : properties.entries()) {
            RemainingLength.write(values, property.id().value());
            switch (property) {
                case Property.ByteValue p -> values.write(p.value());
                case Property.TwoByteValue p -> writeUnsigned(values, p.value(), 2);
                case Property.FourByteValue p -> writeUnsigned(values, p.value(), 4);
                case Property.VariableByteValue p -> RemainingLength.write(values, p.value());
                case Property.BinaryValue p -> BinaryData.write(values, p.value());
                case Property.StringValue p -> MqttString.write(values, p.value());
                case Property.UserProperty p -> {
                    MqttString.write(values, p.name());
                    MqttString.write(values, p.value());
                }
            }
        }
        RemainingLength.write(output, values.size());
        output.writeBytes(values.toByteArray());
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

    private static void writeUnsigned(ByteArrayOutputStream output, long value, int bytes) {
        for (int shift = (bytes - 1) * 8; shift >= 0; shift -= 8) {
            output.write((int) (value >>> shift) & 0xFF);
        }
    }
}
