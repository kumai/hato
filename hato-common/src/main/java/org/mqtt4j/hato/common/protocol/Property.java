package org.mqtt4j.hato.common.protocol;

import java.util.Arrays;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.common.value.Topic;

import static java.util.Objects.requireNonNull;

/// A typed MQTT 5.0 property. Constructors validate the identifier's wire type and value range;
/// the codec validates UTF-8 strings and packet-specific restrictions.
///
/// See [MQTT 5.0, section 2.2.2: Properties](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
public sealed interface Property {

    /// Returns the identifier which determines this property's meaning and wire type.
    PropertyId id();

    /// A one-byte property. MQTT 5.0 defines only boolean values and Maximum QoS (0 or 1).
    record ByteValue(PropertyId id, int value) implements Property {

        public ByteValue {
            id.requireKind(PropertyId.Kind.BYTE);
            checkRange(id, value, 0, 1);
        }
    }

    /// An unsigned two-byte value. Receive Maximum and Topic Alias cannot be zero.
    record TwoByteValue(PropertyId id, int value) implements Property {

        public TwoByteValue {
            id.requireKind(PropertyId.Kind.TWO_BYTE);
            int minimum = id == PropertyId.RECEIVE_MAXIMUM || id == PropertyId.TOPIC_ALIAS ? 1 : 0;
            checkRange(id, value, minimum, 65535);
        }
    }

    /// An unsigned four-byte value, represented by a `long`. Maximum Packet Size cannot be zero.
    record FourByteValue(PropertyId id, long value) implements Property {

        public FourByteValue {
            id.requireKind(PropertyId.Kind.FOUR_BYTE);
            checkRange(id, value, id == PropertyId.MAXIMUM_PACKET_SIZE ? 1 : 0, 0xFFFF_FFFFL);
        }
    }

    /// A Subscription Identifier from 1 to 268435455.
    record VariableByteValue(PropertyId id, int value) implements Property {

        public VariableByteValue {
            id.requireKind(PropertyId.Kind.VARIABLE_BYTE);
            checkRange(id, value, 1, 268435455);
        }
    }

    /// Binary data of at most 65535 bytes, copied on construction and access.
    record BinaryValue(PropertyId id, byte[] value) implements Property {

        public BinaryValue {
            id.requireKind(PropertyId.Kind.BINARY);
            if (value.length > 65535) {
                throw new IllegalArgumentException("Binary property exceeds 65535 bytes");
            }
            value = value.clone();
        }

        @Override
        public byte[] value() {
            return value.clone();
        }

        @Override
        public boolean equals(@Nullable Object other) {
            return other instanceof BinaryValue that
                    && id == that.id && Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return 31 * id.hashCode() + Arrays.hashCode(value);
        }

        /// Reports only the size, because binary properties such as Correlation Data and
        /// Authentication Data carry application data or secrets.
        @Override
        public String toString() {
            return "BinaryValue[id=" + id + ", bytes=" + value.length + "]";
        }
    }

    /// A UTF-8 string. Response Topic additionally requires a nonempty topic without wildcards.
    record StringValue(PropertyId id, String value) implements Property {

        public StringValue {
            id.requireKind(PropertyId.Kind.STRING);
            requireNonNull(value, "Property value must not be null");
            if (id == PropertyId.RESPONSE_TOPIC) {
                new Topic(value);
            }
        }
    }

    /// A User Property (identifier 0x26). Names and values may be empty; duplicates are allowed.
    /// The codec validates the UTF-8 representation of both strings.
    record UserProperty(String name, String value) implements Property {

        public UserProperty {
            requireNonNull(name, "User Property name must not be null");
            requireNonNull(value, "User Property value must not be null");
        }

        @Override
        public PropertyId id() {
            return PropertyId.USER_PROPERTY;
        }

        /// Leaves out the name and the value, which carry application data.
        @Override
        public String toString() {
            return "UserProperty[omitted]";
        }
    }

    private static void checkRange(PropertyId id, long value, long minimum, long maximum) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    id + " must be between " + minimum + " and " + maximum
            );
        }
    }
}
