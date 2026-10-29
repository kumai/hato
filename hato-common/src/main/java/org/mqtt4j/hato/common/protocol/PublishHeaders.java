package org.mqtt4j.hato.common.protocol;

import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.common.value.Qos;

/// The fixed and variable headers of a PUBLISH. Reading locates the payload from the packet's
/// first bytes, so that a reader can give the payload an array of its own before the rest of the
/// packet arrives; only the lengths that lead to the payload are read, and decoding checks the
/// packet. Writing builds the headers of a PUBLISH, its properties included, in one array of
/// their exact size; sizing counts the bytes of a PUBLISH from the sizes of its parts, as writing
/// would take them, without writing it.
final class PublishHeaders {

    private PublishHeaders() {}

    /// The headers of a PUBLISH of `topic`, which must be a valid MQTT string, that is followed
    /// by `payloadSize` bytes. Under MQTT 5.0 the variable header ends with `properties`; an
    /// MQTT 3.1.1 header has none and takes null.
    ///
    /// @throws IllegalArgumentException if a PUBLISH cannot carry `properties`, or the packet
    ///         exceeds the Remaining Length limit
    static byte[] write(
            String topic,
            Qos qos,
            boolean retain,
            boolean dup,
            int packetId,
            @Nullable Properties properties,
            int payloadSize
    ) {
        byte[] name = topic.getBytes(StandardCharsets.UTF_8);
        // Every field is sized before the array is made. Sizing the properties checks them, so
        // that nothing is written for those that cannot be; most publications carry none, which
        // need no checks.
        int propertiesSize = properties == null || properties.isEmpty()
                ? 0
                : Mqtt5Properties.size(properties, PacketType.PUBLISH);
        int variableSize = variableSize(
                2 + name.length,
                qos,
                properties == null ? -1 : propertiesSize
        );
        int remainingLength = remainingLength(variableSize, payloadSize);
        var header = new byte[1 + RemainingLength.encodedSize(remainingLength) + variableSize];
        int flags = (dup ? 8 : 0) | (qos.level() << 1) | (retain ? 1 : 0);
        header[0] = (byte) ((PacketType.PUBLISH.value() << 4) | flags);
        int at = RemainingLength.write(header, 1, remainingLength);
        header[at++] = (byte) (name.length >>> 8);
        header[at++] = (byte) name.length;
        System.arraycopy(name, 0, header, at, name.length);
        at += name.length;
        if (qos != Qos.AT_MOST_ONCE) {
            header[at++] = (byte) (packetId >>> 8);
            header[at++] = (byte) packetId;
        }
        if (properties != null) {
            at = RemainingLength.write(header, at, propertiesSize);
            Mqtt5Properties.write(header, at, properties);
        }
        return header;
    }

    /// The bytes of a PUBLISH of `topic`, which must be a valid MQTT string, whose payload takes
    /// `payloadSize` bytes: the headers that [#write] writes for it and the payload. Under MQTT
    /// 5.0 its properties take `propertiesSize` bytes without their length; MQTT 3.1.1 has none
    /// and takes -1.
    ///
    /// @throws IllegalArgumentException if the packet exceeds the Remaining Length limit
    static int packetSize(String topic, Qos qos, int propertiesSize, int payloadSize) {
        int remainingLength = remainingLength(
                variableSize(MqttString.encodedSize(topic), qos, propertiesSize),
                payloadSize
        );
        return 1 + RemainingLength.encodedSize(remainingLength) + remainingLength;
    }

    // The bytes of the variable header of a PUBLISH whose topic name takes `topicSize` bytes with
    // its length: the name, the packet identifier above QoS 0, and, unless `propertiesSize` is -1,
    // the length of the properties and the `propertiesSize` bytes that they take.
    private static int variableSize(int topicSize, Qos qos, int propertiesSize) {
        return topicSize
                + (qos == Qos.AT_MOST_ONCE ? 0 : 2)
                + (propertiesSize < 0
                        ? 0
                        : RemainingLength.encodedSize(propertiesSize) + propertiesSize);
    }

    // The Remaining Length of a PUBLISH whose variable header and payload take these sizes.
    private static int remainingLength(int variableSize, int payloadSize) {
        if (payloadSize > RemainingLength.MAX_VALUE - variableSize) {
            throw new IllegalArgumentException("PUBLISH exceeds the Remaining Length limit");
        }
        return variableSize + payloadSize;
    }

    /// The size of the fixed and variable headers of the PUBLISH whose first `available` bytes
    /// lie in `bytes` from `offset`, or -1 when those bytes do not locate its payload: they end
    /// before the headers do, or a variable byte integer in them runs past four bytes.
    /// `properties` says whether the headers end with properties, as in MQTT 5.0.
    static int size(byte[] bytes, int offset, int available, boolean properties) {
        int end = offset + available;
        if (available < 1) {
            return -1;
        }
        int qos = ((bytes[offset] & 0xFF) >>> 1) & 3;
        // The fixed header: the first byte, then the Remaining Length.
        int at = skipVariableByteInteger(bytes, offset + 1, end);
        // The topic name and its two-byte length.
        if (at < 0 || end - at < 2) {
            return -1;
        }
        at += 2 + (((bytes[at] & 0xFF) << 8) | (bytes[at + 1] & 0xFF));
        if (qos != 0) {
            at += 2;
        }
        if (properties) {
            // The properties' length, a variable byte integer, then the properties.
            if (at >= end) {
                return -1;
            }
            int length = 0;
            int digits = 0;
            int digit;
            do {
                if (at == end || digits == 4) {
                    return -1;
                }
                digit = bytes[at++] & 0xFF;
                length |= (digit & 0x7F) << (7 * digits++);
            } while ((digit & 0x80) != 0);
            at += length;
        }
        return at <= end ? at - offset : -1;
    }

    // Where the variable byte integer that starts at `at` ends, or -1 when it does not end
    // before `end` or within four bytes.
    private static int skipVariableByteInteger(byte[] bytes, int at, int end) {
        for (int digits = 0; digits < 4; digits++) {
            if (at == end) {
                return -1;
            }
            if ((bytes[at++] & 0x80) == 0) {
                return at;
            }
        }
        return -1;
    }
}
