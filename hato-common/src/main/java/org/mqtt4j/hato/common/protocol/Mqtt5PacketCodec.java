package org.mqtt4j.hato.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.ReasonCode;
import org.mqtt4j.hato.common.value.Topic;
import org.mqtt4j.hato.common.value.TopicFilter;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.value.ReasonCode.MALFORMED_PACKET;
import static org.mqtt4j.hato.common.value.ReasonCode.PROTOCOL_ERROR;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

/// MQTT 5.0 wire encoding and decoding behind [PacketCodec].
///
/// Packet-local rules are checked here; connection state, negotiated limits, alias resolution,
/// and which peer may send a packet belong to the connection owner. Encoding uses the shortest
/// acknowledgement forms. Decoding also accepts explicit success reasons and empty properties
/// where their packet type permits them.
///
/// See [MQTT 5.0, sections 2 and 3](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
final class Mqtt5PacketCodec implements PacketCodec {

    static final Mqtt5PacketCodec instance = new Mqtt5PacketCodec();

    private Mqtt5PacketCodec() {}

    @Override
    public byte[] encode(Packet packet) {
        requireNonNull(packet, "packet must not be null");
        var body = new ByteArrayOutputStream();
        PacketType type;
        switch (packet) {
            case Packet.Connect p -> {
                writeConnect(body, p);
                type = PacketType.CONNECT;
            }
            case Packet.ConnAck p -> {
                body.write(p.sessionPresent() ? 1 : 0);
                writeReason(body, p.reasonCode(), PacketType.CONNACK);
                Mqtt5Properties.write(body, p.properties(), PacketType.CONNACK);
                type = PacketType.CONNACK;
            }
            case Packet.Publish p -> {
                byte[] header = encodePublishHeader(
                        p.topic(),
                        p.qos(),
                        p.retain(),
                        p.dup(),
                        p.packetId(),
                        p.properties(),
                        p.payload().length
                );
                return encodePublish(header, p.payload());
            }
            case Packet.AliasPublish p -> {
                requireTopicAlias(p.properties());
                byte[] header = writePublishHeader(
                        "",
                        p.qos(),
                        p.retain(),
                        p.dup(),
                        p.packetId(),
                        p.properties(),
                        p.payload().length
                );
                return encodePublish(header, p.payload());
            }
            case Packet.PubAck p -> {
                type = PacketType.PUBACK;
                writeUnsignedShort(body, p.packetId());
                writeOptionalReason(body, p.reasonCode(), p.properties(), type);
            }
            case Packet.PubRec p -> {
                type = PacketType.PUBREC;
                writeUnsignedShort(body, p.packetId());
                writeOptionalReason(body, p.reasonCode(), p.properties(), type);
            }
            case Packet.PubRel p -> {
                type = PacketType.PUBREL;
                writeUnsignedShort(body, p.packetId());
                writeOptionalReason(body, p.reasonCode(), p.properties(), type);
            }
            case Packet.PubComp p -> {
                type = PacketType.PUBCOMP;
                writeUnsignedShort(body, p.packetId());
                writeOptionalReason(body, p.reasonCode(), p.properties(), type);
            }
            case Packet.Subscribe p -> {
                writeSubscribe(body, p);
                type = PacketType.SUBSCRIBE;
            }
            case Packet.SubAck p -> {
                type = PacketType.SUBACK;
                writeReasons(body, p.packetId(), p.reasonCodes(), p.properties(), type);
            }
            case Packet.Unsubscribe p -> {
                type = PacketType.UNSUBSCRIBE;
                writeUnsignedShort(body, p.packetId());
                Mqtt5Properties.write(body, p.properties(), type);
                for (TopicFilter filter : p.filters()) {
                    SharedTopicFilter.parse(filter);
                    MqttString.write(body, filter.pattern());
                }
            }
            case Packet.UnsubAck p -> {
                type = PacketType.UNSUBACK;
                writeReasons(body, p.packetId(), p.reasonCodes(), p.properties(), type);
            }
            case Packet.PingReq _ -> type = PacketType.PINGREQ;
            case Packet.PingResp _ -> type = PacketType.PINGRESP;
            case Packet.Disconnect p -> {
                type = PacketType.DISCONNECT;
                writeOptionalReason(body, p.reasonCode(), p.properties(), type);
            }
            case Packet.Auth p -> {
                type = PacketType.AUTH;
                if ((p.reasonCode() != SUCCESS || !p.properties().isEmpty())
                        && p.properties().find(PropertyId.AUTHENTICATION_METHOD).isEmpty()) {
                    throw new IllegalArgumentException("AUTH requires Authentication Method");
                }
                writeOptionalReason(body, p.reasonCode(), p.properties(), type);
            }
        }
        // PUBLISH appends its potentially large payload directly to the final array above.
        // Other bodies use a temporary buffer; change this only if profiling warrants it.
        int packetSize = 1 + RemainingLength.encodedSize(body.size()) + body.size();
        var output = new ByteArrayOutputStream(packetSize);
        output.write((type.value() << 4) | type.fixedFlags().orElseThrow());
        RemainingLength.write(output, body.size());
        output.writeBytes(body.toByteArray());
        return output.toByteArray();
    }

    @Override
    public byte[] encodePublishHeader(
            Topic topic,
            Qos qos,
            boolean retain,
            boolean dup,
            int packetId,
            Properties properties,
            int payloadSize
    ) {
        // Validate the shared packet invariants without allocating the declared payload.
        var packet = new Packet.Publish(topic, new byte[0], qos, retain, dup, packetId, properties);
        return writePublishHeader(
                packet.topic().name(),
                qos,
                retain,
                dup,
                packetId,
                properties,
                payloadSize
        );
    }

    private static byte[] writePublishHeader(
            String topic,
            Qos qos,
            boolean retain,
            boolean dup,
            int packetId,
            Properties properties,
            int payloadSize
    ) {
        if (payloadSize < 0) {
            throw new IllegalArgumentException("PUBLISH payload size must not be negative");
        }
        var variableHeader = new ByteArrayOutputStream();
        MqttString.write(variableHeader, topic);
        if (qos != Qos.AT_MOST_ONCE) {
            writeUnsignedShort(variableHeader, packetId);
        }
        Mqtt5Properties.write(variableHeader, properties, PacketType.PUBLISH);
        if (payloadSize > 268435455 - variableHeader.size()) {
            throw new IllegalArgumentException("PUBLISH exceeds the Remaining Length limit");
        }
        int remainingLength = variableHeader.size() + payloadSize;
        int headerSize = 1 + RemainingLength.encodedSize(remainingLength) + variableHeader.size();
        var header = new ByteArrayOutputStream(headerSize);
        int flags = (dup ? 8 : 0) | (qos.level() << 1) | (retain ? 1 : 0);
        header.write((PacketType.PUBLISH.value() << 4) | flags);
        RemainingLength.write(header, remainingLength);
        header.writeBytes(variableHeader.toByteArray());
        return header.toByteArray();
    }

    @Override
    public Packet decode(byte[] packet) {
        requireNonNull(packet, "packet must not be null");
        var input = ByteBuffer.wrap(packet);
        try {
            int header = input.get() & 0xFF;
            int typeValue = header >>> 4;
            if (typeValue == 0) {
                throw new PacketDecodeException(MALFORMED_PACKET, "Reserved packet type");
            }
            PacketType type = PacketType.of(typeValue);
            int flags = header & 0x0F;
            if (type != PacketType.PUBLISH && flags != type.fixedFlags().orElseThrow()) {
                throw new PacketDecodeException(MALFORMED_PACKET, "Invalid fixed-header flags");
            }
            int remainingLength = RemainingLength.readMinimal(input);
            if (remainingLength != input.remaining()) {
                throw new PacketDecodeException(MALFORMED_PACKET, "Remaining Length mismatch");
            }
            Packet decoded = switch (type) {
                case CONNECT -> readConnect(input);
                case CONNACK -> readConnAck(input);
                case PUBLISH -> readPublish(input, flags);
                case PUBACK -> new Packet.PubAck(
                        readPacketId(input),
                        readOptionalReason(input, type),
                        readOptionalProperties(input, type)
                );
                case PUBREC -> new Packet.PubRec(
                        readPacketId(input),
                        readOptionalReason(input, type),
                        readOptionalProperties(input, type)
                );
                case PUBREL -> new Packet.PubRel(
                        readPacketId(input),
                        readOptionalReason(input, type),
                        readOptionalProperties(input, type)
                );
                case PUBCOMP -> new Packet.PubComp(
                        readPacketId(input),
                        readOptionalReason(input, type),
                        readOptionalProperties(input, type)
                );
                case SUBSCRIBE -> readSubscribe(input);
                case SUBACK -> readSubAck(input);
                case UNSUBSCRIBE -> readUnsubscribe(input);
                case UNSUBACK -> readUnsubAck(input);
                case PINGREQ -> new Packet.PingReq();
                case PINGRESP -> new Packet.PingResp();
                case DISCONNECT -> new Packet.Disconnect(
                        readOptionalReason(input, type),
                        readOptionalProperties(input, type)
                );
                case AUTH -> readAuth(input);
            };
            if (input.hasRemaining()) {
                throw new PacketDecodeException(MALFORMED_PACKET, "Unexpected packet body bytes");
            }
            return decoded;
        } catch (BufferUnderflowException e) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Truncated packet field", e);
        }
    }

    private static void writeConnect(ByteArrayOutputStream output, Packet.Connect packet) {
        if (packet.version() != MqttVersion.V5) {
            throw new IllegalArgumentException("CONNECT version does not match the codec");
        }
        Packet.Will will = packet.will();
        String username = packet.username();
        byte[] password = packet.password();
        int flags = packet.cleanStart() ? 2 : 0;
        if (will != null) {
            flags |= 4 | (will.qos().level() << 3) | (will.retain() ? 32 : 0);
        }
        flags |= username != null ? 128 : 0;
        flags |= password != null ? 64 : 0;
        MqttString.write(output, "MQTT");
        output.write(MqttVersion.V5.protocolLevel());
        output.write(flags);
        writeUnsignedShort(output, packet.keepAliveSeconds());
        Mqtt5Properties.write(output, packet.properties(), PacketType.CONNECT);
        MqttString.write(output, packet.clientId());
        if (will != null) {
            Mqtt5Properties.writeWill(output, will.properties());
            MqttString.write(output, will.topic().name());
            BinaryData.write(output, will.payload());
        }
        if (username != null) {
            MqttString.write(output, username);
        }
        if (password != null) {
            BinaryData.write(output, password);
        }
    }

    private static Packet.Connect readConnect(ByteBuffer input) {
        if (!MqttString.read(input).equals("MQTT")) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "Unexpected MQTT protocol name");
        }
        int level = input.get() & 0xFF;
        if (level != MqttVersion.V5.protocolLevel()) {
            throw new UnsupportedProtocolVersionException(level);
        }
        int flags = input.get() & 0xFF;
        boolean hasWill = (flags & 4) != 0;
        int willQos = (flags >>> 3) & 3;
        boolean willRetain = (flags & 32) != 0;
        if ((flags & 1) != 0 || willQos == 3) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Invalid CONNECT flag bits");
        }
        if (!hasWill && (willQos != 0 || willRetain)) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "Invalid CONNECT flag dependencies");
        }
        int keepAlive = input.getShort() & 0xFFFF;
        Properties properties = Mqtt5Properties.read(input, PacketType.CONNECT);
        String clientId = MqttString.read(input);
        Packet.Will will = null;
        if (hasWill) {
            Properties willProperties = Mqtt5Properties.readWill(input);
            will = new Packet.Will(
                    readTopic(input),
                    BinaryData.read(input),
                    Qos.of(willQos),
                    willRetain,
                    willProperties
            );
        }
        String username = (flags & 128) != 0 ? MqttString.read(input) : null;
        byte[] password = (flags & 64) != 0 ? BinaryData.read(input) : null;
        return new Packet.Connect(
                clientId,
                MqttVersion.V5,
                (flags & 2) != 0,
                keepAlive,
                will,
                username,
                password,
                properties
        );
    }

    private static Packet.ConnAck readConnAck(ByteBuffer input) {
        // MQTT 3.1.1 servers reject an unsupported level with this legacy CONNACK form.
        if (input.remaining() == 2 && input.get(input.position()) == 0
                && input.get(input.position() + 1) == 1) {
            input.position(input.limit());
            return new Packet.ConnAck(false, ReasonCode.UNSUPPORTED_PROTOCOL_VERSION, EMPTY);
        }
        int flags = input.get() & 0xFF;
        if ((flags & 0xFE) != 0) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Invalid CONNACK flags");
        }
        ReasonCode reason = readReason(input, PacketType.CONNACK);
        if (flags != 0 && reason != SUCCESS) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "Refused CONNACK resumes a session");
        }
        return new Packet.ConnAck(
                flags != 0,
                reason,
                Mqtt5Properties.read(input, PacketType.CONNACK)
        );
    }

    private static Packet readPublish(ByteBuffer input, int flags) {
        int qosBits = (flags >>> 1) & 3;
        if (qosBits == 3) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Invalid PUBLISH QoS bits");
        }
        Qos qos = Qos.of(qosBits);
        boolean dup = (flags & 8) != 0;
        if (qos == Qos.AT_MOST_ONCE && dup) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "QoS 0 PUBLISH sets DUP");
        }
        String topicName = MqttString.read(input);
        int packetId = qos == Qos.AT_MOST_ONCE ? 0 : readPacketId(input);
        Properties properties = Mqtt5Properties.read(input, PacketType.PUBLISH);
        var payload = new byte[input.remaining()];
        input.get(payload);
        if (topicName.isEmpty()) {
            if (properties.find(PropertyId.TOPIC_ALIAS).isEmpty()) {
                throw new PacketDecodeException(PROTOCOL_ERROR, "Empty topic requires Topic Alias");
            }
            return new Packet.AliasPublish(
                    payload,
                    qos,
                    (flags & 1) != 0,
                    dup,
                    packetId,
                    properties
            );
        }
        return new Packet.Publish(
                topic(topicName),
                payload,
                qos,
                (flags & 1) != 0,
                dup,
                packetId,
                properties
        );
    }

    private static void writeSubscribe(ByteArrayOutputStream output, Packet.Subscribe packet) {
        writeUnsignedShort(output, packet.packetId());
        Mqtt5Properties.write(output, packet.properties(), PacketType.SUBSCRIBE);
        for (Packet.Subscription subscription : packet.subscriptions()) {
            boolean shared = SharedTopicFilter.parse(subscription.filter()).isPresent();
            if (shared && subscription.noLocal()) {
                throw new IllegalArgumentException("Shared subscription cannot set No Local");
            }
            MqttString.write(output, subscription.filter().pattern());
            int options = subscription.qos().level()
                    | (subscription.noLocal() ? 4 : 0)
                    | (subscription.retainAsPublished() ? 8 : 0)
                    | (subscription.retainHandling() << 4);
            output.write(options);
        }
    }

    private static Packet.Subscribe readSubscribe(ByteBuffer input) {
        int packetId = readPacketId(input);
        Properties properties = Mqtt5Properties.read(input, PacketType.SUBSCRIBE);
        requirePayload(input, "SUBSCRIBE must contain a subscription");
        var subscriptions = new ArrayList<Packet.Subscription>();
        while (input.hasRemaining()) {
            TopicFilter filter = readTopicFilter(input);
            int options = input.get() & 0xFF;
            // [MQTT-3.8.3-5] makes a set reserved bit a malformed packet, whereas section 3.8.3.1
            // classifies QoS 3 and Retain Handling 3 as protocol errors.
            if ((options & 0xC0) != 0) {
                throw new PacketDecodeException(
                        MALFORMED_PACKET,
                        "Reserved subscription option bits"
                );
            }
            int qos = options & 3;
            int retainHandling = (options >>> 4) & 3;
            if (qos == 3 || retainHandling == 3) {
                throw new PacketDecodeException(PROTOCOL_ERROR, "Invalid subscription options");
            }
            boolean noLocal = (options & 4) != 0;
            if (SharedTopicFilter.parse(filter).isPresent() && noLocal) {
                throw new PacketDecodeException(
                        PROTOCOL_ERROR,
                        "Shared subscription sets No Local"
                );
            }
            subscriptions.add(new Packet.Subscription(
                    filter,
                    Qos.of(qos),
                    noLocal,
                    (options & 8) != 0,
                    retainHandling
            ));
        }
        return new Packet.Subscribe(packetId, subscriptions, properties);
    }

    private static Packet.SubAck readSubAck(ByteBuffer input) {
        int packetId = readPacketId(input);
        Properties properties = Mqtt5Properties.read(input, PacketType.SUBACK);
        return new Packet.SubAck(packetId, readReasons(input, PacketType.SUBACK), properties);
    }

    private static Packet.Unsubscribe readUnsubscribe(ByteBuffer input) {
        int packetId = readPacketId(input);
        Properties properties = Mqtt5Properties.read(input, PacketType.UNSUBSCRIBE);
        requirePayload(input, "UNSUBSCRIBE must contain a topic filter");
        var filters = new ArrayList<TopicFilter>();
        while (input.hasRemaining()) {
            filters.add(readTopicFilter(input));
        }
        return new Packet.Unsubscribe(packetId, filters, properties);
    }

    private static Packet.UnsubAck readUnsubAck(ByteBuffer input) {
        int packetId = readPacketId(input);
        Properties properties = Mqtt5Properties.read(input, PacketType.UNSUBACK);
        return new Packet.UnsubAck(packetId, readReasons(input, PacketType.UNSUBACK), properties);
    }

    private static void writeReasons(
            ByteArrayOutputStream output,
            int packetId,
            List<ReasonCode> reasons,
            Properties properties,
            PacketType type
    ) {
        if (reasons.isEmpty()) {
            throw new IllegalArgumentException(type + " must contain a reason code");
        }
        writeUnsignedShort(output, packetId);
        Mqtt5Properties.write(output, properties, type);
        for (ReasonCode reason : reasons) {
            writeReason(output, reason, type);
        }
    }

    private static List<ReasonCode> readReasons(ByteBuffer input, PacketType type) {
        requirePayload(input, type + " must contain a reason code");
        var reasons = new ArrayList<ReasonCode>();
        while (input.hasRemaining()) {
            reasons.add(readReason(input, type));
        }
        return reasons;
    }

    private static void writeOptionalReason(
            ByteArrayOutputStream output,
            ReasonCode reason,
            Properties properties,
            PacketType type
    ) {
        // An absent reason means success. An absent Property Length means no properties.
        if (reason != SUCCESS || !properties.isEmpty()) {
            writeReason(output, reason, type);
        }
        if (!properties.isEmpty() || (type == PacketType.AUTH && reason != SUCCESS)) {
            Mqtt5Properties.write(output, properties, type);
        }
    }

    private static Packet.Auth readAuth(ByteBuffer input) {
        if (!input.hasRemaining()) {
            return new Packet.Auth(SUCCESS, EMPTY);
        }
        ReasonCode reason = readReason(input, PacketType.AUTH);
        Properties properties = Mqtt5Properties.read(input, PacketType.AUTH);
        if (properties.find(PropertyId.AUTHENTICATION_METHOD).isEmpty()) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "AUTH requires Authentication Method");
        }
        return new Packet.Auth(reason, properties);
    }

    private static void writeReason(
            ByteArrayOutputStream output,
            ReasonCode reason,
            PacketType type
    ) {
        if (!allowsReason(type, reason.value())) {
            throw new IllegalArgumentException("Reason code is not allowed in " + type);
        }
        output.write(reason.value());
    }

    private static ReasonCode readOptionalReason(ByteBuffer input, PacketType type) {
        return input.hasRemaining() ? readReason(input, type) : SUCCESS;
    }

    private static Properties readOptionalProperties(ByteBuffer input, PacketType type) {
        return input.hasRemaining() ? Mqtt5Properties.read(input, type) : EMPTY;
    }

    private static ReasonCode readReason(ByteBuffer input, PacketType type) {
        int value = input.get() & 0xFF;
        // Table 2-6 lists Bad authentication method for DISCONNECT too, although the DISCONNECT
        // table of section 3.14.2.1 does not. A peer that follows Table 2-6 is understood, but
        // this codec never writes that DISCONNECT.
        boolean tolerated = type == PacketType.DISCONNECT && value == 0x8C;
        if (!allowsReason(type, value) && !tolerated) {
            throw new PacketDecodeException(
                    PROTOCOL_ERROR,
                    "Reason code is not allowed in " + type
            );
        }
        return ReasonCode.of(value);
    }

    private static boolean allowsReason(PacketType type, int value) {
        // MQTT 5.0 packet-specific reason tables; legality is independent of enum membership.
        return switch (type) {
            case CONNACK -> switch (value) {
                case 0x00, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88,
                     0x89, 0x8A, 0x8C, 0x90, 0x95, 0x97, 0x99, 0x9A, 0x9B, 0x9C,
                     0x9D, 0x9F -> true;
                default -> false;
            };
            case PUBACK, PUBREC -> switch (value) {
                case 0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99 -> true;
                default -> false;
            };
            case PUBREL, PUBCOMP -> value == 0x00 || value == 0x92;
            case SUBACK -> switch (value) {
                case 0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E,
                     0xA1, 0xA2 -> true;
                default -> false;
            };
            case UNSUBACK -> switch (value) {
                case 0x00, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91 -> true;
                default -> false;
            };
            case DISCONNECT -> switch (value) {
                case 0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8D,
                     0x8E, 0x8F, 0x90, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99,
                     0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1, 0xA2 -> true;
                default -> false;
            };
            case AUTH -> value == 0x00 || value == 0x18 || value == 0x19;
            case CONNECT, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PINGREQ, PINGRESP -> false;
        };
    }

    private static Topic readTopic(ByteBuffer input) {
        return topic(MqttString.read(input));
    }

    private static Topic topic(String name) {
        try {
            return new Topic(name);
        } catch (IllegalArgumentException e) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "Invalid topic name", e);
        }
    }

    private static TopicFilter readTopicFilter(ByteBuffer input) {
        String pattern = MqttString.read(input);
        try {
            var filter = new TopicFilter(pattern);
            SharedTopicFilter.parse(filter);
            return filter;
        } catch (IllegalArgumentException e) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "Invalid topic filter", e);
        }
    }

    private static void requireTopicAlias(Properties properties) {
        if (properties.find(PropertyId.TOPIC_ALIAS).isEmpty()) {
            throw new IllegalArgumentException("Empty topic requires Topic Alias");
        }
    }

    private static int readPacketId(ByteBuffer input) {
        int id = input.getShort() & 0xFFFF;
        if (id == 0) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "Packet identifier must not be zero");
        }
        return id;
    }

    private static void requirePayload(ByteBuffer input, String message) {
        if (!input.hasRemaining()) {
            throw new PacketDecodeException(PROTOCOL_ERROR, message);
        }
    }

    private static void writeUnsignedShort(ByteArrayOutputStream output, int value) {
        output.write(value >>> 8);
        output.write(value & 0xFF);
    }
}
