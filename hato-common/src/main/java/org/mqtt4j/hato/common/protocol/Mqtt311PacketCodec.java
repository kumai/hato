package org.mqtt4j.hato.common.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

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

/// MQTT 3.1.1 wire encoding and decoding behind [PacketCodec].
///
/// Return codes are mapped to MQTT 5.0 [ReasonCode] values;
/// absent properties and acknowledgement reasons become [Properties#EMPTY] and `SUCCESS`.
/// MQTT 5.0-only values are rejected when encoding, never silently discarded.
///
/// Following the 3.1.1 receive policy, non-minimal Remaining Length encodings and CONNACK
/// reserved bits are accepted. QoS 0 DUP and Session Present on a refused connection are
/// cleared to fit the packet model; encoding always produces their conforming representations.
///
/// Wire fields are validated before constructing model values. Malformed representations and
/// protocol rule violations receive distinct reasons. Only [Topic] and [TopicFilter] validation
/// exceptions are translated; other model exceptions propagate unchanged.
///
/// See [MQTT 3.1.1 Plus Errata 01, sections 2 and 3](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html).
/// See also [MQTT 5.0, sections 3.1.2 and 3.3.1: malformed CONNECT and PUBLISH bits](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
final class Mqtt311PacketCodec implements PacketCodec {

    static final Mqtt311PacketCodec instance = new Mqtt311PacketCodec();

    private Mqtt311PacketCodec() {}

    /// Encodes exactly one packet, including the fixed header and Remaining Length.
    ///
    /// @throws IllegalArgumentException if a field cannot be represented in MQTT 3.1.1,
    ///         contains invalid text, or makes the packet exceed the Remaining Length limit
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
                requireNoProperties(p.properties());
                body.write(p.sessionPresent() ? 1 : 0);
                body.write(connAckReturnCode(p.reasonCode()));
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
            case Packet.PubAck p -> {
                writeAcknowledgement(body, p.packetId(), p.reasonCode(), p.properties());
                type = PacketType.PUBACK;
            }
            case Packet.PubRec p -> {
                writeAcknowledgement(body, p.packetId(), p.reasonCode(), p.properties());
                type = PacketType.PUBREC;
            }
            case Packet.PubRel p -> {
                writeAcknowledgement(body, p.packetId(), p.reasonCode(), p.properties());
                type = PacketType.PUBREL;
            }
            case Packet.PubComp p -> {
                writeAcknowledgement(body, p.packetId(), p.reasonCode(), p.properties());
                type = PacketType.PUBCOMP;
            }
            case Packet.Subscribe p -> {
                writeSubscribe(body, p);
                type = PacketType.SUBSCRIBE;
            }
            case Packet.SubAck p -> {
                requireNoProperties(p.properties());
                writeUnsignedShort(body, p.packetId());
                for (ReasonCode reason : p.reasonCodes()) {
                    body.write(subAckReturnCode(reason));
                }
                type = PacketType.SUBACK;
            }
            case Packet.Unsubscribe p -> {
                requireNoProperties(p.properties());
                writeUnsignedShort(body, p.packetId());
                for (TopicFilter filter : p.filters()) {
                    MqttString.write(body, filter.pattern());
                }
                type = PacketType.UNSUBSCRIBE;
            }
            case Packet.UnsubAck p -> {
                requireNoProperties(p.properties());
                if (!p.reasonCodes().isEmpty()) {
                    throw new IllegalArgumentException("MQTT 3.1.1 UNSUBACK has no reason codes");
                }
                writeUnsignedShort(body, p.packetId());
                type = PacketType.UNSUBACK;
            }
            case Packet.PingReq _ -> type = PacketType.PINGREQ;
            case Packet.PingResp _ -> type = PacketType.PINGRESP;
            case Packet.Disconnect p -> {
                requireNoProperties(p.properties());
                requireSuccess(p.reasonCode());
                type = PacketType.DISCONNECT;
            }
            case Packet.Auth _ -> throw new IllegalArgumentException("MQTT 3.1.1 has no AUTH");
            case Packet.AliasPublish _ -> throw new IllegalArgumentException(
                    "MQTT 3.1.1 requires a PUBLISH topic name"
            );
        }

        // Other packet types still build their body first. PUBLISH appends its large payload
        // directly to the final array above; apply that approach here if profiling warrants.
        int packetSize = 1 + RemainingLength.encodedSize(body.size()) + body.size();
        var output = new ByteArrayOutputStream(packetSize);
        output.write((type.value() << 4) | type.fixedFlags().orElseThrow());
        RemainingLength.write(output, body.size());
        output.writeBytes(body.toByteArray());
        return output.toByteArray();
    }

    @Override
    public byte[] encodeAliasPublishHeader(
            Qos qos,
            boolean retain,
            boolean dup,
            int packetId,
            Properties properties,
            int payloadSize
    ) {
        throw new IllegalArgumentException("MQTT 3.1.1 requires a PUBLISH topic name");
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
        if (payloadSize < 0) {
            throw new IllegalArgumentException("PUBLISH payload size must not be negative");
        }
        // Reuse the packet model's field invariants without storage for the declared payload.
        var packet = new Packet.Publish(topic, new byte[0], qos, retain, dup, packetId, properties);
        requireNoProperties(packet.properties());
        var variableHeader = new ByteArrayOutputStream();
        MqttString.write(variableHeader, packet.topic().name());
        if (packet.qos() != Qos.AT_MOST_ONCE) {
            writeUnsignedShort(variableHeader, packet.packetId());
        }
        if (payloadSize > 268435455 - variableHeader.size()) {
            throw new IllegalArgumentException("PUBLISH exceeds the Remaining Length limit");
        }
        int remainingLength = variableHeader.size() + payloadSize;
        int headerSize = 1 + RemainingLength.encodedSize(remainingLength) + variableHeader.size();
        var header = new ByteArrayOutputStream(headerSize);
        int flags = (packet.dup() ? 8 : 0)
                | (packet.qos().level() << 1)
                | (packet.retain() ? 1 : 0);
        header.write((PacketType.PUBLISH.value() << 4) | flags);
        RemainingLength.write(header, remainingLength);
        header.writeBytes(variableHeader.toByteArray());
        return header.toByteArray();
    }

    /// Decodes exactly one complete packet, rejecting truncation and extra bytes.
    ///
    /// An empty CONNECT client identifier with Clean Session false is retained so the server
    /// can send CONNACK Identifier Rejected, as required by `MQTT-3.1.3-8`. Decoding a CONNECT
    /// does not itself authorize or accept the connection.
    ///
    /// @throws PacketDecodeException if the packet is malformed or violates a checked rule
    /// @throws UnsupportedProtocolVersionException if CONNECT requests a level other than 4
    @Override
    public Packet decode(byte[] bytes, int offset, int length) {
        return decoded(bytes, offset, length, null, null);
    }

    @Override
    public Packet decode(byte[] bytes, int offset, int length, TopicCache topics) {
        requireNonNull(topics, "topics must not be null");
        return decoded(bytes, offset, length, topics, null);
    }

    @Override
    public int publishHeadersSize(byte[] bytes, int offset, int available) {
        return PublishHeaders.size(bytes, offset, available, false);
    }

    @Override
    public Packet decodePublish(
            byte[] bytes,
            int offset,
            int length,
            byte[] payload,
            TopicCache topics
    ) {
        requireNonNull(payload, "payload must not be null");
        requireNonNull(topics, "topics must not be null");
        return decoded(bytes, offset, length, topics, payload);
    }

    // Decodes the packet in the `length` bytes of `bytes` from `offset`, or, given `apart`, the
    // PUBLISH whose headers lie there and whose payload is `apart`.
    private Packet decoded(
            byte[] bytes,
            int offset,
            int length,
            @Nullable TopicCache topics,
            byte @Nullable [] apart
    ) {
        requireNonNull(bytes, "bytes must not be null");
        var input = ByteBuffer.wrap(bytes, offset, length);
        try {
            int header = input.get() & 0xFF;
            int typeValue = header >>> 4;
            if (typeValue == 0 || typeValue == 15) {
                throw new PacketDecodeException(MALFORMED_PACKET, "Reserved packet type");
            }
            PacketType type = PacketType.of(typeValue);
            int flags = header & 0x0F;
            // [MQTT-2.2.2-2]: validate fixed flags before reading the body.
            if (type != PacketType.PUBLISH && flags != type.fixedFlags().orElseThrow()) {
                throw new PacketDecodeException(MALFORMED_PACKET, "Invalid fixed-header flags");
            }
            if (apart != null && type != PacketType.PUBLISH) {
                throw new IllegalArgumentException("Only a PUBLISH carries a payload apart");
            }
            int remainingLength = RemainingLength.read(input);
            if (remainingLength != input.remaining() + (apart == null ? 0 : apart.length)) {
                throw new PacketDecodeException(MALFORMED_PACKET, "Remaining Length mismatch");
            }

            Packet decoded = switch (type) {
                case CONNECT -> readConnect(input);
                case CONNACK -> readConnAck(input);
                case PUBLISH -> readPublish(input, flags, topics, apart);
                case PUBACK -> new Packet.PubAck(readPacketId(input), SUCCESS, EMPTY);
                case PUBREC -> new Packet.PubRec(readPacketId(input), SUCCESS, EMPTY);
                case PUBREL -> new Packet.PubRel(readPacketId(input), SUCCESS, EMPTY);
                case PUBCOMP -> new Packet.PubComp(readPacketId(input), SUCCESS, EMPTY);
                case SUBSCRIBE -> readSubscribe(input);
                case SUBACK -> readSubAck(input);
                case UNSUBSCRIBE -> readUnsubscribe(input);
                case UNSUBACK -> new Packet.UnsubAck(readPacketId(input), List.of(), EMPTY);
                case PINGREQ -> new Packet.PingReq();
                case PINGRESP -> new Packet.PingResp();
                case DISCONNECT -> new Packet.Disconnect(SUCCESS, EMPTY);
                case AUTH -> throw new PacketDecodeException(MALFORMED_PACKET, "Reserved type");
            };
            if (input.hasRemaining()) {
                throw new PacketDecodeException(
                        MALFORMED_PACKET,
                        "Unexpected packet body bytes"
                );
            }
            return decoded;
        } catch (BufferUnderflowException e) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Truncated packet field", e);
        }
    }

    private static void writeConnect(ByteArrayOutputStream output, Packet.Connect packet) {
        requireNoProperties(packet.properties());
        if (packet.version() != MqttVersion.V3_1_1) {
            throw new IllegalArgumentException("CONNECT version does not match the codec");
        }
        if (packet.clientId().isEmpty() && !packet.cleanStart()) {
            throw new IllegalArgumentException("An empty client identifier requires Clean Session");
        }
        Packet.Will will = packet.will();
        String username = packet.username();
        byte[] password = packet.password();
        if (password != null && username == null) {
            throw new IllegalArgumentException("MQTT 3.1.1 password requires a user name");
        }
        int flags = packet.cleanStart() ? 2 : 0;
        if (will != null) {
            requireNoProperties(will.properties());
            flags |= 4 | (will.qos().level() << 3) | (will.retain() ? 32 : 0);
        }
        flags |= username != null ? 128 : 0;
        flags |= password != null ? 64 : 0;
        MqttString.write(output, "MQTT");
        output.write(packet.version().protocolLevel());
        output.write(flags);
        writeUnsignedShort(output, packet.keepAliveSeconds());
        // [MQTT-3.1.3-1]: field order follows the presence flags.
        MqttString.write(output, packet.clientId());
        if (will != null) {
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
        // [MQTT-3.1.2-1, MQTT-3.1.2-2]: do not parse an unknown version as MQTT 3.1.1.
        if (!MqttString.read(input).equals("MQTT")) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "Unexpected MQTT protocol name");
        }
        int level = input.get() & 0xFF;
        if (level != MqttVersion.V3_1_1.protocolLevel()) {
            throw new UnsupportedProtocolVersionException(level);
        }
        int flags = input.get() & 0xFF;
        boolean hasWill = (flags & 4) != 0;
        int willQos = (flags >>> 3) & 3;
        boolean willRetain = (flags & 32) != 0;
        boolean hasUsername = (flags & 128) != 0;
        boolean hasPassword = (flags & 64) != 0;
        // [MQTT-3.1.2-3, MQTT-3.1.2-14]; MQTT 5.0 sections 3.1.2.3/6 classify these as malformed.
        if ((flags & 1) != 0 || willQos == 3) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Invalid CONNECT flag bits");
        }
        // [MQTT-3.1.2-11, MQTT-3.1.2-22]: valid bit values with invalid dependencies.
        if ((!hasWill && (willQos != 0 || willRetain))
                || (hasPassword && !hasUsername)) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "Invalid CONNECT flag dependencies");
        }
        int keepAlive = input.getShort() & 0xFFFF;
        String clientId = MqttString.read(input);
        Packet.Will will = hasWill ? new Packet.Will(
                readTopic(input),
                BinaryData.read(input),
                Qos.of(willQos),
                willRetain,
                EMPTY
        ) : null;
        String username = hasUsername ? MqttString.read(input) : null;
        byte[] password = hasPassword ? BinaryData.read(input) : null;
        return new Packet.Connect(
                clientId, MqttVersion.V3_1_1, (flags & 2) != 0, keepAlive,
                will, username, password, EMPTY
        );
    }

    private static Packet.ConnAck readConnAck(ByteBuffer input) {
        int flags = input.get() & 0xFF;
        ReasonCode reason = switch (input.get() & 0xFF) {
            case 0 -> SUCCESS;
            case 1 -> ReasonCode.UNSUPPORTED_PROTOCOL_VERSION;
            case 2 -> ReasonCode.CLIENT_IDENTIFIER_NOT_VALID;
            case 3 -> ReasonCode.SERVER_UNAVAILABLE;
            case 4 -> ReasonCode.BAD_USER_NAME_OR_PASSWORD;
            case 5 -> ReasonCode.NOT_AUTHORIZED;
            default -> throw new PacketDecodeException(MALFORMED_PACKET, "Invalid CONNACK code");
        };
        // Sections 3.2.2.1/2 prescribe sender behavior without requiring receiver rejection.
        // Ignore reserved bits; a refused connection cannot have a resumed session in the model.
        return new Packet.ConnAck(reason == SUCCESS && (flags & 1) != 0, reason, EMPTY);
    }

    private static Packet.Publish readPublish(
            ByteBuffer input,
            int flags,
            @Nullable TopicCache topics,
            byte @Nullable [] apart
    ) {
        int qosBits = (flags >>> 1) & 3;
        // [MQTT-3.3.1-4]; MQTT 5.0 section 3.3.1.2 classifies QoS 3 as malformed.
        if (qosBits == 3) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Invalid PUBLISH QoS bits");
        }
        Qos qos = Qos.of(qosBits);
        Topic topic = topics == null ? readTopic(input) : readTopic(input, topics);
        int packetId = (qos == Qos.AT_MOST_ONCE) ? 0 : readPacketId(input);
        byte[] payload = apart != null ? apart : remaining(input);
        // [MQTT-3.3.1-2] constrains the sender; ignore DUP for QoS 0 on receipt.
        boolean dup = (qos != Qos.AT_MOST_ONCE) && ((flags & 8) != 0);
        return new Packet.Publish(topic, payload, qos, (flags & 1) != 0, dup, packetId, EMPTY);
    }

    private static void writeAcknowledgement(
            ByteArrayOutputStream output,
            int packetId,
            ReasonCode reason,
            Properties properties
    ) {
        requireNoProperties(properties);
        requireSuccess(reason);
        writeUnsignedShort(output, packetId);
    }

    private static void writeSubscribe(ByteArrayOutputStream output, Packet.Subscribe packet) {
        requireNoProperties(packet.properties());
        writeUnsignedShort(output, packet.packetId());
        for (Packet.Subscription subscription : packet.subscriptions()) {
            if (subscription.noLocal() || subscription.retainAsPublished()
                    || subscription.retainHandling() != 0
            ) {
                throw new IllegalArgumentException("MQTT 3.1.1 cannot encode subscription options");
            }
            MqttString.write(output, subscription.filter().pattern());
            output.write(subscription.qos().level());
        }
    }

    private static Packet.Subscribe readSubscribe(ByteBuffer input) {
        int packetId = readPacketId(input);
        // [MQTT-3.8.3-3]: an empty SUBSCRIBE payload is a protocol violation.
        if (!input.hasRemaining()) {
            throw new PacketDecodeException(
                    PROTOCOL_ERROR, "SUBSCRIBE must contain a subscription"
            );
        }
        var subscriptions = new ArrayList<Packet.Subscription>();
        while (input.hasRemaining()) {
            TopicFilter filter = readTopicFilter(input);
            // [MQTT-3-8.3-4]: the entire byte must be 0, 1, or 2, including reserved bits.
            int requestedQos = input.get() & 0xFF;
            if (requestedQos > 2) {
                throw new PacketDecodeException(
                        MALFORMED_PACKET, "Invalid SUBSCRIBE Requested QoS"
                );
            }
            Qos qos = Qos.of(requestedQos);
            subscriptions.add(new Packet.Subscription(filter, qos, false, false, 0));
        }
        return new Packet.Subscribe(packetId, subscriptions, EMPTY);
    }

    private static Packet.SubAck readSubAck(ByteBuffer input) {
        int packetId = readPacketId(input);
        // [MQTT-3.8.4-5]: at least one result for the corresponding non-empty SUBSCRIBE.
        if (!input.hasRemaining()) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "SUBACK must contain a return code");
        }
        var reasons = new ArrayList<ReasonCode>();
        while (input.hasRemaining()) {
            ReasonCode reason = switch (input.get() & 0xFF) {
                case 0 -> SUCCESS;
                case 1 -> ReasonCode.GRANTED_QOS_1;
                case 2 -> ReasonCode.GRANTED_QOS_2;
                case 0x80 -> ReasonCode.UNSPECIFIED_ERROR;
                default -> throw new PacketDecodeException(
                        MALFORMED_PACKET, "Invalid SUBACK code"
                );
            };
            reasons.add(reason);
        }
        return new Packet.SubAck(packetId, reasons, EMPTY);
    }

    private static Packet.Unsubscribe readUnsubscribe(ByteBuffer input) {
        int packetId = readPacketId(input);
        // [MQTT-3.10.3-2]: an empty UNSUBSCRIBE payload is a protocol violation.
        if (!input.hasRemaining()) {
            throw new PacketDecodeException(
                    PROTOCOL_ERROR, "UNSUBSCRIBE must contain a topic filter"
            );
        }
        var filters = new ArrayList<TopicFilter>();
        while (input.hasRemaining()) {
            filters.add(readTopicFilter(input));
        }
        return new Packet.Unsubscribe(packetId, filters, EMPTY);
    }

    private static Topic readTopic(ByteBuffer input) {
        // Keep UTF-8 failures outside the value conversion; section 4.7 governs name validity.
        String name = MqttString.read(input);
        try {
            return new Topic(name);
        } catch (IllegalArgumentException e) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "Invalid topic name", e);
        }
    }

    // Reads a topic as readTopic(ByteBuffer) does, taking it from `topics` when it holds the name.
    private static Topic readTopic(ByteBuffer input, TopicCache topics) {
        Topic cached = topics.find(input);
        if (cached != null) {
            return cached;
        }
        int fieldStart = input.position();
        Topic topic = readTopic(input);
        topics.remember(input, fieldStart, topic);
        return topic;
    }

    private static TopicFilter readTopicFilter(ByteBuffer input) {
        // Only translate the value constructor's validation, after decoding the UTF-8 field.
        String pattern = MqttString.read(input);
        try {
            return new TopicFilter(pattern);
        } catch (IllegalArgumentException e) {
            throw new PacketDecodeException(PROTOCOL_ERROR, "Invalid topic filter", e);
        }
    }

    // The rest of the packet: a PUBLISH's payload that arrived with its headers.
    private static byte[] remaining(ByteBuffer input) {
        var bytes = new byte[input.remaining()];
        input.get(bytes);
        return bytes;
    }

    private static int readPacketId(ByteBuffer input) {
        int id = input.getShort() & 0xFFFF;
        if (id == 0) {
            throw new PacketDecodeException(
                    PROTOCOL_ERROR, "Packet identifier must not be zero"
            );
        }
        return id;
    }

    private static void writeUnsignedShort(ByteArrayOutputStream output, int value) {
        // Packet constructors have already checked the identifier and Keep Alive ranges.
        output.write(value >>> 8);
        output.write(value & 0xFF);
    }

    private static int connAckReturnCode(ReasonCode reason) {
        // MQTT 3.1.1 Table 3.1; the numbers differ from MQTT 5.0 Reason Codes.
        return switch (reason) {
            case SUCCESS -> 0;
            case UNSUPPORTED_PROTOCOL_VERSION -> 1;
            case CLIENT_IDENTIFIER_NOT_VALID -> 2;
            case SERVER_UNAVAILABLE -> 3;
            case BAD_USER_NAME_OR_PASSWORD -> 4;
            case NOT_AUTHORIZED -> 5;
            default -> throw new IllegalArgumentException("Reason has no CONNACK return code");
        };
    }

    private static int subAckReturnCode(ReasonCode reason) {
        return switch (reason) {
            case SUCCESS -> 0;
            case GRANTED_QOS_1 -> 1;
            case GRANTED_QOS_2 -> 2;
            case UNSPECIFIED_ERROR -> 0x80;
            default -> throw new IllegalArgumentException("Reason has no SUBACK return code");
        };
    }

    private static void requireNoProperties(Properties properties) {
        if (!properties.isEmpty()) {
            throw new IllegalArgumentException("MQTT 3.1.1 has no properties");
        }
    }

    private static void requireSuccess(ReasonCode reason) {
        if (reason != SUCCESS) {
            throw new IllegalArgumentException("MQTT 3.1.1 cannot encode this reason code");
        }
    }
}
