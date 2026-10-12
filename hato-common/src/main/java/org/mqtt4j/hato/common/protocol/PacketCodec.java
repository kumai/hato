package org.mqtt4j.hato.common.protocol;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Set;

import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.Topic;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.common.value.ReasonCode.MALFORMED_PACKET;
import static org.mqtt4j.hato.common.value.ReasonCode.PROTOCOL_ERROR;

/// Converts complete MQTT Control Packets to and from the version-independent [Packet] model.
///
/// Each implementation owns the wire format and validation rules of one MQTT version.
/// Use [#of] after selecting a version, or [#decodeConnect] for the first incoming CONNECT.
/// MQTT 3.1.1 and MQTT 5.0 share the same model; each codec enforces its own wire restrictions.
///
/// Instances are stateless and may be shared. Decoding does not retain the input array;
/// encoding returns a new array. Callers must not modify inputs during a call. Packet ordering,
/// identifier allocation, session state, and connection closure belong to the connection owner.
public interface PacketCodec {

    /// Returns the codec for `version`.
    static PacketCodec of(MqttVersion version) {
        requireNonNull(version, "version must not be null");
        return switch (version) {
            case V3_1_1 -> Mqtt311PacketCodec.instance;
            case V5 -> Mqtt5PacketCodec.instance;
        };
    }

    /// Encodes exactly one packet, including the fixed header and Remaining Length.
    ///
    /// @throws IllegalArgumentException if a field cannot be represented in this MQTT version,
    ///         contains invalid text, or makes the packet exceed the Remaining Length limit
    byte[] encode(Packet packet);

    /// Prepares a PUBLISH's fixed and variable headers without accessing its payload.
    ///
    /// Remaining Length includes `payloadSize`, so the full wire size is the returned array's
    /// length plus `payloadSize`. The returned array is newly allocated and belongs to the caller;
    /// treat it as immutable when passing it to another thread or to [#encodePublish].
    ///
    /// @throws IllegalArgumentException if `payloadSize` is negative, the PUBLISH fields are
    ///         inconsistent or unsupported by this version, or Remaining Length exceeds its limit
    byte[] encodePublishHeader(
            Topic topic,
            Qos qos,
            boolean retain,
            boolean dup,
            int packetId,
            Properties properties,
            int payloadSize
    );

    /// Appends a payload to its prepared PUBLISH header in a new complete packet array.
    ///
    /// `header` must be the unchanged result of [#encodePublishHeader] with `payload.length` as
    /// its declared payload size. This assembly does not validate the header again. Neither input
    /// is retained or modified; callers must not modify either input during the call.
    default byte[] encodePublish(byte[] header, byte[] payload) {
        requireNonNull(header, "header must not be null");
        requireNonNull(payload, "payload must not be null");
        var packet = new byte[header.length + payload.length];
        System.arraycopy(header, 0, packet, 0, header.length);
        System.arraycopy(payload, 0, packet, header.length, payload.length);
        return packet;
    }

    /// Decodes exactly one complete packet, rejecting truncation and extra bytes.
    ///
    /// Decoding a CONNECT does not itself authorize or accept the connection. In MQTT 3.1.1,
    /// an empty client identifier with Clean Session false is retained so the server can
    /// send CONNACK Identifier Rejected, as required by `MQTT-3.1.3-8`.
    ///
    /// @throws PacketDecodeException if the packet is malformed or violates a checked rule
    /// @throws UnsupportedProtocolVersionException if CONNECT requests an unsupported level
    Packet decode(byte[] packet);

    /// Decodes the first CONNECT before the connection's MQTT version has been selected.
    ///
    /// The protocol name and level are inspected before version-specific fields. Levels 4 and 5
    /// name MQTT 3.1.1 and 5.0. A level outside `supported`, the versions the server implements,
    /// is reported through a separate exception before the rest of the packet is interpreted,
    /// so the server can send its version refusal whatever the body contains. Client identifier
    /// acceptance remains the server's responsibility, as with [#decode].
    ///
    /// @throws PacketDecodeException if this is not CONNECT or its format is invalid
    /// @throws UnsupportedProtocolVersionException if the protocol level is not in `supported`
    static Packet.Connect decodeConnect(byte[] packet, Set<MqttVersion> supported) {
        requireNonNull(packet, "packet must not be null");
        requireNonNull(supported, "supported must not be null");
        var input = ByteBuffer.wrap(packet);
        try {
            int header = input.get() & 0xFF;
            int typeValue = header >>> 4;
            // Before CONNECT selects a version, retain the original first-packet validation.
            if (typeValue == 0 || typeValue == 15) {
                throw new PacketDecodeException(MALFORMED_PACKET, "Reserved first packet type");
            }
            PacketType type = PacketType.of(typeValue);
            if (type != PacketType.PUBLISH && (header & 0x0F) != type.fixedFlags().orElseThrow()) {
                throw new PacketDecodeException(
                        MALFORMED_PACKET,
                        "Invalid fixed-header flags"
                );
            }
            if (type != PacketType.CONNECT) {
                throw new PacketDecodeException(PROTOCOL_ERROR, "Expected CONNECT");
            }
            // The selected codec applies its version's minimum-length rule below.
            int remainingLength = RemainingLength.read(input);
            if (remainingLength != input.remaining()) {
                throw new PacketDecodeException(MALFORMED_PACKET, "Remaining Length mismatch");
            }
            if (!MqttString.read(input).equals("MQTT")) {
                throw new PacketDecodeException(PROTOCOL_ERROR, "Unexpected MQTT protocol name");
            }
            int level = input.get() & 0xFF;
            MqttVersion version = switch (level) {
                case 4 -> MqttVersion.V3_1_1;
                case 5 -> MqttVersion.V5;
                default -> throw new UnsupportedProtocolVersionException(level);
            };
            if (!supported.contains(version)) {
                throw new UnsupportedProtocolVersionException(level);
            }
            return (Packet.Connect) of(version).decode(packet);
        } catch (BufferUnderflowException e) {
            throw new PacketDecodeException(MALFORMED_PACKET, "Truncated CONNECT prelude", e);
        }
    }
}
