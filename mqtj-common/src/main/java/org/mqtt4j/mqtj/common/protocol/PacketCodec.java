package org.mqtt4j.mqtj.common.protocol;

import org.mqtt4j.mqtj.common.value.MqttVersion;
import org.mqtt4j.mqtj.common.value.Qos;
import org.mqtt4j.mqtj.common.value.Topic;

import static java.util.Objects.requireNonNull;

/// Converts complete MQTT Control Packets to and from the version-independent [Packet] model.
///
/// Each implementation owns the wire format and validation rules of one MQTT version.
/// Use [#of] after selecting a version, or [#decodeConnect] for the first incoming CONNECT.
/// Currently only MQTT 3.1.1 is implemented; the MQTT 5.0-shaped model is shared by both versions.
///
/// Instances are stateless and may be shared. Decoding does not retain the input array;
/// encoding returns a new array. Callers must not modify inputs during a call. Packet ordering,
/// identifier allocation, session state, and connection closure belong to the connection owner.
public interface PacketCodec {

    /// Returns the codec for `version`.
    ///
    /// @throws IllegalArgumentException if `version` is not implemented (currently MQTT 5.0)
    static PacketCodec of(MqttVersion version) {
        requireNonNull(version, "version must not be null");
        return switch (version) {
            case V3_1_1 -> Mqtt311PacketCodec.instance;
            // TODO: MQTT 5.0: a broker that supports only 3.1.1 refuses a 5.0 CONNECT with the
            //       3.1.1 CONNACK (Remaining Length 2, return code 0x01), as its [MQTT-3.1.2-2]
            //       requires. Decode that as UNSUPPORTED_PROTOCOL_VERSION, not a malformed packet.
            case V5 -> throw new IllegalArgumentException("MQTT 5.0 codec is not implemented");
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
    /// The protocol name and level are inspected before version-specific fields. Only level 4
    /// is currently implemented. An unsupported level is preserved in a separate exception so
    /// the server can choose its refusal response. Client identifier acceptance remains the
    /// server's responsibility, as with [#decode].
    ///
    /// @throws PacketDecodeException if this is not CONNECT or its format is invalid
    /// @throws UnsupportedProtocolVersionException if the protocol level is not implemented
    static Packet.Connect decodeConnect(byte[] packet) {
        // Only 3.1.1 is implemented. When adding another codec, select it here from CONNECT's
        // common prelude before delegating version-specific decoding.
        return Mqtt311PacketCodec.decodeConnect(packet);
    }
}
