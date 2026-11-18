package org.mqtt4j.hato.broker.internal;

import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.PacketDecodeException;
import org.mqtt4j.hato.internal.protocol.Properties;

// What the broker checks of a packet from a client beyond what decoding checks, as the packet
// arrives.
//
// A CONNECT comes from a peer that the broker knows nothing of yet, and it carries few properties
// in its two blocks, its own and its Will's. A decoded property holds many times its bytes on the
// wire, and CONNECTs wait for the authenticator by the hundred, so one with more properties than a
// block takes is refused as too large.
final class PacketChecks {

    // The properties that each block of a CONNECT holds at most: its own and its Will's.
    static final int MAX_CONNECT_PROPERTIES = 256;

    private PacketChecks() {
    }

    // Throws PacketDecodeException with Packet too large if `packet` is a CONNECT with more
    // properties in a block than MAX_CONNECT_PROPERTIES.
    static void check(Packet packet) {
        if (packet instanceof Packet.Connect connect) {
            counted(connect.properties());
            Packet.Will will = connect.will();
            if (will != null) {
                counted(will.properties());
            }
        }
    }

    private static void counted(Properties properties) {
        if (properties.entries().size() > MAX_CONNECT_PROPERTIES) {
            throw new PacketDecodeException(
                    ReasonCode.PACKET_TOO_LARGE,
                    "More than " + MAX_CONNECT_PROPERTIES + " properties in a block of a CONNECT"
            );
        }
    }
}
