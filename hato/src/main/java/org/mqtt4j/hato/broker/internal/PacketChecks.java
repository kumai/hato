package org.mqtt4j.hato.broker.internal;

import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.PacketCodec;
import org.mqtt4j.hato.internal.protocol.PacketDecodeException;
import org.mqtt4j.hato.internal.protocol.Properties;
import org.mqtt4j.hato.internal.protocol.Property;

// What the broker checks of a packet from a client beyond what decoding checks, as the packet
// arrives.
//
// No string of a packet may contain a control character, from U+0001 to U+001F or from U+007F
// to U+009F, or a noncharacter. MQTT advises against them and lets a receiver treat a packet that
// carries one as malformed, and clients do: libmosquitto, for one, drops its connection. The
// broker passes topics and properties on to other clients, so it refuses them as Mosquitto's
// broker does, rather than let the text of one client end the connections of others. The client
// of this library still takes them from a broker.
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

    // Throws PacketDecodeException, with Malformed Packet if a string of `packet` contains a
    // control character or a noncharacter, and with Packet too large if `packet` is a CONNECT with
    // more properties in a block than MAX_CONNECT_PROPERTIES.
    static void check(Packet packet) {
        switch (packet) {
            case Packet.Connect connect -> {
                Packet.Will will = connect.will();
                counted(connect.properties());
                if (will != null) {
                    counted(will.properties());
                }
                text(connect.clientId());
                String username = connect.username();
                if (username != null) {
                    text(username);
                }
                if (will != null) {
                    text(will.topic().name());
                    properties(will.properties());
                }
                properties(connect.properties());
            }
            case Packet.Publish publish -> {
                text(publish.topic().name());
                properties(publish.properties());
            }
            case Packet.Subscribe subscribe -> {
                for (Packet.Subscription subscription : subscribe.subscriptions()) {
                    text(subscription.filter().pattern());
                }
                properties(subscribe.properties());
            }
            case Packet.Unsubscribe unsubscribe -> {
                for (TopicFilter filter : unsubscribe.filters()) {
                    text(filter.pattern());
                }
                properties(unsubscribe.properties());
            }
            case Packet.AliasPublish publish -> properties(publish.properties());
            case Packet.ConnAck answer -> properties(answer.properties());
            case Packet.PubAck answer -> properties(answer.properties());
            case Packet.PubRec answer -> properties(answer.properties());
            case Packet.PubRel answer -> properties(answer.properties());
            case Packet.PubComp answer -> properties(answer.properties());
            case Packet.SubAck answer -> properties(answer.properties());
            case Packet.UnsubAck answer -> properties(answer.properties());
            case Packet.Disconnect disconnect -> properties(disconnect.properties());
            case Packet.Auth auth -> properties(auth.properties());
            case Packet.PingReq _, Packet.PingResp _ -> {
                // No strings.
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

    // Checks the strings of `properties`, and that there are no more of them than decoding
    // takes: a packet of an in-process client, which arrives undecoded, may have more, which no
    // client that the broker passes it on to over a network would take.
    private static void properties(Properties properties) {
        if (properties.entries().size() > PacketCodec.MAX_PROPERTIES) {
            throw new PacketDecodeException(
                    ReasonCode.PACKET_TOO_LARGE,
                    "More than " + PacketCodec.MAX_PROPERTIES + " MQTT properties"
            );
        }
        for (Property property : properties.entries()) {
            switch (property) {
                case Property.StringValue value -> text(value.value());
                case Property.UserProperty pair -> {
                    text(pair.name());
                    text(pair.value());
                }
                case Property.ByteValue _,
                     Property.TwoByteValue _,
                     Property.FourByteValue _,
                     Property.VariableByteValue _,
                     Property.BinaryValue _ -> {
                    // No string.
                }
            }
        }
    }

    // Checks one string, which decoding has found well formed: every high surrogate has its low
    // one after it.
    private static void text(String text) {
        for (int index = 0; index < text.length(); index++) {
            char unit = text.charAt(index);
            boolean refused;
            if (Character.isHighSurrogate(unit)) {
                int codePoint = Character.toCodePoint(unit, text.charAt(++index));
                // The last two code points of every plane are noncharacters.
                refused = (codePoint & 0xFFFE) == 0xFFFE;
            } else {
                refused = unit < 0x20
                        || (unit >= 0x7F && unit < 0xA0)
                        || (unit >= 0xFDD0 && unit <= 0xFDEF)
                        || unit >= 0xFFFE;
            }
            if (refused) {
                throw new PacketDecodeException(
                        ReasonCode.MALFORMED_PACKET,
                        "MQTT string with a control character or a noncharacter"
                );
            }
        }
    }
}
