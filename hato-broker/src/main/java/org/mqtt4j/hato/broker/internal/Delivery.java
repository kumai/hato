package org.mqtt4j.hato.broker.internal;

import java.util.ArrayList;
import java.util.List;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;

// One publication on its way to one session, sized for the session's protocol version. The
// packet identifier and whether it was sent belong to routing.
final class Delivery implements Outgoing {

    private final Packet.Publish publication;

    final Qos qos;

    // The publisher's retain flag for a Retain As Published subscription, and false
    // otherwise, as for every live delivery in MQTT 3.1.1.
    private final boolean retain;

    // The publisher's properties reach MQTT 5.0 receivers unchanged, followed by the
    // Subscription Identifiers of the subscriptions that the delivery is for; MQTT 3.1.1 has
    // none. They are fixed here, so a resend carries the same ones (MQTT 5.0 section 3.8.4).
    private final Properties properties;

    final int byteCount;

    int packetId;

    boolean sent;

    Delivery(
            Packet.Publish publication,
            Qos qos,
            boolean retain,
            List<Integer> identifiers,
            MqttVersion version
    ) {
        this.publication = publication;
        this.qos = qos;
        this.retain = retain;
        properties = switch (version) {
            case V3_1_1 -> Properties.EMPTY;
            case V5 -> withIdentifiers(publication.properties(), identifiers);
        };
        byte[] encodedHeader = PacketCodec.of(version).encodePublishHeader(
                publication.topic(),
                qos,
                retain,
                false,
                (qos == Qos.AT_MOST_ONCE) ? 0 : 1,
                properties,
                publication.payload().length
        );
        byteCount = encodedHeader.length + publication.payload().length;
    }

    private static Properties withIdentifiers(Properties properties, List<Integer> identifiers) {
        if (identifiers.isEmpty()) {
            return properties;
        }
        var entries = new ArrayList<>(properties.entries());
        for (int identifier : identifiers) {
            entries.add(
                    new Property.VariableByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER, identifier)
            );
        }
        return new Properties(entries);
    }

    @Override
    public Packet.Publish packet() {
        return new Packet.Publish(
                publication.topic(),
                publication.payload(),
                qos,
                retain,
                qos != Qos.AT_MOST_ONCE && sent,
                packetId,
                properties
        );
    }
}
