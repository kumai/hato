package org.mqtt4j.hato.broker.internal;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
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

    // The publisher's properties reach MQTT 5.0 receivers unchanged; MQTT 3.1.1 has none.
    private final Properties properties;

    final int byteCount;

    int packetId;

    boolean sent;

    Delivery(Packet.Publish publication, Qos qos, boolean retain, MqttVersion version) {
        this.publication = publication;
        this.qos = qos;
        this.retain = retain;
        properties = switch (version) {
            case V3_1_1 -> Properties.EMPTY;
            case V5 -> publication.properties();
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
