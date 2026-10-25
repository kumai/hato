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
    // none. They are fixed here, so a resend carries the same ones (MQTT 5.0 section 3.8.4), but
    // a Message Expiry Interval among them is sent less the time waited.
    private final Properties properties;

    // When the broker received the publication, on its clock, and the publication's Message
    // Expiry Interval in seconds, or -1 without one. MQTT 3.1.1 receivers learn nothing of it,
    // but an expired copy does not reach them either.
    private final long received;

    private final long lifetime;

    final int byteCount;

    int packetId;

    boolean sent;

    Delivery(
            Packet.Publish publication,
            long received,
            Qos qos,
            boolean retain,
            List<Integer> identifiers,
            MqttVersion version
    ) {
        this.publication = publication;
        this.received = received;
        lifetime = lifetimeOf(publication);
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

    // Whether the delivery waited its whole lifetime, after which it must not start
    // [MQTT-3.3.2-5].
    boolean expired(long now) {
        return expired(lifetime, received, now);
    }

    // The Message Expiry Interval of `publication`, in seconds, or -1 without one.
    static long lifetimeOf(Packet.Publish publication) {
        Property expiry = publication.properties()
                .find(PropertyId.MESSAGE_EXPIRY_INTERVAL)
                .orElse(null);
        return switch (expiry) {
            case Property.FourByteValue(var _, var value) -> value;
            case null, default -> -1;
        };
    }

    // Whether a message of `lifetime` seconds, received at `received`, has waited that long by
    // `now`. A lifetime of 0 has passed at once.
    static boolean expired(long lifetime, long received, long now) {
        return lifetime >= 0 && now - received >= lifetime * 1_000_000_000L;
    }

    @Override
    public Packet.Publish packet(long now) {
        return new Packet.Publish(
                publication.topic(),
                publication.payload(),
                qos,
                retain,
                qos != Qos.AT_MOST_ONCE && sent,
                packetId,
                remainingAt(now)
        );
    }

    // The properties with the Message Expiry Interval less the whole seconds waited, which is
    // what is left of it rounded up [MQTT-3.3.2-6]. A resend of an exchange that started in time
    // goes on with 0 once the lifetime is used up.
    private Properties remainingAt(long now) {
        if (lifetime < 0 || properties.isEmpty()) {
            return properties;
        }
        long remaining = Math.max(0, lifetime - (now - received) / 1_000_000_000L);
        var left = new Property.FourByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, remaining);
        List<Property> entries = properties.entries().stream()
                .map(property -> property.id() == left.id() ? left : property)
                .toList();
        return new Properties(entries);
    }
}
