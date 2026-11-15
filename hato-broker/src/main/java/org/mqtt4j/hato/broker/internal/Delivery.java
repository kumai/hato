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
// packet identifier belongs to the session's actor. Whether it was sent is set by the writer of
// the connection that it went to, while that connection has not ended, and read by the actor for
// the DUP flag of a resend. The payload stays the publication's, which its deliveries to other
// sessions share: the writer prepares the headers as it writes them, before the payload.
final class Delivery implements Outgoing, Pending {

    private final Packet.Publish publication;

    // The room that the publication takes in the broker's memory for deliveries, which its
    // deliveries to all sessions share.
    final PublicationMemory memory;

    private final PacketCodec codec;

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

    // The bytes of the PUBLISH that carries the delivery to its session's client, for what the
    // protocol and the connection's send queue measure in packets.
    final int byteCount;

    // What the delivery holds on the heap beside its publication: itself, its places in its
    // session's queue and window, and a sequence of properties of its own if it adds
    // Subscription Identifiers to the publication's.
    final long ownBytes;

    // What the limits that bound memory count for the delivery: its publication, which it
    // shares with the publication's other deliveries, and what it holds beside.
    final long memoryBytes;

    // Where the delivery stands among what its session has queued: the session's number for
    // it, set under the session's inbox as the delivery comes to wait.
    long order;

    // Whether the delivery has room in the broker's memory for deliveries, which its session
    // takes for it once it waits and gives back as it leaves the session. The session's, under
    // its inbox.
    boolean hasRoom;

    int packetId;

    volatile boolean sent;

    Delivery(
            Packet.Publish publication,
            PublicationMemory memory,
            long received,
            Qos qos,
            boolean retain,
            List<Integer> identifiers,
            MqttVersion version
    ) {
        this.publication = publication;
        this.memory = memory;
        this.received = received;
        lifetime = lifetimeOf(publication);
        this.qos = qos;
        this.retain = retain;
        properties = switch (version) {
            case V3_1_1 -> Properties.EMPTY;
            case V5 -> withIdentifiers(publication.properties(), identifiers);
        };
        codec = PacketCodec.of(version);
        byteCount = codec.publishSize(
                publication.topic(),
                qos,
                properties,
                publication.payload().length
        );
        ownBytes = ownSize(publication, identifiers, version);
        memoryBytes = memory.bytes() + ownBytes;
    }

    // What a delivery of `publication` to a receiver of `version` that carries `identifiers`
    // holds on the heap beside the publication. Only an MQTT 5.0 delivery with Subscription
    // Identifiers has a sequence of properties of its own: the publication's properties, which
    // it shares, and the identifiers, which are its own too.
    static long ownSize(
            Packet.Publish publication,
            List<Integer> identifiers,
            MqttVersion version
    ) {
        long own = MemoryBudget.DELIVERY_BYTES;
        if (version == MqttVersion.V5 && !identifiers.isEmpty()) {
            own += PublicationMemory.sequenceSize(
                    publication.properties().entries().size() + identifiers.size(),
                    identifiers.size()
            );
        }
        return own;
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

    // The headers of the PUBLISH that carries the delivery at `now`, before its payload. DUP
    // marks a resend of a delivery that an earlier connection started to send, as `sentBefore`
    // says [MQTT-3.3.1-1], but never one at QoS 0 [MQTT-3.3.1-2].
    byte[] header(boolean sentBefore, long now) {
        return codec.encodePublishHeader(
                publication.topic(),
                qos,
                retain,
                qos != Qos.AT_MOST_ONCE && sentBefore,
                packetId,
                remainingAt(now),
                publication.payload().length
        );
    }

    byte[] payload() {
        return publication.payload();
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
