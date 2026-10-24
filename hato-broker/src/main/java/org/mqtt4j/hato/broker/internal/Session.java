package org.mqtt4j.hato.broker.internal;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.io.SendQueue;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketIds;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

// Every session field, including the active connection, belongs to routing.
final class Session {

    private final Routing routing;

    private final BrokerSettings settings;

    final String clientId;

    // A session continues only with the protocol version that created it, so its deliveries
    // are sized and encoded for one version.
    final MqttVersion version;

    // Whether the session outlives its connection: Clean Session 0 in MQTT 3.1.1, or a
    // positive Session Expiry Interval in MQTT 5.0. A connection that resumes the session
    // sets it again from its own CONNECT.
    boolean kept;

    // Each filter's subscription with the options that the client requested.
    final Map<TopicFilter, Packet.Subscription> subscriptions = new HashMap<>();

    private final PacketIds packetIds = new PacketIds();

    private final LinkedHashMap<Integer, Outgoing> inflight = new LinkedHashMap<>();

    // Replays and PUBRELs waiting for space in the current connection's send queue, at most
    // one per inflight identifier. It lives for one connection: resume() rebuilds it from
    // inflight, and detaching the connection drops it.
    private final LinkedHashMap<Integer, Outgoing> deferred = new LinkedHashMap<>();

    // Only identifiers are retained for inbound QoS 2. The set grows by doubling on first use,
    // so the 16-bit identifier space bounds it to 16 KiB.
    final BitSet received = new BitSet();

    private final ArrayDeque<Delivery> pending = new ArrayDeque<>();

    @Nullable Connection connection;

    private long inflightBytes;

    private long pendingBytes;

    Session(Routing routing, String clientId, MqttVersion version, boolean kept) {
        this.routing = routing;
        settings = routing.settings;
        this.clientId = clientId;
        this.version = version;
        this.kept = kept;
    }

    // Delivers `publication` from `publisher` for the subscriptions that match it.
    // Overlapping subscriptions share one copy at their highest QoS [MQTT-3.3.4-2]. A retained
    // publication takes a second copy when only some of them keep its retain flag, so that
    // each subscription gets the flag it asked for [MQTT-3.3.1-12] [MQTT-3.3.1-13].
    void deliverMatching(Packet.Publish publication, Session publisher) {
        // The highest QoS granted by the matching subscriptions with Retain As Published, and
        // by the other matching subscriptions; -1 while none matches.
        int asPublished = -1;
        int cleared = -1;
        for (Packet.Subscription subscription : subscriptions.values()) {
            // No Local withholds a client's own publications [MQTT-3.8.3-3].
            boolean withheld = subscription.noLocal() && publisher == this;
            if (withheld || !subscription.filter().matches(publication.topic())) {
                continue;
            }
            int level = subscription.qos().level();
            if (subscription.retainAsPublished()) {
                asPublished = Math.max(asPublished, level);
            } else {
                cleared = Math.max(cleared, level);
            }
        }
        int published = publication.qos().level();
        if (publication.retain() && asPublished >= 0 && cleared >= 0) {
            deliver(publication, Qos.of(Math.min(asPublished, published)), true);
            deliver(publication, Qos.of(Math.min(cleared, published)), false);
        } else if (asPublished >= 0 || cleared >= 0) {
            Qos qos = Qos.of(Math.min(Math.max(asPublished, cleared), published));
            deliver(publication, qos, publication.retain() && asPublished >= 0);
        }
    }

    // Sends the retained messages that match `subscription`, which the client has just made
    // or replaced. Each one arrives with RETAIN 1, whatever Retain As Published says, at the
    // lower of its QoS and the granted one ([MQTT-3.3.1-8] in MQTT 3.1.1). No Local does not
    // withhold the retained messages of the subscriber's own client: brokers differ here,
    // and Mosquitto and HiveMQ CE send them while EMQX withholds them.
    //
    // Retained messages take the window and the queue of every other delivery, so a
    // subscription that matches more of them than those hold discards the session like any
    // other overflow. A deliberate simplification: sending them as the window frees up would
    // lift that ceiling, if productization needs it.
    void deliverRetained(Packet.Subscription subscription) {
        // A copy, because a delivery that overflows closes the connection, and the Will that
        // its end publishes can change the retained messages.
        for (Routing.Retained entry : routing.retainedMessages()) {
            Packet.Publish publication = entry.publication();
            if (!subscription.filter().matches(publication.topic())) {
                continue;
            }
            // An overflow discarded the session; the rest would only fill a dead one.
            if (routing.sessions.get(clientId) != this) {
                return;
            }
            int level = Math.min(subscription.qos().level(), publication.qos().level());
            deliver(publication, Qos.of(level), true);
        }
    }

    private void deliver(Packet.Publish publication, Qos qos, boolean retain) {
        Connection peer = connection;
        if (peer != null && peer.isEnded()) {
            detach(peer);
        }
        boolean online = connection != null;
        if (!online && (!kept || qos == Qos.AT_MOST_ONCE)) {
            return;
        }
        var delivery = new Delivery(publication, qos, retain, version);
        // A frame that can never fit must not block the head indefinitely.
        if (delivery.byteCount > settings.sendBytes()
                || (qos != Qos.AT_MOST_ONCE && delivery.byteCount > settings.maxInflightBytes())) {
            discard("delivery exceeds send window");
            return;
        }
        pending.addLast(delivery);
        pendingBytes += delivery.byteCount;
        drain();
        if (pending.size() > settings.maxQueued() || pendingBytes > settings.maxQueuedBytes()) {
            discard("delivery queue full");
        }
    }

    void resume() {
        deferred.clear();
        deferred.putAll(inflight);
        drain();
    }

    void drain() {
        Connection peer = connection;
        if (peer == null || peer.isEnded()) {
            return;
        }
        // Replays and PUBRELs precede new deliveries in the regular queue. When it is full,
        // draining pauses here and the writer resumes it after freeing space.
        while (!deferred.isEmpty()) {
            Outgoing outgoing = deferred.firstEntry().getValue();
            if (peer.enqueue(outgoing) != SendQueue.OfferResult.ACCEPTED) {
                return;
            }
            deferred.pollFirstEntry();
        }
        Delivery delivery;
        while ((delivery = pending.peekFirst()) != null) {
            boolean acknowledged = delivery.qos != Qos.AT_MOST_ONCE;
            if (acknowledged && (inflight.size() == settings.maxInflight()
                    || delivery.byteCount > settings.maxInflightBytes() - inflightBytes)) {
                return;
            }
            if (acknowledged) {
                delivery.packetId = packetIds.allocate().orElseThrow();
            }
            SendQueue.OfferResult result = peer.enqueue(delivery);
            if (result != SendQueue.OfferResult.ACCEPTED) {
                if (acknowledged) {
                    packetIds.release(delivery.packetId);
                    delivery.packetId = 0;
                }
                // An ACK can free the logical window before the writer frees physical space.
                return;
            }
            pending.removeFirst();
            pendingBytes -= delivery.byteCount;
            if (acknowledged) {
                inflight.put(delivery.packetId, delivery);
                inflightBytes += delivery.byteCount;
            }
        }
    }

    void acknowledge(int id, boolean complete) {
        Outgoing outgoing = inflight.get(id);
        if (outgoing == null) {
            return;
        }
        boolean expected = complete
                ? outgoing instanceof Release
                : outgoing instanceof Delivery delivery && delivery.qos == Qos.AT_LEAST_ONCE;
        if (!expected) {
            throw new PacketDecodeException(
                    ReasonCode.PROTOCOL_ERROR,
                    "Acknowledgement does not match the outgoing QoS stage"
            );
        }
        end(id, outgoing);
        drain();
    }

    void release(int id, ReasonCode reason) {
        Outgoing outgoing = inflight.get(id);
        if (outgoing == null) {
            return;
        }
        // Drop the body on PUBREC, but retain the identifier and count until PUBCOMP.
        // Moving to the end preserves PUBREC order for both initial and replayed PUBREL.
        switch (outgoing) {
            case Delivery delivery -> {
                if (delivery.qos != Qos.EXACTLY_ONCE) {
                    throw new PacketDecodeException(
                            ReasonCode.PROTOCOL_ERROR,
                            "PUBREC does not match the outgoing QoS stage"
                    );
                }
                if (reason.value() >= 0x80) {
                    // An MQTT 5.0 receiver that refuses the message ends the exchange here;
                    // only a successful PUBREC leads to PUBREL.
                    end(id, delivery);
                } else {
                    inflightBytes -= delivery.byteCount;
                    var release = new Release(id);
                    inflight.putLast(id, release);
                    deferred.putLast(id, release);
                }
            }
            // Duplicate PUBREC shares an unsent response without moving it behind later ids.
            case Release release -> deferred.putIfAbsent(id, release);
        }
        drain();
    }

    // Forgets an outgoing exchange and returns its identifier and window share.
    private void end(int id, Outgoing outgoing) {
        inflight.remove(id);
        deferred.remove(id);
        packetIds.release(id);
        if (outgoing instanceof Delivery delivery) {
            inflightBytes -= delivery.byteCount;
        }
    }

    void detach(Connection peer) {
        if (connection != peer) {
            return;
        }
        connection = null;
        deferred.clear();
        if (!kept) {
            routing.sessions.remove(clientId, this);
            clear();
            return;
        }
        // QoS 0 belongs to the connection; only acknowledged deliveries survive it.
        pending.removeIf(delivery -> {
            if (delivery.qos == Qos.AT_MOST_ONCE) {
                pendingBytes -= delivery.byteCount;
                return true;
            }
            return false;
        });
    }

    void clear() {
        subscriptions.clear();
        received.clear();
        inflight.clear();
        deferred.clear();
        pending.clear();
        inflightBytes = 0;
        pendingBytes = 0;
        connection = null;
    }

    private void discard(String reason) {
        Connection peer = connection;
        routing.sessions.remove(clientId, this);
        clear();
        if (peer != null) {
            peer.close(reason);
        }
    }
}
