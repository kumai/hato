package org.mqtt4j.hato.broker.internal;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.concurrent.Timer;
import org.mqtt4j.hato.broker.internal.common.io.SendQueue;
import org.mqtt4j.hato.broker.internal.common.log.Log;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketIds;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static java.util.Objects.requireNonNull;

// Every session field, including the active connection, belongs to routing.
final class Session {

    private static final Log log = Log.of(Session.class);

    private final Routing routing;

    private final BrokerSettings settings;

    final String clientId;

    // A session continues only with the protocol version that created it, so its deliveries
    // are sized and encoded for one version.
    final MqttVersion version;

    // A Session Expiry Interval of 0xFFFFFFFF, which keeps a session without end.
    static final long NEVER = 0xFFFF_FFFFL;

    // How long the session outlives a connection, in seconds: 0 ends it with its connection,
    // and NEVER keeps it, as MQTT 3.1.1 Clean Session 0 does, which leaves retention to the
    // broker. A connection that resumes the session sets it again from its own CONNECT, and
    // the client's DISCONNECT can change it (MQTT 5.0 section 3.14.2.2.2).
    long expiry;

    // The broker's time at which the session lost its connection, from which its expiry and the
    // delay of its Will count.
    private long detachedAt;

    // The Will of the connection that ended last, which waits `willDelay` seconds before it goes
    // out, or less if the session ends first [MQTT-3.1.2-8]; none waits while a connection is
    // attached. The timer comes when the delay or the expiry has passed, whichever is earlier.
    private Packet.@Nullable Will will;

    private long willDelay;

    private Timer.@Nullable Cancellable willTimer;

    // Each filter's subscription with the options that the client requested.
    final Map<TopicFilter, Subscribed> subscriptions = new HashMap<>();

    private final PacketIds packetIds = new PacketIds();

    private final LinkedHashMap<Integer, Outgoing> inflight = new LinkedHashMap<>();

    // Replays and PUBRELs waiting for space in the current connection's send queue, at most
    // one per inflight identifier. It lives for one connection: resume() rebuilds it from
    // inflight, and detaching the connection drops it.
    private final LinkedHashMap<Integer, Outgoing> deferred = new LinkedHashMap<>();

    // The identifiers of the QoS 1 and QoS 2 PUBLISH packets that went to the current
    // connection and await their PUBACK, PUBCOMP, or failing PUBREC: the client's send quota in
    // use [MQTT-3.3.4-9]. An exchange that continues from an earlier connection with a PUBREL
    // takes none of it (MQTT 5.0 section 4.9). Like deferred, it lives for one connection; the
    // count saves counting the bits for each delivery.
    private final BitSet outstanding = new BitSet();

    private int outstandingCount;

    // Only identifiers are retained for inbound QoS 2. The set grows by doubling on first use,
    // so the 16-bit identifier space bounds it to 16 KiB.
    final BitSet received = new BitSet();

    private final ArrayDeque<Delivery> pending = new ArrayDeque<>();

    @Nullable Connection connection;

    private long inflightBytes;

    private long pendingBytes;

    Session(Routing routing, String clientId, MqttVersion version, long expiry) {
        this.routing = routing;
        settings = routing.settings;
        this.clientId = clientId;
        this.version = version;
        this.expiry = expiry;
    }

    // Whether the session outlives its connection: Clean Session 0 in MQTT 3.1.1, or a
    // positive Session Expiry Interval in MQTT 5.0.
    boolean kept() {
        return expiry > 0;
    }

    // Whether the session has lost its connection for longer than its expiry, so that the
    // broker must discard it [MQTT-4.1.0-2].
    boolean expired(long now) {
        return connection == null
                && expiry != NEVER
                && now - detachedAt >= expiry * 1_000_000_000L;
    }

    // Keeps the Will of the connection that just ended until `delay` seconds have passed since
    // the session lost it, or until the session ends if that comes first [MQTT-3.1.2-8]. Called
    // under routing, after the detachment from which both count.
    //
    // The broker decides from the time whether the Will goes out, as with the expiry: when a
    // connection continues the session, when the session ends, and when the timer comes. The
    // timer only makes sure that someone looks once the time has come, so a timer that comes
    // late, or after its cancellation, changes nothing.
    void awaitWill(Packet.Will lastWill, long delay) {
        will = lastWill;
        willDelay = delay;
        try {
            // A Session Expiry Interval of NEVER is at least any delay.
            willTimer = settings.scheduler().schedule(
                    Duration.ofSeconds(Math.min(delay, expiry)),
                    this::checkWill
            );
        } catch (RuntimeException failure) {
            // Publishing the Will early beats never publishing it.
            log.warn("Broker on port {} could not delay the Will of client {}",
                    routing.port, clientId, failure);
            will = null;
            routing.publishWill(lastWill, this);
        }
    }

    // Whether a Will waits with the session and its delay has passed by `now`, so that a
    // connection that continues the session comes too late to cancel it [MQTT-3.1.3-9].
    boolean willDue(long now) {
        return will != null && now - detachedAt >= willDelay * 1_000_000_000L;
    }

    // Takes the Will that waits with the session, to publish it, and cancels its timer; null
    // when none waits.
    Packet.@Nullable Will takeWill() {
        Packet.Will waiting = will;
        cancelWill();
        return waiting;
    }

    // Drops the Will that waits with the session, if any, as a connection that continues the
    // session within the delay does [MQTT-3.1.3-9].
    void cancelWill() {
        will = null;
        Timer.Cancellable timer = willTimer;
        willTimer = null;
        if (timer == null) {
            return;
        }
        try {
            timer.cancel();
        } catch (Throwable failure) {
            // A failing injected timer must not keep the session from ending.
            log.warn("Cancelling the Will timer of client {} on port {} failed",
                    clientId, routing.port, failure);
        }
    }

    // The timer of the waiting Will, which comes once its delay or the session's expiry has
    // passed. Unlike the connection timers, which leave the routing work to the reader and the
    // writer, the Will has no thread of its own, so the shared timer takes routing to publish
    // it. A deliberate simplification: the timer thread waits for routing, which is held for
    // routing work but never for reads or writes, and the other timers of the distribution wait
    // with it; handing the publication to a thread of its own would remove that wait, if
    // productization needs it.
    private void checkWill() {
        try {
            synchronized (routing) {
                long now = settings.scheduler().nanoTime();
                if (expired(now)) {
                    discard();
                } else if (willDue(now)) {
                    routing.publishWill(requireNonNull(takeWill()), this);
                }
            }
        } catch (Throwable failure) {
            log.warn("Broker on port {} failed to publish the Will of client {}",
                    routing.port, clientId, failure);
        }
    }

    // Delivers `publication` from `publisher` for the subscriptions that match it.
    // Overlapping subscriptions share one copy at their highest QoS [MQTT-3.3.4-2], which
    // carries the Subscription Identifiers of them all [MQTT-3.3.4-4]. A retained publication
    // takes a second copy when only some of them keep its retain flag, so that each
    // subscription gets the flag it asked for [MQTT-3.3.1-12] [MQTT-3.3.1-13]; each copy then
    // carries the identifiers of the subscriptions that receive it [MQTT-3.3.4-5].
    void deliverMatching(Packet.Publish publication, Session publisher, long received) {
        // The highest QoS granted by the matching subscriptions with Retain As Published, and
        // by the other matching subscriptions; -1 while none matches. Their identifiers repeat
        // one that several of them have, as MQTT 5.0 section 3.3.4 describes.
        int asPublished = -1;
        int cleared = -1;
        List<Integer> asPublishedIdentifiers = List.of();
        List<Integer> clearedIdentifiers = List.of();
        for (Subscribed subscribed : subscriptions.values()) {
            Packet.Subscription subscription = subscribed.subscription();
            // No Local withholds a client's own publications [MQTT-3.8.3-3].
            boolean withheld = subscription.noLocal() && publisher == this;
            if (withheld || !subscription.filter().matches(publication.topic())) {
                continue;
            }
            int level = subscription.qos().level();
            if (subscription.retainAsPublished()) {
                asPublished = Math.max(asPublished, level);
                asPublishedIdentifiers = withIdentifier(asPublishedIdentifiers, subscribed);
            } else {
                cleared = Math.max(cleared, level);
                clearedIdentifiers = withIdentifier(clearedIdentifiers, subscribed);
            }
        }
        int published = publication.qos().level();
        if (publication.retain() && asPublished >= 0 && cleared >= 0) {
            deliver(
                    publication,
                    received,
                    Qos.of(Math.min(asPublished, published)),
                    true,
                    asPublishedIdentifiers
            );
            deliver(
                    publication,
                    received,
                    Qos.of(Math.min(cleared, published)),
                    false,
                    clearedIdentifiers
            );
        } else if (asPublished >= 0 || cleared >= 0) {
            Qos qos = Qos.of(Math.min(Math.max(asPublished, cleared), published));
            var identifiers = new ArrayList<>(asPublishedIdentifiers);
            identifiers.addAll(clearedIdentifiers);
            boolean retain = publication.retain() && asPublished >= 0;
            deliver(publication, received, qos, retain, identifiers);
        }
    }

    // `identifiers` with the identifier of `subscribed`, if it has one. The list is created for
    // the first identifier, so that a session without them allocates none.
    private static List<Integer> withIdentifier(List<Integer> identifiers, Subscribed subscribed) {
        if (subscribed.identifier() == 0) {
            return identifiers;
        }
        List<Integer> added = identifiers.isEmpty() ? new ArrayList<>() : identifiers;
        added.add(subscribed.identifier());
        return added;
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
    void deliverRetained(Subscribed subscribed) {
        Packet.Subscription subscription = subscribed.subscription();
        // They result from this subscription alone, and carry its identifier [MQTT-3.3.4-3].
        List<Integer> identifiers = subscribed.identifier() == 0
                ? List.of()
                : List.of(subscribed.identifier());
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
            deliver(publication, entry.received(), Qos.of(level), true, identifiers);
        }
    }

    private void deliver(
            Packet.Publish publication,
            long received,
            Qos qos,
            boolean retain,
            List<Integer> identifiers
    ) {
        Connection peer = connection;
        if (peer != null && peer.isEnded()) {
            detach(peer);
        }
        boolean online = connection != null;
        if (!online && (!kept() || qos == Qos.AT_MOST_ONCE)) {
            return;
        }
        var delivery = new Delivery(publication, received, qos, retain, identifiers, version);
        // A connected client that takes no packet this large loses only this delivery, before
        // the broker's own limits below could discard the whole session.
        Connection current = connection;
        if (current != null && tooLargeFor(current, delivery)) {
            return;
        }
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
        outstanding.clear();
        outstandingCount = 0;
        Connection peer = connection;
        if (peer == null) {
            return;
        }
        // A publication that an earlier connection sent first can be larger than the new one
        // takes; it ends as if answered, and the rest go out again.
        for (Map.Entry<Integer, Outgoing> entry : new LinkedHashMap<>(inflight).entrySet()) {
            if (entry.getValue() instanceof Delivery replay && tooLargeFor(peer, replay)) {
                end(entry.getKey(), replay);
            } else {
                deferred.put(entry.getKey(), entry.getValue());
            }
        }
        drain();
    }

    void drain() {
        Connection peer = connection;
        if (peer == null || peer.isEnded()) {
            return;
        }
        // Replays and PUBRELs precede new deliveries in the regular queue. A PUBREL goes out
        // whatever the client's send quota [MQTT-3.3.4-10], while a replayed PUBLISH waits for
        // room in it, and new deliveries wait behind the replays. When the queue is full,
        // draining pauses here and the writer resumes it after freeing space.
        boolean replaysWait = false;
        Iterator<Outgoing> waiting = deferred.values().iterator();
        while (waiting.hasNext()) {
            Outgoing outgoing = waiting.next();
            Delivery replay = outgoing instanceof Delivery value ? value : null;
            if (replay != null && outstandingCount >= peer.client.sendQuota) {
                replaysWait = true;
                continue;
            }
            if (peer.enqueue(outgoing) != SendQueue.OfferResult.ACCEPTED) {
                return;
            }
            waiting.remove();
            if (replay != null) {
                countOutstanding(replay.packetId);
            }
        }
        if (replaysWait) {
            return;
        }
        // A delivery that waited its whole Message Expiry Interval before it could start goes
        // without being sent [MQTT-3.3.2-5]. Expired ones are found here rather than with a
        // timer each, so they keep their place in the queue limits until the queue moves; a timer
        // would release them at once, if productization needs that.
        long now = settings.scheduler().nanoTime();
        Delivery delivery;
        while ((delivery = pending.peekFirst()) != null) {
            if (delivery.expired(now)) {
                pending.removeFirst();
                pendingBytes -= delivery.byteCount;
                continue;
            }
            // A delivery queued for an earlier connection can be larger than this one takes.
            // It goes as if sent and answered, without an identifier or a place in the window.
            if (tooLargeFor(peer, delivery)) {
                pending.removeFirst();
                pendingBytes -= delivery.byteCount;
                continue;
            }
            boolean acknowledged = delivery.qos != Qos.AT_MOST_ONCE;
            if (acknowledged && (inflight.size() == settings.maxInflight()
                    || delivery.byteCount > settings.maxInflightBytes() - inflightBytes
                    || outstandingCount >= peer.client.sendQuota)) {
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
                countOutstanding(delivery.packetId);
            }
        }
    }

    private void countOutstanding(int id) {
        outstanding.set(id);
        outstandingCount++;
    }

    // Whether `delivery` is larger than the client on `peer` takes. The broker discards such a
    // delivery and behaves as if it had sent it [MQTT-3.1.2-25]; this logs that.
    private boolean tooLargeFor(Connection peer, Delivery delivery) {
        int limit = peer.client.sendLimit;
        if (delivery.byteCount <= limit) {
            return false;
        }
        log.debug(
                "Broker on port {} discarded a {}-byte delivery for client {}, which takes at most"
                        + " {} bytes",
                routing.port,
                delivery.byteCount,
                clientId,
                limit
        );
        return true;
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

    // Forgets an outgoing exchange and returns its identifier, its window share, and its place
    // in the client's send quota.
    private void end(int id, Outgoing outgoing) {
        inflight.remove(id);
        deferred.remove(id);
        packetIds.release(id);
        if (outgoing instanceof Delivery delivery) {
            inflightBytes -= delivery.byteCount;
        }
        if (outstanding.get(id)) {
            outstanding.clear(id);
            outstandingCount--;
        }
    }

    void detach(Connection peer) {
        if (connection != peer) {
            return;
        }
        connection = null;
        deferred.clear();
        outstanding.clear();
        outstandingCount = 0;
        if (!kept()) {
            routing.sessions.remove(clientId, this);
            clear();
            return;
        }
        detachedAt = settings.scheduler().nanoTime();
        // QoS 0 belongs to the connection; only acknowledged deliveries survive it.
        pending.removeIf(delivery -> {
            if (delivery.qos == Qos.AT_MOST_ONCE) {
                pendingBytes -= delivery.byteCount;
                return true;
            }
            return false;
        });
    }

    // Drops the session state, the Will that waits with it included, which a closing broker
    // does not publish.
    void clear() {
        subscriptions.clear();
        received.clear();
        inflight.clear();
        deferred.clear();
        outstanding.clear();
        outstandingCount = 0;
        pending.clear();
        inflightBytes = 0;
        pendingBytes = 0;
        connection = null;
        cancelWill();
    }

    // Ends the session, which no connection continues, as its expiry passes or its deliveries
    // overflow. Its state goes, and a Will that waits with it goes out now, because the session
    // ends before the delay [MQTT-3.1.2-8]. Discarding it again does nothing. Called under
    // routing.
    void discard() {
        if (!routing.sessions.remove(clientId, this)) {
            return;
        }
        Packet.Will waiting = takeWill();
        clear();
        if (waiting != null) {
            routing.publishWill(waiting, this);
        }
    }

    private void discard(String reason) {
        Connection peer = connection;
        discard();
        if (peer != null) {
            peer.close(reason);
        }
    }

    // A subscription with the Subscription Identifier that its SUBSCRIBE gave it, or 0, which no
    // identifier takes, for none. Making the subscription again replaces both (MQTT 5.0 section
    // 3.8.4).
    record Subscribed(Packet.Subscription subscription, int identifier) {
    }
}
