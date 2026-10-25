package org.mqtt4j.hato.broker.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.log.Log;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.Topic;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

// What the connections of one broker share: its sessions, its retained messages, and the rules
// that tests set. The monitor of this object is the routing lock, which guards all of it, every
// session, and the fields of each connection that say they belong to routing.
//
// This global lock and linear session/subscription scan bound routing throughput. Replace
// them with an index and finer ownership if broker measurements justify productization.
// Lock order is routing -> Connection. Readers/writers release their monitor before routing;
// connection timers stop I/O without routing, and reader/writer cleanup detaches the connection
// later. The timer of a waiting Will takes routing while it holds no Connection monitor.
final class Routing {

    private static final Log log = Log.of(Routing.class);

    final BrokerSettings settings;

    // The port that the broker listens on, for thread names and logs.
    final int port;

    final Set<Connection> connections = new HashSet<>();

    final Map<String, Session> sessions = new HashMap<>();

    // The retained message of each topic, guarded by routing, in the order they were stored.
    // A new subscription scans them all, which the retained limits keep short; an index by topic
    // level would replace the scan if productization needs it.
    private final Map<Topic, Retained> retained = new LinkedHashMap<>();

    private long retainedBytes;

    // Whether the retained limits have refused a message yet; only the first refusal warns.
    private boolean retainedLimitReached;

    // The test controls, guarded by routing like the packets they act on. Until a test sets
    // them, they change nothing.

    // Rules for later subscriptions to exactly these filters.
    private final Map<TopicFilter, SubscriptionRule> subscriptionRules = new HashMap<>();

    // Rules that refuse later publications to the topics that their filters match, in the order
    // set. A topic that several of them match takes the one set last.
    private final LinkedHashMap<TopicFilter, Reason> publicationRules = new LinkedHashMap<>();

    // Rules that refuse later CONNECT requests with exactly these client identifiers.
    private final Map<String, Reason> connectionRules = new HashMap<>();

    // The kinds of response that the connections of these clients hold back.
    private final Set<Hold> holds = new HashSet<>();

    // What the broker announces to the MQTT 5.0 connections that it accepts from now on.
    Capabilities capabilities;

    boolean closed;

    Routing(BrokerSettings settings, int port) {
        this.settings = settings;
        this.port = port;
        capabilities = settings.capabilities();
    }

    void refuseSubscriptions(String filter, Reason reason) {
        var rule = new SubscriptionRule.Refusal(reason);
        var key = new TopicFilter(filter);
        synchronized (this) {
            subscriptionRules.put(key, rule);
        }
    }

    void grantAtMost(String filter, int maximumLevel) {
        var rule = new SubscriptionRule.Limit(Qos.of(maximumLevel));
        var key = new TopicFilter(filter);
        synchronized (this) {
            subscriptionRules.put(key, rule);
        }
    }

    void clearSubscriptionRule(String filter) {
        var key = new TopicFilter(filter);
        synchronized (this) {
            subscriptionRules.remove(key);
        }
    }

    void refusePublications(String filter, Reason reason) {
        var key = new TopicFilter(filter);
        synchronized (this) {
            // Setting a rule again makes it the last one set.
            publicationRules.remove(key);
            publicationRules.put(key, reason);
        }
    }

    void clearPublicationRule(String filter) {
        var key = new TopicFilter(filter);
        synchronized (this) {
            publicationRules.remove(key);
        }
    }

    void refuseConnections(String clientId, Reason reason) {
        synchronized (this) {
            connectionRules.put(clientId, reason);
        }
    }

    void clearConnectionRule(String clientId) {
        synchronized (this) {
            connectionRules.remove(clientId);
        }
    }

    void holdResponses(Class<? extends Packet> type, String clientId) {
        var hold = new Hold(type, clientId);
        synchronized (this) {
            holds.add(hold);
        }
    }

    void releaseResponses(Class<? extends Packet> type, String clientId) {
        var hold = new Hold(type, clientId);
        synchronized (this) {
            holds.remove(hold);
            for (Connection connection : connections) {
                if (connection.clientId.equals(clientId)) {
                    connection.client.release(type);
                }
            }
        }
    }

    List<BrokerServer.Held> heldResponses() {
        synchronized (this) {
            var all = new ArrayList<BrokerServer.Held>();
            for (Connection connection : connections) {
                for (Packet response : connection.client.held) {
                    all.add(new BrokerServer.Held(connection.clientId, response));
                }
            }
            return List.copyOf(all);
        }
    }

    Set<String> connectedClients() {
        synchronized (this) {
            var clients = new HashSet<String>();
            for (Session session : sessions.values()) {
                Connection peer = session.connection;
                if (peer != null && !peer.isEnded()) {
                    clients.add(session.clientId);
                }
            }
            return Set.copyOf(clients);
        }
    }

    void disconnect(String clientId, Reason reason) {
        synchronized (this) {
            Session session = sessions.get(clientId);
            Connection peer = (session == null) ? null : session.connection;
            if (peer != null) {
                Packet.Disconnect disconnect = reason.fitted(
                        properties -> new Packet.Disconnect(reason.code(), properties),
                        peer.client.sendLimit
                );
                peer.disconnect(disconnect, "disconnected by a test");
            }
        }
    }

    // A connection copies the capabilities as routing accepts it, so each one works by those
    // before or those after this replacement, never by a mix.
    void announce(Capabilities value) {
        synchronized (this) {
            capabilities = value;
        }
    }

    // The test rule for subscriptions to exactly `filter`, or null without one. Called under
    // routing.
    @Nullable SubscriptionRule subscriptionRule(TopicFilter filter) {
        return subscriptionRules.get(filter);
    }

    // The reason for refusing a publication to `topic`: that of the rule set last among those
    // whose filters match it, or null when none does. Called under routing.
    @Nullable Reason publicationRefusal(Topic topic) {
        Reason refusal = null;
        for (Map.Entry<TopicFilter, Reason> rule : publicationRules.entrySet()) {
            if (rule.getKey().matches(topic)) {
                refusal = rule.getValue();
            }
        }
        return refusal;
    }

    // The reason for refusing a CONNECT with exactly `clientId`, or null without a rule.
    // Called under routing.
    @Nullable Reason connectionRefusal(String clientId) {
        return connectionRules.get(clientId);
    }

    // Whether a test holds back the responses of `type` to `clientId`. Called under routing.
    boolean holdsBack(Class<? extends Packet> type, String clientId) {
        return holds.contains(new Hold(type, clientId));
    }

    // Discards the sessions whose expiry has passed since their connection ended
    // [MQTT-4.1.0-2], and publishes the Wills that wait with them. The broker looks for them
    // when it looks a session up for a CONNECT and when it routes, in place of a timer per
    // session; only a session with a waiting Will has a timer, which comes at its expiry if the
    // Will's delay is longer. So an expired session keeps its queued deliveries, within the
    // session limits, until then. A timer for every session would release them at once, if
    // productization needs that. Called under routing.
    void discardExpiredSessions() {
        long now = settings.scheduler().nanoTime();
        // A Will routes, and routing looks for expired sessions again, so they are listed before
        // any of them is discarded; discarding one twice does nothing.
        List<Session> expired = sessions.values().stream()
                .filter(session -> session.expired(now))
                .toList();
        for (Session session : expired) {
            session.discard();
        }
    }

    // Publishes `will` as its client would have published the message, from the session of
    // `publisher`. A closing broker publishes none, because every session goes with it. Called
    // under routing.
    void publishWill(Packet.Will will, Session publisher) {
        if (closed) {
            return;
        }
        // The Will properties travel as the publication's, without the delay, which only
        // the Will has; User Properties keep their order [MQTT-3.1.3-10]. The packet
        // identifier is a placeholder, because every delivery takes its own.
        var properties = new Properties(will.properties().entries().stream()
                .filter(property -> property.id() != PropertyId.WILL_DELAY_INTERVAL)
                .toList());
        var publication = new Packet.Publish(
                will.topic(),
                will.payload(),
                will.qos(),
                will.retain(),
                false,
                (will.qos() == Qos.AT_MOST_ONCE) ? 0 : 1,
                properties
        );
        route(publication, publisher);
    }

    // Delivers `publication` from `publisher`'s client to every matching subscription, after
    // making it the retained message of its topic if its RETAIN flag asks for that. Called under
    // routing.
    void route(Packet.Publish publication, Session publisher) {
        discardExpiredSessions();
        // A Message Expiry Interval counts from here.
        long received = settings.scheduler().nanoTime();
        if (publication.retain()) {
            retain(publication, received);
        }
        // Snapshot permits an overloaded target to remove itself during delivery.
        for (Session target : List.copyOf(sessions.values())) {
            target.deliverMatching(publication, publisher, received);
        }
    }

    // A copy of the retained messages, in the order they were stored, after discarding those
    // whose Message Expiry Interval has passed (MQTT 5.0 section 3.3.1.3). They are looked for
    // here rather than with a timer each, so an expired one keeps its share of the retained
    // limits until a subscription looks; a timer would release it at once, if productization
    // needs that. Called under routing.
    List<Retained> retainedMessages() {
        long now = settings.scheduler().nanoTime();
        retained.values().removeIf(entry -> {
            if (!entry.expired(now)) {
                return false;
            }
            retainedBytes -= entry.byteCount();
            return true;
        });
        return List.copyOf(retained.values());
    }

    // Ends every connection and forgets every session and retained message, as the broker
    // closes. Called under routing.
    void clear() {
        for (Connection connection : List.copyOf(connections)) {
            connection.close("broker closed");
        }
        sessions.values().forEach(Session::clear);
        sessions.clear();
        retained.clear();
        retainedBytes = 0;
    }

    // Makes `publication` the retained message of its topic in place of the previous one
    // [MQTT-3.3.1-5]. An empty payload only removes the previous one [MQTT-3.3.1-6]
    // [MQTT-3.3.1-7]. A message beyond the retained limits is not kept either, which leaves its
    // topic without a retained message rather than with an outdated one.
    private void retain(Packet.Publish publication, long received) {
        Retained previous = retained.remove(publication.topic());
        if (previous != null) {
            retainedBytes -= previous.byteCount();
        }
        if (publication.payload().length == 0) {
            return;
        }
        // A retained message counts as the MQTT 5.0 PUBLISH that carries it with its properties.
        byte[] header = PacketCodec.of(MqttVersion.V5).encodePublishHeader(
                publication.topic(),
                publication.qos(),
                true,
                false,
                (publication.qos() == Qos.AT_MOST_ONCE) ? 0 : 1,
                publication.properties(),
                publication.payload().length
        );
        var entry = new Retained(
                publication,
                header.length + publication.payload().length,
                received
        );
        if (retained.size() >= settings.maxRetained()
                || entry.byteCount() > settings.maxRetainedBytes() - retainedBytes) {
            if (!retainedLimitReached) {
                retainedLimitReached = true;
                log.warn(
                        "Broker on port {} reached its retained message limits and keeps no"
                                + " retained message for topic {}; later refusals log at DEBUG",
                        port,
                        publication.topic().name()
                );
            } else {
                log.debug(
                        "Broker on port {} keeps no retained message for topic {}: limits reached",
                        port,
                        publication.topic().name()
                );
            }
            return;
        }
        retained.put(publication.topic(), entry);
        retainedBytes += entry.byteCount();
    }

    // A retained message, its share of the retained size limit, and when the broker received it.
    record Retained(Packet.Publish publication, long byteCount, long received) {

        boolean expired(long now) {
            return Delivery.expired(Delivery.lifetimeOf(publication), received, now);
        }
    }

    // A test rule for subscriptions to one filter, which refuses them or grants them at most a
    // QoS.
    sealed interface SubscriptionRule {

        record Refusal(Reason reason) implements SubscriptionRule {
        }

        record Limit(Qos maximum) implements SubscriptionRule {
        }
    }

    // One kind of response that the connections of one client hold back.
    private record Hold(Class<? extends Packet> type, String clientId) {
    }
}
