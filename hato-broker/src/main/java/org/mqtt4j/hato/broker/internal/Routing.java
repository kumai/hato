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
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.Topic;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

// What the connections of one broker share: its sessions, its retained messages, and the rules
// that tests set. The monitor of this object is the routing lock, which guards all of it, every
// session, and the fields of each connection that say they belong to routing.
//
// This global lock and linear session/subscription scan bound routing throughput. Replace
// them with an index and finer ownership if broker measurements justify productization.
// Lock order is routing -> Connection. Readers/writers release their monitor before routing;
// timers stop I/O without routing, and reader/writer cleanup detaches the connection later.
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

    // The kinds of response that the connections of these clients hold back.
    private final Set<Hold> holds = new HashSet<>();

    boolean closed;

    Routing(BrokerSettings settings, int port) {
        this.settings = settings;
        this.port = port;
    }

    void refuseSubscriptions(String filter) {
        var rule = new SubscriptionRule(null);
        var key = new TopicFilter(filter);
        synchronized (this) {
            subscriptionRules.put(key, rule);
        }
    }

    void grantAtMost(String filter, int maximumLevel) {
        var rule = new SubscriptionRule(Qos.of(maximumLevel));
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

    void disconnect(String clientId) {
        synchronized (this) {
            Session session = sessions.get(clientId);
            Connection peer = (session == null) ? null : session.connection;
            if (peer != null) {
                peer.disconnect(ReasonCode.ADMINISTRATIVE_ACTION, "disconnected by a test");
            }
        }
    }

    // The highest QoS that the test rules let `requested` have: the requested QoS without a
    // rule for its filter, the rule's maximum with one, and null when the rule refuses it.
    // Called under routing.
    @Nullable Qos allowedQos(Packet.Subscription requested) {
        SubscriptionRule rule = subscriptionRules.get(requested.filter());
        return (rule == null) ? requested.qos() : rule.maximum();
    }

    // Whether a test holds back the responses of `type` to `clientId`. Called under routing.
    boolean holdsBack(Class<? extends Packet> type, String clientId) {
        return holds.contains(new Hold(type, clientId));
    }

    // Delivers `publication` from `publisher`'s client to every matching subscription, after
    // making it the retained message of its topic if its RETAIN flag asks for that. Called under
    // routing.
    void route(Packet.Publish publication, Session publisher) {
        if (publication.retain()) {
            retain(publication);
        }
        // Snapshot permits an overloaded target to remove itself during delivery.
        for (Session target : List.copyOf(sessions.values())) {
            target.deliverMatching(publication, publisher);
        }
    }

    // A copy of the retained messages, in the order they were stored. Called under routing.
    List<Retained> retainedMessages() {
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
    private void retain(Packet.Publish publication) {
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
        var entry = new Retained(publication, header.length + publication.payload().length);
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

    // A retained message and its share of the retained size limit.
    record Retained(Packet.Publish publication, long byteCount) {
    }

    // A test rule for subscriptions to one filter: they are refused without a maximum, and
    // granted at most the maximum otherwise.
    private record SubscriptionRule(@Nullable Qos maximum) {
    }

    // One kind of response that the connections of one client hold back.
    private record Hold(Class<? extends Packet> type, String clientId) {
    }
}
