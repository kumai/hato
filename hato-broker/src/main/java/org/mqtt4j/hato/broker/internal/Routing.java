package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.Topic;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

// What the connections of one broker share: its sessions and connections, its retained
// messages, and the rules that tests set. Each session's actor handles the packets of its
// connections (see Session), and what they share here is concurrent or replaced as a whole;
// the retained messages have a lock of their own, the topic lock.
//
// Routing a publication looks through every session and its subscriptions, which bounds its
// throughput with many sessions; an index of the subscriptions would replace the scan if broker
// measurements justify productization. Lock order is topic lock -> session inbox -> Connection.
final class Routing {

    final BrokerSettings settings;

    final Reactor[] reactors;

    final AtomicInteger nextReactor = new AtomicInteger();

    final ThreadPoolExecutor actors;

    // The port that the broker listens on, for thread names and logs.
    final int port;

    final Set<Connection> connections = ConcurrentHashMap.newKeySet();

    final Map<String, Session> sessions = new ConcurrentHashMap<>();

    // How many sessions in the table outlive their connection, which the session limit bounds.
    // A CONNECT that makes such a session takes a place, unless the session that it resumes or
    // replaces had one, and the place goes as the session ends or stops outliving its
    // connection.
    private final AtomicInteger persistentSessions = new AtomicInteger();

    final RetainedMessages retained;

    // The test controls. Until a test sets them, they change nothing.

    // Rules for later subscriptions to exactly these filters.
    private final Map<TopicFilter, SubscriptionRule> subscriptionRules = new ConcurrentHashMap<>();

    // Rules that refuse later publications to the topics that their filters match, in the order
    // set. A topic that several of them match takes the one set last. A change replaces the map,
    // which is never changed once set, so the order changes at once for every reader.
    private final AtomicReference<Map<TopicFilter, Reason>> publicationRules =
            new AtomicReference<>(Map.of());

    // Rules that refuse later CONNECT requests with exactly these client identifiers.
    private final Map<String, Reason> connectionRules = new ConcurrentHashMap<>();

    // The kinds of response that the connections of these clients hold back.
    private final Set<Hold> holds = ConcurrentHashMap.newKeySet();

    // What the broker announces to the MQTT 5.0 connections that it accepts from now on.
    volatile Capabilities capabilities;

    volatile boolean closed;

    Routing(BrokerSettings settings, int port, Runnable stopBroker) throws IOException {
        this.settings = settings;
        this.port = port;
        retained = new RetainedMessages(settings, port);
        capabilities = settings.capabilities();
        reactors = new Reactor[settings.selectors()];
        int workers = settings.actors();
        var nextWorker = new AtomicInteger();
        actors = new ThreadPoolExecutor(
                workers,
                workers,
                0,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                loop -> new Worker(
                        loop,
                        "hato-broker-" + port + "-actor-" + nextWorker.getAndIncrement()
                )
        ) {

            // Whatever a task did, the connections that it queued output for go to their
            // selectors as it ends.
            @Override
            protected void afterExecute(Runnable task, @Nullable Throwable failure) {
                var worker = (Worker) Thread.currentThread();
                worker.handOver();
            }
        };
        try {
            for (int i = 0; i < reactors.length; i++) {
                reactors[i] = new Reactor(this, i, stopBroker);
            }
        } catch (IOException | RuntimeException | Error failure) {
            for (Reactor reactor : reactors) {
                if (reactor != null) {
                    reactor.close();
                }
            }
            actors.shutdown();
            throw failure;
        }
    }

    void refuseSubscriptions(String filter, Reason reason) {
        subscriptionRules.put(new TopicFilter(filter), new SubscriptionRule.Refusal(reason));
    }

    void grantAtMost(String filter, int maximumLevel) {
        var rule = new SubscriptionRule.Limit(Qos.of(maximumLevel));
        subscriptionRules.put(new TopicFilter(filter), rule);
    }

    void clearSubscriptionRule(String filter) {
        subscriptionRules.remove(new TopicFilter(filter));
    }

    void refusePublications(String filter, Reason reason) {
        var key = new TopicFilter(filter);
        publicationRules.updateAndGet(rules -> {
            var changed = new LinkedHashMap<>(rules);
            // Setting a rule again makes it the last one set.
            changed.remove(key);
            changed.put(key, reason);
            return changed;
        });
    }

    void clearPublicationRule(String filter) {
        var key = new TopicFilter(filter);
        publicationRules.updateAndGet(rules -> {
            var changed = new LinkedHashMap<>(rules);
            changed.remove(key);
            return changed;
        });
    }

    void refuseConnections(String clientId, Reason reason) {
        connectionRules.put(clientId, reason);
    }

    void clearConnectionRule(String clientId) {
        connectionRules.remove(clientId);
    }

    void holdResponses(Class<? extends Packet> type, String clientId) {
        holds.add(new Hold(type, clientId));
    }

    void releaseResponses(Class<? extends Packet> type, String clientId) {
        holds.remove(new Hold(type, clientId));
        // A response that a connection sends from now on still queues behind those held, so none
        // goes ahead of them.
        for (Connection connection : connections) {
            if (connection.clientId.equals(clientId)) {
                connection.client.release(type);
            }
        }
    }

    List<BrokerServer.Held> heldResponses() {
        var all = new ArrayList<BrokerServer.Held>();
        for (Connection connection : connections) {
            for (Packet response : connection.client.heldResponses()) {
                all.add(new BrokerServer.Held(connection.clientId, response));
            }
        }
        return List.copyOf(all);
    }

    Set<String> connectedClients() {
        var clients = new HashSet<String>();
        for (Session session : sessions.values()) {
            Connection peer = session.connection;
            if (peer != null && !peer.isEnded()) {
                clients.add(session.clientId);
            }
        }
        return Set.copyOf(clients);
    }

    void disconnect(String clientId, Reason reason) {
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

    // A connection copies the capabilities as it accepts the CONNECT, so each one works by those
    // before or those after this replacement, never by a mix.
    void announce(Capabilities value) {
        capabilities = value;
    }

    // The test rule for subscriptions to exactly `filter`, or null without one.
    @Nullable SubscriptionRule subscriptionRule(TopicFilter filter) {
        return subscriptionRules.get(filter);
    }

    // The reason for refusing a publication to `topic`: that of the rule set last among those
    // whose filters match it, or null when none does.
    @Nullable Reason publicationRefusal(Topic topic) {
        Reason refusal = null;
        for (Map.Entry<TopicFilter, Reason> rule : publicationRules.get().entrySet()) {
            if (rule.getKey().matches(topic)) {
                refusal = rule.getValue();
            }
        }
        return refusal;
    }

    // The reason for refusing a CONNECT with exactly `clientId`, or null without a rule.
    @Nullable Reason connectionRefusal(String clientId) {
        return connectionRules.get(clientId);
    }

    // Whether a test holds back the responses of `type` to `clientId`.
    boolean holdsBack(Class<? extends Packet> type, String clientId) {
        return holds.contains(new Hold(type, clientId));
    }

    // Takes a place among the persistent sessions for a session that a CONNECT makes, and returns
    // false when the session limit leaves none. An expired session gives its place up as its
    // timer comes [MQTT-4.1.0-2].
    boolean takePersistentPlace() {
        int limit = settings.maxSessions();
        int before = persistentSessions.getAndUpdate(count -> (count < limit) ? count + 1 : count);
        return before < limit;
    }

    // Frees the place of a session that ends, or stops outliving its connection.
    void freePersistentPlace() {
        persistentSessions.decrementAndGet();
    }

    // Publishes `will` as its client would have published the message, from the session of
    // `publisher`. A closing broker publishes none, because every session goes with it. Called
    // on the actor of `publisher`.
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
    // making it the retained message of its topic if its RETAIN flag asks for that. Called on the
    // actor of `publisher`, which drains the sessions that it delivers to, here where no thread
    // holds their turn: under the topic lock for a retained publication.
    void route(Packet.Publish publication, Session publisher) {
        // A Message Expiry Interval counts from here.
        long received = settings.scheduler().nanoTime();
        if (!publication.retain()) {
            deliver(publication, publisher, received);
            return;
        }
        synchronized (retained) {
            retained.store(publication, received);
            deliver(publication, publisher, received);
        }
    }

    private void deliver(Packet.Publish publication, Session publisher, long received) {
        // The table lets a target leave it during delivery, as one whose connection ended does.
        for (Session target : sessions.values()) {
            target.deliverMatching(publication, publisher, received);
        }
    }

    // Ends every connection and session and forgets every retained message, as the broker
    // closes. Each session ends here if no thread holds its turn, and otherwise soon after.
    void clear() {
        for (Connection connection : connections) {
            connection.close("broker closed");
        }
        sessions.values().forEach(Session::close);
        sessions.clear();
        persistentSessions.set(0);
        synchronized (retained) {
            retained.clear();
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
