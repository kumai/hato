package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.BrokerListener;
import org.mqtt4j.hato.broker.BrokerListener.DisconnectReason;
import org.mqtt4j.hato.internal.log.Log;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Properties;
import org.mqtt4j.hato.internal.protocol.PropertyId;

// What the connections of one broker share: its sessions and connections, its retained
// messages, and the rules that tests set. Each session's actor handles the packets of its
// connections (see Session), and what they share here is concurrent or replaced as a whole;
// the retained messages have a lock of their own, the topic lock.
//
// Routing a publication looks through every session and its subscriptions, which bounds its
// throughput with many sessions; an index of the subscriptions would replace the scan if broker
// measurements justify productization. Lock order is topic lock -> session inbox -> Connection.
final class Routing {

    private static final Log log = Log.of(Routing.class);

    // How long a closing broker that checks its memory for deliveries waits for the sessions
    // and the pool to end before it gives the check up.
    private static final long MEMORY_CHECK_NANOS = 1_000_000_000L;

    // How long a thread of the authentication pool waits for another decision before it ends.
    private static final long AUTHENTICATION_IDLE_SECONDS = 30;

    // How many notices of connections wait for a listener that is behind, for each client that
    // the broker may serve: the end of a connection and the start of the next, twice over, as
    // when every client connects again after an outage while the listener is behind.
    private static final int NOTICES_PER_CLIENT = 4;

    final BrokerSettings settings;

    final Reactor[] reactors;

    // The loop that serves the connections of clients in the same JVM.
    final InprocLoop inproc;

    final AtomicInteger nextReactor = new AtomicInteger();

    final ThreadPoolExecutor actors;

    // The port that the broker listens on, for thread names and logs.
    final int port;

    final Set<Connection> connections = ConcurrentHashMap.newKeySet();

    // How many of the connections wait for the broker to accept a CONNECT on them, which their
    // limit bounds. A connection counts from its registration with its selector, which the
    // accepting thread makes, until the broker accepts its client or the connection ends. The
    // accepting thread waits while they are at their limit, and one that leaves wakes it.
    final AtomicInteger awaitingConnect = new AtomicInteger();

    // Wakes the accepting thread as a place frees among the connections that wait for their
    // CONNECT.
    private final Runnable placeFreed;

    // How many clients the broker serves, which the client limit bounds: the connections that
    // it has accepted a CONNECT on and whose end has not come to their session yet. A
    // connection that takes a session over from another takes over its place too.
    private final AtomicInteger clients = new AtomicInteger();

    // Whether the broker has warned yet that the client limit refused a CONNECT.
    private final AtomicBoolean clientLimitReached = new AtomicBoolean();

    final Map<String, Session> sessions = new ConcurrentHashMap<>();

    // How many sessions in the table outlive their connection, which the session limit bounds.
    // A CONNECT that makes such a session takes a place, unless the session that it resumes or
    // replaces had one, and the place goes as the session ends or stops outliving its
    // connection.
    private final AtomicInteger persistentSessions = new AtomicInteger();

    final RetainedMessages retained;

    // The memory that the deliveries of all the sessions may take together.
    final MemoryBudget memory;

    // The threads of the actor pool, each of which keeps some of that memory to itself.
    private final List<Worker> actorThreads = new CopyOnWriteArrayList<>();

    // How many sessions the closing broker has yet to clear, on whatever thread has their turn.
    private final AtomicInteger clearing = new AtomicInteger();

    // The deliveries that the limits have dropped, by QoS level, as the sessions log them, and
    // the publications that the broker refused because they were outside their client's scope.
    private final AtomicLongArray dropped = new AtomicLongArray(3);

    private final AtomicLong refusedPublications = new AtomicLong();

    // Tells the application's listener, if there is one, of the clients that connect and
    // disconnect and of dropped QoS 1 and 2 deliveries.
    final @Nullable Notices notices;

    // Tells the application's listener, if there is one, of a failure that stopped the broker.
    final @Nullable FailureNotice failureNotice;

    // Runs the application's authenticator, if there is one, on threads of its own: at most as
    // many decisions at once as the settings allow, while further CONNECTs wait in its queue,
    // and none on a thread of the broker's I/O or actors, since a decision may wait. A thread
    // that has had no decision for a while ends.
    final @Nullable ThreadPoolExecutor authentications;

    // Where the selectors, the actor pool, and the listener's threads report their ends.
    final Termination termination;

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

    // What the broker announces to the MQTT 5.0 connections that it accepts from now on. Its
    // Maximum Packet Size is never above the broker's own limit, which holds whatever a test
    // announces.
    volatile Capabilities capabilities;

    volatile boolean closed;

    // `stopBroker` ends the broker for the failure of a selector, and `placeFreed` wakes the
    // accepting thread as a connection stops waiting for its CONNECT.
    Routing(
            BrokerSettings settings,
            int port,
            Consumer<Throwable> stopBroker,
            Runnable placeFreed,
            Termination termination
    ) throws IOException {
        this.settings = settings;
        this.port = port;
        this.placeFreed = placeFreed;
        this.termination = termination;
        retained = new RetainedMessages(
                settings.maxRetained(),
                settings.maxRetainedBytes(),
                settings.scheduler(),
                port
        );
        memory = new MemoryBudget(
                settings.maxDeliveryMemory(),
                settings.deliveryMemoryUnit(),
                port
        );
        BrokerListener listener = settings.listener();
        // The clients that wait for a report of their own are bounded by the sessions that the
        // broker can have at once. So a client with a session finds a place among them unless
        // clients that have gone still hold some.
        long sessions = (long) settings.maxClients() + settings.maxSessions();
        notices = (listener == null) ? null : new Notices(
                listener,
                port,
                (int) Math.min(sessions, Integer.MAX_VALUE),
                (int) Math.min(
                        (long) NOTICES_PER_CLIENT * settings.maxClients(),
                        Integer.MAX_VALUE
                ),
                termination
        );
        failureNotice = (listener == null) ? null : new FailureNotice(listener, port, termination);
        authentications = (settings.authenticator() == null)
                ? null
                : authenticationPool(
                        settings.maxAuthentications(),
                        settings.maxAwaitingConnect(),
                        port,
                        termination
                );
        capabilities = settings.capabilities().withPacketSizeWithin(settings.maxPacketSize());
        reactors = new Reactor[settings.selectors()];
        inproc = new InprocLoop(this, stopBroker);
        int workers = settings.actors();
        var nextWorker = new AtomicInteger();
        actors = new ThreadPoolExecutor(
                workers,
                workers,
                0,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                loop -> {
                    var worker = new Worker(
                            loop,
                            "hato-broker-" + port + "-actor-" + nextWorker.getAndIncrement()
                    );
                    actorThreads.add(worker);
                    return worker;
                }
        ) {

            // Whatever a task did, the connections that it queued output for go to their
            // selectors as it ends.
            @Override
            protected void afterExecute(Runnable task, @Nullable Throwable failure) {
                var worker = (Worker) Thread.currentThread();
                worker.handOver();
            }

            // The pool ends once the broker has shut it down and its threads have ended.
            @Override
            protected void terminated() {
                termination.partEnded();
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
            if (authentications != null) {
                authentications.shutdown();
            }
            throw failure;
        }
    }

    // The pool that runs the application's authenticator, which ends its part of `termination`
    // once the broker has shut it down and its decisions have returned. A CONNECT that finds
    // `decisions` running waits in the queue for one of them to end, for its deadline at most
    // (see Client.decide). The queue holds `waiting`, as many connections as may wait for
    // their CONNECT, which one that waits for its decision still does; only the CONNECTs of
    // clients that their deadline refused meanwhile can fill it (see Client.authenticate). The
    // threads start as decisions come, and end once idle.
    private static ThreadPoolExecutor authenticationPool(
            int decisions,
            int waiting,
            int port,
            Termination termination
    ) {
        var nextAuthentication = new AtomicInteger();
        var pool = new ThreadPoolExecutor(
                decisions,
                decisions,
                AUTHENTICATION_IDLE_SECONDS,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(waiting),
                task -> {
                    int number = nextAuthentication.getAndIncrement();
                    return Thread.ofPlatform().daemon()
                            .name("hato-broker-" + port + "-auth-" + number)
                            .unstarted(task);
                }
        ) {

            // The pool ends once the broker has shut it down and its decisions have returned.
            @Override
            protected void terminated() {
                termination.partEnded();
            }
        };
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    // Counts a connection out of those that wait for their CONNECT. The accepting thread waits
    // while they are at their limit, so the one that frees a place wakes it.
    void leftAwaitingConnect() {
        if (awaitingConnect.getAndDecrement() >= settings.maxAwaitingConnect()) {
            placeFreed.run();
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

    // Ends the connection of the client `clientId` for the application, telling an MQTT 5.0
    // client `reason`, and returns false if the client has no connection or it was ending
    // already.
    boolean disconnect(String clientId, Reason reason) {
        Session session = sessions.get(clientId);
        Connection peer = (session == null) ? null : session.connection;
        if (peer == null) {
            return false;
        }
        Packet.Disconnect disconnect = reason.fitted(
                properties -> new Packet.Disconnect(reason.code(), properties),
                peer.client.sendLimit
        );
        return peer.disconnect(
                disconnect,
                "disconnected by the application",
                DisconnectReason.APPLICATION
        );
    }

    // A connection copies the capabilities as it accepts the CONNECT, so each one works by those
    // before or those after this replacement, never by a mix.
    void announce(Capabilities value) {
        capabilities = value.withPacketSizeWithin(settings.maxPacketSize());
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

    // Takes a place among the clients for a connection whose CONNECT the broker accepts, and
    // returns false when the client limit leaves none.
    boolean takeClientPlace() {
        int limit = settings.maxClients();
        int before = clients.getAndUpdate(count -> (count < limit) ? count + 1 : count);
        if (before < limit) {
            return true;
        }
        if (clientLimitReached.compareAndSet(false, true)) {
            log.warn("Broker on port {} serves {} clients, its limit, and refuses further ones;"
                    + " later refusals are logged at info level", port, limit);
        }
        return false;
    }

    // Frees the place of a client whose connection has ended.
    void freeClientPlace() {
        clients.decrementAndGet();
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

    // How many clients the broker serves now, as the client limit counts them.
    int clients() {
        return clients.get();
    }

    // How many sessions outlive their connection now, as the session limit counts them.
    int persistentSessions() {
        return persistentSessions.get();
    }

    // Counts `count` deliveries at QoS `level` that a session's limits have dropped.
    void countDropped(int level, long count) {
        if (count != 0) {
            dropped.addAndGet(level, count);
        }
    }

    // How many deliveries at QoS `level` the limits have dropped so far.
    long dropped(int level) {
        return dropped.get(level);
    }

    // Counts a publication that the broker refused because it is outside its client's scope.
    void refusedPublication() {
        refusedPublications.incrementAndGet();
    }

    long refusedPublications() {
        return refusedPublications.get();
    }

    // How many deliveries wait in the sessions for their clients now.
    // Ceiling: it visits every session under its inbox, so its time grows with the sessions; a
    // count that the queues kept as they change would cost the delivery path instead.
    long queuedDeliveries() {
        long queued = 0;
        for (Session session : sessions.values()) {
            queued += session.queued();
        }
        return queued;
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
    // making it the retained message of its topic if its RETAIN flag asks for that. Called on
    // the actor of `publisher`, which drains the sessions that it delivers to, here where no
    // thread holds their turn: under the topic lock for a retained publication.
    void route(Packet.Publish publication, Session publisher) {
        // A Message Expiry Interval counts from here.
        long received = settings.scheduler().nanoTime();
        // What the publication holds on the heap, which its deliveries and its retained message
        // share.
        var room = new PublicationMemory(publication);
        if (!publication.retain()) {
            deliver(publication, room, publisher, received);
            return;
        }
        synchronized (retained) {
            retained.store(publication, received, room);
            deliver(publication, room, publisher, received);
        }
    }

    private void deliver(
            Packet.Publish publication,
            PublicationMemory room,
            Session publisher,
            long received
    ) {
        // The table lets a target leave it during delivery, as one whose connection ended does.
        for (Session target : sessions.values()) {
            target.deliverMatching(publication, room, publisher, received);
        }
    }

    // Counts a session that the closing broker has asked to clear itself, and one that has.
    void sessionClearing() {
        clearing.incrementAndGet();
    }

    void sessionCleared() {
        clearing.decrementAndGet();
    }

    // Checks that the memory for deliveries adds up once the broker has closed: no delivery
    // left its session without giving its room back, and nothing is handed out any more but
    // the room that the pool's threads keep. What does not add up fails as an assertion with
    // the numbers; otherwise the answer is true, so that the call stands in an `assert`. A
    // broker that runs with assertions, as in tests, checks so as it closes, which makes every
    // test of the broker a test of the counting too. The check waits a moment for the sessions
    // that other threads still clear and for the pool to end, and takes what it cannot judge
    // in that time to be in order.
    boolean deliveryMemoryAddsUp() {
        long deadline = System.nanoTime() + MEMORY_CHECK_NANOS;
        while ((clearing.get() > 0 || !actors.isTerminated())
                && System.nanoTime() - deadline < 0) {
            LockSupport.parkNanos(1_000_000);
        }
        long lost = memory.lostDeliveries();
        if (lost != 0) {
            throw new AssertionError(
                    lost + " deliveries left their sessions without giving their room in the"
                            + " broker's memory for deliveries back"
            );
        }
        if (clearing.get() > 0 || !actors.isTerminated()) {
            return true;
        }
        long kept = 0;
        for (Worker worker : actorThreads) {
            kept += worker.memory;
        }
        long lent = memory.lent();
        if (lent != kept) {
            throw new AssertionError(
                    "The broker's memory for deliveries has " + lent + " bytes handed out after"
                            + " every session has ended, of which its threads keep " + kept
            );
        }
        return true;
    }

    // Ends every connection and session and forgets every retained message, as the broker
    // closes. Each session ends here if no thread holds its turn, and otherwise soon after.
    void clear() {
        for (Connection connection : connections) {
            connection.close("broker closed", null);
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
