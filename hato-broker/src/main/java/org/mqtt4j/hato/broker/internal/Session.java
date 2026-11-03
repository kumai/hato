package org.mqtt4j.hato.broker.internal;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
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

// The session of one client identifier, with its actor: the thread that holds the session's turn
// runs the events posted to the session, one at a time and in the order posted, and only that
// thread touches the session's state and the clients of its connections. The monitor of this
// object is the session's inbox. It guards the events and the turn, what waits for the window,
// the attached connection, and the subscriptions, so that publishers can queue deliveries for
// the session without its turn.
//
// A thread that posts an event while no thread holds the turn takes it and runs the events
// itself, which spares waking another thread. A connection's reader and writer run a few so,
// then hand the rest over to a new virtual thread and get back to their socket. A thread that
// already holds another session's turn, such as a publisher that drains a subscriber, runs only
// the event that it came for, so that no thread holds more than a few turns at once. Timers only
// post. A thread takes a turn only while no other holds it, and one that holds a turn never
// waits for another, so the actors cannot deadlock; a reader waits, for room in the inbox or for
// its CONNECT, only while it holds none. Lock order is topic lock -> inbox -> Connection.
//
// One actor serves a client identifier while the session table holds it: a CONNECT that starts
// a new session resets the state in place. The actor leaves the table only as its session ends,
// and a later CONNECT for the identifier makes a new one.
final class Session {

    private static final Log log = Log.of(Session.class);

    // How many events a thread that takes the turn runs in place, besides a connection's reader
    // and writer, which run as many as the settings say: a thread that holds another session's
    // turn, or closes the broker, only the one that it came for, and a timer none.
    static final int ALONE = 1;

    static final int ELSEWHERE = 0;

    // The received packets that may wait in the inbox. A reader that finds no room waits until
    // the actor takes one, which holds its client back through TCP; one packet always fits,
    // whatever its size.
    private static final int RECEIVED_COUNT = 16;

    private static final long RECEIVED_BYTES = 1024 * 1024;

    // A Session Expiry Interval of 0xFFFFFFFF, which keeps a session without end.
    static final long NEVER = 0xFFFF_FFFFL;

    private final Routing routing;

    private final BrokerSettings settings;

    final String clientId;

    // A session continues only with the protocol version that created it, so its deliveries
    // are sized and encoded for one version. A new session sets it as it replaces the
    // subscriptions, which publishers check under the inbox.
    volatile MqttVersion version;

    // How long the session outlives a connection, in seconds: 0 ends it with its connection,
    // and NEVER keeps it, as MQTT 3.1.1 Clean Session 0 does, which leaves retention to the
    // broker. A connection that resumes the session sets it again from its own CONNECT, and
    // the client's DISCONNECT can change it (MQTT 5.0 section 3.14.2.2.2). An actor without a
    // session has 0.
    volatile long expiry;

    // The broker's time at which the session lost its connection, from which its expiry and the
    // delay of its Will count.
    private volatile long detachedAt;

    // The Will of the connection that ended last, which waits `willDelay` seconds before it goes
    // out, or less if the session ends first [MQTT-3.1.2-8]; none waits while a connection is
    // attached. The timer comes when the delay or the expiry has passed, whichever is earlier.
    private Packet.@Nullable Will will;

    private long willDelay;

    private Timer.@Nullable Cancellable willTimer;

    // Each filter's subscription with the options that the client requested, in the order the
    // client made them. A change replaces the map under the inbox, and the map is never changed
    // once set.
    private volatile Map<TopicFilter, Subscribed> subscriptions = Map.of();

    private PacketIds packetIds = new PacketIds();

    private final LinkedHashMap<Integer, Outgoing> inflight = new LinkedHashMap<>();

    private long inflightBytes;

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

    // What the inbox guards from here on, besides the subscriptions.

    // What waits for the window or for room in the connection's send queue, in the order it came:
    // deliveries, and the cursors of the retained messages that new subscriptions matched. QoS 0
    // deliveries and acknowledged ones count against rooms of their own, each as large as the
    // queue limits, so that QoS 0 deliveries that a client falls behind on cannot push out
    // acknowledged ones.
    private final ArrayDeque<Pending> pending = new ArrayDeque<>();

    private final Room atMostOnceRoom = new Room();

    private final Room acknowledgedRoom = new Room();

    // The retained messages that the cursors still hold. Their room is as large as the retained
    // limits, so that one subscription can take everything that the broker retains, while the
    // cursors of a client that subscribes again and again without taking them never hold more.
    private final Room retainedRoom = new Room();

    // Whether the session has logged a dropped delivery at warning level yet.
    private boolean dropLogged;

    // The connection attached to the session, which publishers and the test controls read.
    volatile @Nullable Connection connection;

    // The events that wait for the actor, in the order posted, and the received packets among
    // them, which their limits bound.
    private final ArrayDeque<Runnable> events = new ArrayDeque<>();

    private int receivedCount;

    private long receivedBytes;

    // Whether a drain waits among the events: one drain serves every request made before it
    // runs.
    private boolean drainDue;

    private final Runnable drainEvent = this::drain;

    // The thread that holds the turn, or null while none does, and then no event waits.
    private @Nullable Thread runner;

    // Set once the session has ended and left the session table: deliveries pass it by, and a
    // CONNECT that comes to it goes to the table again.
    private boolean ended;

    Session(Routing routing, String clientId, MqttVersion version) {
        this.routing = routing;
        settings = routing.settings;
        this.clientId = clientId;
        this.version = version;
    }

    // Whether the session outlives its connection: Clean Session 0 in MQTT 3.1.1, or a
    // positive Session Expiry Interval in MQTT 5.0.
    boolean kept() {
        return expiry > 0;
    }

    // Whether the session has lost its connection for longer than its expiry, so that the
    // broker must discard it [MQTT-4.1.0-2].
    boolean expired(long now) {
        long seconds = expiry;
        return connection == null
                && seconds > 0
                && seconds != NEVER
                && now - detachedAt >= seconds * 1_000_000_000L;
    }

    // Whether the session has ended and left the session table.
    synchronized boolean hasEnded() {
        return ended;
    }

    // Posts `event`, which the actor runs after those posted before. If no thread holds the
    // turn, the calling thread takes it and runs up to `inPlace` events here, and a new virtual
    // thread runs the rest, or all of them for 0. A thread that holds this session's turn
    // already only queues the event.
    void post(Runnable event, int inPlace) {
        Thread claimed;
        synchronized (this) {
            events.addLast(event);
            claimed = claimTurn(inPlace);
        }
        start(claimed, inPlace);
    }

    // Posts a packet that `from` received, as post() does for a connection's reader, which
    // reserved room for its response if `answered`. The reader waits while the received packets
    // that wait already fill their limits.
    void receive(Connection from, Packet packet, boolean answered, int bytes)
            throws InterruptedException {
        var event = new Received(from, packet, answered, bytes);
        Thread claimed;
        synchronized (this) {
            while (receivedCount >= RECEIVED_COUNT
                    || (receivedCount > 0 && bytes > RECEIVED_BYTES - receivedBytes)) {
                wait();
            }
            events.addLast(event);
            receivedCount++;
            receivedBytes += bytes;
            claimed = claimTurn(settings.inPlace());
        }
        start(claimed, settings.inPlace());
    }

    // Asks the actor to send what waits, as post() does with a drain.
    void requestDrain(int inPlace) {
        Thread claimed;
        synchronized (this) {
            queueDrain();
            claimed = claimTurn(inPlace);
        }
        start(claimed, inPlace);
    }

    // Queues a drain behind the events posted so far, unless one waits already. Called under the
    // inbox.
    private void queueDrain() {
        if (!drainDue) {
            drainDue = true;
            events.addLast(drainEvent);
        }
    }

    // Gives the turn, if no thread holds it, to the calling thread when it runs events in place,
    // and else to a new virtual thread, and returns the thread that took it; null when another
    // thread holds it. Called under the inbox.
    private @Nullable Thread claimTurn(int inPlace) {
        if (runner != null) {
            return null;
        }
        runner = (inPlace > 0) ? Thread.currentThread() : newRunner();
        return runner;
    }

    // Runs the events on the thread that has just taken the turn.
    private void start(@Nullable Thread claimed, int inPlace) {
        if (claimed == Thread.currentThread()) {
            run(inPlace);
        } else if (claimed != null) {
            claimed.start();
        }
    }

    private Thread newRunner() {
        return Thread.ofVirtual()
                .name(routing.runnerName)
                .unstarted(() -> run(Integer.MAX_VALUE));
    }

    // Runs the events while the calling thread holds the turn: at most `budget` of them, after
    // which it hands the rest over to a new virtual thread, and lets go of the turn once none is
    // left.
    private void run(int budget) {
        for (int ran = 0; ran < budget; ran++) {
            Runnable next;
            synchronized (this) {
                next = events.pollFirst();
                if (next == null) {
                    runner = null;
                    return;
                }
                if (next == drainEvent) {
                    drainDue = false;
                } else if (next instanceof Received taken) {
                    receivedCount--;
                    receivedBytes -= taken.bytes;
                    // A reader may wait for room.
                    notifyAll();
                }
            }
            perform(next);
        }
        Thread successor;
        synchronized (this) {
            if (events.isEmpty()) {
                runner = null;
                return;
            }
            successor = newRunner();
            runner = successor;
        }
        successor.start();
    }

    private void perform(Runnable event) {
        try {
            event.run();
        } catch (Throwable failure) {
            // One event's failure must not stop the session's others.
            log.warn("Broker on port {} failed to handle an event of client {}",
                    routing.port, clientId, failure);
        }
    }

    // Handles a packet that `from` received, while `from` is still the session's connection;
    // one from a connection that the session has left goes unhandled. A packet that breaks the
    // protocol ends the connection, as one that cannot be read does.
    private void handle(Received received) {
        Connection from = received.from;
        try {
            if (connection == from && !from.isEnded()) {
                settings.beforeHandle().accept(received.packet);
                from.client.process(this, received.packet);
            }
        } catch (Throwable failure) {
            from.failed(failure);
        } finally {
            if (received.answered) {
                from.responseSettled();
            }
        }
    }

    // Keeps the Will of the connection that just ended until `delay` seconds have passed since
    // the session lost it, or until the session ends if that comes first [MQTT-3.1.2-8]. Called
    // after the detachment from which both count.
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
                    this::willTimeUp
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
    // passed. It only posts, so that the shared timer thread, which the other timers of the
    // distribution share, waits for no session.
    private void willTimeUp() {
        try {
            post(this::checkExpiry, ELSEWHERE);
        } catch (Throwable failure) {
            log.warn("Broker on port {} failed to look at the Will of client {}",
                    routing.port, clientId, failure);
        }
    }

    // Ends the session if its expiry has passed, which publishes its Will, and otherwise
    // publishes a Will whose delay has passed.
    void checkExpiry() {
        long now = settings.scheduler().nanoTime();
        if (expired(now)) {
            discard();
        } else if (willDue(now)) {
            routing.publishWill(requireNonNull(takeWill()), this);
        }
    }

    // Delivers `publication` from `publisher` for the subscriptions that match it.
    // Overlapping subscriptions share one copy at their highest QoS [MQTT-3.3.4-2], which
    // carries the Subscription Identifiers of them all [MQTT-3.3.4-4]. A retained publication
    // takes a second copy when only some of them keep its retain flag, so that each
    // subscription gets the flag it asked for [MQTT-3.3.1-12] [MQTT-3.3.1-13]; each copy then
    // carries the identifiers of the subscriptions that receive it [MQTT-3.3.4-5].
    //
    // The subscriptions are matched without the inbox and checked under it: a change made
    // meanwhile, such as an UNSUBSCRIBE, has the publication matched again, so that none goes to
    // a subscription once the client has learned that it ended [MQTT-3.10.4-2]. Called by the
    // publisher's actor, which drains the session here if it can take its turn, or holds it as
    // the publisher's own; otherwise the actor that holds the turn drains it.
    void deliverMatching(Packet.Publish publication, Session publisher, long received) {
        while (true) {
            Map<TopicFilter, Subscribed> matchedOn = subscriptions;
            MqttVersion sized = version;
            // The highest QoS granted by the matching subscriptions with Retain As Published,
            // and by the other matching subscriptions; -1 while none matches. Their identifiers
            // repeat one that several of them have, as MQTT 5.0 section 3.3.4 describes.
            int asPublished = -1;
            int cleared = -1;
            List<Integer> asPublishedIdentifiers = List.of();
            List<Integer> clearedIdentifiers = List.of();
            for (Subscribed subscribed : matchedOn.values()) {
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
            List<Delivery> deliveries;
            if (publication.retain() && asPublished >= 0 && cleared >= 0) {
                deliveries = List.of(
                        new Delivery(
                                publication,
                                received,
                                Qos.of(Math.min(asPublished, published)),
                                true,
                                asPublishedIdentifiers,
                                sized
                        ),
                        new Delivery(
                                publication,
                                received,
                                Qos.of(Math.min(cleared, published)),
                                false,
                                clearedIdentifiers,
                                sized
                        )
                );
            } else if (asPublished >= 0 || cleared >= 0) {
                Qos qos = Qos.of(Math.min(Math.max(asPublished, cleared), published));
                var identifiers = new ArrayList<>(asPublishedIdentifiers);
                identifiers.addAll(clearedIdentifiers);
                boolean retain = publication.retain() && asPublished >= 0;
                deliveries = List.of(
                        new Delivery(publication, received, qos, retain, identifiers, sized)
                );
            } else {
                return;
            }
            Thread claimed = null;
            synchronized (this) {
                if (subscriptions != matchedOn || version != sized) {
                    continue;
                }
                if (ended) {
                    return;
                }
                boolean here = runner == null || runner == Thread.currentThread();
                boolean waits = false;
                for (Delivery delivery : deliveries) {
                    waits |= admit(delivery, received, here);
                }
                if (!waits) {
                    return;
                }
                if (!here) {
                    // The thread that holds the turn drains the session.
                    queueDrain();
                    return;
                }
                if (runner == null) {
                    queueDrain();
                    claimed = claimTurn(ALONE);
                }
            }
            if (claimed == null) {
                // This thread holds the turn already, as for the publisher's own session.
                drain();
            } else {
                run(ALONE);
            }
            return;
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

    // Queues `delivery` behind what waits, unless the session goes without it, and returns
    // whether it waits for a connection that can take it now. The queue limits apply to what
    // still waits once the drain that `drainsNext` promises has sent what it can, and otherwise
    // at once. Called under the inbox.
    private boolean admit(Delivery delivery, long received, boolean drainsNext) {
        Connection peer = connection;
        boolean online = peer != null && !peer.isEnded();
        // Only acknowledged deliveries wait for a session's next connection, and only while the
        // session lasts.
        if (!online && (!kept() || delivery.qos == Qos.AT_MOST_ONCE || expired(received))) {
            return false;
        }
        // A connected client that takes no packet this large loses this delivery before it
        // takes room in the queue.
        if (peer != null && online && tooLargeFor(peer, delivery)) {
            return false;
        }
        // A frame that can never fit must not block the head indefinitely, so it goes alone.
        if (neverFits(delivery)) {
            dropped("it is larger than the send window");
            return false;
        }
        pending.addLast(delivery);
        Room room = roomOf(delivery);
        room.add(delivery.byteCount);
        if (!online || !drainsNext) {
            trim(room);
        }
        return online;
    }

    // Applies the queue limits to the deliveries that `room` counts, which wait once what could
    // go out has gone. One larger than the whole queue would never fit, so it goes alone rather
    // than with the older ones; then the oldest go until the room holds its limits again. Called
    // under the inbox.
    private void trim(Room room) {
        if (!overflows(room)) {
            return;
        }
        Iterator<Pending> queued = pending.iterator();
        while (queued.hasNext()) {
            if (queued.next() instanceof Delivery waiting
                    && roomOf(waiting) == room
                    && waiting.byteCount > settings.maxQueuedBytes()) {
                queued.remove();
                room.remove(waiting.byteCount);
                dropped("it is larger than the delivery queue");
            }
        }
        dropOldest(room);
    }

    // Makes or replaces the subscription to its filter. Retain Handling 0 queues the retained
    // messages that it matches for every SUBSCRIBE, 1 only for a new subscription, and 2, the
    // only other value, never [MQTT-3.3.1-9] [MQTT-3.3.1-10] [MQTT-3.3.1-11]; MQTT 3.1.1 uses 0.
    // They wait for the next drain, which follows SUBACK. Under the topic lock, a retained
    // publication either is among them or is delivered to the subscription, and the deliveries
    // after the subscription queue behind them. Runs on the actor.
    void subscribe(Subscribed subscribed) {
        Packet.Subscription subscription = subscribed.subscription();
        synchronized (routing.retained) {
            Map<TopicFilter, Subscribed> current = subscriptions;
            boolean sendsRetained = switch (subscription.retainHandling()) {
                case 0 -> true;
                case 1 -> !current.containsKey(subscription.filter());
                default -> false;
            };
            List<RetainedMessages.Message> matched = sendsRetained
                    ? routing.retained.matching(subscription.filter())
                    : List.of();
            var changed = new LinkedHashMap<>(current);
            changed.put(subscription.filter(), subscribed);
            synchronized (this) {
                queueRetained(subscribed, matched);
                subscriptions = changed;
            }
        }
    }

    // Ends the subscription to `filter`, and returns whether there was one. The retained
    // messages that still wait for it go too: the broker completes only the deliveries that it
    // has started [MQTT-3.10.4-3], and they have not started. Runs on the actor.
    boolean unsubscribe(TopicFilter filter) {
        synchronized (this) {
            dropRetained(filter);
            Map<TopicFilter, Subscribed> current = subscriptions;
            if (!current.containsKey(filter)) {
                return false;
            }
            var changed = new LinkedHashMap<>(current);
            changed.remove(filter);
            subscriptions = changed;
            return true;
        }
    }

    // Queues `matched`, the retained messages that match `subscribed`, which the client is
    // making or replacing. Each one arrives with RETAIN 1, whatever Retain As Published says, at
    // the lower of its QoS and the granted one ([MQTT-3.3.1-8] in MQTT 3.1.1). No Local does not
    // withhold the retained messages of the subscriber's own client: brokers differ here,
    // and Mosquitto and HiveMQ CE send them while EMQX withholds them.
    //
    // They take the window like every other delivery, but wait as one cursor in the queue,
    // which no delivery queue limit drops, so the subscription receives all of them, however
    // many it matches. A cursor that still waits for the same filter stays: replacing the
    // subscription must lose none of its messages, while Retain Handling 0 sends the retained
    // ones again [MQTT-3.8.4-3]. Called under the topic lock and the inbox.
    private void queueRetained(Subscribed subscribed, List<RetainedMessages.Message> matched) {
        if (matched.isEmpty()) {
            return;
        }
        for (RetainedMessages.Message message : matched) {
            retainedRoom.add(message.byteCount());
        }
        Packet.Subscription subscription = subscribed.subscription();
        // They result from this subscription alone, and carry its identifier [MQTT-3.3.4-3].
        List<Integer> identifiers = subscribed.identifier() == 0
                ? List.of()
                : List.of(subscribed.identifier());
        pending.addLast(new RetainedCursor(subscription, identifiers, matched, version));
        dropOldestRetained();
    }

    // Drops the oldest retained messages that the cursors hold until their room holds its limits
    // again. The newest cursor holds part of what the broker retains and fits the room alone, so
    // only older cursors lose any, which takes SUBSCRIBEs faster than the client takes their
    // retained messages. Called under the inbox.
    private void dropOldestRetained() {
        for (Pending queued : pending) {
            if (queued instanceof RetainedCursor cursor) {
                while (retainedOverflows() && cursor.hasNext()) {
                    retainedRoom.remove(cursor.advance());
                    dropped("its retained messages wait beyond what the broker retains");
                }
            }
        }
    }

    // Drops the retained messages that still wait for `filter`. Called under the inbox.
    private void dropRetained(TopicFilter filter) {
        Iterator<Pending> queued = pending.iterator();
        while (queued.hasNext()) {
            if (queued.next() instanceof RetainedCursor cursor && cursor.filter.equals(filter)) {
                queued.remove();
                retainedRoom.remove(cursor.remainingCount(), cursor.remainingBytes());
            }
        }
    }

    private boolean retainedOverflows() {
        return retainedRoom.count > settings.maxRetained()
                || retainedRoom.bytes > settings.maxRetainedBytes();
    }

    // Drops the oldest queued deliveries of the kind that `room` counts until it holds its
    // limits again, so that a client that falls behind gets the newest ones. Deliveries in the
    // connection's send queue or in flight are on their way and stay. A deliberate
    // simplification: finding and removing them takes time in proportion to the queue's length,
    // which its limits keep short; a queue for each kind, merged by arrival as they are sent,
    // would make it constant if the limits grow. Called under the inbox.
    private void dropOldest(Room room) {
        Iterator<Pending> queued = pending.iterator();
        while (overflows(room) && queued.hasNext()) {
            if (queued.next() instanceof Delivery oldest && roomOf(oldest) == room) {
                queued.remove();
                room.remove(oldest.byteCount);
                dropped("its delivery queue is full");
            }
        }
    }

    // Whether `delivery` is larger than the send queue or, if acknowledged, the window ever takes.
    private boolean neverFits(Delivery delivery) {
        return delivery.byteCount > settings.sendBytes()
                || (delivery.qos != Qos.AT_MOST_ONCE
                        && delivery.byteCount > settings.maxInflightBytes());
    }

    private Room roomOf(Delivery delivery) {
        return (delivery.qos == Qos.AT_MOST_ONCE) ? atMostOnceRoom : acknowledgedRoom;
    }

    private boolean overflows(Room room) {
        return room.count > settings.maxQueued() || room.bytes > settings.maxQueuedBytes();
    }

    // Logs a delivery that the session goes without: the first at warning level, so that the
    // client that falls behind shows, and later ones at debug level. Called under the inbox.
    private void dropped(String cause) {
        if (!dropLogged) {
            dropLogged = true;
            log.warn("Broker on port {} dropped a delivery for client {}: {}; later ones are"
                    + " logged at debug level", routing.port, clientId, cause);
        } else {
            log.debug("Broker on port {} dropped a delivery for client {}: {}",
                    routing.port, clientId, cause);
        }
    }

    // Attaches `peer`, which a CONNECT accepted, and queues its CONNACK, so that publishers find
    // the connection only behind it. Returns false if the connection's end has come already,
    // and then did not reach the session.
    boolean attach(Connection peer, Packet.ConnAck connAck) {
        synchronized (this) {
            connection = peer;
            return peer.attach(this, connAck);
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

    // Sends what the window and the connection's send queue take now, and applies the queue
    // limits to what still waits. Runs on the actor.
    void drain() {
        sendWaiting();
        synchronized (this) {
            trim(atMostOnceRoom);
            trim(acknowledgedRoom);
        }
    }

    private void sendWaiting() {
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
        // Publishers queue for the session meanwhile only behind what this takes. A deliberate
        // simplification: they wait while this encodes, which moving the encoding to the writer
        // would end.
        synchronized (this) {
            Pending head;
            while ((head = pending.peekFirst()) != null) {
                Delivery delivery = switch (head) {
                    case Delivery queued -> queued;
                    case RetainedCursor cursor -> cursor.current();
                };
                // The cursor has sent all of its retained messages.
                if (delivery == null) {
                    pending.removeFirst();
                    continue;
                }
                // A retained QoS 0 message whose connection ended goes, like a queued QoS 0
                // delivery.
                if (head instanceof RetainedCursor cursor && cursor.leavesOut(delivery)) {
                    consume(head);
                    continue;
                }
                if (delivery.expired(now)) {
                    consume(head);
                    continue;
                }
                // A delivery queued for an earlier connection, or a retained one, can be larger
                // than this one takes. It goes as if sent and answered, without an identifier or
                // a place in the window.
                if (tooLargeFor(peer, delivery)) {
                    consume(head);
                    continue;
                }
                // A retained message can also be larger than the broker ever sends; a queued
                // delivery cannot, as it was dropped before it was queued.
                if (neverFits(delivery)) {
                    consume(head);
                    dropped("it is larger than the send window");
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
                consume(head);
                if (acknowledged) {
                    inflight.put(delivery.packetId, delivery);
                    inflightBytes += delivery.byteCount;
                    countOutstanding(delivery.packetId);
                }
            }
        }
    }

    // Moves the queue past the delivery at `head`: a queued delivery leaves the queue and its
    // room, and a cursor moves on to its next retained message, which leaves the retained room.
    // Called under the inbox.
    private void consume(Pending head) {
        switch (head) {
            case Delivery queued -> {
                pending.removeFirst();
                roomOf(queued).remove(queued.byteCount);
            }
            case RetainedCursor cursor -> retainedRoom.remove(cursor.advance());
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

    // Detaches `peer` if it is the session's connection. A session that does not outlive the
    // connection ends with it.
    void detach(Connection peer) {
        synchronized (this) {
            if (connection != peer) {
                return;
            }
            connection = null;
            if (kept()) {
                detachedAt = settings.scheduler().nanoTime();
                // QoS 0 belongs to the connection; only acknowledged deliveries survive it.
                Iterator<Pending> queued = pending.iterator();
                while (queued.hasNext()) {
                    switch (queued.next()) {
                        case Delivery delivery -> {
                            if (delivery.qos == Qos.AT_MOST_ONCE) {
                                queued.remove();
                            }
                        }
                        case RetainedCursor cursor -> cursor.keepAcknowledgedOnly();
                    }
                }
                atMostOnceRoom.clear();
            }
        }
        deferred.clear();
        outstanding.clear();
        outstandingCount = 0;
        if (!kept()) {
            leave();
            clear();
        }
    }

    // Takes the actor out of the session table if it has no session, as after a CONNECT that
    // made it and was refused.
    void leaveIfUnused() {
        if (connection == null && !kept()) {
            leave();
        }
    }

    // Marks the session as ended and takes it out of the session table, so that deliveries pass
    // it by and a CONNECT for its identifier makes a new actor.
    private void leave() {
        synchronized (this) {
            ended = true;
        }
        routing.sessions.remove(clientId, this);
    }

    // Drops the session state, the Will that waits with it included, which leaves the actor
    // without a session.
    void clear() {
        synchronized (this) {
            subscriptions = Map.of();
            pending.clear();
            atMostOnceRoom.clear();
            acknowledgedRoom.clear();
            retainedRoom.clear();
            connection = null;
        }
        expiry = 0;
        received.clear();
        inflight.clear();
        deferred.clear();
        outstanding.clear();
        outstandingCount = 0;
        inflightBytes = 0;
        cancelWill();
    }

    // Starts a new session of `newVersion` that outlives its connection for `newExpiry` seconds
    // in place of this one, whose Will the caller has taken.
    void reset(MqttVersion newVersion, long newExpiry) {
        clear();
        synchronized (this) {
            version = newVersion;
            dropLogged = false;
        }
        expiry = newExpiry;
        packetIds = new PacketIds();
    }

    // Ends the session as its expiry has passed, but keeps the actor for the CONNECT that found
    // it so: its place among the persistent sessions and its state go, and the Will that waits
    // with it is returned to go out now, because the session ends before the delay
    // [MQTT-3.1.2-8].
    Packet.@Nullable Will expire() {
        routing.freePersistentPlace();
        Packet.Will waiting = takeWill();
        clear();
        return waiting;
    }

    // Ends the session, which no connection continues, as its expiry passes, and publishes the
    // Will that waits with it. Discarding it again does nothing.
    private void discard() {
        if (ended) {
            return;
        }
        Packet.Will waiting = expire();
        leave();
        if (waiting != null) {
            routing.publishWill(waiting, this);
        }
    }

    // Ends the session as the broker closes. Its state goes, the Will that waits with it
    // included, which a closing broker does not publish.
    void close() {
        post(() -> {
            leave();
            clear();
        }, ALONE);
    }

    // A packet that `from` received, which the actor handles; `answered` says whether its reader
    // reserved room for the response.
    private final class Received implements Runnable {

        final Connection from;

        final Packet packet;

        final boolean answered;

        final int bytes;

        Received(Connection from, Packet packet, boolean answered, int bytes) {
            this.from = from;
            this.packet = packet;
            this.answered = answered;
            this.bytes = bytes;
        }

        @Override
        public void run() {
            handle(this);
        }
    }

    // A subscription with the Subscription Identifier that its SUBSCRIBE gave it, or 0, which no
    // identifier takes, for none. Making the subscription again replaces both (MQTT 5.0 section
    // 3.8.4).
    record Subscribed(Packet.Subscription subscription, int identifier) {
    }

    // The number and bytes of what waits of one kind: queued deliveries, or the retained messages
    // that cursors hold.
    private static final class Room {

        int count;

        long bytes;

        void add(long size) {
            count++;
            bytes += size;
        }

        void remove(long size) {
            remove(1, size);
        }

        void remove(int number, long size) {
            count -= number;
            bytes -= size;
        }

        void clear() {
            count = 0;
            bytes = 0;
        }
    }
}
