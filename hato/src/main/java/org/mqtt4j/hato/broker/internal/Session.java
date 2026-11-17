package org.mqtt4j.hato.broker.internal;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.internal.concurrent.Timer;
import org.mqtt4j.hato.internal.io.SendQueue;
import org.mqtt4j.hato.internal.log.Log;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.PacketDecodeException;
import org.mqtt4j.hato.internal.protocol.PacketIds;

import static java.util.Objects.requireNonNull;

// The session of one client identifier, with its actor: the thread that holds the session's turn
// runs the events posted to the session, one at a time and in the order posted, and only that
// thread touches the session's state and the clients of its connections. The monitor of this
// object is the session's inbox. It guards the events and the turn, what waits for the window,
// the attached connection, and the subscriptions, so that publishers can queue deliveries for
// the session without its turn.
//
// The selector and timers normally post work for the actor pool. A publisher can take a
// subscriber's idle turn to drain it in place. A thread holding a turn never waits for another,
// and the selector pauses reads when the inbox has no room. Lock order is
// topic lock -> inbox -> Connection.
//
// One actor serves a client identifier while the session table holds it: a CONNECT that starts
// a new session resets the state in place. The actor leaves the table only as its session ends,
// and a later CONNECT for the identifier makes a new one.
final class Session {

    private static final Log log = Log.of(Session.class);

    // How many events a thread that takes a free turn runs in place. A publisher that takes a
    // subscriber's turn runs only the drain that it came for, and the broker's selectors and
    // timers run none: they leave the turn to the pool.
    static final int ALONE = 1;

    static final int ELSEWHERE = 0;

    // How many events a pool thread runs before it lets the turns that wait in the pool go
    // first, so that a session with events without end does not keep the thread.
    // Ceiling: an event that routes a publication to many sessions counts as one, so the time
    // of a turn has no bound. A limit on the time or on the deliveries would give it one.
    private static final int TURN_EVENTS = 256;

    // The received packets that may wait in the inbox. A reader that finds no room waits until
    // the actor takes one, which holds its client back through TCP; one packet always fits,
    // whatever its size.
    //
    // A selector hands over the packets of a whole read, and each wait costs it a wake and the
    // actor a turn of its own. So the limit is as large as a connection's other queues, which a
    // read seldom exceeds.
    private static final int RECEIVED_COUNT = 128;

    // The limit that a QoS 0 publication finds instead. A client's other packets are bounded
    // elsewhere: the broker answers them within the room that it keeps for its responses, or
    // they acknowledge deliveries that its window let out. Only this limit and TCP hold back
    // QoS 0 publications, and a publisher that gets far ahead of the actor overruns the queues
    // of its subscribers, which drop what they cannot take.
    private static final int UNACKNOWLEDGED_COUNT = 16;

    private static final long RECEIVED_BYTES = 1024 * 1024;

    // A Session Expiry Interval of 0xFFFFFFFF, which keeps a session without end.
    static final long NEVER = 0xFFFF_FFFFL;

    // Why a delivery is dropped that the broker's memory for deliveries has no room for.
    private static final String MEMORY_USED_UP = "the broker's memory for deliveries is used up";

    private final Routing routing;

    private final BrokerSettings settings;

    final String clientId;

    // A session continues only with the protocol version that created it, so its deliveries
    // are sized and encoded for one version. A new session sets it as it replaces the
    // subscriptions, which publishers check under the inbox.
    volatile MqttVersion version;

    // How long the session outlives a connection, in seconds, which is never longer than the
    // broker keeps a session: 0 ends it with its connection, and NEVER keeps it. MQTT 3.1.1
    // Clean Session 0 leaves retention to the broker, and so gets the longest that the broker
    // gives. A connection that resumes the session sets it again from its own CONNECT, and
    // the client's DISCONNECT can change it (MQTT 5.0 section 3.14.2.2.2). An actor without a
    // session has 0.
    volatile long expiry;

    // The broker's time at which the session lost its connection, from which its expiry and the
    // delay of its Will count.
    private volatile long detachedAt;

    // The Will of the connection that ended last, which waits `willDelay` seconds before it goes
    // out, or less if the session ends first [MQTT-3.1.2-8]; none waits while a connection is
    // attached.
    private Packet.@Nullable Will will;

    private long willDelay;

    // The timer of the session while no connection has it, which comes when the delay of its
    // Will or its expiry passes, whichever is earlier. A session that lasts without end and has
    // no Will waiting has none.
    private Timer.@Nullable Cancellable timer;

    // Each filter's subscription with the options that the client requested, in the order the
    // client made them. A change replaces the map under the inbox, and the map is never changed
    // once set.
    private volatile Map<TopicFilter, Subscribed> subscriptions = Map.of();

    // The characters of the filters of all the subscriptions, which the subscription limits
    // bound with their number.
    private long subscriptionLength;

    // Whether the session has logged a refused subscription at warning level yet.
    private boolean refusalLogged;

    private PacketIds packetIds = new PacketIds();

    private final LinkedHashMap<Integer, Outgoing> inflight = new LinkedHashMap<>();

    // What the deliveries in the window hold on the heap, which the window's byte limit
    // bounds.
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

    // What waits for the window or for room in the connection's send queue: deliveries, and the
    // cursors of the retained messages that new subscriptions matched. It goes out in the order
    // it came, but each kind waits in a queue of its own: QoS 0 deliveries, acknowledged ones,
    // and the cursors. The session numbers what it queues, and what goes out next is the
    // earliest among the heads of the three queues. So the oldest delivery of a kind is at the
    // head of its queue, and dropping it takes no longer when more waits. The two kinds of
    // delivery have limits of their own, so that QoS 0 deliveries that a client falls behind on
    // cannot push out acknowledged ones.
    private final DeliveryQueue atMostOnceQueue;

    private final DeliveryQueue acknowledgedQueue;

    // Few sessions ever have a cursor, so the queue starts small. A cursor leaves it with its
    // last retained message, whichever way that one goes, so every cursor here has one left.
    //
    // Ceiling: the queue's array keeps the length that the most cursors at once gave it until
    // the session ends, and the memory for deliveries does not count it. The session's limits
    // on waiting retained messages bound that length, at about one element for each message
    // that may wait, which is a small part of what the subscription limits let a session hold
    // anyway. The queues of deliveries keep their longest length in the same way. Making a
    // queue again once it holds a small part of its length would lift this.
    private final ArrayDeque<RetainedCursor> cursors = new ArrayDeque<>(1);

    // How many of the cursors have room in the broker's memory, for themselves and for the
    // retained messages that they hold. They are the oldest: those that came later have none
    // until the session has sent what it could.
    private int cursorsWithRoom;

    // The number that the next delivery or cursor to wait gets.
    private long nextOrder;

    // How many of the session's deliveries have room in the broker's memory for deliveries:
    // those that have waited in its queues, those that were unanswered in its window when it
    // lost a connection, and the retained messages that have waited in its cursors, with those
    // cursors themselves. Each gives its room back as it leaves the session, so the number is
    // zero once the session is cleared; if not, one of them left without giving its room back.
    private int withRoom;

    // The retained messages that the cursors still hold. Their room is as large as the retained
    // limits, so that one subscription can take everything that the broker retains, while the
    // cursors of a client that subscribes again and again without taking them never hold more.
    private final Room retainedRoom;

    // What the cursors that have room in the broker's memory hold on the heap, themselves and
    // their retained messages with room: what they could give back at most.
    private long retainedWithRoom;

    // Whether the session has logged a dropped delivery at warning level yet.
    private boolean dropLogged;

    // The selector paused on a full packet inbox and needs a wake when it has room.
    private @Nullable Connection waitingReader;

    // At most one log statement per session waits for the pool, even if many deliveries are
    // dropped. It takes the deliveries dropped since the last one, by QoS level, to the log and
    // to the broker's counts.
    private @Nullable String dropCause;

    private final long[] droppedByLevel = new long[3];

    // Where the session counts the QoS 1 and 2 deliveries that the limits keep from it, for the
    // application to hear of: the count that it got for the first of them since its client's
    // last report. Null before any, and while the reports have no place for the client.
    private Notices.@Nullable Count unreported;

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

    // Who has the turn. It is free, held by the thread that runs the events, or promised to a
    // task that waits for a thread of the pool: the two fields below say which, and are never
    // both set.

    // The thread that holds the turn, or null while none does.
    private @Nullable Thread runner;

    // Whether a task that will run the events waits in the pool. No thread holds the turn then,
    // and none can take it.
    private boolean turnQueued;

    // Set once the session has ended and left the session table: deliveries pass it by, and a
    // CONNECT that comes to it goes to the table again.
    private boolean ended;

    Session(Routing routing, String clientId, MqttVersion version) {
        this.routing = routing;
        settings = routing.settings;
        this.clientId = clientId;
        this.version = version;
        atMostOnceQueue = new DeliveryQueue(
                settings.maxQueuedAtMostOnce(),
                settings.maxQueuedAtMostOnceBytes()
        );
        acknowledgedQueue = new DeliveryQueue(settings.maxQueued(), settings.maxQueuedBytes());
        retainedRoom = new Room(settings.maxRetained(), settings.maxRetainedBytes());
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

    // Whether no thread runs the session's events and none is to. What only the actor touches
    // is then as the last turn left it, which handed it over as it let go of the turn under the
    // inbox, and it stays so while the caller holds the inbox, which taking the turn needs.
    synchronized boolean idle() {
        return runner == null && !turnQueued && events.isEmpty();
    }

    // Whether the session is idle and has nothing for its client: no delivery or retained
    // message waits to go out, and none that went out awaits the client's answer. As the
    // session is idle, reading its window, which only the actor touches, is safe here.
    synchronized boolean settled() {
        return idle()
                && atMostOnceQueue.deliveries.isEmpty()
                && acknowledgedQueue.deliveries.isEmpty()
                && cursors.isEmpty()
                && inflight.isEmpty()
                && deferred.isEmpty();
    }

    // How many deliveries wait for the client: in the queues, and as the retained messages that
    // the cursors of new subscriptions still hold.
    synchronized int queued() {
        return atMostOnceQueue.deliveries.size()
                + acknowledgedQueue.deliveries.size()
                + retainedRoom.count;
    }

    // Posts `event`, which the actor runs after those posted before. If the turn is free, the
    // calling thread takes it and runs up to `inPlace` events here, and a pool thread runs the
    // rest, or all of them for ELSEWHERE. While the turn is taken, also by the calling thread,
    // the event is only queued.
    void post(Runnable event, int inPlace) {
        boolean claimed;
        synchronized (this) {
            events.addLast(event);
            claimed = claimTurn(inPlace);
        }
        if (claimed) {
            start(inPlace);
        }
    }

    // Posts a packet that `from` received, as post() does for a connection's reader, which
    // reserved room for its response if `answered`. The selector pauses reads
    // while received packets fill their limits.
    boolean receive(Connection from, Packet packet, boolean answered, int bytes) {
        var event = new Received(from, packet, answered, bytes);
        boolean unacknowledged = !answered
                && (packet instanceof Packet.Publish || packet instanceof Packet.AliasPublish);
        int limit = unacknowledged ? UNACKNOWLEDGED_COUNT : RECEIVED_COUNT;
        boolean claimed;
        synchronized (this) {
            if (receivedCount >= limit
                    || (receivedCount > 0 && bytes > RECEIVED_BYTES - receivedBytes)) {
                waitingReader = from;
                return false;
            }
            events.addLast(event);
            receivedCount++;
            receivedBytes += bytes;
            claimed = claimTurn(ELSEWHERE);
        }
        if (claimed) {
            start(ELSEWHERE);
        }
        return true;
    }

    // Asks the actor to send what waits, as post() does with a drain for the pool.
    void requestDrain() {
        boolean claimed;
        synchronized (this) {
            queueDrain();
            claimed = claimTurn(ELSEWHERE);
        }
        if (claimed) {
            start(ELSEWHERE);
        }
    }

    // Queues a drain behind the events posted so far, unless one waits already. Called under the
    // inbox.
    private void queueDrain() {
        if (!drainDue) {
            drainDue = true;
            events.addLast(drainEvent);
        }
    }

    // Whether the turn is free. Called under the inbox.
    private boolean turnFree() {
        return runner == null && !turnQueued;
    }

    // Takes the turn if it is free: for the calling thread when it runs events in place, and
    // else for a task of the pool. Returns whether it took the turn, which the caller then
    // starts outside the inbox. Called under the inbox.
    private boolean claimTurn(int inPlace) {
        if (!turnFree()) {
            return false;
        }
        if (inPlace > 0) {
            runner = Thread.currentThread();
        } else {
            turnQueued = true;
        }
        return true;
    }

    // Starts the turn that the calling thread has just claimed with the same `inPlace`: runs
    // it here, or submits the task that runs it to the pool.
    private void start(int inPlace) {
        if (inPlace > 0) {
            run(inPlace);
        } else {
            submitTurn();
        }
    }

    private void submitTurn() {
        try {
            routing.actors.execute(this::runQueued);
        } catch (RejectedExecutionException failure) {
            if (!routing.closed) {
                throw failure;
            }
            // A reserved turn can reach the executor after shutdown. Drain its queued
            // cleanup on the submitting thread.
            runQueued();
        }
    }

    // The task that waits in the pool for a queued turn: the thread that runs it holds the turn
    // from here on.
    private void runQueued() {
        synchronized (this) {
            turnQueued = false;
            runner = Thread.currentThread();
        }
        run(TURN_EVENTS);
    }

    // Runs the events while the calling thread holds the turn: at most `budget` of them, after
    // which it hands the rest over to the pool, and lets go of the turn once none is
    // left.
    private void run(int budget) {
        for (int ran = 0; ran < budget || routing.closed; ran++) {
            Runnable next;
            Connection reader = null;
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
                    if (waitingReader != null) {
                        reader = waitingReader;
                        waitingReader = null;
                    }
                }
            }
            if (reader != null) {
                // The reader fills the inbox again while this turn goes on.
                reader.wakeNow();
            }
            perform(next);
        }
        synchronized (this) {
            runner = null;
            if (events.isEmpty()) {
                return;
            }
            turnQueued = true;
        }
        submitTurn();
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
            from.failed("packet handling", failure);
        } finally {
            if (received.answered) {
                from.responseSettled();
            }
        }
    }

    // Keeps the Will of the connection that just ended until `delay` seconds have passed since
    // the session lost it, or until the session ends if that comes first [MQTT-3.1.2-8]. Called
    // after the detachment from which both count, before the timer is set.
    void awaitWill(Packet.Will lastWill, long delay) {
        will = lastWill;
        willDelay = delay;
    }

    // Whether a Will waits with the session and its delay has passed by `now`, so that a
    // connection that continues the session comes too late to cancel it [MQTT-3.1.3-9].
    boolean willDue(long now) {
        return will != null && now - detachedAt >= willDelay * 1_000_000_000L;
    }

    // Takes the Will that waits with the session, to publish it; null when none waits.
    Packet.@Nullable Will takeWill() {
        Packet.Will waiting = will;
        will = null;
        return waiting;
    }

    // Drops the Will that waits with the session, if any, as a connection that continues the
    // session within the delay does [MQTT-3.1.3-9].
    void cancelWill() {
        will = null;
    }

    // Sets the session's timer anew if no connection has the session: it comes when the delay of
    // the Will that waits or the session's expiry passes, whichever is earlier. Both count from
    // the detachment, so a timer set late still comes on time.
    //
    // The broker decides from the time whether the Will goes out and the session ends: when a
    // connection continues the session, when the session ends, and when the timer comes. The
    // timer only makes sure that someone looks once the time has come, so a timer that comes
    // late, or after its cancellation, changes nothing. Runs on the actor.
    void armTimer() {
        cancelTimer();
        if (connection != null || ended || !kept()) {
            return;
        }
        Packet.Will waiting = will;
        long seconds = expiry;
        if (waiting == null && seconds == NEVER) {
            return;
        }
        // Seconds from the detachment. A Session Expiry Interval of NEVER is at least any delay.
        long due = (waiting == null) ? seconds : Math.min(willDelay, seconds);
        try {
            long left = due * 1_000_000_000L - (settings.scheduler().nanoTime() - detachedAt);
            timer = settings.scheduler().schedule(
                    Duration.ofNanos(Math.max(left, 0)),
                    this::timeUp
            );
        } catch (RuntimeException failure) {
            // Publishing the Will early beats never publishing it. The session lasts until a
            // connection for it finds it expired.
            log.warn("Broker on port {} could not set the timer of client {}",
                    routing.port, clientId, failure);
            if (waiting != null) {
                will = null;
                routing.publishWill(waiting, this);
            }
        }
    }

    private void cancelTimer() {
        Timer.Cancellable set = timer;
        timer = null;
        if (set == null) {
            return;
        }
        try {
            set.cancel();
        } catch (Throwable failure) {
            // A failing injected timer must not keep the session from ending.
            log.warn("Cancelling the timer of client {} on port {} failed",
                    clientId, routing.port, failure);
        }
    }

    // The session's timer, which only posts, so that the shared timer thread, which the other
    // timers of the brokers share, waits for no session.
    private void timeUp() {
        try {
            post(this::checkExpiry, ELSEWHERE);
        } catch (Throwable failure) {
            log.warn("Broker on port {} failed to look at the session of client {}",
                    routing.port, clientId, failure);
        }
    }

    // Ends the session if its expiry has passed, which publishes its Will, and otherwise
    // publishes a Will whose delay has passed and sets the timer again for the expiry.
    private void checkExpiry() {
        long now = settings.scheduler().nanoTime();
        if (expired(now)) {
            discard();
        } else if (willDue(now)) {
            Packet.Will due = requireNonNull(takeWill());
            armTimer();
            routing.publishWill(due, this);
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
    void deliverMatching(
            Packet.Publish publication,
            PublicationMemory room,
            Session publisher,
            long received
    ) {
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
                                room,
                                received,
                                Qos.of(Math.min(asPublished, published)),
                                true,
                                asPublishedIdentifiers,
                                sized
                        ),
                        new Delivery(
                                publication,
                                room,
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
                        new Delivery(
                                publication,
                                room,
                                received,
                                qos,
                                retain,
                                identifiers,
                                sized
                        )
                );
            } else {
                return;
            }
            boolean claimed = false;
            synchronized (this) {
                if (subscriptions != matchedOn || version != sized) {
                    continue;
                }
                if (ended) {
                    return;
                }
                boolean here = turnFree() || runner == Thread.currentThread();
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
                if (turnFree()) {
                    queueDrain();
                    claimed = claimTurn(ALONE);
                }
            }
            if (claimed) {
                run(ALONE);
            } else {
                // This thread holds the turn already, as for the publisher's own session.
                drain();
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
    // whether it waits for a connection that can take it now. The queue's limits and the
    // memory's apply to what still waits once the drain that `drainsNext` promises has sent
    // what it can; without that promise they apply at once if the session has no connection
    // or the queue is full. Called under the inbox.
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
            dropped(delivery.qos, "it is larger than the send window");
            return false;
        }
        DeliveryQueue queue = queueOf(delivery);
        delivery.order = nextOrder++;
        queue.add(delivery);
        if (!online) {
            // Nothing sends for a session without a connection, so the delivery waits from
            // now on.
            trim(queue);
        } else if (!drainsNext && queue.overflows()) {
            // The thread that has the session's turn sends what it can soon, and what waits
            // then takes its room. A queue that is full cannot leave it at that: its limits
            // have something go now, and which one depends on whether the memory has room
            // for what came, so what came waits from now on.
            trim(queue);
        }
        return online;
    }

    // Takes room in the broker's memory for `delivery`: for itself, and for its publication
    // unless another delivery of it has that already. Returns false when the memory has none.
    // Called under the inbox.
    private boolean takeRoom(Delivery delivery) {
        MemoryBudget memory = routing.memory;
        if (!memory.take(delivery.ownBytes)) {
            return false;
        }
        if (!delivery.memory.hold(memory)) {
            memory.give(delivery.ownBytes);
            return false;
        }
        delivery.hasRoom = true;
        withRoom++;
        return true;
    }

    // Takes room in the broker's memory for `delivery` whatever the memory has left. Called
    // under the inbox.
    private void takeRoomAnyway(Delivery delivery) {
        MemoryBudget memory = routing.memory;
        memory.takeAnyway(delivery.ownBytes);
        delivery.memory.holdAnyway(memory);
        delivery.hasRoom = true;
        withRoom++;
    }

    // Gives the room of `delivery` in the broker's memory back. Called under the inbox.
    private void giveRoom(Delivery delivery) {
        MemoryBudget memory = routing.memory;
        delivery.hasRoom = false;
        withRoom--;
        delivery.memory.release(memory);
        memory.give(delivery.ownBytes);
    }

    // Drops `delivery`, which has just left its queue, for `cause`. Called under the inbox.
    private void drop(Delivery delivery, String cause) {
        if (delivery.hasRoom) {
            giveRoom(delivery);
        }
        dropped(delivery.qos, cause);
    }

    // Sees to the deliveries of `queue` that have come to wait since the last time: those
    // that are still there once what could go out has gone, which stand at its end in the
    // order they came and are the ones without room in the broker's memory. Each takes its
    // room there first. Without room, the session's oldest deliveries of the kind make room
    // for the new one; they stay if they could not make enough, and the session then goes
    // without the new one alone, as it does when none waits. Only then do the queue's own
    // limits apply to a delivery that has its room: the oldest go until the queue holds its
    // limits again, so that a client that falls behind gets the newest ones. In this order
    // the queue gives up nothing for a delivery that the memory then refuses. One larger
    // than the whole queue would never fit, so it goes alone rather than with the older ones.
    // Deliveries in the connection's send queue or in flight are on their way and stay.
    // Called under the inbox.
    private void trim(DeliveryQueue queue) {
        int arrived = queue.deliveries.size() - queue.withRoom;
        if (arrived == 0) {
            return;
        }
        // They leave the queue for the moment, so that what the queue holds is what has room,
        // oldest first, and come back in their order as they get theirs.
        var waiting = new Delivery[arrived];
        for (int index = arrived - 1; index >= 0; index--) {
            waiting[index] = queue.deliveries.removeLast();
            queue.left(waiting[index]);
        }
        for (Delivery delivery : waiting) {
            if (delivery.memoryBytes > queue.maxBytes) {
                dropped(delivery.qos, "it is larger than the delivery queue");
                continue;
            }
            if (!makeRoom(queue, delivery)) {
                continue;
            }
            queue.add(delivery);
            while (queue.overflows()) {
                drop(queue.removeOldest(), "its delivery queue is full");
            }
        }
    }

    // Takes room for `delivery`, which is to wait in `queue`, at the cost of the queue's
    // oldest deliveries if need be. Returns false if the session goes without `delivery`, and
    // then none of the others has gone for it.
    private boolean makeRoom(DeliveryQueue queue, Delivery delivery) {
        if (takeRoom(delivery)) {
            return true;
        }
        int oldest = oldestThatMakeRoom(queue, delivery);
        if (oldest < 0 || !handOver(queue, oldest, delivery)) {
            dropped(delivery.qos, MEMORY_USED_UP);
            return false;
        }
        return true;
    }

    // Has the `oldest` oldest deliveries of `queue` hand their room in the broker's memory
    // over to `delivery`, and takes them out of the queue: as many as the count before found
    // to make enough room. That count was of its moment. Other sessions take room and come to
    // hold publications meanwhile, so the room is made sure of before any delivery goes, and
    // none goes unless `delivery` stays. Returns false if the room is not to be had now, and
    // then nothing has changed.
    //
    // The new delivery joins the holders of its publication if there are any, and then needs
    // room for itself alone. Each of the oldest brings what it holds itself, and the room of
    // its publication if it lets go of that as the last holder: the session keeps this room
    // in hand instead of giving it back to the memory, where another session could take it
    // first. What is still missing comes from the memory in one piece. If the memory has not
    // got it, the publications get their holders back with the room in hand, which cannot
    // fail. Called under the inbox.
    private boolean handOver(DeliveryQueue queue, int oldest, Delivery delivery) {
        settings.beforeHandOver().accept(clientId);
        MemoryBudget memory = routing.memory;
        boolean joined = delivery.memory.holdIfHeld();
        long needed = delivery.ownBytes + (joined ? 0 : delivery.memory.bytes());
        // The publications that the oldest deliveries let go of as their last holders, each
        // at the place of its delivery among them.
        var released = new @Nullable PublicationMemory[oldest];
        long inHand = 0;
        Iterator<Delivery> oldestFirst = queue.deliveries.iterator();
        for (int index = 0; index < oldest; index++) {
            Delivery waiting = oldestFirst.next();
            inHand += waiting.ownBytes;
            if (waiting.memory.letGoAsLast()) {
                released[index] = waiting.memory;
                inHand += waiting.memory.bytes();
            }
        }
        long missing = needed - inHand;
        if (missing > 0 && !memory.take(missing)) {
            holdAgain(released);
            if (joined) {
                delivery.memory.release(memory);
            }
            return false;
        }
        for (int index = 0; index < oldest; index++) {
            Delivery gone = queue.removeOldest();
            gone.hasRoom = false;
            withRoom--;
            if (released[index] == null) {
                gone.memory.release(memory);
            }
            dropped(gone.qos, MEMORY_USED_UP);
        }
        if (!joined) {
            delivery.memory.holdWithRoom(memory);
        }
        if (missing < 0) {
            memory.give(-missing);
        }
        delivery.hasRoom = true;
        withRoom++;
        return true;
    }

    // Gives the publications in `released` their holders back, which let go of them as the
    // last ones and kept the room in hand for a hand-over that does not come about.
    private void holdAgain(@Nullable PublicationMemory[] released) {
        for (PublicationMemory publication : released) {
            if (publication != null) {
                publication.holdWithRoom(routing.memory);
            }
        }
    }

    // How many of the oldest deliveries of `queue`, all of which have room, must go for
    // `delivery` to find room in the broker's memory: none if it finds room as things are
    // now, and -1 if they do not make it, so that none of them goes in vain.
    //
    // A delivery gives back what it holds itself, and its publication's room only if no
    // other delivery holds that too: the room of a shared publication stays taken until the
    // last holder lets go. For the same reason `delivery` needs its publication's room only
    // if nothing holds it yet. A publication that two deliveries of this queue hold counts
    // for neither of them, which errs towards keeping what waits.
    //
    // No further one goes once those before it count for as much in the queue as the new one
    // does. Without that, a session would give up many deliveries that give little back, as
    // those of shared publications do, for a single new one: all that waits for it, if the
    // one that gives enough back happens to be its newest.
    //
    // Other sessions take and give room meanwhile, so the answer is of this moment, and nothing
    // goes on the strength of it alone: handOver() makes sure of the room first. Called under
    // the inbox.
    private int oldestThatMakeRoom(DeliveryQueue queue, Delivery delivery) {
        long needed = delivery.ownBytes + (delivery.memory.held() ? 0 : delivery.memory.bytes());
        long missing = routing.memory.shortOf(needed);
        // What the queue counts is the most that its deliveries could give back.
        if (missing > queue.bytes) {
            return -1;
        }
        long given = 0;
        int count = 0;
        for (Delivery waiting : queue.deliveries) {
            if (missing <= 0) {
                break;
            }
            if (given >= delivery.memoryBytes) {
                return -1;
            }
            missing -= waiting.ownBytes;
            if (waiting.memory.heldOnce()) {
                missing -= waiting.memory.bytes();
            }
            given += waiting.memoryBytes;
            count++;
        }
        return (missing <= 0) ? count : -1;
    }

    // Makes or replaces the subscription to its filter, and returns false if the subscription
    // limits leave no room for a new one, which then subscribes to nothing; replacing a
    // subscription takes no room. Retain Handling 0 queues the retained messages that it
    // matches for every SUBSCRIBE, 1 only for a new subscription, and 2, the only other value,
    // never [MQTT-3.3.1-9] [MQTT-3.3.1-10] [MQTT-3.3.1-11]; MQTT 3.1.1 uses 0. They wait for the
    // next drain, which follows SUBACK. Under the topic lock, a retained publication either is
    // among them or is delivered to the subscription, and the deliveries after the subscription
    // queue behind them. Runs on the actor.
    boolean subscribe(Subscribed subscribed) {
        Packet.Subscription subscription = subscribed.subscription();
        int length = subscription.filter().pattern().length();
        boolean added = !subscriptions.containsKey(subscription.filter());
        if (added && (subscriptions.size() >= settings.maxSubscriptions()
                || length > settings.maxSubscriptionLength() - subscriptionLength)) {
            logRefusal(subscription.filter());
            return false;
        }
        if (added) {
            subscriptionLength += length;
        }
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
        // Outside the topic lock, which the retained messages and subscriptions of every
        // client wait for.
        synchronized (this) {
            limitRetained();
        }
        return true;
    }

    // Logs that the subscription limits refused `filter`: the first time for the session as a
    // warning, because the client now misses what it asked for.
    private void logRefusal(TopicFilter filter) {
        if (!refusalLogged) {
            refusalLogged = true;
            log.warn("Broker on port {} refused the subscription of client {} to {}: its"
                    + " subscription limits are reached; later ones are logged at debug level",
                    routing.port, clientId, filter.pattern());
        } else {
            log.debug("Broker on port {} refused the subscription of client {} to {}: its"
                    + " subscription limits are reached", routing.port, clientId, filter.pattern());
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
            subscriptionLength -= filter.pattern().length();
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
    // ones again [MQTT-3.8.4-3]. The session's limits on what its cursors hold apply right
    // after, once the topic lock is free (see limitRetained). Called under the topic lock and
    // the inbox.
    private void queueRetained(Subscribed subscribed, List<RetainedMessages.Message> matched) {
        if (matched.isEmpty()) {
            return;
        }
        for (RetainedMessages.Message message : matched) {
            retainedRoom.add(message.memory().bytes());
        }
        Packet.Subscription subscription = subscribed.subscription();
        // They result from this subscription alone, and carry its identifier [MQTT-3.3.4-3].
        List<Integer> identifiers = subscribed.identifier() == 0
                ? List.of()
                : List.of(subscribed.identifier());
        cursors.addLast(
                new RetainedCursor(subscription, identifiers, matched, version, nextOrder++)
        );
    }

    // Sees to it that the session's cursors hold no more retained messages than may wait for
    // it, once a subscription has queued its own. If they do, some go now, and which ones
    // depends on whether the broker's memory has room for those that came. So the cursors
    // that came take their room first, as they do when they still wait after the session has
    // sent what it could (see takeRoomForRetained): a message that finds none goes alone. Only
    // then do the oldest go until the room holds its limits again. In this order the session
    // gives up no older message for one that the memory then refuses. Called under the inbox.
    private void limitRetained() {
        if (retainedRoom.overflows()) {
            takeRoomForRetained();
            dropOldestRetained();
        }
    }

    // Drops the oldest retained messages that the cursors hold until their room holds its limits
    // again. The newest cursor holds part of what the broker retains and fits the room alone, so
    // only older cursors lose any, which takes SUBSCRIBEs faster than the client takes their
    // retained messages. Called under the inbox.
    private void dropOldestRetained() {
        Iterator<RetainedCursor> oldest = cursors.iterator();
        while (retainedRoom.overflows() && oldest.hasNext()) {
            RetainedCursor cursor = oldest.next();
            while (retainedRoom.overflows() && cursor.hasNext()) {
                Qos qos = cursor.nextQos();
                pass(cursor);
                dropped(qos, "its retained messages wait beyond what the broker retains");
            }
            if (!cursor.hasNext()) {
                oldest.remove();
                left(cursor);
            }
        }
    }

    // Drops the retained messages that still wait for `filter`. Called under the inbox.
    private void dropRetained(TopicFilter filter) {
        Iterator<RetainedCursor> queued = cursors.iterator();
        while (queued.hasNext()) {
            RetainedCursor cursor = queued.next();
            if (cursor.filter.equals(filter)) {
                queued.remove();
                retainedRoom.remove(cursor.remainingCount(), cursor.remainingBytes());
                cursor.eachWithRoom(this::giveRoom);
                left(cursor);
            }
        }
    }

    // Takes account of `cursor` having left the session's cursors, whichever way: it gives
    // back the room that it had for itself in the broker's memory, if it had waited. Called
    // under the inbox.
    private void left(RetainedCursor cursor) {
        long room = letGoOfOwnRoom(cursor);
        if (room > 0) {
            routing.memory.give(room);
        }
    }

    // Has `cursor`, which leaves the session's cursors, let go of the room that it had for
    // itself in the broker's memory, and returns that room for the caller to give back or to
    // hand on: none if the cursor had not waited. Called under the inbox.
    private long letGoOfOwnRoom(RetainedCursor cursor) {
        if (!cursor.hasRoom) {
            return 0;
        }
        cursor.hasRoom = false;
        cursorsWithRoom--;
        withRoom--;
        retainedWithRoom -= cursor.ownBytes;
        return cursor.ownBytes;
    }

    // Moves `cursor` past its next retained message, which goes out or is dropped: the message
    // leaves the retained room, and gives back the room that it had in the broker's memory if
    // it had waited. Called under the inbox, only while the cursor has a message left.
    private void pass(RetainedCursor cursor) {
        boolean hadRoom = cursor.nextHasRoom();
        RetainedMessages.Message message = cursor.advance();
        retainedRoom.remove(message.memory().bytes());
        if (hadRoom) {
            giveRoom(message);
        }
    }

    // Gives the room of `message`, which a cursor held, in the broker's memory back. Called
    // under the inbox.
    private void giveRoom(RetainedMessages.Message message) {
        withRoom--;
        retainedWithRoom -= message.memory().bytes();
        message.memory().release(routing.memory);
    }

    // Takes room in the broker's memory for the cursors that have none yet, and for the
    // retained messages that they hold: the cursors that came since the last time and still
    // wait once the session has sent what it could, which stand at the end in the order they
    // came. A cursor holds its messages whatever becomes of them in the store, so they are
    // memory that waits for the session like its deliveries, and so is the cursor itself.
    // Without room, the session's oldest waiting retained messages make room for a later one,
    // as its deliveries do for one another; they stay if they could not make enough, and the
    // session then goes without that one alone. A cursor takes its own room with the first
    // of its messages that finds room, so nothing older goes for a cursor whose messages then
    // find none: such a cursor leaves without having taken anything, while its subscription
    // stays. Called under the inbox.
    private void takeRoomForRetained() {
        int arrived = cursors.size() - cursorsWithRoom;
        if (arrived == 0) {
            return;
        }
        // They leave the queue for the moment, and each comes back as the queue's last one
        // while it and its messages take their room, so that the queue holds what has room,
        // oldest first, and what is without room finds the oldest ones before it.
        var waiting = new RetainedCursor[arrived];
        for (int index = arrived - 1; index >= 0; index--) {
            waiting[index] = cursors.removeLast();
        }
        for (RetainedCursor cursor : waiting) {
            cursors.addLast(cursor);
            RetainedMessages.Message message;
            while ((message = cursor.withoutRoom()) != null) {
                if (makeRoom(cursor, message)) {
                    cursor.roomTaken();
                } else {
                    Qos qos = cursor.qosOf(message);
                    cursor.dropWithoutRoom();
                    retainedRoom.remove(message.memory().bytes());
                    dropped(qos, MEMORY_USED_UP);
                }
            }
            // A cursor that lost every message has nothing to wait for. It has no room of its
            // own to give back: that comes with a message that finds room, and such a message
            // stays, or makes room for a later one of the cursor that does.
            if (!cursor.hasNext()) {
                cursors.removeLast();
            }
        }
    }

    // Takes room in the broker's memory for `message`, which waits in `cursor`, and for what
    // the cursor holds itself if it has none yet: both or neither. Returns false when the
    // memory has none. Called under the inbox.
    private boolean takeRoom(RetainedCursor cursor, RetainedMessages.Message message) {
        MemoryBudget memory = routing.memory;
        long own = cursor.hasRoom ? 0 : cursor.ownBytes;
        if (own > 0 && !memory.take(own)) {
            return false;
        }
        if (!message.memory().hold(memory)) {
            if (own > 0) {
                memory.give(own);
            }
            return false;
        }
        roomTaken(cursor, message);
        return true;
    }

    // Notes that `message` has its room in the broker's memory now, and with it `cursor`, in
    // which it waits, if that had none yet. Called under the inbox.
    private void roomTaken(RetainedCursor cursor, RetainedMessages.Message message) {
        if (!cursor.hasRoom) {
            cursor.hasRoom = true;
            cursorsWithRoom++;
            withRoom++;
            retainedWithRoom += cursor.ownBytes;
        }
        withRoom++;
        retainedWithRoom += message.memory().bytes();
    }

    // Takes room for `message`, which waits in `cursor`, the queue's last one, and with it
    // for the cursor itself if that has none yet, at the cost of the oldest retained messages
    // that wait with room if need be. The two count as one, so that no older message goes for
    // a cursor whose message then finds no room. Returns false if the session goes without
    // `message`, and then nothing older has gone for it.
    private boolean makeRoom(RetainedCursor cursor, RetainedMessages.Message message) {
        if (takeRoom(cursor, message)) {
            return true;
        }
        PublicationMemory memory = message.memory();
        long own = cursor.hasRoom ? 0 : cursor.ownBytes;
        int oldest = oldestRetainedThatMakeRoom(
                memory,
                own + (memory.held() ? 0 : memory.bytes()),
                own + memory.bytes()
        );
        return oldest >= 0 && handOver(oldest, cursor, message);
    }

    // Has the `oldest` oldest retained messages that wait with room hand their room in the
    // broker's memory over to `message`, which waits in `cursor`, and to the cursor itself if
    // it has none yet, and drops them. It makes sure of the room before any of them goes, as
    // handOver() does for deliveries, so that none goes unless `message` stays. A cursor that
    // loses its last message this way brings the room that it had for itself as well. Returns
    // false if the room is not to be had now, and then nothing has changed. Called under the
    // inbox.
    private boolean handOver(int oldest, RetainedCursor cursor, RetainedMessages.Message message) {
        settings.beforeHandOver().accept(clientId);
        MemoryBudget memory = routing.memory;
        PublicationMemory publication = message.memory();
        boolean joined = publication.holdIfHeld();
        long needed = (cursor.hasRoom ? 0 : cursor.ownBytes) + (joined ? 0 : publication.bytes());
        // The publications that the oldest messages' cursors let go of as their last holders,
        // each at the place of its message among them.
        var released = new @Nullable PublicationMemory[oldest];
        long inHand = 0;
        int index = 0;
        for (RetainedCursor waiting : cursors) {
            if (index == oldest) {
                break;
            }
            int left = waiting.remainingCount();
            int position = waiting.firstWithRoom();
            while (position >= 0 && index < oldest) {
                PublicationMemory held = waiting.at(position).memory();
                if (held.letGoAsLast()) {
                    released[index] = held;
                    inHand += held.bytes();
                }
                index++;
                left--;
                // The cursor whose message is taking its room keeps that message, so only
                // cursors that have waited lose their last one here.
                if (left == 0) {
                    inHand += waiting.ownBytes;
                }
                position = waiting.nextWithRoom(position);
            }
        }
        long missing = needed - inHand;
        if (missing > 0 && !memory.take(missing)) {
            holdAgain(released);
            if (joined) {
                publication.release(memory);
            }
            return false;
        }
        for (index = 0; index < oldest; index++) {
            // The oldest message that waits with room is the next one of the first cursor in
            // the queue, because the cursors take their room in the order in which their
            // messages go out.
            RetainedCursor first = cursors.getFirst();
            Qos qos = first.nextQos();
            RetainedMessages.Message gone = first.advance();
            retainedRoom.remove(gone.memory().bytes());
            withRoom--;
            retainedWithRoom -= gone.memory().bytes();
            if (released[index] == null) {
                gone.memory().release(memory);
            }
            dropped(qos, MEMORY_USED_UP);
            if (!first.hasNext()) {
                cursors.removeFirst();
                // The room that the cursor had for itself is in hand already.
                long _ = letGoOfOwnRoom(first);
            }
        }
        if (!joined) {
            publication.holdWithRoom(memory);
        }
        if (missing < 0) {
            memory.give(-missing);
        }
        roomTaken(cursor, message);
        return true;
    }

    // How many of the oldest retained messages that wait with room must go for `needed` bytes
    // to find room in the broker's memory, for a new message that counts for `counted` bytes
    // together with its cursor if that has no room yet: none if they find room as things are
    // now, and -1 if the oldest ones do not make it, so that none of them goes in vain.
    //
    // A message gives its room back only if nothing else holds it: the room of one that
    // another cursor or a delivery holds as well, of this session or of another, stays taken
    // until the last holder lets go. So a message that several of these cursors hold counts
    // for none of them, which errs towards keeping what waits. Nor does the new message
    // itself give anything back where it waits for an earlier subscription too: `arriving` is
    // its room, which the new holder keeps. A cursor gives back its own room with its last
    // message. And as with deliveries, no further message goes once those before it count for
    // as much as the new one does.
    //
    // The answer is of this moment, and nothing goes on the strength of it alone: handOver()
    // makes sure of the room first. Called under the inbox.
    private int oldestRetainedThatMakeRoom(PublicationMemory arriving, long needed, long counted) {
        long missing = routing.memory.shortOf(needed);
        // What the cursors count is the most that they and their messages could give back.
        if (missing > retainedWithRoom) {
            return -1;
        }
        long given = 0;
        int count = 0;
        for (RetainedCursor cursor : cursors) {
            int left = cursor.remainingCount();
            int position = cursor.firstWithRoom();
            while (position >= 0) {
                if (missing <= 0) {
                    return count;
                }
                if (given >= counted) {
                    return -1;
                }
                PublicationMemory memory = cursor.at(position).memory();
                if (memory != arriving && memory.heldOnce()) {
                    missing -= memory.bytes();
                }
                given += memory.bytes();
                count++;
                left--;
                // The cursor whose message is taking its room keeps that message, so only
                // cursors that have waited lose their last one here.
                if (left == 0) {
                    missing -= cursor.ownBytes;
                    given += cursor.ownBytes;
                }
                position = cursor.nextWithRoom(position);
            }
        }
        return (missing <= 0) ? count : -1;
    }

    // Whether `delivery` is larger than the send queue or, if acknowledged, the window ever
    // takes: as a packet for the send queue, and by what it holds on the heap for both.
    private boolean neverFits(Delivery delivery) {
        return delivery.byteCount > settings.sendBytes()
                || delivery.memoryBytes > settings.sendBytes()
                || (delivery.qos != Qos.AT_MOST_ONCE
                        && delivery.memoryBytes > settings.maxInflightBytes());
    }

    private DeliveryQueue queueOf(Delivery delivery) {
        return (delivery.qos == Qos.AT_MOST_ONCE) ? atMostOnceQueue : acknowledgedQueue;
    }

    // What goes out next: the delivery or cursor that came first among those that wait, or null
    // if nothing waits. Called under the inbox.
    private @Nullable Pending head() {
        Delivery delivery = atMostOnceQueue.deliveries.peekFirst();
        Delivery other = acknowledgedQueue.deliveries.peekFirst();
        if (delivery == null || (other != null && other.order < delivery.order)) {
            delivery = other;
        }
        RetainedCursor cursor = cursors.peekFirst();
        if (cursor == null || (delivery != null && delivery.order < cursor.order)) {
            return delivery;
        }
        return cursor;
    }

    // Records under the inbox that the broker's limits kept a delivery at `qos` from the
    // session. The log statement runs without the inbox or topic lock, and adds what it logs to
    // the broker's counts. A QoS 1 or 2 delivery is also counted for the application's listener,
    // which hears of the client's count on a thread of its own. The session adds to the count
    // that it has; once the report has taken that one, or if it has none, it gets the next from
    // the reports.
    private void dropped(Qos qos, String cause) {
        Notices notices = routing.notices;
        if (qos != Qos.AT_MOST_ONCE && notices != null) {
            Notices.Count count = unreported;
            if (count == null || !count.add()) {
                unreported = notices.add(clientId);
            }
        }
        droppedByLevel[qos.level()]++;
        if (dropCause == null && !routing.closed) {
            dropCause = cause;
            try {
                routing.actors.execute(this::logDrops);
            } catch (RejectedExecutionException failure) {
                if (!routing.closed) {
                    throw failure;
                }
            }
        }
    }

    private void logDrops() {
        String cause;
        long count = 0;
        boolean first;
        synchronized (this) {
            cause = requireNonNull(dropCause);
            dropCause = null;
            for (int level = 0; level < droppedByLevel.length; level++) {
                count += droppedByLevel[level];
                routing.countDropped(level, droppedByLevel[level]);
                droppedByLevel[level] = 0;
            }
            first = !dropLogged;
            dropLogged = true;
        }
        if (first) {
            log.warn("Broker on port {} dropped a delivery for client {}: {}; later ones are"
                    + " logged at debug level", routing.port, clientId, cause);
        }
        if (!first || count > 1) {
            log.debug("Broker on port {} dropped {} deliveries for client {}; first cause: {}",
                    routing.port, first ? count - 1 : count, clientId, cause);
        }
    }

    // Holds the session that a client resumes to `scope`, the scope of the connection that
    // resumes it, which may allow less than those of the connections that made the session.
    // The subscriptions outside the scope end, with the retained messages that wait for them,
    // in one change of the map, which has publishers that matched the old one match again. The
    // deliveries that wait, and those in the window that await their answer, go unless the
    // scope lets the client receive their topics; otherwise a message that waits for a
    // subscription that ends stays, as after an UNSUBSCRIBE. One in the window is withdrawn,
    // as withdraw() describes. A PUBREL that awaits its PUBCOMP stays, as the client has that
    // message already, and QoS 0 deliveries ended with the connection that they waited for. A
    // scope without limits, which every client of a broker without an authenticator has,
    // leaves the session as it is. Runs on the actor before the connection attaches, once the
    // connection that had the session can send no more, so that whether a delivery went out
    // stays as read.
    void limitTo(Authenticator.Scope scope) {
        if (scope == Authenticator.Scope.all()) {
            return;
        }
        int endedSubscriptions = 0;
        int withheld = 0;
        synchronized (this) {
            var kept = new LinkedHashMap<TopicFilter, Subscribed>();
            for (Map.Entry<TopicFilter, Subscribed> entry : subscriptions.entrySet()) {
                TopicFilter filter = entry.getKey();
                if (scope.permitsSubscribe(filter)) {
                    kept.put(filter, entry.getValue());
                } else {
                    dropRetained(filter);
                    subscriptionLength -= filter.pattern().length();
                    endedSubscriptions++;
                }
            }
            if (endedSubscriptions > 0) {
                subscriptions = kept;
            }
            Iterator<Delivery> waiting = acknowledgedQueue.deliveries.iterator();
            while (waiting.hasNext()) {
                Delivery delivery = waiting.next();
                if (!receives(scope, delivery)) {
                    waiting.remove();
                    acknowledgedQueue.left(delivery);
                    if (delivery.hasRoom) {
                        giveRoom(delivery);
                    }
                    withheld++;
                }
            }
        }
        for (Map.Entry<Integer, Outgoing> entry : new LinkedHashMap<>(inflight).entrySet()) {
            if (entry.getValue() instanceof Delivery delivery && !receives(scope, delivery)) {
                withdraw(entry.getKey(), delivery);
                withheld++;
            }
        }
        if (endedSubscriptions > 0 || withheld > 0) {
            log.info("Broker on port {} ended {} subscriptions and dropped {} deliveries of"
                    + " client {} outside the scope of its new connection",
                    routing.port, endedSubscriptions, withheld, clientId);
        }
    }

    // Whether `scope` lets the client receive `delivery`: whether it would let the client
    // subscribe to the name of the delivery's topic, a filter that matches that topic alone.
    private static boolean receives(Authenticator.Scope scope, Delivery delivery) {
        return scope.permitsSubscribe(new TopicFilter(delivery.topic().name()));
    }

    // Takes `delivery`, which awaits its answer under the identifier `id`, out of the window
    // without sending it again. A QoS 2 delivery that went out goes on to its PUBREL, as after
    // a PUBREC, which the session sends in its place as it resumes: the client may hold the
    // identifier, and would take the next PUBLISH with it for a duplicate. Any other ends as
    // if answered. Runs on the actor, once the connections that had the delivery can send no
    // more.
    private void withdraw(int id, Delivery delivery) {
        if (delivery.qos == Qos.EXACTLY_ONCE && delivery.sent) {
            inflightBytes -= delivery.memoryBytes;
            leaveWindow(delivery);
            inflight.replace(id, new Release(id));
        } else {
            end(id, delivery);
        }
    }

    // Attaches `peer`, which a CONNECT accepted, and queues its CONNACK, so that publishers find
    // the connection only behind it. The session's timer stops, because a session with a
    // connection neither expires nor has a Will waiting. Returns false if the connection's end
    // has come already, and then did not reach the session.
    boolean attach(Connection peer, Packet.ConnAck connAck) {
        cancelTimer();
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
        // takes; it is withdrawn, and what remains goes out again in its order.
        for (Map.Entry<Integer, Outgoing> entry : new LinkedHashMap<>(inflight).entrySet()) {
            int id = entry.getKey();
            if (entry.getValue() instanceof Delivery replay && tooLargeFor(peer, replay)) {
                withdraw(id, replay);
            }
            Outgoing remaining = inflight.get(id);
            if (remaining != null) {
                deferred.put(id, remaining);
            }
        }
        drain();
    }

    // Sends what the window and the connection's send queue take now, and applies the queue
    // limits to what still waits. Runs on the actor.
    void drain() {
        sendWaiting();
        synchronized (this) {
            trim(atMostOnceQueue);
            trim(acknowledgedQueue);
            takeRoomForRetained();
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
        // Publishers queue for the session meanwhile only behind what this takes, which hands
        // the deliveries over as they are: the connection's writer prepares their packets.
        synchronized (this) {
            Pending head;
            while ((head = head()) != null) {
                Delivery delivery = switch (head) {
                    case Delivery queued -> queued;
                    case RetainedCursor cursor -> cursor.current();
                };
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
                    dropped(delivery.qos, "it is larger than the send window");
                    continue;
                }
                boolean acknowledged = delivery.qos != Qos.AT_MOST_ONCE;
                if (acknowledged && (inflight.size() == settings.maxInflight()
                        || delivery.memoryBytes > settings.maxInflightBytes() - inflightBytes
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
                if (!acknowledged) {
                    consume(head);
                    continue;
                }
                // A delivery that had waited keeps its room in the broker's memory as it moves
                // from the queue into the window. A retained message gives the room that it
                // had in its cursor back: its delivery is new to the window.
                switch (head) {
                    case Delivery queued -> queueOf(queued).removeOldest();
                    case RetainedCursor cursor -> passFirst(cursor);
                }
                inflight.put(delivery.packetId, delivery);
                inflightBytes += delivery.memoryBytes;
                countOutstanding(delivery.packetId);
            }
        }
    }

    // Moves past the delivery at `head`, which leaves the session here: it went to the
    // connection's send queue as a QoS 0 delivery, or goes without being sent. A queued
    // delivery, the oldest of its kind, leaves its queue and gives back the room that it had
    // in the broker's memory if it had waited, and a cursor moves on to its next retained
    // message, which leaves the retained room. Called under the inbox.
    private void consume(Pending head) {
        switch (head) {
            case Delivery queued -> {
                queueOf(queued).removeOldest();
                if (queued.hasRoom) {
                    giveRoom(queued);
                }
            }
            case RetainedCursor cursor -> passFirst(cursor);
        }
    }

    // Moves the first cursor in the queue past its next retained message, as pass() does, and
    // takes the cursor out of the queue with its last one. Called under the inbox.
    private void passFirst(RetainedCursor cursor) {
        pass(cursor);
        if (!cursor.hasNext()) {
            cursors.removeFirst();
            left(cursor);
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
                    inflightBytes -= delivery.memoryBytes;
                    leaveWindow(delivery);
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
            inflightBytes -= delivery.memoryBytes;
            leaveWindow(delivery);
        }
        if (outstanding.get(id)) {
            outstanding.clear(id);
            outstandingCount--;
        }
    }

    // Gives back the room that `delivery` has in the broker's memory, if any, as the window
    // lets go of it: its exchange ended, or went on to the PUBREL, which needs the delivery no
    // more. Runs on the actor, which alone sees the deliveries in the window.
    private void leaveWindow(Delivery delivery) {
        if (delivery.hasRoom) {
            synchronized (this) {
                giveRoom(delivery);
            }
        }
    }

    // Empties `queue`, whose deliveries give back the room that they have in the broker's
    // memory. Called under the inbox.
    private void empty(DeliveryQueue queue) {
        for (Delivery delivery : queue.deliveries) {
            if (delivery.hasRoom) {
                giveRoom(delivery);
            }
        }
        queue.clear();
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
                empty(atMostOnceQueue);
                for (RetainedCursor cursor : cursors) {
                    cursor.keepAcknowledgedOnly();
                }
                // From here on the session waits for a connection with everything that it
                // holds, so all of it takes room in the broker's memory: the unanswered
                // deliveries too, which the session cannot go without, whatever room is left.
                for (Outgoing outgoing : inflight.values()) {
                    if (outgoing instanceof Delivery delivery && !delivery.hasRoom) {
                        takeRoomAnyway(delivery);
                    }
                }
                trim(acknowledgedQueue);
                takeRoomForRetained();
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

    // Has the session, which a takeover that failed on the way left without a connection, wait
    // as if its connection had just ended: a kept session expires its expiry from now, with
    // its timer set anew, and one that is not kept ends. A session that still has the
    // connection that the takeover meant to end waits for that one's end instead.
    void abandoned() {
        if (connection != null) {
            return;
        }
        if (kept()) {
            detachedAt = settings.scheduler().nanoTime();
            armTimer();
        } else {
            leave();
            clear();
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

    // Drops the session state, the Will that waits with it and the timer included, which leaves
    // the actor without a session.
    void clear() {
        int unaccounted;
        synchronized (this) {
            subscriptions = Map.of();
            empty(atMostOnceQueue);
            empty(acknowledgedQueue);
            for (RetainedCursor cursor : cursors) {
                cursor.eachWithRoom(this::giveRoom);
                left(cursor);
            }
            cursors.clear();
            retainedRoom.clear();
            connection = null;
            for (Outgoing outgoing : inflight.values()) {
                if (outgoing instanceof Delivery delivery && delivery.hasRoom) {
                    giveRoom(delivery);
                }
            }
            // Every delivery that the session held has given its room back by now. A number
            // that is left tells of deliveries that left the session without doing so.
            unaccounted = withRoom;
            withRoom = 0;
        }
        if (unaccounted != 0) {
            routing.memory.lost(unaccounted);
        }
        subscriptionLength = 0;
        expiry = 0;
        received.clear();
        inflight.clear();
        deferred.clear();
        outstanding.clear();
        outstandingCount = 0;
        inflightBytes = 0;
        cancelWill();
        cancelTimer();
    }

    // Starts a new session of `newVersion` that outlives its connection for `newExpiry` seconds
    // in place of this one, whose Will the caller has taken.
    void reset(MqttVersion newVersion, long newExpiry) {
        clear();
        synchronized (this) {
            version = newVersion;
            dropLogged = false;
        }
        refusalLogged = false;
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
        routing.sessionClearing();
        post(() -> {
            try {
                leave();
                clear();
            } finally {
                routing.sessionCleared();
            }
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

    // The deliveries of one kind that wait, oldest first, with what they hold on the heap in
    // bytes and the limits on their number and on those bytes.
    private static final class DeliveryQueue {

        final ArrayDeque<Delivery> deliveries = new ArrayDeque<>();

        final int maxCount;

        final long maxBytes;

        long bytes;

        // How many of the deliveries have room in the broker's memory for deliveries. They
        // are the oldest: those that came later have none until the limits are applied again.
        int withRoom;

        DeliveryQueue(int maxCount, long maxBytes) {
            this.maxCount = maxCount;
            this.maxBytes = maxBytes;
        }

        // Whether more waits than the limits allow.
        boolean overflows() {
            return deliveries.size() > maxCount || bytes > maxBytes;
        }

        void add(Delivery delivery) {
            deliveries.addLast(delivery);
            bytes += delivery.memoryBytes;
            if (delivery.hasRoom) {
                withRoom++;
            }
        }

        // Takes the oldest delivery out, and returns it.
        Delivery removeOldest() {
            Delivery oldest = deliveries.removeFirst();
            left(oldest);
            return oldest;
        }

        // Takes account of `delivery` having left the deliveries, before it gives any room in
        // the broker's memory back.
        void left(Delivery delivery) {
            bytes -= delivery.memoryBytes;
            if (delivery.hasRoom) {
                withRoom--;
            }
        }

        void clear() {
            deliveries.clear();
            bytes = 0;
            withRoom = 0;
        }
    }

    // The number and bytes of the retained messages that cursors hold, and the limits on them.
    private static final class Room {

        final int maxCount;

        final long maxBytes;

        int count;

        long bytes;

        Room(int maxCount, long maxBytes) {
            this.maxCount = maxCount;
            this.maxBytes = maxBytes;
        }

        // Whether more waits than the limits allow.
        boolean overflows() {
            return count > maxCount || bytes > maxBytes;
        }

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
