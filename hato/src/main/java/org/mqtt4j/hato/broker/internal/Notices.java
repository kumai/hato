package org.mqtt4j.hato.broker.internal;

import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongFunction;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.BrokerListener;
import org.mqtt4j.hato.broker.BrokerListener.DisconnectReason;
import org.mqtt4j.hato.internal.log.Log;

// Tells the application's listener what befalls the broker's clients, a failure that stops the
// broker aside (see FailureNotice): that a client has connected, that its connection has ended,
// and how many QoS 1 and 2 deliveries the broker's limits have kept from it. The listener is the
// application's code and may take its time, so it runs on a thread of its own, which no
// selector, pool thread, or publisher waits for. The thread tells of these in the order in which
// they came to wait, a client's dropped deliveries in the place of the first of them.
//
// The notices of connections wait in a queue of their own. Its limit bounds what a listener that
// is behind holds: a notice beyond it drops the oldest one that waits, which is counted and
// logged once. The listener then still hears what happened last, and nothing waits for it.
//
// What waits for the listener of dropped deliveries is a count for each client, by its
// identifier, and the listener hears of all the deliveries that a client has lost since its last
// report in one call. The count belongs to the identifier, not to a session: a client that comes
// back with a new session again and again still waits once, and nothing here keeps a session
// that has ended. The identifiers that wait are bounded as well. The deliveries of a client that
// finds their limit reached go into one count without a client, which the listener hears of
// apart. So a slow listener holds nothing up and keeps a bounded amount: it only learns of later
// drops later, together, and of those beyond the limit without their clients.
//
// A session comes here for the first delivery that it loses after a report, and adds the ones
// that follow to the count that it got then, without the lock, until the next report takes that
// count.
//
// The thread tells in rounds. Once something waits, it waits a moment more before it takes what
// waits, and then tells of what waits by then. So the deliveries of a burst reach the listener
// in one call, and a session that goes on losing deliveries comes here once in a round: without
// the wait it came after nearly every report, each time for the lock and to wake the thread. The
// listener hears of everything that moment later.
final class Notices {

    private static final Log log = Log.of(Notices.class);

    // How long the thread waits, once something waits to be told, before it tells of it. A
    // session that goes on losing deliveries comes for the lock, and wakes the thread, about once
    // in this time, and the listener hears of everything this much later.
    private static final long GATHERING_NANOS = 1_000_000;

    private final BrokerListener listener;

    // The port that the broker listens on, for the thread's name and logs.
    private final int port;

    // How many clients wait for a report of their own dropped deliveries at most.
    private final int limit;

    // How many notices of connections wait at most.
    private final int noticeLimit;

    // What the monitor of this object guards, from here on.

    // The counts of dropped deliveries to report, by client, in the order in which the clients
    // came to wait.
    private final LinkedHashMap<String, Count> waiting = new LinkedHashMap<>();

    // The dropped deliveries to report without a client: those of the clients that found the
    // limit reached.
    private long unattributed;

    // Whether a client has found the limit reached yet, which is logged the first time.
    private boolean limitReached;

    // The notices of connections that wait, oldest first.
    private final ArrayDeque<Notice> notices = new ArrayDeque<>();

    // How many notices of connections the limit has dropped, and whether that has been logged.
    // Written under this, and read without it (see droppedNotices).
    private volatile long droppedNotices;

    private boolean noticeLimitReached;

    // The place in the order of the next count or notice to wait.
    private long nextOrder;

    private final Thread thread;

    // Where the thread reports its end.
    private final Termination termination;

    private volatile boolean closed;

    Notices(
            BrokerListener listener,
            int port,
            int limit,
            int noticeLimit,
            Termination termination
    ) {
        this.listener = listener;
        this.port = port;
        this.limit = limit;
        this.noticeLimit = noticeLimit;
        this.termination = termination;
        thread = Thread.ofPlatform().daemon()
                .name("hato-broker-" + port + "-notices")
                .unstarted(this::run);
    }

    void start() {
        thread.start();
    }

    // Counts `deliveries`, at least one, that the limits kept from the client `clientId`, for the
    // listener to hear of, and returns the count that the client's sessions add its later ones
    // to, or null if the client found no place and each of its deliveries has to come here. The
    // caller holds the inbox of a session, so this never waits for the listener, and it logs
    // once at most in the life of the broker.
    @Nullable Count add(String clientId, long deliveries) {
        Count count;
        // Whether nothing waited to be told until now.
        boolean first;
        // Whether this client is the first of all to find the limit reached.
        boolean firstBeyond = false;
        synchronized (this) {
            count = waiting.get(clientId);
            if (count != null) {
                // The count still waits, so it takes the deliveries.
                boolean _ = count.add(deliveries);
                return count;
            }
            first = nothingWaits();
            if (waiting.size() < limit) {
                count = new Count(nextOrder++, deliveries);
                waiting.put(clientId, count);
            } else {
                long before = unattributed;
                unattributed += deliveries;
                if (before != 0) {
                    return null;
                }
                firstBeyond = !limitReached;
                limitReached = true;
            }
        }
        if (first) {
            wake();
        }
        if (firstBeyond) {
            log.warn("Broker on port {} holds the dropped deliveries of {} clients for its"
                    + " listener, which is behind, and reports those of further clients in one"
                    + " count without their identifiers", port, limit);
        }
        return count;
    }

    // Has the listener hear that the broker has accepted `connection`. Called on the actor of the
    // client's session, as the other notices of its connections are, which keeps them in order.
    void connected(BrokerListener.Connection connection) {
        queue(order -> new Notice.Connected(order, connection));
    }

    // Has the listener hear that `connection` has ended for `reason`.
    void disconnected(BrokerListener.Connection connection, DisconnectReason reason) {
        queue(order -> new Notice.Disconnected(order, connection, reason));
    }

    // How many notices of connections the limit has dropped so far. It takes no lock, so that
    // the application's threads, virtual ones among them, take none that the broker's threads
    // wait for.
    long droppedNotices() {
        return droppedNotices;
    }

    // Queues the notice that `notice` makes with its place in the order, behind the others, and
    // drops the oldest one that waits if the queue is full.
    private void queue(LongFunction<Notice> notice) {
        boolean first;
        boolean firstDrop = false;
        synchronized (this) {
            if (closed) {
                return;
            }
            first = nothingWaits();
            if (notices.size() >= noticeLimit) {
                notices.removeFirst();
                droppedNotices++;
                firstDrop = !noticeLimitReached;
                noticeLimitReached = true;
            }
            notices.addLast(notice.apply(nextOrder++));
        }
        if (first) {
            wake();
        }
        if (firstDrop) {
            log.warn("Broker on port {} holds {} notices of connections for its listener, which"
                    + " is behind, and drops the oldest of them from now on", port, noticeLimit);
        }
    }

    // Wakes the thread, which waits only while nothing waits to be told, so what comes to wait
    // behind something else finds it awake or about to look.
    private void wake() {
        LockSupport.unpark(thread);
    }

    // Ends the notices as the broker closes. A call of the listener that has begun goes on, and
    // nothing waits for it; no other one follows.
    void close() {
        closed = true;
        LockSupport.unpark(thread);
    }

    private void run() {
        try {
            while (!closed) {
                int waiters = await();
                // A count or notice and the count without a client take turns, so that neither
                // kind of report keeps the other waiting for as long as it has more to say. Where
                // nothing else waits, the round still goes once, for the count without a client.
                // What comes to wait during the round is told of in the next one.
                for (int turn = 0; turn < Math.max(waiters, 1) && !closed; turn++) {
                    if (turn < waiters) {
                        tellNext();
                    }
                    if (!closed) {
                        reportUnattributed();
                    }
                }
            }
        } finally {
            termination.partEnded();
        }
    }

    // Waits until something waits to be told, and then a moment more, in which the others of its
    // burst gather. Returns how many counts and notices wait by then: those are the ones that the
    // round which follows tells of. It returns early, with what waits at that time, once the
    // notices have ended.
    private int await() {
        while (!closed && nothingWaits()) {
            LockSupport.park(this);
            // An interrupted thread no longer parks, and this one has nothing to stop for.
            boolean _ = Thread.interrupted();
        }
        long deadline = System.nanoTime() + GATHERING_NANOS;
        long left;
        while (!closed && (left = deadline - System.nanoTime()) > 0) {
            LockSupport.parkNanos(this, left);
            boolean _ = Thread.interrupted();
        }
        synchronized (this) {
            return waiting.size() + notices.size();
        }
    }

    // Whether nothing waits to be told.
    private synchronized boolean nothingWaits() {
        return waiting.isEmpty() && unattributed == 0 && notices.isEmpty();
    }

    // Tells the listener of the count or notice that has waited longest, if one waits.
    private void tellNext() {
        Notice next;
        synchronized (this) {
            Map.Entry<String, Count> drops = waiting.firstEntry();
            Notice notice = notices.peekFirst();
            if (notice != null && (drops == null || notice.order() < drops.getValue().order)) {
                next = notices.removeFirst();
            } else if (drops != null) {
                waiting.pollFirstEntry();
                Count count = drops.getValue();
                next = new Notice.Dropped(count.order, drops.getKey(), count.take());
            } else {
                return;
            }
        }
        tell(next);
    }

    // Tells the listener of the deliveries that wait without a client, if there are any.
    private void reportUnattributed() {
        long count;
        synchronized (this) {
            count = unattributed;
            unattributed = 0;
        }
        if (count != 0) {
            report(() -> listener.onDroppedUnattributed(count), count, null);
        }
    }

    // Makes `call`, which tells the listener of `count` deliveries of the client `clientId`, or
    // without a client if that is null.
    private void report(Runnable call, long count, @Nullable String clientId) {
        try {
            call.run();
        } catch (Throwable failure) {
            // The listener's failure is the application's; the next report still goes out.
            String clients = (clientId == null) ? "further clients" : "client " + clientId;
            log.warn("Broker on port {} could not report {} dropped deliveries of {}",
                    port, count, clients, failure);
        } finally {
            // Interrupt status that a call leaves must not reach the next one.
            boolean _ = Thread.interrupted();
        }
    }

    // Tells the listener of `notice`.
    private void tell(Notice notice) {
        switch (notice) {
            case Notice.Dropped(var _, var clientId, var count) -> report(
                    () -> listener.onDropped(clientId, count),
                    count,
                    clientId
            );
            case Notice.Connected(var _, var connection) -> call(
                    () -> listener.onConnected(connection),
                    notice
            );
            case Notice.Disconnected(var _, var connection, var reason) -> call(
                    () -> listener.onDisconnected(connection, reason),
                    notice
            );
        }
    }

    // Makes `call`, which tells the listener of `notice`, a notice of a connection.
    private void call(Runnable call, Notice notice) {
        try {
            call.run();
        } catch (Throwable failure) {
            // As after a report, the next notice still goes out.
            log.warn("Broker on port {} could not tell its listener of {}", port, notice, failure);
        } finally {
            boolean _ = Thread.interrupted();
        }
    }

    // The deliveries to report for one client, the first of which made it wait, and its place in
    // the order of what waits. The client's sessions add to the count without the lock for as
    // long as it waits. The report takes it for good: a delivery that comes after that is not
    // added, and its session comes for a new count.
    static final class Count {

        // What the count holds once its report has taken it, so far below zero that no number
        // of late deliveries brings it back up.
        private static final long TAKEN = Long.MIN_VALUE;

        private final AtomicLong deliveries;

        private final long order;

        private Count(long order, long deliveries) {
            this.order = order;
            this.deliveries = new AtomicLong(deliveries);
        }

        // Counts `more` deliveries, and returns false if the report has taken the count, which
        // then leaves them uncounted.
        boolean add(long more) {
            return deliveries.getAndAdd(more) >= 0;
        }

        // The deliveries so far, which the caller reports. Called once, under the monitor of
        // the notices, for a count that has just stopped waiting.
        private long take() {
            return deliveries.getAndSet(TAKEN);
        }
    }

    // What the listener hears of, with its place in the order of what waits: a notice of a
    // connection, which waits as it is, or the dropped deliveries of a client, which the thread
    // makes of the client's count as it takes the count.
    private sealed interface Notice {

        long order();

        record Connected(long order, BrokerListener.Connection connection) implements Notice {}

        record Disconnected(
                long order,
                BrokerListener.Connection connection,
                DisconnectReason reason
        ) implements Notice {}

        record Dropped(long order, String clientId, long count) implements Notice {}
    }
}
