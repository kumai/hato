package org.mqtt4j.hato.broker.internal;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.log.Log;

// Tells the application how many QoS 1 and 2 deliveries the broker's limits have kept from each
// client. The listener is the application's code and may take its time, so it runs on a thread
// of its own, which no selector, pool thread, or publisher waits for.
//
// What waits for the listener is a count for each client, by its identifier, and the listener
// hears of all the deliveries that a client has lost since its last report in one call. The
// count belongs to the identifier, not to a session: a client that comes back with a new session
// again and again still waits once, and nothing here keeps a session that has ended. The
// identifiers that wait are bounded as well. The deliveries of a client that finds their limit
// reached go into one count without a client, which the listener hears of apart. So a slow
// listener holds nothing up and keeps a bounded amount: it only learns of later drops later,
// together, and of those beyond the limit without their clients.
//
// A session comes here for the first delivery that it loses after a report, and adds the ones
// that follow to the count that it got then, without the lock, until the next report takes that
// count.
//
// The thread reports in rounds. Once deliveries wait, it waits a moment more before it takes
// their counts, and then tells the listener of each client that waits by then. So the
// deliveries of a burst reach the listener in one call, and a session that goes on losing
// deliveries comes here once in a round: without the wait it came after nearly every report,
// each time for the lock and to wake the thread. The listener hears of a delivery that moment
// later.
final class DropReports {

    private static final Log log = Log.of(DropReports.class);

    // How long the thread waits, once deliveries wait to be reported, before it reports them.
    // A session that goes on losing deliveries comes for the lock, and wakes the thread, about
    // once in this time, and the listener hears of a delivery this much later.
    private static final long GATHERING_NANOS = 1_000_000;

    private final BrokerSettings.DropListener listener;

    // The port that the broker listens on, for the thread's name and logs.
    private final int port;

    // How many clients wait for a report of their own at most.
    private final int limit;

    // What the monitor of this object guards, from here on.

    // The counts to report, by client, in the order in which the clients came to wait.
    private final LinkedHashMap<String, Count> waiting = new LinkedHashMap<>();

    // The deliveries to report without a client: those of the clients that found the limit
    // reached.
    private long unattributed;

    // Whether a client has found the limit reached yet, which is logged the first time.
    private boolean limitReached;

    private final Thread thread;

    private volatile boolean closed;

    DropReports(BrokerSettings.DropListener listener, int port, int limit) {
        this.listener = listener;
        this.port = port;
        this.limit = limit;
        thread = Thread.ofPlatform().daemon()
                .name("hato-broker-" + port + "-reports")
                .unstarted(this::run);
    }

    void start() {
        thread.start();
    }

    // Counts one delivery that the limits kept from the client `clientId`, for the listener to
    // hear of, and returns the count that the client's sessions add its later ones to, or null
    // if the client found no place and each of its deliveries has to come here. The caller holds
    // the inbox of a session, so this never waits for the listener, and it logs once at most in
    // the life of the broker.
    @Nullable Count add(String clientId) {
        Count count;
        // Whether nothing waited to be reported until now.
        boolean first;
        // Whether this client is the first of all to find the limit reached.
        boolean firstBeyond = false;
        synchronized (this) {
            count = waiting.get(clientId);
            if (count != null) {
                // The count still waits, so it takes the delivery.
                boolean _ = count.add();
                return count;
            }
            first = waiting.isEmpty() && unattributed == 0;
            if (waiting.size() < limit) {
                count = new Count();
                waiting.put(clientId, count);
            } else {
                if (unattributed++ != 0) {
                    return null;
                }
                firstBeyond = !limitReached;
                limitReached = true;
            }
        }
        if (first) {
            // The thread waits to be woken only while nothing waits to be reported, so what
            // comes to wait behind something else finds it awake or about to look.
            LockSupport.unpark(thread);
        }
        if (firstBeyond) {
            log.warn("Broker on port {} holds the dropped deliveries of {} clients for its"
                    + " listener, which is behind, and reports those of further clients in one"
                    + " count without their identifiers", port, limit);
        }
        return count;
    }

    // Ends the reports as the broker closes. A call of the listener that has begun goes on, and
    // nothing waits for it; no other one follows.
    void close() {
        closed = true;
        LockSupport.unpark(thread);
    }

    private void run() {
        while (!closed) {
            int clients = awaitDeliveries();
            // One client and the count without a client take turns, so that neither kind of
            // report keeps the other waiting for as long as it has more to say. Where no client
            // waits, the round still goes once, for the count without one. The clients that
            // come to wait during the round get their reports in the next one.
            for (int turn = 0; turn < Math.max(clients, 1) && !closed; turn++) {
                if (turn < clients) {
                    reportNextClient();
                }
                if (!closed) {
                    reportUnattributed();
                }
            }
        }
    }

    // Waits until deliveries wait to be reported, and then a moment more, in which the others
    // of their burst gather in the counts. Returns how many clients wait by then: those are
    // the ones that the round which follows reports. It returns early, with what waits at that
    // time, once the reports have ended.
    private int awaitDeliveries() {
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
            return waiting.size();
        }
    }

    // Whether no delivery waits to be reported, with a client or without.
    private boolean nothingWaits() {
        synchronized (this) {
            return waiting.isEmpty() && unattributed == 0;
        }
    }

    // Tells the listener of the client that has waited longest, if one waits.
    private void reportNextClient() {
        String clientId;
        long count;
        synchronized (this) {
            Map.Entry<String, Count> next = waiting.pollFirstEntry();
            if (next == null) {
                return;
            }
            clientId = next.getKey();
            count = next.getValue().take();
        }
        report(() -> listener.dropped(clientId, count), count, clientId);
    }

    // Tells the listener of the deliveries that wait without a client, if there are any.
    private void reportUnattributed() {
        long count;
        synchronized (this) {
            count = unattributed;
            unattributed = 0;
        }
        if (count != 0) {
            report(() -> listener.droppedUnattributed(count), count, null);
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

    // The deliveries to report for one client, the first of which made it wait. The client's
    // sessions add to the count without the lock for as long as it waits. The report takes it
    // for good: a delivery that comes after that is not added, and its session comes for a new
    // count.
    static final class Count {

        // What the count holds once its report has taken it, so far below zero that no number
        // of late deliveries brings it back up.
        private static final long TAKEN = Long.MIN_VALUE;

        private final AtomicLong deliveries = new AtomicLong(1);

        private Count() {
        }

        // Counts one more delivery, and returns false if the report has taken the count, which
        // then leaves the delivery uncounted.
        boolean add() {
            return deliveries.getAndIncrement() >= 0;
        }

        // The deliveries so far, which the caller reports. Called once, under the monitor of
        // the reports, for a count that has just stopped waiting.
        private long take() {
            return deliveries.getAndSet(TAKEN);
        }
    }
}
