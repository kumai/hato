package org.mqtt4j.hato.broker.internal;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

import org.mqtt4j.hato.broker.internal.common.log.Log;

// Tells the application how many QoS 1 and 2 deliveries the broker's limits have kept from a
// session. The listener is the application's code and may take its time, so it runs on a thread
// of its own, which no selector, pool thread, or publisher waits for.
//
// A session with drops to report waits here once, however many more follow, and the listener
// then hears of them all in one call. So what waits is bounded by the number of sessions, and a
// slow listener holds nothing up: it only learns of later drops later, and together.
final class DropReports {

    private static final Log log = Log.of(DropReports.class);

    private final BrokerSettings.DropListener listener;

    // The port that the broker listens on, for the thread's name and logs.
    private final int port;

    // The sessions that have drops to report, each at most once.
    private final ConcurrentLinkedQueue<Session> waiting = new ConcurrentLinkedQueue<>();

    private final Thread thread;

    private volatile boolean closed;

    DropReports(BrokerSettings.DropListener listener, int port) {
        this.listener = listener;
        this.port = port;
        thread = Thread.ofPlatform().daemon()
                .name("hato-broker-" + port + "-reports")
                .unstarted(this::run);
    }

    void start() {
        thread.start();
    }

    // Has the listener hear of the drops of `session`, which has just got its first one since
    // its last report. Never waits: the caller holds the session's inbox.
    void add(Session session) {
        waiting.add(session);
        LockSupport.unpark(thread);
    }

    // Ends the reports as the broker closes. A call of the listener that has begun goes on, and
    // nothing waits for it; no other one follows.
    void close() {
        closed = true;
        LockSupport.unpark(thread);
    }

    private void run() {
        while (!closed) {
            Session session = waiting.poll();
            if (session == null) {
                LockSupport.park(this);
                // An interrupted thread no longer parks, and this one has nothing to stop for.
                boolean _ = Thread.interrupted();
                continue;
            }
            // The session waited here for its first drop since its last report, so the count is
            // at least one.
            long count = session.takeUnreportedDrops();
            try {
                listener.dropped(session.clientId, count);
            } catch (Throwable failure) {
                // The listener's failure is the application's; the next report still goes out.
                log.warn("Broker on port {} could not report {} dropped deliveries of client {}",
                        port, count, session.clientId, failure);
            } finally {
                // Interrupt status that a call leaves must not reach the next one.
                boolean _ = Thread.interrupted();
            }
        }
    }
}
