package org.mqtt4j.hato.broker.internal;

import java.util.concurrent.atomic.AtomicLong;

import org.mqtt4j.hato.internal.log.Log;

// The memory that the deliveries that wait for the sessions of one broker may take together.
// The limits of a session bound what waits for that session alone, and many sessions that are
// away or behind would still fill the heap. So a delivery takes room here once it waits: when
// it is still in its session's queue after the session has sent what it could, when it comes
// to a queue that is full already, and when its session loses its connection while the
// delivery is unanswered in the window. It keeps the room until it leaves the session: what it
// holds on the heap itself, and what its publication holds, once for all the deliveries of
// that publication that have room (see PublicationMemory). The retained messages that wait
// for a new subscription take room in the same way, for as long as their cursor holds them,
// and so does the cursor for what it holds itself. A session that finds no room drops what
// waits for it as it does for its own limits, but only what gives enough room back as it goes.
//
// A delivery that goes straight out takes no room and costs nothing here. That is what keeps
// the count off the path of a client that keeps up: counting every delivery from the moment
// its session accepts it cost a publication to 200 subscribers about a tenth more of the
// broker's processor time.
//
// A session that is behind takes and gives room for delivery after delivery, each on a thread
// of the actor pool. So such a thread keeps some room of its own, takes from it, and gives
// back to it, and touches the shared count only to fetch a unit more or to return what it
// holds beyond two units. What the threads keep is room that no other thread can use: at
// most two units for each thread.
//
// Ceiling: what is on its way through a connection takes no room: the unanswered deliveries
// in the window of a session that has a connection, and what waits in a connection's send
// queue. Their own limits bound what they hold on the heap for each connection, so a client
// that stays connected and neither answers nor reads holds up to those limits outside this
// budget, and all the clients together as many times as much. Having a connection that falls
// behind take room for them would close that.
final class MemoryBudget {

    // What a delivery holds on the heap besides its publication and a sequence of properties
    // of its own: the delivery itself, and its places in its session's queue and window.
    static final int DELIVERY_BYTES = 128;

    private static final Log log = Log.of(MemoryBudget.class);

    private final long limit;

    private final long unit;

    // The port that the broker listens on, for logs.
    private final int port;

    // What the budget has handed out: to deliveries and their publications, and to the threads
    // of the actor pool as room of their own.
    private final AtomicLong lent = new AtomicLong();

    // How many deliveries left their sessions without giving their room back, which is a
    // defect of the broker: that room is lost until the broker closes.
    private final AtomicLong lostDeliveries = new AtomicLong();

    MemoryBudget(long limit, long unit, int port) {
        this.limit = limit;
        this.unit = unit;
        this.port = port;
    }

    // Takes `bytes` of room, and returns false when the budget has none left for them.
    boolean take(long bytes) {
        if (!(Thread.currentThread() instanceof Worker worker)) {
            return borrow(bytes);
        }
        long kept = worker.memory;
        if (kept >= bytes) {
            worker.memory = kept - bytes;
            return true;
        }
        // A unit more than is needed saves the next trips; near the limit, what is needed.
        long needed = bytes - kept;
        long fetched = Math.max(needed, unit);
        if (!borrow(fetched)) {
            if (fetched == needed || !borrow(needed)) {
                return false;
            }
            fetched = needed;
        }
        worker.memory = kept + fetched - bytes;
        return true;
    }

    // Takes `bytes` of room whatever the budget has left, for what a session cannot go without.
    // The budget then has less than no room until as much has come back.
    void takeAnyway(long bytes) {
        lent.addAndGet(bytes);
    }

    // Gives `bytes` of room back.
    void give(long bytes) {
        if (!(Thread.currentThread() instanceof Worker worker)) {
            lent.addAndGet(-bytes);
            return;
        }
        long kept = worker.memory + bytes;
        if (kept > 2 * unit) {
            lent.addAndGet(unit - kept);
            kept = unit;
        }
        worker.memory = kept;
    }

    // How many bytes more would have to come back before `bytes` found room, which is none if
    // the number is not positive: a session asks before it drops what waits for it to make
    // room. The answer looks at the shared count as it is and at what the calling thread
    // keeps, so it can be wrong by what other threads do meanwhile.
    long shortOf(long bytes) {
        long kept = (Thread.currentThread() instanceof Worker worker) ? worker.memory : 0;
        return bytes - kept - (limit - lent.get());
    }

    private boolean borrow(long bytes) {
        long before = lent.get();
        while (bytes <= limit - before) {
            if (lent.compareAndSet(before, before + bytes)) {
                return true;
            }
            before = lent.get();
        }
        return false;
    }

    // Records that `deliveries` deliveries left a session without giving their room back.
    void lost(long deliveries) {
        if (lostDeliveries.getAndAdd(deliveries) == 0) {
            log.warn("Broker on port {} lost count of {} deliveries in its memory for"
                    + " deliveries, which is a defect; their room stays taken", port, deliveries);
        }
    }

    long lostDeliveries() {
        return lostDeliveries.get();
    }

    // What the budget has handed out, the room that the pool's threads keep included.
    long lent() {
        return lent.get();
    }
}
