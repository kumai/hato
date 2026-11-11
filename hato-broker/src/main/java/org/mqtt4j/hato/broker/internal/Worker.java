package org.mqtt4j.hato.broker.internal;

import java.util.HashSet;

// A thread of the actor pool. It keeps the connections that its task has queued output for, and
// has their selectors look at them when the task ends rather than as each packet is queued. A
// selector that hears of the first packet writes while the actor still queues the rest, a call
// into the operating system for each part; one that hears after the turn writes them together.
//
// Ceiling: a task that blocks keeps what it holds for that long, so the output of the events
// before the one that blocks waits with it. Handing over before an event that can block, or
// after a time limit, would lift this.
final class Worker extends Thread {

    // A task that reaches this many connections hands them over at once, so that the selectors
    // of a large fan-out start to write while the publication still goes to the other
    // subscribers.
    private static final int HELD_COUNT = 64;

    // Touched by this thread alone.
    private final HashSet<Connection> held = new HashSet<>();

    Worker(Runnable loop, String name) {
        super(loop, name);
        setDaemon(true);
    }

    // Keeps `connection` for the end of the task that this thread runs.
    void hold(Connection connection) {
        if (held.add(connection) && held.size() == HELD_COUNT) {
            handOver();
        }
    }

    // Has the selector look at `connection` now if this thread keeps it: what the task queued
    // for it cannot wait for the end of the task.
    void handOver(Connection connection) {
        if (held.remove(connection)) {
            connection.wakeNow();
        }
    }

    // Has the selectors look at the connections kept so far.
    void handOver() {
        for (Connection connection : held) {
            connection.wakeNow();
        }
        held.clear();
    }
}
