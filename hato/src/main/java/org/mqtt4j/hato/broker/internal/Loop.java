package org.mqtt4j.hato.broker.internal;

// The thread that does the work of the connections assigned to it, one task at a time: a
// selector, which serves sockets, or the in-process loop, which serves the connections of
// clients in the same JVM.
sealed interface Loop permits Reactor, InprocLoop {

    // Has the loop look at `connection`: at what waits to be written, and at the input that it
    // can take again. A thread of the actor pool keeps the request until its task ends (see
    // Worker).
    void wake(Connection connection);

    // Queues the readiness task of `connection` now, unless one waits already.
    void schedule(Connection connection);

    // Runs `action` for `connection` on the loop's thread.
    void execute(Connection connection, Runnable action);
}
