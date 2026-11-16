package org.mqtt4j.hato.broker.internal;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.mqtt4j.hato.internal.inproc.InprocBrokers;
import org.mqtt4j.hato.internal.inproc.InprocLink;
import org.mqtt4j.hato.internal.log.Log;

// The thread that serves a broker's in-process connections, as a selector serves sockets. It
// sleeps in park while no task waits, so that a client's thread, which may be virtual, wakes it
// with an unpark: a selector's wakeup takes a lock that the selector takes too, and a virtual
// thread that waits for such a lock can keep a platform thread waiting behind it while every
// carrier is busy. It takes the links of new connections from clients, and each becomes a
// connection over a MemoryWire.
//
// Ceiling: every in-process connection of the broker shares this thread. More loops, chosen as
// selectors are, would lift that.
final class InprocLoop implements Loop, InprocBrokers.Endpoint {

    private static final Log log = Log.of(InprocLoop.class);

    private final ConcurrentLinkedQueue<Reactor.Task> tasks = new ConcurrentLinkedQueue<>();

    // The links that clients have asked to connect with, which the loop has yet to take.
    private final ConcurrentLinkedQueue<InprocLink> links = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean waking = new AtomicBoolean();

    private final Routing routing;

    // The thread of the loop. It ends the connections that it serves before it ends.
    final Thread thread;

    // Ends the broker for the failure that ends this loop's thread.
    private final Consumer<Throwable> stopBroker;

    private volatile boolean closed;

    InprocLoop(Routing routing, Consumer<Throwable> stopBroker) {
        this.routing = routing;
        this.stopBroker = stopBroker;
        thread = Thread.ofPlatform().daemon()
                .name("hato-broker-" + routing.port + "-inproc")
                .unstarted(this::run);
    }

    // Starts the thread even if the broker has ended meanwhile: the thread then ends at once,
    // and reports its end as every part does.
    void start() {
        thread.start();
    }

    @Override
    public void connect(InprocLink link) {
        if (closed) {
            link.refuse();
            return;
        }
        links.add(link);
        // The loop refuses the links that wait as it ends; one that came too late is refused here.
        if (closed && links.remove(link)) {
            link.refuse();
        } else {
            unpark();
        }
    }

    @Override
    public void wake(Connection connection) {
        if (Thread.currentThread() instanceof Worker worker) {
            worker.hold(connection);
        } else {
            schedule(connection);
        }
    }

    @Override
    public void schedule(Connection connection) {
        if (connection.scheduled.compareAndSet(false, true)) {
            enqueue(connection.readiness);
        }
    }

    @Override
    public void execute(Connection connection, Runnable action) {
        enqueue(new Reactor.Task(connection, action));
    }

    // Counts `connection` among the broker's connections, as a selector counts one that it
    // registers. Runs on the loop's thread, which ends the connections that it counted as it
    // ends, so registration needs no lock against the end. Returns false once the loop or the
    // broker has closed.
    boolean register(Connection connection) {
        if (closed || routing.closed) {
            return false;
        }
        routing.connections.add(connection);
        routing.awaitingConnect.incrementAndGet();
        return true;
    }

    private void enqueue(Reactor.Task task) {
        if (closed) {
            return;
        }
        tasks.add(task);
        if (closed) {
            tasks.remove(task);
        } else {
            unpark();
        }
    }

    private void unpark() {
        if (Thread.currentThread() != thread && waking.compareAndSet(false, true)) {
            LockSupport.unpark(thread);
        }
    }

    private void run() {
        try {
            while (!closed) {
                InprocLink link;
                while ((link = links.poll()) != null) {
                    accept(link);
                }
                // As on a selector, a busy connection can requeue itself, so a slice of tasks
                // ends before the loop looks at new links again.
                for (int i = 0; i < 256; i++) {
                    Reactor.Task task = tasks.poll();
                    if (task == null) {
                        break;
                    }
                    try {
                        task.action().run();
                    } catch (Throwable failure) {
                        // What fails in the work of one connection ends that connection alone,
                        // an Error included, as on a selector.
                        task.connection().failed("in-process task", failure);
                    }
                }
                // Clearing before the queue check lets a later producer unpark the loop; a task
                // queued before the clear is found here.
                waking.set(false);
                if (tasks.isEmpty() && links.isEmpty() && !closed) {
                    LockSupport.park(this);
                }
                if (Thread.interrupted()) {
                    // An interrupted thread no longer parks, so the loop would turn without rest.
                    // It ends instead, and the broker with it, as a selector does.
                    throw new InterruptedException("Interrupted while serving connections");
                }
            }
        } catch (Throwable failure) {
            if (!closed) {
                try {
                    stopBroker.accept(failure);
                } finally {
                    log.error("Broker in-process loop on port {} failed", routing.port, failure);
                }
            }
        } finally {
            closed = true;
            try {
                InprocLink link;
                while ((link = links.poll()) != null) {
                    link.refuse();
                }
                for (Connection connection : routing.connections) {
                    if (connection.loop == this) {
                        connection.dispose();
                    }
                }
            } finally {
                tasks.clear();
                routing.termination.partEnded();
            }
        }
    }

    // Makes a connection of `link` and has the loop serve it, or refuses the link: as the
    // accepting thread closes a TCP connection at once while as many connections as the broker
    // allows wait for their CONNECT.
    private void accept(InprocLink link) {
        if (routing.awaitingConnect.get() >= routing.settings.maxAwaitingConnect()) {
            log.debug("Broker on port {} refused an in-process connection, because {}"
                    + " connections wait for their CONNECT", routing.port,
                    routing.settings.maxAwaitingConnect());
            link.refuse();
            return;
        }
        Connection connection = null;
        boolean started;
        try {
            connection = new Connection(new MemoryWire(link, this, routing), routing);
            started = connection.start();
        } catch (RuntimeException failure) {
            // A failure such as an injected timer's ends this connection alone, as the accepting
            // thread's does for one over TCP.
            log.warn("Starting an in-process broker peer failed", failure);
            if (connection != null) {
                connection.close("start failed");
            }
            link.refuse();
            return;
        }
        if (!started) {
            link.refuse();
            return;
        }
        Connection accepted = connection;
        link.accept(() -> schedule(accepted));
    }

    void close() {
        closed = true;
        LockSupport.unpark(thread);
    }
}
