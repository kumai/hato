package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.mqtt4j.hato.internal.log.Log;

// One of the broker's selectors, which does the socket I/O of the connections assigned to it.
// Actor work has a separate fixed pool.
// A connection queues at most one readiness task; the number of connections bounds that queue.
final class Reactor implements Loop, AutoCloseable {

    private static final Log log = Log.of(Reactor.class);

    private final Selector selector = Selector.open();

    private final ConcurrentLinkedQueue<Task> tasks = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean waking = new AtomicBoolean();

    private final Routing routing;

    // The thread that runs the selector. It closes the sockets of the selector's connections
    // before it ends.
    final Thread thread;

    // Ends the broker for the failure that ends this selector's thread.
    private final Consumer<Throwable> stopBroker;

    private final Traffic traffic = new Traffic();

    private volatile boolean closed;

    Reactor(Routing routing, int index, Consumer<Throwable> stopBroker) throws IOException {
        this.routing = routing;
        this.stopBroker = stopBroker;
        thread = Thread.ofPlatform().daemon()
                .name("hato-broker-" + routing.port + "-selector-" + index)
                .unstarted(this::run);
    }

    // Starts the thread even if the broker has ended meanwhile, as when another selector failed
    // at once: the thread then ends at once, and reports its end as every selector does.
    synchronized void start() {
        thread.start();
    }

    // Seals registration with close(), so even a peer whose registration task has not
    // run yet is visible to the selector's shutdown.
    synchronized boolean register(Connection connection, SocketWire wire) {
        if (closed || routing.closed) {
            return false;
        }
        routing.connections.add(connection);
        routing.awaitingConnect.incrementAndGet();
        execute(connection, () -> {
            try {
                if (wire.key == null && !connection.isEnded()) {
                    wire.key = wire.channel().register(selector, SelectionKey.OP_READ, wire);
                }
            } catch (IOException failure) {
                connection.failed("registration", failure);
            }
        });
        return true;
    }

    // Has the selector look at `connection`. A thread of the actor pool keeps the request until
    // its task ends (see Worker).
    @Override
    public void wake(Connection connection) {
        if (Thread.currentThread() instanceof Worker worker) {
            worker.hold(connection);
        } else {
            schedule(connection);
        }
    }

    // Queues the readiness task of `connection` now, unless one waits already.
    @Override
    public void schedule(Connection connection) {
        if (connection.scheduled.compareAndSet(false, true)) {
            enqueue(connection.readiness);
        }
    }

    @Override
    public void execute(Connection connection, Runnable action) {
        enqueue(new Task(connection, action));
    }

    @Override
    public Traffic traffic() {
        return traffic;
    }

    private void enqueue(Task task) {
        if (closed) {
            return;
        }
        tasks.add(task);
        if (closed) {
            tasks.remove(task);
        } else if (Thread.currentThread() != thread && waking.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    private void run() {
        try {
            while (!closed) {
                // A hot peer can requeue itself. Limit the task slice so that socket readiness
                // and other peers are checked even while that peer keeps producing work.
                for (int i = 0; i < 256; i++) {
                    Task task = tasks.poll();
                    if (task == null) {
                        break;
                    }
                    try {
                        task.action().run();
                    } catch (Throwable failure) {
                        // As on a session's actor, what fails in the work of one connection
                        // ends that connection alone, an Error included. Only a failure outside
                        // the tasks is the selector's own, which closes the broker below.
                        task.connection().failed("selector task", failure);
                    }
                }
                // Clearing before the queue check lets a later producer wake select; a task
                // queued before the clear is found here, even if it shared the earlier wakeup.
                waking.set(false);
                routing.settings.beforeSelect().run();
                if (tasks.isEmpty()) {
                    selector.select();
                } else {
                    selector.selectNow();
                }
                if (Thread.interrupted()) {
                    // An interrupted thread no longer waits in a selector, so the loop would
                    // turn without rest. The thread ends instead, and the broker with it,
                    // because no other thread serves the connections of this selector. The
                    // accepting thread ends the broker for an interrupt too.
                    throw new InterruptedIOException("Interrupted while serving connections");
                }
                Iterator<SelectionKey> selected = selector.selectedKeys().iterator();
                while (selected.hasNext()) {
                    SelectionKey key = selected.next();
                    selected.remove();
                    if (key.isValid()) {
                        var wire = (SocketWire) key.attachment();
                        if (key.isReadable()) {
                            wire.readable = true;
                        }
                        wake(wire.connection());
                    }
                }
            }
        } catch (Throwable failure) {
            if (!closed) {
                try {
                    stopBroker.accept(failure);
                } finally {
                    log.error("Broker selector on port {} failed", routing.port, failure);
                }
            }
        } finally {
            close();
            try {
                for (Connection connection : routing.connections) {
                    if (connection.loop == this) {
                        connection.dispose();
                    }
                }
            } finally {
                tasks.clear();
                closeSelector();
                routing.termination.partEnded();
            }
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (thread.getState() == Thread.State.NEW) {
            closeSelector();
        } else {
            selector.wakeup();
        }
    }

    private void closeSelector() {
        try {
            selector.close();
        } catch (IOException failure) {
            log.warn("Closing broker selector on port {} failed", routing.port, failure);
        }
    }

    record Task(Connection connection, Runnable action) {
    }
}
