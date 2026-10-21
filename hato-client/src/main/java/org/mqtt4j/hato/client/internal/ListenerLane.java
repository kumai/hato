package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.util.ArrayDeque;

import org.mqtt4j.hato.client.internal.common.log.Log;

/// Runs one listener's callbacks serially, independently of I/O, operation completion, and the
/// threads that submit them. Callbacks wait in one bounded FIFO. When it is full, the oldest
/// callback that has not started is dropped and counted, so a slow listener delays only its own
/// later callbacks and never an attempt or an operation.
final class ListenerLane {

    private static final Log log = Log.of(ListenerLane.class);

    private final URI endpoint;

    private final String clientId;

    // How logs name the listener, such as "Connection listener".
    private final String name;

    // The role in the name of the lane's thread.
    private final String threadRole;

    // Waiting callbacks, excluding the one running.
    private final int queueLimit;

    // Called without the lock each time the lane runs out of callbacks.
    private final Runnable onIdle;

    private final ArrayDeque<Runnable> callbacks = new ArrayDeque<>();

    private boolean running;

    // Whether a callback has started and not yet returned.
    private boolean delivering;

    private long dropped;

    ListenerLane(
            URI endpoint,
            String clientId,
            String name,
            String threadRole,
            int queueLimit,
            Runnable onIdle
    ) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.name = name;
        this.threadRole = threadRole;
        this.queueLimit = queueLimit;
        this.onIdle = onIdle;
    }

    /// Queues `callback` behind the earlier ones; a full queue drops its oldest waiting callback.
    void submit(Runnable callback) {
        boolean firstDrop = false;
        synchronized (callbacks) {
            if (callbacks.size() == queueLimit) {
                callbacks.removeFirst();
                firstDrop = dropped++ == 0;
            }
            callbacks.addLast(callback);
            if (!running) {
                running = true;
                Thread.ofVirtual().name("hato-" + clientId + "-" + threadRole).start(this::run);
            }
        }
        if (firstDrop) {
            log.warn("{} for {} at {} fell behind; dropping oldest notifications",
                    name, clientId, endpoint);
        }
    }

    /// Callbacks not yet returned, including a running one.
    int usage() {
        synchronized (callbacks) {
            return callbacks.size() + (delivering ? 1 : 0);
        }
    }

    /// Callbacks dropped because the listener fell behind.
    long dropped() {
        synchronized (callbacks) {
            return dropped;
        }
    }

    /// No callback is queued or running.
    boolean idle() {
        synchronized (callbacks) {
            return !running;
        }
    }

    private void run() {
        while (true) {
            Runnable callback;
            synchronized (callbacks) {
                delivering = false;
                callback = callbacks.pollFirst();
                if (callback == null) {
                    running = false;
                } else {
                    delivering = true;
                }
            }
            if (callback == null) {
                onIdle.run();
                return;
            }
            try {
                callback.run();
            } catch (Throwable t) {
                log.warn("{} failed for {} at {}", name, clientId, endpoint, t);
            } finally {
                // Interrupt state left by a callback must not reach the next queued callback.
                boolean _ = Thread.interrupted();
            }
        }
    }
}
