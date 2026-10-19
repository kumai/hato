package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.util.ArrayDeque;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttSubscribeException;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.client.internal.common.log.Log;

/// Runs connection notifications serially, independently of I/O and operation completion.
/// Notifications wait in one bounded FIFO. When it is full, the oldest notification that has
/// not started is dropped and counted, so a slow listener delays only later notifications and
/// never an attempt or an operation.
final class ListenerLane {

    private static final Log log = Log.of(ListenerLane.class);

    // Waiting notifications, excluding the one running. Attempt numbers reveal what a listener
    // that fell this far behind has lost.
    private static final int QUEUE_LIMIT = 64;

    private final URI endpoint;

    private final String clientId;

    private final ConnectionListener listener;

    // Called without the lock each time the lane runs out of notifications.
    private final Runnable onIdle;

    private final ArrayDeque<Event> events = new ArrayDeque<>();

    private boolean running;

    // Whether a callback has started and not yet returned.
    private boolean delivering;

    private long dropped;

    ListenerLane(URI endpoint, String clientId, ConnectionListener listener, Runnable onIdle) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.listener = listener;
        this.onIdle = onIdle;
    }

    void connected(ConnAck connAck) {
        submit(new Event.Connected(connAck));
    }

    void disconnected(Disconnection disconnection) {
        submit(new Event.Disconnected(disconnection));
    }

    void connectFailed(Disconnection failure) {
        submit(new Event.ConnectFailed(failure));
    }

    void resubscribeFailed(Subscription subscription, MqttSubscribeException failure) {
        submit(new Event.ResubscribeFailed(subscription, failure));
    }

    private void submit(Event event) {
        boolean firstDrop = false;
        synchronized (events) {
            if (events.size() == QUEUE_LIMIT) {
                events.removeFirst();
                firstDrop = dropped++ == 0;
            }
            events.addLast(event);
            if (!running) {
                running = true;
                Thread.ofVirtual().name("hato-" + clientId + "-listener").start(this::run);
            }
        }
        if (firstDrop) {
            log.warn("Connection listener for {} at {} fell behind; dropping oldest notifications",
                    clientId, endpoint);
        }
    }

    /// Notifications not yet returned from the listener, including a running one.
    int usage() {
        synchronized (events) {
            return events.size() + (delivering ? 1 : 0);
        }
    }

    /// Notifications dropped because the listener fell behind.
    long dropped() {
        synchronized (events) {
            return dropped;
        }
    }

    /// No notification is queued or running.
    boolean idle() {
        synchronized (events) {
            return !running;
        }
    }

    private void run() {
        while (true) {
            Event event;
            synchronized (events) {
                delivering = false;
                event = events.pollFirst();
                if (event == null) {
                    running = false;
                } else {
                    delivering = true;
                }
            }
            if (event == null) {
                onIdle.run();
                return;
            }
            try {
                switch (event) {
                    case Event.Connected(var connAck) -> listener.onConnected(connAck);
                    case Event.Disconnected(var disconnection) ->
                            listener.onDisconnected(disconnection);
                    case Event.ConnectFailed(var failure) -> listener.onConnectFailed(failure);
                    case Event.ResubscribeFailed(var subscription, var failure) ->
                            listener.onResubscribeFailed(subscription, failure);
                }
            } catch (Throwable t) {
                log.warn("Connection listener failed for {} at {}", clientId, endpoint, t);
            } finally {
                // Interrupt state left by a callback must not reach the next queued callback.
                boolean _ = Thread.interrupted();
            }
        }
    }

    private sealed interface Event {

        record Connected(ConnAck connAck) implements Event {}

        record Disconnected(Disconnection disconnection) implements Event {}

        record ConnectFailed(Disconnection failure) implements Event {}

        record ResubscribeFailed(
                Subscription subscription,
                MqttSubscribeException failure
        ) implements Event {}
    }
}
