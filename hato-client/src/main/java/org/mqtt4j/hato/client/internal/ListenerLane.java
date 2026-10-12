package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.concurrent.Semaphore;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttSubscribeException;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.client.internal.common.log.Log;

/// Runs connection notifications serially, independently of I/O and operation completion.
/// A reservation remains held until its callback returns, including a throwing callback.
final class ListenerLane {

    private static final Log log = Log.of(ListenerLane.class);

    // Each connection attempt takes two, so 32 attempts can wait behind a slow callback.
    private static final int NOTIFICATION_LIMIT = 64;

    private static final int SUBSCRIPTION_NOTIFICATION_LIMIT = 256;

    private final URI endpoint;

    private final String clientId;

    private final ConnectionListener listener;

    private final Semaphore slots = new Semaphore(NOTIFICATION_LIMIT);

    // Registered subscriptions retain this reservation until retirement or failure reporting.
    // A blocked failure callback therefore cannot turn repeated re-creation into an unbounded
    // queue, even after its subscription is removed from the core and delivery lanes.
    private final Semaphore subscriptionSlots = new Semaphore(SUBSCRIPTION_NOTIFICATION_LIMIT);

    private final ArrayDeque<Event> events = new ArrayDeque<>();

    private boolean running;

    ListenerLane(URI endpoint, String clientId, ConnectionListener listener) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.listener = listener;
    }

    /// Takes both connection notifications of one attempt, or returns null at capacity.
    @Nullable Reservation tryReserveConnection() {
        return slots.tryAcquire(2) ? new Reservation() : null;
    }

    boolean tryReserveSubscription() {
        return subscriptionSlots.tryAcquire();
    }

    void releaseSubscription() {
        subscriptionSlots.release();
    }

    void submitResubscribeFailure(Subscription subscription, MqttSubscribeException failure) {
        submit(new Event.ResubscribeFailed(subscription, failure));
    }

    private void submit(Event event) {
        synchronized (events) {
            events.addLast(event);
            if (!running) {
                running = true;
                Thread.ofVirtual().name("hato-" + clientId + "-listener").start(this::run);
            }
        }
    }

    int usage() {
        return NOTIFICATION_LIMIT - slots.availablePermits();
    }

    int subscriptionUsage() {
        return SUBSCRIPTION_NOTIFICATION_LIMIT - subscriptionSlots.availablePermits();
    }

    private void run() {
        while (true) {
            Event event;
            synchronized (events) {
                event = events.pollFirst();
                if (event == null) {
                    running = false;
                    return;
                }
            }
            try {
                switch (event) {
                    case Event.Connected(var connAck) -> listener.onConnected(connAck);
                    case Event.Disconnected(var disconnection) ->
                            listener.onDisconnected(disconnection);
                    case Event.ResubscribeFailed(var subscription, var failure) ->
                            listener.onResubscribeFailed(subscription, failure);
                }
            } catch (Throwable t) {
                log.warn("Connection listener failed for {} at {}", clientId, endpoint, t);
            } finally {
                // Interrupt state left by a callback must not reach the next queued callback.
                Thread.interrupted();
                switch (event) {
                    case Event.Connected _, Event.Disconnected _ -> slots.release();
                    case Event.ResubscribeFailed _ -> subscriptionSlots.release();
                }
            }
        }
    }

    /// The connection notifications that one attempt has not submitted yet. After admission
    /// only the actor uses it. Closing returns what remains, once.
    final class Reservation {

        private int remaining = 2;

        void connected(ConnAck connAck) {
            take();
            submit(new Event.Connected(connAck));
        }

        void disconnected(Disconnection disconnection) {
            take();
            submit(new Event.Disconnected(disconnection));
        }

        void close() {
            slots.release(remaining);
            remaining = 0;
        }

        private void take() {
            if (remaining == 0) {
                throw new IllegalStateException("No connection notification remains reserved");
            }
            remaining--;
        }
    }

    private sealed interface Event {

        record Connected(ConnAck connAck) implements Event {}

        record Disconnected(Disconnection disconnection) implements Event {}

        record ResubscribeFailed(
                Subscription subscription,
                MqttSubscribeException failure
        ) implements Event {}
    }
}
