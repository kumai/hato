package org.mqtt4j.hato.client.internal;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttCapacityException;

/// Completes a client's termination observations once, after close, nothing remains through
/// which the client calls application code. It holds no reference to the client itself: a part
/// that never returns, such as a credentials supplier that ignores interruption, reaches only
/// this object and the parts it watches, so it cannot keep a closed client reachable.
final class ClientTermination {

    /// What termination still waits for.
    enum Pending {

        /// The actor's shutdown cleanup.
        ACTOR,

        /// A subscription that has not terminated, with a running handler or dispatcher.
        LANES,

        /// A listener notification that is queued or running.
        LISTENER,

        /// A credentials supplier call that has not returned.
        CREDENTIALS,

        /// A started transport that has not reported its close, or the I/O sender.
        TRANSPORTS,

        /// A future that the client completes, until its synchronous continuations return.
        COMPLETIONS
    }

    private final String threadName;

    private final Semaphore waiterSlots;

    private final int waiterLimit;

    // Operation slots, including those of connect() and disconnect(), return once their
    // future's synchronous continuations have run; so do waiter slots. After the cleanup has
    // completed every pending future, held slots therefore count completions still running.
    private final List<Semaphore> operationSlots;

    private final int operationLimit;

    // Bound once by watch(), before the actor starts.
    private @Nullable ListenerLane listeners;

    private @Nullable CredentialsEvaluator credentials;

    // Set once by the actor; until then nothing can complete, and checks skip the lock.
    private volatile boolean actorEnded;

    private boolean lanesEnded;

    private boolean transportsClosed;

    private boolean terminated;

    // This client's own observations. They hold waiter slots, but termination waits neither for
    // them nor for their continuations.
    private final List<CompletableFuture<Void>> waiters = new ArrayList<>();

    ClientTermination(
            String threadName,
            Semaphore waiterSlots,
            int waiterLimit,
            List<Semaphore> operationSlots,
            int operationLimit
    ) {
        this.threadName = threadName;
        this.waiterSlots = waiterSlots;
        this.waiterLimit = waiterLimit;
        this.operationSlots = List.copyOf(operationSlots);
        this.operationLimit = operationLimit;
    }

    /// Binds the parts that report here when they become idle. Without credentials, none is
    /// evaluated.
    synchronized void watch(ListenerLane listeners, @Nullable CredentialsEvaluator credentials) {
        this.listeners = listeners;
        this.credentials = credentials;
    }

    /// An observation within the client-wide waiter limit; a completed one after termination.
    synchronized CompletableFuture<Void> observe() {
        if (terminated) {
            return CompletableFuture.completedFuture(null);
        }
        if (!waiterSlots.tryAcquire()) {
            return CompletableFuture.failedFuture(new MqttCapacityException("Waiter capacity"));
        }
        var future = new CompletableFuture<Void>();
        waiters.add(future);
        return future;
    }

    /// The actor has finished its cleanup: it has ended every subscription and transport and
    /// completed every pending future, and it calls no application code afterwards.
    void actorEnded() {
        actorEnded = true;
        check();
    }

    /// Every subscription has terminated after the cleanup.
    void lanesEnded() {
        synchronized (this) {
            lanesEnded = true;
        }
        check();
    }

    /// Every started transport has reported its close, and the I/O sender has exited.
    void transportsClosed() {
        synchronized (this) {
            transportsClosed = true;
        }
        check();
    }

    /// Completes the observations once nothing is pending. Parts call this without holding their
    /// own locks whenever they may have become idle, including before close.
    void check() {
        if (!actorEnded) {
            return;
        }
        synchronized (this) {
            if (terminated || !pendingLocked().isEmpty()) {
                return;
            }
            terminated = true;
            completeObservations();
        }
    }

    /// What termination still waits for, for tests until applications get statistics.
    synchronized Set<Pending> pending() {
        return terminated ? EnumSet.noneOf(Pending.class) : pendingLocked();
    }

    /// Caller holds this monitor.
    private Set<Pending> pendingLocked() {
        var pending = EnumSet.noneOf(Pending.class);
        if (!actorEnded) {
            pending.add(Pending.ACTOR);
        }
        if (!lanesEnded) {
            pending.add(Pending.LANES);
        }
        ListenerLane lane = listeners;
        if (lane == null || !lane.idle()) {
            pending.add(Pending.LISTENER);
        }
        CredentialsEvaluator evaluator = credentials;
        if (evaluator != null && !evaluator.idle()) {
            pending.add(Pending.CREDENTIALS);
        }
        if (!transportsClosed) {
            pending.add(Pending.TRANSPORTS);
        }
        if (heldOperations() != 0 || heldWaiters() != waiters.size()) {
            pending.add(Pending.COMPLETIONS);
        }
        return pending;
    }

    private int heldOperations() {
        int available = 0;
        for (Semaphore slots : operationSlots) {
            available += slots.availablePermits();
        }
        return operationLimit - available;
    }

    private int heldWaiters() {
        return waiterLimit - waiterSlots.availablePermits();
    }

    /// Caller holds this monitor, but only for one thread start: as for other observations, each
    /// gets its own thread, so a slow continuation delays no other, and its slot returns once
    /// its synchronous continuations have run.
    private void completeObservations() {
        if (waiters.isEmpty()) {
            return;
        }
        List<CompletableFuture<Void>> completing = List.copyOf(waiters);
        waiters.clear();
        Thread.ofVirtual().name(threadName).start(() -> {
            for (CompletableFuture<Void> future : completing) {
                Thread.ofVirtual().name(threadName).start(() -> {
                    try {
                        future.complete(null);
                    } finally {
                        waiterSlots.release();
                    }
                });
            }
        });
    }
}
