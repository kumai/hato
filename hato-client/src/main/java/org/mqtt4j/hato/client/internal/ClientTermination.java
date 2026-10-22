package org.mqtt4j.hato.client.internal;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.CapacityLimit;
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

        /// A connection or metrics listener callback that is queued or running.
        LISTENER,

        /// A credentials supplier call that has not returned.
        CREDENTIALS,

        /// A started transport that has not reported its close, the I/O sender, or the HttpClient
        /// that WebSocket transports share.
        TRANSPORTS,

        /// A future that the client completes, until its synchronous continuations return.
        COMPLETIONS
    }

    private final String threadName;

    private final Semaphore waiterSlots;

    private final int waiterLimit;

    // Operation slots, including those of connect() and disconnect(), return just before their
    // future completes, and completing counts those completions until their synchronous
    // continuations have run; a completion takes the count before its slot returns. Waiter
    // slots return once their continuations have run. After the cleanup has completed every
    // pending future, held slots and the count therefore cover the completions still running.
    private final List<Semaphore> operationSlots;

    private final AtomicInteger completing = new AtomicInteger();

    private final int operationLimit;

    // Bound once by watch(), before the actor starts.
    private @Nullable ListenerLane listeners;

    private @Nullable ListenerLane metrics;

    private @Nullable CredentialsEvaluator credentials;

    // Set once by the actor; until then nothing can complete, and checks skip the lock.
    private volatile boolean actorEnded;

    private boolean lanesEnded;

    private boolean transportsClosed;

    private boolean terminated;

    // This client's own observations. They hold waiter slots, but termination waits neither for
    // them nor for their continuations.
    private final List<CompletableFuture<Void>> waiters = new ArrayList<>();

    // Counts an observation refused because the waiter slots are full.
    private final Runnable observationRefused;

    ClientTermination(
            String threadName,
            Semaphore waiterSlots,
            int waiterLimit,
            List<Semaphore> operationSlots,
            int operationLimit,
            Runnable observationRefused
    ) {
        this.threadName = threadName;
        this.waiterSlots = waiterSlots;
        this.waiterLimit = waiterLimit;
        this.operationSlots = List.copyOf(operationSlots);
        this.operationLimit = operationLimit;
        this.observationRefused = observationRefused;
    }

    /// Binds the parts that report here when they become idle. Without a metrics listener, no
    /// timing is reported, and without credentials, none is evaluated.
    synchronized void watch(
            ListenerLane listeners,
            @Nullable ListenerLane metrics,
            @Nullable CredentialsEvaluator credentials
    ) {
        this.listeners = listeners;
        this.metrics = metrics;
        this.credentials = credentials;
    }

    /// An observation within the client-wide waiter limit; a completed one after termination.
    synchronized CompletableFuture<Void> observe() {
        if (terminated) {
            return CompletableFuture.completedFuture(null);
        }
        if (!waiterSlots.tryAcquire()) {
            observationRefused.run();
            return CompletableFuture.failedFuture(new MqttCapacityException(
                    "Waiter capacity",
                    CapacityLimit.OBSERVATIONS
            ));
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

    /// Every started transport has reported its close, the I/O sender has exited, and the
    /// HttpClient of WebSocket transports, if any, has ended with its threads.
    void transportsClosed() {
        synchronized (this) {
            transportsClosed = true;
        }
        check();
    }

    /// Counts an operation's completion until its future's synchronous continuations return.
    /// Call it before the operation slot returns, then [#completionFinished()] once they have.
    void completionStarted() {
        completing.incrementAndGet();
    }

    void completionFinished() {
        completing.decrementAndGet();
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
        ListenerLane timings = metrics;
        if (lane == null || !lane.idle() || (timings != null && !timings.idle())) {
            pending.add(Pending.LISTENER);
        }
        CredentialsEvaluator evaluator = credentials;
        if (evaluator != null && !evaluator.idle()) {
            pending.add(Pending.CREDENTIALS);
        }
        if (!transportsClosed) {
            pending.add(Pending.TRANSPORTS);
        }
        if (heldOperations() != 0 || completing.get() != 0 || heldWaiters() != waiters.size()) {
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
