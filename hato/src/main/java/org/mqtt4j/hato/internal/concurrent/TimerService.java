package org.mqtt4j.hato.internal.concurrent;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/// A daemon timer thread for the clients and another for the brokers of the JVM, so that the
/// timers of one never wait behind those of the other. Tasks must return promptly; owners handle
/// their failures.
public final class TimerService implements Timer {

    /// The timer of every client.
    public static final TimerService client = new TimerService("client");

    /// The timer of every broker.
    public static final TimerService broker = new TimerService("broker");

    private final ScheduledThreadPoolExecutor executor;

    // The executor starts its thread with the first task, so a JVM that runs only clients never
    // starts the brokers' thread, and the other way round.
    private TimerService(String owner) {
        executor = new ScheduledThreadPoolExecutor(
                1,
                Thread.ofPlatform().daemon().name("hato-" + owner + "-timer").factory()
        );
        executor.setRemoveOnCancelPolicy(true);
    }

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }

    /// Runs a task once after a nonnegative delay. Cancellation releases the waiting task.
    @Override
    public Cancellable schedule(Duration delay, Runnable task) {
        if (delay.isNegative()) {
            throw new IllegalArgumentException("Delay must not be negative");
        }
        ScheduledFuture<?> future = executor.schedule(task, delay.toNanos(), NANOSECONDS);
        return () -> future.cancel(false);
    }
}
