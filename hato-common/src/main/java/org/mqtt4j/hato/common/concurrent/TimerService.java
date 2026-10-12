package org.mqtt4j.hato.common.concurrent;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/// One daemon timer per distribution. Tasks must return promptly; owners handle their failures.
public final class TimerService implements Timer {

    public static final TimerService instance = new TimerService();

    private final ScheduledThreadPoolExecutor executor;

    private TimerService() {
        // Each distribution embeds a copy under org.mqtt4j.hato.<distribution>; naming the
        // thread after it tells the copies apart in one JVM.
        String distribution = TimerService.class.getPackageName().split("\\.")[3];
        executor = new ScheduledThreadPoolExecutor(
                1,
                Thread.ofPlatform().daemon().name("hato-" + distribution + "-timer").factory()
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
