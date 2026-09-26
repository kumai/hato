package org.mqtt4j.mqtj.client.internal;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.mqtt4j.mqtj.spi.TimerScheduler;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/// One daemon timer shared by all clients. Tasks only enqueue events for their owners.
public final class DefaultTimerScheduler implements TimerScheduler {

    public static final DefaultTimerScheduler instance = new DefaultTimerScheduler();

    private final ScheduledThreadPoolExecutor executor;

    private DefaultTimerScheduler() {
        executor = new ScheduledThreadPoolExecutor(
                1,
                Thread.ofPlatform().daemon().name("mqtj-timer").factory()
        );
        executor.setRemoveOnCancelPolicy(true);
    }

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }

    @Override
    public Cancellable schedule(Duration delay, Runnable task) {
        if (delay.isNegative()) {
            throw new IllegalArgumentException("Delay must not be negative");
        }
        ScheduledFuture<?> future = executor.schedule(task, delay.toNanos(), NANOSECONDS);
        return () -> future.cancel(false);
    }
}
