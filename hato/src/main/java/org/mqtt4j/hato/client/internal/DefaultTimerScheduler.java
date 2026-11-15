package org.mqtt4j.hato.client.internal;

import java.time.Duration;

import org.mqtt4j.hato.TimerScheduler;
import org.mqtt4j.hato.internal.concurrent.Timer;
import org.mqtt4j.hato.internal.concurrent.TimerService;

/// One daemon timer shared by all clients. Tasks only enqueue events for their owners.
public final class DefaultTimerScheduler implements TimerScheduler {

    public static final DefaultTimerScheduler instance = new DefaultTimerScheduler();

    private DefaultTimerScheduler() {
    }

    @Override
    public long nanoTime() {
        return TimerService.client.nanoTime();
    }

    @Override
    public Cancellable schedule(Duration delay, Runnable task) {
        Timer.Cancellable cancellation = TimerService.client.schedule(delay, task);
        return cancellation::cancel;
    }
}
