package org.mqtt4j.hato.client.internal;

import java.time.Duration;

import org.mqtt4j.hato.TimerScheduler;
import org.mqtt4j.hato.client.internal.common.concurrent.Timer;
import org.mqtt4j.hato.client.internal.common.concurrent.TimerService;

/// One daemon timer shared by all clients. Tasks only enqueue events for their owners.
public final class DefaultTimerScheduler implements TimerScheduler {

    public static final DefaultTimerScheduler instance = new DefaultTimerScheduler();

    private DefaultTimerScheduler() {
    }

    @Override
    public long nanoTime() {
        return TimerService.instance.nanoTime();
    }

    @Override
    public Cancellable schedule(Duration delay, Runnable task) {
        Timer.Cancellable cancellation = TimerService.instance.schedule(delay, task);
        return cancellation::cancel;
    }
}
