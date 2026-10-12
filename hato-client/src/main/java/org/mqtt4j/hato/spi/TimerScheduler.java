package org.mqtt4j.hato.spi;

import java.time.Duration;

/// Monotonic time and delayed execution. Methods must not block. Callbacks may run inline;
/// they must be short and must not run application code. Cancellation is best effort, so an
/// already dispatched callback may still run. The client does not own or close this scheduler.
/// Runtime exceptions from time reads or scheduling fail the affected operation or connection;
/// the client remains reusable. Cancellation failures are logged and cleanup continues.
public interface TimerScheduler {

    /// A monotonic nanosecond counter, compared using differences, like System.nanoTime().
    long nanoTime();

    /// Runs a task once after a nonnegative delay.
    Cancellable schedule(Duration delay, Runnable task);

    @FunctionalInterface
    interface Cancellable {

        void cancel();
    }
}
