package org.mqtt4j.mqtj.common.concurrent;

import java.time.Duration;

/// Monotonic time and delayed execution using the same time source. Methods must not block.
/// Callbacks may run inline when already due and must return promptly. Cancellation is best
/// effort: an already dispatched callback may still run. Owners handle callback failures.
public interface Timer {

    /// A monotonic nanosecond counter, compared using differences, like System.nanoTime().
    long nanoTime();

    /// Runs a task once, no earlier than the given nonnegative delay.
    Cancellable schedule(Duration delay, Runnable task);

    @FunctionalInterface
    interface Cancellable {

        void cancel();
    }
}
