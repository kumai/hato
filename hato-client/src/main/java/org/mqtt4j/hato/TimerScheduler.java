package org.mqtt4j.hato;

import java.time.Duration;

/// Monotonic time and delayed execution. Methods must not block. Callbacks may run inline;
/// they must be short and must not run application code. Cancellation is best effort, so an
/// already dispatched callback may still run. The client does not own or close this scheduler.
/// Runtime exceptions from time reads or scheduling fail the affected operation or connection
/// and stop automatic reconnection; the client remains reusable through a later
/// [MqttClient#connect()]. A failed read for the timing of a [MetricsListener] event only leaves
/// that event unreported. Cancellation failures are logged and cleanup continues.
public interface TimerScheduler {

    /// {@return a monotonic nanosecond counter, compared using differences, like
    /// [System#nanoTime()]}
    long nanoTime();

    /// Runs a task once after a nonnegative delay.
    ///
    /// @param delay
    ///     the delay before the task runs
    /// @param task
    ///     the task to run
    /// @return
    ///     a handle that cancels the task
    Cancellable schedule(Duration delay, Runnable task);

    /// A handle to a task scheduled with [TimerScheduler#schedule(Duration, Runnable)].
    @FunctionalInterface
    interface Cancellable {

        /// Cancels the task. Cancellation is best effort, so an already dispatched task may still
        /// run.
        void cancel();
    }
}
