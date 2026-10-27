package org.mqtt4j.hato.testkit;

import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.mqtt4j.hato.TimerScheduler;

import static java.util.Objects.requireNonNull;

/// A monotonic clock starting at zero. Advancing executes due tasks on the calling thread,
/// in deadline and registration order. It does not wait for work those tasks enqueue elsewhere.
/// Tasks may schedule or cancel other tasks; concurrent or recursive advance calls are
/// rejected with [IllegalStateException].
public final class ManualTimerScheduler implements TimerScheduler {

    private final PriorityQueue<Task> tasks = new PriorityQueue<>(
            Comparator.comparingLong(Task::deadline).thenComparingLong(Task::sequence)
    );

    private long now;
    private long sequence;
    private boolean advancing;

    /// Creates a scheduler whose clock reads zero and that has no tasks.
    public ManualTimerScheduler() {
    }

    @Override
    public synchronized long nanoTime() {
        return now;
    }

    @Override
    public synchronized Cancellable schedule(Duration delay, Runnable action) {
        long nanos = nonnegativeNanos(delay);
        var task = new Task(Math.addExact(now, nanos), sequence++, requireNonNull(action, "task"));
        tasks.add(task);
        return () -> cancel(task);
    }

    private synchronized void cancel(Task task) {
        // A linear removal keeps cancelled tasks from retaining their captures. A timer wheel
        // would be appropriate only if very large simulated workloads make this a bottleneck.
        tasks.remove(task);
    }

    /// Advances time, including tasks newly scheduled within the elapsed interval.
    ///
    /// @param duration
    ///     the time to advance by
    /// @throws IllegalArgumentException
    ///     if `duration` is negative
    public void advance(Duration duration) {
        long target;
        synchronized (this) {
            if (advancing) {
                throw new IllegalStateException("Concurrent or recursive advance");
            }
            target = Math.addExact(now, nonnegativeNanos(duration));
            advancing = true;
        }
        try {
            while (true) {
                Task task;
                synchronized (this) {
                    if (tasks.isEmpty() || tasks.element().deadline() > target) {
                        now = target;
                        return;
                    }
                    task = tasks.remove();
                    now = task.deadline();
                }
                task.action().run();
            }
        } finally {
            synchronized (this) {
                advancing = false;
            }
        }
    }

    /// {@return the number of tasks still retained by the scheduler, excluding cancelled or
    /// dispatched tasks}
    public synchronized int pendingTaskCount() {
        return tasks.size();
    }

    private static long nonnegativeNanos(Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("Duration must not be negative");
        }
        return duration.toNanos();
    }

    private record Task(long deadline, long sequence, Runnable action) {
    }
}
