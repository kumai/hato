package org.mqtt4j.hato.client.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;

import static java.util.concurrent.TimeUnit.SECONDS;

/// An application executor that a test drives by hand. It holds each task it accepts until the
/// test runs it on the test's own thread, one at a time. It can instead run tasks on the thread
/// that submits them, refuse them, or hand back the ones it holds, as `shutdownNow()` does.
final class ManualExecutor implements Executor {

    private final LinkedBlockingQueue<Runnable> held = new LinkedBlockingQueue<>();

    private final AtomicInteger executions = new AtomicInteger();

    // Runs on the submitting thread at the start of every execute() call.
    volatile Runnable onExecute = () -> { };

    private volatile boolean direct;

    private volatile @Nullable RuntimeException refusal;

    @Override
    public void execute(Runnable task) {
        executions.incrementAndGet();
        onExecute.run();
        RuntimeException failure = refusal;
        if (failure != null) {
            throw failure;
        }
        if (direct) {
            task.run();
        } else {
            held.add(task);
        }
    }

    /// Runs the oldest held task on the calling thread, waiting briefly for one to arrive.
    void runNext() {
        Runnable task;
        try {
            task = held.poll(5, SECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
        if (task == null) {
            throw new AssertionError("No task was submitted");
        }
        task.run();
    }

    int held() {
        return held.size();
    }

    /// Counts execute() calls, including refused ones.
    int executions() {
        return executions.get();
    }

    /// Runs later tasks on the thread that submits them, as a direct executor or a caller-runs
    /// policy does.
    void runDirectly() {
        direct = true;
    }

    /// Throws `failure` from later execute() calls.
    void refuse(RuntimeException failure) {
        refusal = failure;
    }

    /// Refuses later tasks and returns the held ones without running them.
    List<Runnable> shutdownNow() {
        refuse(new RejectedExecutionException("Executor has shut down"));
        var tasks = new ArrayList<Runnable>();
        held.drainTo(tasks);
        return tasks;
    }
}
