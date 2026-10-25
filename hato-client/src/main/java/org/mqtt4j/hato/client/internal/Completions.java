package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.common.concurrent.Timer;
import org.mqtt4j.hato.client.internal.common.log.Log;

/// Runs a client's operation completions, independently of I/O, in the order they are submitted.
///
/// One platform thread takes the completions while each returns promptly, so that a stream of
/// them costs no thread start apiece; starting a virtual thread for each completion made small
/// publications cost about twice the processor time. A completion whose synchronous
/// continuations run past the takeover limit while others wait hands those others to a new
/// thread, so that one blocked continuation delays the other completions by about that limit
/// and never keeps them from completing. The thread that ran it ends once it returns. A thread
/// that waits for the idle limit without a completion ends too, and the next completion starts
/// another.
///
/// Both limits are real time, like the blocking that they detect, so the timer here is a real
/// one rather than the client's TimerScheduler.
final class Completions {

    /// How long a running completion may hold up waiting ones before a new thread takes them
    /// over. Shorter takes over sooner after a continuation blocks, at the price of a thread
    /// start for each continuation that merely runs long.
    static final Duration TAKEOVER = Duration.ofMillis(10);

    /// How long a thread waits without a completion before it ends.
    static final Duration IDLE = Duration.ofSeconds(10);

    private static final Log log = Log.of(Completions.class);

    private final URI endpoint;

    private final String clientId;

    // Each thread's name is this followed by a count, which tells apart a thread that took over.
    private final String threadName;

    private final Timer timer;

    private final long takeoverNanos;

    private final long idleNanos;

    // The fields below are guarded by this monitor.
    private final ArrayDeque<Runnable> waiting = new ArrayDeque<>();

    // The thread that takes waiting completions, or null when none runs. A thread that has been
    // replaced finishes its running completion and takes no other.
    private @Nullable Taker active;

    private boolean checkScheduled;

    private int started;

    Completions(
            URI endpoint,
            String clientId,
            String threadName,
            Timer timer,
            Duration takeover,
            Duration idle
    ) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.threadName = threadName;
        this.timer = timer;
        this.takeoverNanos = takeover.toNanos();
        this.idleNanos = idle.toNanos();
    }

    /// Queues `completion` behind the earlier ones, and starts a thread to run it if none runs.
    synchronized void submit(Runnable completion) {
        waiting.addLast(completion);
        Taker taker = active;
        if (taker == null) {
            start();
        } else if (taker.idle) {
            notifyAll();
        } else {
            scheduleCheck(taker);
        }
    }

    // Caller holds this monitor. Starts a thread that takes the waiting completions from now on.
    private void start() {
        var taker = new Taker();
        active = taker;
        Thread.ofPlatform()
                .daemon()
                .name(threadName + started++)
                .start(() -> run(taker));
    }

    // Caller holds this monitor. While `taker` runs a completion that others wait behind,
    // schedules one check of whether that completion has run past the takeover limit. The
    // timer may run the check at once, which then holds this monitor again on this thread.
    private void scheduleCheck(Taker taker) {
        if (checkScheduled || !taker.running || waiting.isEmpty()) {
            return;
        }
        checkScheduled = true;
        long ran = timer.nanoTime() - taker.since;
        timer.schedule(Duration.ofNanos(Math.max(takeoverNanos - ran, 0)), this::check);
    }

    private synchronized void check() {
        checkScheduled = false;
        Taker taker = active;
        if (taker == null || !taker.running || waiting.isEmpty()) {
            return;
        }
        if (timer.nanoTime() - taker.since >= takeoverNanos) {
            // The running completion holds up the waiting ones, so a new thread takes them.
            start();
        } else {
            // A later completion has started since the check was scheduled.
            scheduleCheck(taker);
        }
    }

    private void run(Taker self) {
        while (true) {
            Runnable completion = next(self);
            if (completion == null) {
                return;
            }
            try {
                completion.run();
            } catch (Throwable t) {
                log.warn("Completing an operation failed for {} at {}", clientId, endpoint, t);
            } finally {
                // Interrupt state left by a continuation must not reach the next completion.
                boolean _ = Thread.interrupted();
            }
        }
    }

    // The next completion for `self`, waiting for one while there is none, or null once `self`
    // has been replaced or has waited for the idle limit.
    private synchronized @Nullable Runnable next(Taker self) {
        self.running = false;
        long idleSince = timer.nanoTime();
        while (active == self) {
            Runnable completion = waiting.pollFirst();
            if (completion != null) {
                self.running = true;
                self.since = timer.nanoTime();
                scheduleCheck(self);
                return completion;
            }
            long left = idleNanos - (timer.nanoTime() - idleSince);
            if (left <= 0) {
                active = null;
                return null;
            }
            self.idle = true;
            try {
                wait(Math.max(left / 1_000_000, 1));
            } catch (InterruptedException _) {
                // Only a continuation can have reached this thread to interrupt it; the loop
                // waits again for the time that is left.
            } finally {
                self.idle = false;
            }
        }
        return null;
    }

    // One thread that takes completions, guarded by the monitor of its Completions.
    private static final class Taker {

        // Whether it runs a completion, which started at since.
        private boolean running;

        private long since;

        // Whether it waits for a completion.
        private boolean idle;
    }
}
