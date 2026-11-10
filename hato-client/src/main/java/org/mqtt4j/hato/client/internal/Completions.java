package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.common.concurrent.Timer;
import org.mqtt4j.hato.client.internal.common.log.Log;

/// Runs a client's completions, of its operations and of its observations such as
/// Subscription.established(), independently of I/O, in the order they are submitted, on threads
/// that run no other client's completions, so that thread state that one client's continuations
/// leave behind never reaches another client's.
///
/// A client whose core runs on platform threads, as CoreThreads describes, takes its completions
/// on a platform thread, so that they do not wait for a carrier. For the other clients, a
/// virtual thread of the client takes the completions while each returns promptly, and parks
/// while there are none, so that a stream of them costs no thread start apiece; starting a
/// virtual thread for each completion made small publications cost about twice the processor
/// time. On a single carrier, though, a virtual thread runs the completions in turn with the
/// threads that read and write, and pipelined publications lost about a third of their rate. So
/// when completions pile up behind the virtual thread, a platform thread of the client takes
/// them over, which runs beside those threads. At most as many clients as the JVM has
/// processors hold a platform thread at once, like the platform readers; the others stay on
/// their virtual threads.
///
/// A completion whose synchronous continuations run past the takeover limit while others wait
/// hands those others to a new thread, so that one blocked continuation delays the other
/// completions by about that limit and never keeps them from completing. The thread that ran it
/// ends once it returns, except the client's platform thread. A client keeps one platform thread
/// at most: the thread that takes over from it is a virtual one, and the platform thread takes
/// the completions back once its continuation returns. A platform thread for each takeover
/// would have the operating system hold a thread for each blocked continuation: ten seconds of
/// 500 QoS 1 publications a second, whose continuations never returned, left a client 776
/// platform threads. A thread that has parked for the idle limit ends too, and gives back the
/// client's platform thread if it was one; the next completion starts another thread.
///
/// The completion of an observation, submitted with [#submitIndependent(Runnable)], does not wait
/// for a thread that takes over: once a completion holds up the waiting ones past the takeover
/// limit, each waiting observation starts on a virtual thread of its own. So every observation
/// starts within about that limit, however many of their continuations block, and observations
/// whose continuations return promptly cost no thread start.
///
/// Each thread runs with the context class loader of the thread that created the client, which
/// the client's other threads inherit, and inherits no inheritable thread-local values.
///
/// Both limits are real time, like the blocking and the idleness that they detect, so the timer
/// here is a real one rather than the client's TimerScheduler. A lock rather than this monitor
/// guards the state, so that a virtual thread that waits for it resumes without the JDK's
/// unblocker thread.
final class Completions {

    /// How long a running completion may hold up waiting ones before a new thread takes them
    /// over. Shorter takes over sooner after a continuation blocks, at the price of a thread
    /// start for each continuation that merely runs long.
    static final Duration TAKEOVER = Duration.ofMillis(10);

    /// How long a thread parks without a completion before it ends.
    static final Duration IDLE = Duration.ofSeconds(10);

    /// The completions that, waiting behind a virtual thread as it finishes one, have it hand
    /// them to a platform thread of the client. A client that completes its operations one or a
    /// few at a time stays on its virtual thread, which costs it less.
    static final int STREAMING = 8;

    private static final Log log = Log.of(Completions.class);

    // The platform threads that all clients may hold at once, one each.
    private static final Semaphore platformThreads = new Semaphore(
            Runtime.getRuntime().availableProcessors()
    );

    private final URI endpoint;

    private final String clientId;

    // Each thread's name is this followed by a count, which tells apart a thread that took over.
    private final String threadName;

    private final Timer timer;

    private final long takeoverNanos;

    private final long idleNanos;

    // The platform threads that this client draws from: those of all clients, but in tests.
    private final Semaphore slots;

    // Whether the client's completions run on its platform thread, which its core holds a place
    // for.
    private final boolean platformCore;

    // The context class loader of the thread that created the client, which constructs this.
    private final @Nullable ClassLoader contextLoader;

    private final ReentrantLock lock = new ReentrantLock();

    // The fields below are guarded by the lock.
    private final ArrayDeque<Runnable> waiting = new ArrayDeque<>();

    // The thread that takes waiting completions, or null when none runs. A thread that has been
    // replaced finishes its running completion and takes no other, unless it is the client's
    // platform thread.
    private @Nullable Taker active;

    // Whether the client holds one of the platform threads, so that its completions run on its
    // platform thread until that thread ends for idleness.
    private boolean streaming;

    // The client's platform thread, or null when it has none. While a continuation holds it, a
    // virtual thread is active instead.
    private @Nullable Taker platformTaker;

    private boolean checkScheduled;

    private boolean idleCheckScheduled;

    private int started;

    Completions(
            URI endpoint,
            String clientId,
            String threadName,
            Timer timer,
            Duration takeover,
            Duration idle,
            CoreThreads cores
    ) {
        this(endpoint, clientId, threadName, timer, takeover, idle, cores, platformThreads);
    }

    Completions(
            URI endpoint,
            String clientId,
            String threadName,
            Timer timer,
            Duration takeover,
            Duration idle,
            CoreThreads cores,
            Semaphore slots
    ) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.threadName = threadName;
        this.timer = timer;
        this.takeoverNanos = takeover.toNanos();
        this.idleNanos = idle.toNanos();
        this.slots = slots;
        platformCore = cores.platform();
        contextLoader = Thread.currentThread().getContextClassLoader();
    }

    /// Queues `completion` behind the earlier ones, and starts a thread to run it if none runs.
    void submit(Runnable completion) {
        Thread parked = queue(completion);
        if (parked != null) {
            // Woken once the lock is free, so that it does not wait for the lock at once.
            LockSupport.unpark(parked);
        }
    }

    /// Queues `completion` like [#submit(Runnable)], except that once another completion holds up
    /// the waiting ones past the takeover limit, it starts on a virtual thread of its own rather
    /// than waiting for the thread that takes over.
    void submitIndependent(Runnable completion) {
        submit(new Independent(completion));
    }

    // Queues `completion`, and returns the thread to wake if the one that takes completions
    // has parked.
    private @Nullable Thread queue(Runnable completion) {
        lock.lock();
        try {
            waiting.addLast(completion);
            Taker taker = active;
            if (taker == null) {
                start(platformDue());
                return null;
            }
            if (taker.parked) {
                taker.parked = false;
                return taker.thread;
            }
            scheduleCheck(taker);
            return null;
        } finally {
            lock.unlock();
        }
    }

    // Caller holds the lock. Starts a thread of the client, a platform one if `platform`, that
    // takes the waiting completions from now on. A thread that the JVM cannot start leaves the
    // current one in charge.
    private void start(boolean platform) {
        var taker = new Taker(platform);
        Thread thread = thread(platform, () -> run(taker));
        taker.thread = thread;
        thread.start();
        active = taker;
        if (platform) {
            platformTaker = taker;
        }
    }

    // Caller holds the lock. Whether the next thread to take completions is to be a platform
    // one: the client's completions belong on its platform thread, and that thread has ended.
    private boolean platformDue() {
        return (platformCore || streaming) && platformTaker == null;
    }

    // Caller holds the lock. A thread of the client, a platform one if `platform`, that runs
    // `task`.
    private Thread thread(boolean platform, Runnable task) {
        Thread.Builder builder = platform ? Thread.ofPlatform().daemon() : Thread.ofVirtual();
        Thread thread = builder.name(threadName + started++)
                .inheritInheritableThreadLocals(false)
                .unstarted(task);
        thread.setContextClassLoader(contextLoader);
        return thread;
    }

    // Caller holds the lock. While `taker` runs a completion that others wait behind,
    // schedules one check of whether that completion has run past the takeover limit. The
    // timer may run the check at once, which then takes the lock again on this thread.
    private void scheduleCheck(Taker taker) {
        if (checkScheduled || !taker.running || waiting.isEmpty()) {
            return;
        }
        checkScheduled = true;
        long ran = timer.nanoTime() - taker.since;
        timer.schedule(Duration.ofNanos(Math.max(takeoverNanos - ran, 0)), this::check);
    }

    private void check() {
        lock.lock();
        try {
            checkScheduled = false;
            Taker taker = active;
            if (taker == null || !taker.running || waiting.isEmpty()) {
                return;
            }
            if (timer.nanoTime() - taker.since >= takeoverNanos) {
                // The running completion holds up the waiting ones: each independent one starts
                // on a thread of its own, and a new thread takes the others.
                startIndependent();
                if (!waiting.isEmpty()) {
                    start(platformDue());
                }
            } else {
                // A later completion has started since the check was scheduled.
                scheduleCheck(taker);
            }
        } finally {
            lock.unlock();
        }
    }

    // Caller holds the lock. Takes each independent completion out of the waiting ones, and
    // starts it on a virtual thread of its own.
    private void startIndependent() {
        for (Iterator<Runnable> waiters = waiting.iterator(); waiters.hasNext(); ) {
            if (waiters.next() instanceof Independent independent) {
                waiters.remove();
                thread(false, () -> complete(independent)).start();
            }
        }
    }

    // Caller holds the lock. Unless one is pending, schedules a check, after `delayNanos`, of
    // whether the parked thread has parked for the idle limit.
    private void scheduleIdleCheck(long delayNanos) {
        if (!idleCheckScheduled) {
            idleCheckScheduled = true;
            timer.schedule(Duration.ofNanos(delayNanos), this::idleCheck);
        }
    }

    private void idleCheck() {
        lock.lock();
        try {
            idleCheckScheduled = false;
            Taker taker = active;
            if (taker == null || !taker.parked) {
                // A thread that runs a completion schedules another check when it parks.
                return;
            }
            long parkedFor = timer.nanoTime() - taker.parkedSince;
            if (parkedFor < idleNanos) {
                scheduleIdleCheck(idleNanos - parkedFor);
                return;
            }
            // The thread ends once it wakes, and the next completion starts another.
            active = null;
            if (taker == platformTaker) {
                platformTaker = null;
            }
            if (streaming) {
                streaming = false;
                slots.release();
            }
            taker.parked = false;
            LockSupport.unpark(taker.thread);
        } finally {
            lock.unlock();
        }
    }

    private void run(Taker self) {
        while (true) {
            Runnable completion = next(self);
            if (completion == null) {
                return;
            }
            complete(completion);
        }
    }

    private void complete(Runnable completion) {
        try {
            completion.run();
        } catch (Throwable t) {
            log.warn("Completing an operation failed for {} at {}", clientId, endpoint, t);
        } finally {
            // Interrupt state left by a continuation must not reach the next completion.
            boolean _ = Thread.interrupted();
        }
    }

    // The next completion for `self`, parking while there is none, or null once `self` has been
    // replaced or has parked for the idle limit.
    private @Nullable Runnable next(Taker self) {
        lock.lock();
        try {
            self.running = false;
            if (self == platformTaker && active != self) {
                // A continuation held the client's platform thread past the takeover limit, and
                // a virtual thread took the waiting completions over. The platform thread takes
                // them back, and the virtual one ends once it has finished its own, if it has not
                // ended for idleness already.
                Taker replacing = active;
                if (platformCore || streaming) {
                    active = self;
                    if (replacing != null && replacing.parked) {
                        replacing.parked = false;
                        LockSupport.unpark(replacing.thread);
                    }
                } else {
                    // The virtual thread ended for idleness and gave back the client's hold of a
                    // platform thread, so this one ends too.
                    platformTaker = null;
                }
            }
            while (active == self) {
                // A client whose completions belong on its platform thread already, which a
                // continuation may hold meanwhile, starts no second one.
                if (!self.platform
                        && !self.platformFailed
                        && !platformCore
                        && !streaming
                        && waiting.size() >= STREAMING
                        && slots.tryAcquire()) {
                    // The completions pile up, so a platform thread of the client takes them.
                    try {
                        start(true);
                        streaming = true;
                        return null;
                    } catch (OutOfMemoryError failure) {
                        // The JVM could not start another thread, so this one goes on.
                        slots.release();
                        self.platformFailed = true;
                        log.warn("Completing operations for {} at {} stays on a virtual thread",
                                clientId, endpoint, failure);
                    }
                }
                Runnable completion = waiting.pollFirst();
                if (completion != null) {
                    self.running = true;
                    self.since = timer.nanoTime();
                    scheduleCheck(self);
                    return completion;
                }
                self.parked = true;
                self.parkedSince = timer.nanoTime();
                scheduleIdleCheck(idleNanos);
                lock.unlock();
                try {
                    LockSupport.park(this);
                } finally {
                    lock.lock();
                }
                self.parked = false;
                // Park returns at once while the interrupt status is set, which only a
                // continuation can have set, so clear it before parking again.
                boolean _ = Thread.interrupted();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    // A completion that starts on a thread of its own once others hold up the waiting ones.
    private record Independent(Runnable completion) implements Runnable {

        @Override
        public void run() {
            completion.run();
        }
    }

    // One thread that takes completions, guarded by the lock of its Completions.
    private static final class Taker {

        // Whether it is a platform thread, of which a client has one at most: while its core runs
        // on platform threads, or while it holds one of the platform threads.
        private final boolean platform;

        // Set before the thread starts.
        private @Nullable Thread thread;

        // Whether it runs a completion, which started at since.
        private boolean running;

        private long since;

        // Whether it parks for want of a completion, as it has since parkedSince.
        private boolean parked;

        private long parkedSince;

        // Whether the JVM could not start a platform thread to take over from this one.
        private boolean platformFailed;

        Taker(boolean platform) {
            this.platform = platform;
        }
    }
}
