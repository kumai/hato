package org.mqtt4j.hato.broker.internal;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.mqtt4j.hato.internal.log.Log;

// Tells when a broker has ended: its accepting thread, each of its selectors, its actor pool,
// and the threads that call the application's listener, if there is one. Each of these reports
// its end here once, as the last thing that it does.
//
// A thread of its own completes the end once every part has ended. The broker starts it last,
// once every part has started, so that it is there even if the process can start no more
// threads later, and a start that fails part of the way leaves it unstarted. Nothing of the
// broker waits for it. Parts that end before it starts are not missed: it counts before it
// waits. The application observes the end through observations, each of which completes
// on a thread of its own and runs its synchronous continuations there. So a continuation that
// waits, even for another observation, holds up neither the other observations nor any part of
// the broker, close() included, which waits for the accepting thread and the selectors to end.
// An observation that can have no thread of its own completes on the end's thread instead: it
// still holds up no part of the broker, but the observations after it wait for it.
final class Termination {

    // How long the end gives the logging backend to take what was logged until then, as long as
    // MqttBroker.close() gives it.
    private static final Duration LOG_WAIT = Duration.ofSeconds(1);

    private final AtomicInteger running;

    // Completes on the end's thread. Its only continuations start the observations.
    private final CompletableFuture<Void> ended = new CompletableFuture<>();

    // Starts a thread that runs a task: the observations' threads. Tests have it fail.
    private final Consumer<Runnable> startThread;

    // The thread that waits for the parts to end and completes the end.
    private final Thread thread;

    Termination(int parts, int port) {
        this(parts, port, task -> Thread.ofPlatform().daemon()
                .name("hato-broker-" + port + "-end")
                .start(task));
    }

    Termination(int parts, int port, Consumer<Runnable> startThread) {
        running = new AtomicInteger(parts);
        this.startThread = startThread;
        thread = Thread.ofPlatform().daemon()
                .name("hato-broker-" + port + "-end")
                .unstarted(this::run);
    }

    // Starts the end's thread, once every part of the broker has started.
    void start() {
        thread.start();
    }

    // Reports the end of one part of the broker; each part calls this once.
    void partEnded() {
        if (running.decrementAndGet() == 0) {
            LockSupport.unpark(thread);
        }
    }

    private void run() {
        while (running.get() != 0) {
            LockSupport.park(this);
            // An interrupted thread no longer parks, and this one has nothing to stop for.
            boolean _ = Thread.interrupted();
        }
        // An application may exit as soon as its broker has ended, also when a failure ended it
        // and nothing closed it. What the broker logged until now, such as that failure, has
        // reached the logging backend by then; if it has not within the limit, the end goes on
        // without it. A close() that waits for the same at the same time adds nothing to it.
        boolean _ = Log.awaitWritten(LOG_WAIT);
        ended.complete(null);
    }

    // An observation of the end, independent of every other one: it completes on a thread of
    // its own, normally, once every part has ended, and cancelling it changes nothing else.
    // Ceiling: each observation made before the end takes a thread then, so an application that
    // makes many rather than keeping one pays a thread for each; a limit on the observations
    // that wait, as the client has, would bound that.
    CompletableFuture<Void> observe() {
        if (ended.isDone()) {
            return CompletableFuture.completedFuture(null);
        }
        return ended.thenRunAsync(() -> { }, this::startObservation);
    }

    // Completes an observation on a new thread.
    private void startObservation(Runnable completion) {
        try {
            startThread.accept(completion);
        } catch (OutOfMemoryError | RuntimeException _) {
            // A process that cannot start a thread still learns of the end, here: on the end's
            // thread, or on the application's own if it observes just as the end comes.
            completion.run();
        }
    }
}
