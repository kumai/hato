package org.mqtt4j.hato.broker.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

// Tells when a broker has ended: its accepting thread, each of its selectors, its actor pool,
// and the threads that call the application's listener, if there is one. Each of these reports
// its end here once, as the last thing that it does, and the last of them completes the end.
//
// The application observes the end through observations, each of which completes on a thread
// of its own and runs its synchronous continuations there. So a continuation that waits, even
// for another observation, holds up neither the other observations nor any thread of the
// broker, close() included, which waits for the accepting thread and the selectors to end.
final class Termination {

    private final AtomicInteger running;

    // The port that the broker listens on, for the threads' names.
    private final int port;

    // Completes on the part that ends last. Its only continuations start the observations.
    private final CompletableFuture<Void> ended = new CompletableFuture<>();

    Termination(int parts, int port) {
        running = new AtomicInteger(parts);
        this.port = port;
    }

    // Reports the end of one part of the broker; each part calls this once.
    void partEnded() {
        if (running.decrementAndGet() == 0) {
            ended.complete(null);
        }
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
            Thread.ofPlatform().daemon()
                    .name("hato-broker-" + port + "-end")
                    .start(completion);
        } catch (OutOfMemoryError | RuntimeException _) {
            // A process that cannot start a thread still learns of the end, here, with the
            // observation's continuations on the part that ended last.
            completion.run();
        }
    }
}
