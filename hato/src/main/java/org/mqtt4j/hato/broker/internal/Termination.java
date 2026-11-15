package org.mqtt4j.hato.broker.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

// Tells when a broker has ended: its accepting thread, each of its selectors, its actor pool,
// and the thread that calls the application's listener, if there is one. Each of these reports
// its end here once, as the last thing that it does.
//
// The last of them completes the future on a thread of its own, which runs the application's
// continuations of it. So these hold up no thread of the broker, and with it no call of close(),
// which waits for the accepting thread and the selectors to end.
final class Termination {

    private final AtomicInteger running;

    // The port that the broker listens on, for the thread's name.
    private final int port;

    private final CompletableFuture<Void> ended = new CompletableFuture<>();

    Termination(int parts, int port) {
        running = new AtomicInteger(parts);
        this.port = port;
    }

    // Reports the end of one part of the broker; each part calls this once.
    void partEnded() {
        if (running.decrementAndGet() != 0) {
            return;
        }
        try {
            Thread.ofPlatform().daemon()
                    .name("hato-broker-" + port + "-end")
                    .start(() -> ended.complete(null));
        } catch (OutOfMemoryError | RuntimeException _) {
            // A process that cannot start a thread still learns of the end, here, with the
            // continuations on the part that ended last.
            ended.complete(null);
        }
    }

    // Completes, normally, once every part of the broker has ended.
    CompletableFuture<Void> future() {
        return ended;
    }
}
