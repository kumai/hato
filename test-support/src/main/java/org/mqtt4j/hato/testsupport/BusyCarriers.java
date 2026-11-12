package org.mqtt4j.hato.testsupport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/// As many virtual threads as the scheduler has carriers, each computing without blocking until
/// stopped, which keeps every carrier busy meanwhile, as an application that computes on virtual
/// threads of its own can.
public final class BusyCarriers {

    private final AtomicBoolean stopped = new AtomicBoolean();

    private final List<Thread> threads = new ArrayList<>();

    private BusyCarriers() {
    }

    /// Starts the threads, and returns once each of them runs on a carrier that it then keeps.
    public static BusyCarriers start() throws InterruptedException {
        int carriers = Integer.getInteger(
                "jdk.virtualThreadScheduler.parallelism",
                Runtime.getRuntime().availableProcessors()
        );
        var busy = new BusyCarriers();
        var running = new CountDownLatch(carriers);
        for (int i = 0; i < carriers; i++) {
            busy.threads.add(Thread.ofVirtual().start(() -> {
                running.countDown();
                while (!busy.stopped.get()) {
                    Thread.onSpinWait();
                }
            }));
        }
        // Each thread counts down on a carrier that it then keeps.
        running.await();
        return busy;
    }

    /// Ends the threads, and returns once the carriers are free again.
    public void stop() throws InterruptedException {
        stopped.set(true);
        for (Thread thread : threads) {
            thread.join();
        }
    }
}
