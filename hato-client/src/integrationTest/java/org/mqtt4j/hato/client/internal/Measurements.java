package org.mqtt4j.hato.client.internal;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.mqtt4j.hato.spi.MetricsListener;

/// What the measurement tests share. They print their results instead of asserting limits.
final class Measurements {

    private Measurements() {
    }

    static void print(String format, Object... arguments) {
        System.out.println(String.format(format, arguments));
    }

    /// The number of carrier threads that run virtual threads, and the Java runtime, for the
    /// heading of a result.
    static String runtime() {
        String carriers = System.getProperty(
                "jdk.virtualThreadScheduler.parallelism",
                String.valueOf(Runtime.getRuntime().availableProcessors())
        );
        return "carriers " + carriers + ", Java " + Runtime.version();
    }

    static long[] sorted(long[] values) {
        long[] copy = values.clone();
        Arrays.sort(copy);
        return copy;
    }

    /// The value below which `percent` percent of the values in `sorted` lie.
    static long percentile(long[] sorted, int percent) {
        return sorted[Math.min(sorted.length - 1, sorted.length * percent / 100)];
    }

    /// The median of `runs` runs of `run`, after one more run that warms up the code paths.
    static long median(int runs, Run run) throws Exception {
        long _ = run.run();
        var samples = new long[runs];
        for (int index = 0; index < runs; index++) {
            samples[index] = run.run();
        }
        return percentile(sorted(samples), 50);
    }

    /// Waits, spinning, until `counter` reaches `target`. Fails after a minute without it, rather
    /// than spinning on after the client has stopped.
    static void awaitCount(AtomicLong counter, long target) {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
        while (counter.get() < target) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Counted " + counter.get() + " of " + target);
            }
            Thread.onSpinWait();
        }
    }

    /// The heap in use once garbage collection has settled, in bytes.
    static long heapAfterGc() throws InterruptedException {
        for (int index = 0; index < 3; index++) {
            System.gc();
            Thread.sleep(50);
        }
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    /// A metrics listener that only counts its events, as a listener that records them cheaply
    /// would.
    static MetricsListener countingListener() {
        var events = new LongAdder();
        return new MetricsListener() {
            @Override
            public void onPublishCompleted(PublishCompletion completion) {
                events.increment();
            }

            @Override
            public void onDeliveryStarted(DeliveryStart start) {
                events.increment();
            }
        };
    }

    @FunctionalInterface
    interface Run {

        long run() throws Exception;
    }
}
