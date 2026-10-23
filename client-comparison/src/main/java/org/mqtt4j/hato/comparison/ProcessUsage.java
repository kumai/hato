package org.mqtt4j.hato.comparison;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;

/// What the process has used up to one moment: the monotonic time of that moment, CPU time across
/// all threads, the bytes that all threads have allocated on the heap, virtual threads included,
/// and garbage collections with their accumulated time. Subtracting one reading from a later one
/// gives what a stretch of the run used.
record ProcessUsage(long nanos, long cpuNanos, long allocatedBytes, long gcCount, long gcMillis) {

    private static final OperatingSystemMXBean os =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    private static final ThreadMXBean threads =
            (ThreadMXBean) ManagementFactory.getThreadMXBean();

    static ProcessUsage now() {
        long count = 0;
        long millis = 0;
        for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            // A collector reports -1 for a figure that it does not keep.
            count += Math.max(0, collector.getCollectionCount());
            millis += Math.max(0, collector.getCollectionTime());
        }
        return new ProcessUsage(
                System.nanoTime(),
                os.getProcessCpuTime(),
                threads.getTotalThreadAllocatedBytes(),
                count,
                millis
        );
    }

    ProcessUsage since(ProcessUsage start) {
        return new ProcessUsage(
                nanos - start.nanos,
                cpuNanos - start.cpuNanos,
                allocatedBytes - start.allocatedBytes,
                gcCount - start.gcCount,
                gcMillis - start.gcMillis
        );
    }
}
