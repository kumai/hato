package org.mqtt4j.hato.comparison;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/// The CPU time that each live platform thread has used, in all and in user mode, so that a run
/// can show which threads its CPU time went to, and how much of it the kernel took.
///
/// Virtual threads have no CPU time of their own: theirs counts towards the carrier threads that
/// run them. Threads that the JVM hides, such as those of the JIT compilers and the garbage
/// collector, are left out, and so are threads that ended during the stretch.
final class ThreadUsage {

    private static final ThreadMXBean threads = ManagementFactory.getThreadMXBean();

    private ThreadUsage() {
    }

    /// One thread's name and the CPU time it has used so far, in nanoseconds.
    record Times(String name, long cpuNanos, long userNanos) {}

    /// The CPU time of every live platform thread, by thread id.
    static Map<Long, Times> now() {
        var times = new HashMap<Long, Times>();
        for (ThreadInfo info : threads.getThreadInfo(threads.getAllThreadIds())) {
            // A thread that ended after its id was listed has no information.
            if (info == null) {
                continue;
            }
            long cpu = threads.getThreadCpuTime(info.getThreadId());
            long user = threads.getThreadUserTime(info.getThreadId());
            if (cpu >= 0 && user >= 0) {
                times.put(info.getThreadId(), new Times(info.getThreadName(), cpu, user));
            }
        }
        return times;
    }

    /// Breaks down the CPU time used between `start` and `end` by thread, in microseconds per
    /// message, most first. Threads whose names differ only in a trailing number, such as the
    /// carriers of virtual threads, are added up in one line. A last line holds what the process
    /// used beyond the listed threads.
    ///
    /// @param processCpuNanos the CPU time that the whole process used in the stretch
    /// @param messages the messages that came off the wire in the stretch
    static List<String> report(
            Map<Long, Times> start,
            Map<Long, Times> end,
            long processCpuNanos,
            long messages
    ) {
        // What each group of threads used: CPU time, user time, and the number of threads.
        var groups = new HashMap<String, long[]>();
        long listed = 0;
        for (Map.Entry<Long, Times> entry : end.entrySet()) {
            Times last = entry.getValue();
            Times first = start.getOrDefault(entry.getKey(), new Times(last.name(), 0, 0));
            long cpu = last.cpuNanos() - first.cpuNanos();
            long user = last.userNanos() - first.userNanos();
            long[] group = groups.computeIfAbsent(
                    last.name().replaceFirst("\\d+$", "#"),
                    _ -> new long[3]
            );
            group[0] += cpu;
            group[1] += user;
            group[2]++;
            listed += cpu;
        }
        var lines = new ArrayList<String>();
        groups.entrySet().stream()
                .filter(group -> group.getValue()[0] > 0)
                .sorted(Comparator.comparingLong(group -> -group.getValue()[0]))
                .forEach(group -> lines.add(line(
                        group.getKey() + " (" + group.getValue()[2] + ")",
                        group.getValue()[0],
                        group.getValue()[0] - group.getValue()[1],
                        messages
                )));
        lines.add(line(
                "other: JIT, GC, VM, ended threads",
                processCpuNanos - listed,
                -1,
                messages
        ));
        return lines;
    }

    private static String line(String name, long cpuNanos, long systemNanos, long messages) {
        String system = systemNanos < 0
                ? ""
                : String.format(", of which system %7.2f", systemNanos / 1e3 / messages);
        return String.format("%-48s %8.2f µs/msg%s", name, cpuNanos / 1e3 / messages, system);
    }
}
