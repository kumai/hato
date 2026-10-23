package org.mqtt4j.hato.comparison;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/// The subscriber side of one run, in a JVM of its own: connects a client of one library to a
/// broker, subscribes, prints [#READY], counts what reaches the handler, and prints the result as
/// one line that starts with [#RESULT_PREFIX], for the driver to read.
///
/// Arguments, as `key=value` words: `library`, `port`, `filter`, `qos`, `expected`, and
/// `warmup`; optionally `host` and `clientId`; `workMicros` with `workMode` for what the handler
/// does with each message beyond counting it, either `sleep` to block like a call to a database
/// or `spin` to keep a processor busy; and `threads=true` to print, ahead of the result, where the
/// CPU time of the measured stretch went by thread.
public final class Subject {

    static final String READY = "READY";

    static final String RESULT_PREFIX = "RESULT ";

    // A client that drops messages ends its run once none has arrived for this long.
    private static final Duration IDLE = Duration.ofSeconds(2);

    private static final Duration LIMIT = Duration.ofMinutes(10);

    private Subject() {
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(Arrays.asList(args));
        Library library = Library.of(arguments.text("library"));
        var reception = new Reception(
                arguments.number("expected"),
                arguments.number("warmup"),
                Boolean.parseBoolean(arguments.text("threads", "false"))
        );
        Runnable work = work(
                arguments.number("workMicros", 0),
                arguments.text("workMode", "sleep")
        );

        // Paho and Hato log through java.util.logging. HiveMQ and Netty find SLF4J on the class
        // path, which the driver needs, without a provider, and log nowhere. Logging stays off so
        // that no client pays for output that the others do not produce.
        LogManager.getLogManager().reset();
        Logger.getLogger("").setLevel(Level.OFF);

        try (SubscriberClient client = library.connect(
                arguments.text("host", "127.0.0.1"),
                (int) arguments.number("port"),
                arguments.text("clientId", "comparison-" + library.label())
        )) {
            client.subscribe(
                    arguments.text("filter"),
                    (int) arguments.number("qos"),
                    (sequence, sentMicros) -> {
                        reception.handle(sequence, sentMicros);
                        work.run();
                    }
            );
            System.out.println(READY);
            Reception.Result result = reception.await(IDLE, LIMIT);
            for (String line : reception.threadReport(result)) {
                System.out.println(line);
            }
            System.out.println(
                    RESULT_PREFIX + result.format() + " connectionsLost=" + client.connectionsLost()
            );
        }
        System.out.flush();
        // Some clients leave threads behind that would keep the JVM alive.
        System.exit(0);
    }

    /// What the handler does with each message after counting it: nothing for 0 microseconds;
    /// otherwise block for `micros`, as a call to a database would, or keep the processor busy
    /// for as long.
    private static Runnable work(long micros, String mode) {
        long nanos = micros * 1_000;
        if (nanos == 0) {
            return () -> {
            };
        }
        return switch (mode) {
            case "sleep" -> () -> {
                long deadline = System.nanoTime() + nanos;
                for (long left = nanos; left > 0; left = deadline - System.nanoTime()) {
                    LockSupport.parkNanos(left);
                }
            };
            case "spin" -> () -> {
                long deadline = System.nanoTime() + nanos;
                while (System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
            };
            default -> throw new IllegalArgumentException("Unknown work mode: " + mode);
        };
    }
}
