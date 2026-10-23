package org.mqtt4j.hato.comparison;

import java.time.Duration;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/// The subscriber side of one run, in a JVM of its own: connects a client of one library to the
/// stub broker, subscribes, counts what reaches the handler, and prints the result as one line
/// that starts with [#RESULT_PREFIX], for the driver to read.
///
/// Arguments, as `key=value` words: `library`, `port`, `filter`, `qos`, `expected`, and
/// `warmup`.
public final class Subject {

    static final String RESULT_PREFIX = "RESULT ";

    // A client that drops messages ends its run once none has arrived for this long.
    private static final Duration IDLE = Duration.ofSeconds(2);

    private static final Duration LIMIT = Duration.ofMinutes(10);

    private Subject() {
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(Arrays.asList(args));
        Library library = Library.of(arguments.text("library"));
        var reception = new Reception(arguments.number("expected"), arguments.number("warmup"));

        // Paho and Hato log through java.util.logging, and HiveMQ finds no logging library on
        // the class path. Logging stays off so that no client pays for output that the others do
        // not produce.
        LogManager.getLogManager().reset();
        Logger.getLogger("").setLevel(Level.OFF);

        try (SubscriberClient client = library.connect(
                "127.0.0.1",
                (int) arguments.number("port"),
                "comparison-" + library.label()
        )) {
            client.subscribe(
                    arguments.text("filter"),
                    (int) arguments.number("qos"),
                    reception::handle
            );
            Reception.Result result = reception.await(IDLE, LIMIT);
            System.out.println(
                    RESULT_PREFIX + result.format() + " connectionsLost=" + client.connectionsLost()
            );
        }
        System.out.flush();
        // Some clients leave threads behind that would keep the JVM alive.
        System.exit(0);
    }
}
