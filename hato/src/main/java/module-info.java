/// The Hato client and embedded broker. Applications see only the exported packages; everything
/// else is internal and may change without notice. The broker's package is experimental and
/// outside the API compatibility promise.
@SuppressWarnings("module") // Exports to the testkit, which this module cannot see.
module org.mqtt4j.hato {
    // Nullness annotations are compile-time only. Never 'transitive': that would force module-path
    // consumers to have org.jspecify at compile time.
    requires static org.jspecify;

    // WebSocket transports use the JDK's HTTP client.
    requires java.net.http;

    // The log looks for a logging bridge that the JDK's own search cannot reach.
    uses java.lang.System.LoggerFinder;

    exports org.mqtt4j.hato;
    exports org.mqtt4j.hato.broker;

    // The broker's test controls, which the testkit turns into its public operations, and the
    // log, which hands the testkit's statements to the backend as it does the client's and the
    // broker's.
    exports org.mqtt4j.hato.broker.internal.control to org.mqtt4j.hato.testkit;
    exports org.mqtt4j.hato.internal.log to org.mqtt4j.hato.testkit;
}
