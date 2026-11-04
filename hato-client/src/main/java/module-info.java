/// The Hato client. Applications see only the exported packages; everything else is internal
/// and may change without notice.
module org.mqtt4j.hato.client {
    // Nullness annotations are compile-time only. Never 'transitive': that would force module-path
    // consumers to have org.jspecify at compile time.
    requires static org.jspecify;

    // WebSocket transports use the JDK's HTTP client.
    requires java.net.http;

    // The embedded log looks for a logging bridge that the JDK's own search cannot reach.
    uses java.lang.System.LoggerFinder;

    exports org.mqtt4j.hato;
}
