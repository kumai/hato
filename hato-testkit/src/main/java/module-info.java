/// Test support for applications that use Hato.
/// The shared implementation is embedded privately here as well; the kit never reaches into the
/// internals of the client or broker jars.
module org.mqtt4j.hato.testkit {
    // Nullness annotations are compile-time only and never 'transitive'.
    requires static org.jspecify;

    requires transitive org.mqtt4j.hato.client;
    requires transitive org.mqtt4j.hato.broker;

    exports org.mqtt4j.hato.testkit;
}
