/// Test support for applications that use Hato.
module org.mqtt4j.hato.testkit {
    // Nullness annotations are compile-time only and never 'transitive'.
    requires static org.jspecify;

    // The shared implementation is embedded privately here as well. The kit never reaches into
    // the internals of the client, and reaches the broker only through the test controls that
    // the broker exports to it alone.
    requires transitive org.mqtt4j.hato.client;
    requires transitive org.mqtt4j.hato.broker;

    exports org.mqtt4j.hato.testkit;
}
