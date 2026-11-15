/// Test support for applications that use Hato.
module org.mqtt4j.hato.testkit {
    // Nullness annotations are compile-time only and never 'transitive'.
    requires static org.jspecify;

    // The kit never reaches into the internals of the client. It reaches the broker only through
    // the test controls, and logs only through the log, both of which Hato exports to it alone.
    requires transitive org.mqtt4j.hato;

    exports org.mqtt4j.hato.testkit;
}
