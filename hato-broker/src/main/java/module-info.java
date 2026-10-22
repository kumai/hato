/// Experimental in-memory MQTT broker for tests, outside the API compatibility promise.
/// The embedded shared implementation is private; no client module is required.
@SuppressWarnings("module") // The test controls go to the testkit, which the broker cannot see.
module org.mqtt4j.hato.broker {
    exports org.mqtt4j.hato.broker;

    // The test controls, which the testkit turns into its public operations.
    exports org.mqtt4j.hato.broker.internal.control to org.mqtt4j.hato.testkit;

    // Nullness annotations are compile-time only and never 'transitive'.
    requires static org.jspecify;
}
