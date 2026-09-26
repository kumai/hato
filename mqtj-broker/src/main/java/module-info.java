/// Experimental in-memory MQTT broker for tests, outside the API compatibility promise.
/// The embedded shared implementation is private; no client module is required.
module org.mqtt4j.mqtj.broker {
    exports org.mqtt4j.mqtj.broker;

    // Nullness annotations are compile-time only and never 'transitive'.
    requires static org.jspecify;
}
