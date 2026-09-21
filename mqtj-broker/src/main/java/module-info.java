/// In-memory, single-node MQTT broker. Packages are exported as the broker takes shape.
/// The embedded shared implementation is private; no client module is required.
module org.mqtt4j.mqtj.broker {
    // Nullness annotations are compile-time only and never 'transitive'.
    requires static org.jspecify;
}
