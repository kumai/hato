package org.mqtt4j.hato.broker.internal;

// What a session sends through the regular queue of its connection: a delivery, or the PUBREL
// that continues one.
sealed interface Outgoing permits Delivery, Release {
}
