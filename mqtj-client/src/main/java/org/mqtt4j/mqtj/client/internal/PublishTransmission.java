package org.mqtt4j.mqtj.client.internal;

/// I/O-owned receipt shared by attempts of one publication across connections. The sender
/// records transport admission here without mutating the actor-owned session.
final class PublishTransmission {

    private volatile boolean accepted;

    boolean accepted() {
        return accepted;
    }

    void markAccepted() {
        accepted = true;
    }
}
