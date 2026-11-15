package org.mqtt4j.hato.client.internal;

/// I/O-owned transport receipt. The sole sender records write start and admission without
/// mutating the actor-owned session. A peer response or closure can precede an inline return.
final class SendReceipt {

    private volatile boolean started;

    private volatile boolean accepted;

    boolean started() {
        return started;
    }

    boolean accepted() {
        return accepted;
    }

    void markStarted() {
        started = true;
    }

    void markAccepted() {
        // Acceptance implies the write started; an early PUBREL is judged by started alone.
        started = true;
        accepted = true;
    }
}
