package org.mqtt4j.hato.client.internal;

/// I/O-owned transport receipt. The sole sender records admission without mutating the
/// actor-owned session; a connection can end before an inline send returns its result.
final class SendReceipt {

    private volatile boolean accepted;

    boolean accepted() {
        return accepted;
    }

    void markAccepted() {
        accepted = true;
    }
}
