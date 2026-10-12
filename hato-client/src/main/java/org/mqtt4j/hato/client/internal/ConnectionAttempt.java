package org.mqtt4j.hato.client.internal;

import org.jspecify.annotations.Nullable;

/// Identifies one connection attempt independently of its connections and public operation.
/// Attempts use object identity; a zero operation denotes an attempt without a public future.
final class ConnectionAttempt {

    final long operation;

    // Reserved where the shell decided the attempt; the core only carries it back to the shell.
    final ListenerLane.@Nullable Reservation notifications;

    ConnectionAttempt(long operation, ListenerLane.@Nullable Reservation notifications) {
        this.operation = operation;
        this.notifications = notifications;
    }
}
