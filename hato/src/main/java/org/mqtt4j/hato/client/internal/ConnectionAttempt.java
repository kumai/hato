package org.mqtt4j.hato.client.internal;

/// Identifies one connection attempt independently of its connections. Attempts use object
/// identity. `operation` is the connect() call that started the attempt, or zero for an automatic
/// reconnection; either kind also completes the connect() calls that joined the request.
final class ConnectionAttempt {

    final long operation;

    ConnectionAttempt(long operation) {
        this.operation = operation;
    }
}
