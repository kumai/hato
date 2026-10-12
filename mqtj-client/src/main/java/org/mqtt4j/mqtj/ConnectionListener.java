package org.mqtt4j.mqtj;

/// Receives connection endings serially, independently of I/O and operation completion.
/// Failed connection attempts are reported only by the connect future. Listener exceptions
/// are logged and do not affect the client. Ordering relative to future completion is unspecified.
@FunctionalInterface
public interface ConnectionListener {

    /// Called once for each established connection that ends, including local disconnect or
    /// close, peer closure, protocol errors, overload, and internal failure.
    void onDisconnected(Disconnection disconnection);
}
