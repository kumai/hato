package org.mqtt4j.hato.broker;

/// Receives the notices of a broker, one call at a time and apart from the broker's I/O and the
/// handling of its clients: a listener that takes its time delays only its own later notices.
/// The default methods do nothing, so a listener overrides the ones that it needs.
///
/// Listener exceptions are logged and do not affect the broker. Interrupt status left by a call
/// is cleared before the next call.
public interface BrokerListener {

    /// Called when the broker's limits have kept QoS 1 or QoS 2 deliveries from the client
    /// `clientId`, whose connection and session go on without them. While the listener takes its
    /// time, the broker counts on, so one call can report many deliveries. QoS 0 deliveries that
    /// the limits drop are not reported.
    ///
    /// @param clientId
    ///   the identifier of the client that lost deliveries
    /// @param count
    ///   how many deliveries the client has lost since the last call for it
    default void onDropped(String clientId, long count) {
    }

    /// Called for QoS 1 and QoS 2 deliveries that clients have lost while the listener fell
    /// behind. The broker keeps counts for a bounded number of clients until the listener takes
    /// them, and counts the losses of further clients together, without their identifiers.
    ///
    /// @param count
    ///   how many such deliveries clients have lost since the last call
    default void onDroppedUnattributed(long count) {
    }

    /// Called once if an internal failure stops the broker, rather than [MqttBroker#close()]; a
    /// broker that the application closes first never calls it. The broker logs the failure as
    /// well. The listener hears of nothing after it.
    ///
    /// The broker goes on ending without waiting for this call, so its listening port may still
    /// be in use and its connections open while the call runs. [MqttBroker#close()], whether
    /// called here or later, returns once the port is free and every connection is closed, and
    /// [MqttBroker#terminated()] completes only after this call has returned.
    ///
    /// @param failure
    ///   what stopped the broker
    default void onFailed(Throwable failure) {
    }
}
