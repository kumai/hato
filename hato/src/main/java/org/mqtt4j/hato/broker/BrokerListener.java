package org.mqtt4j.hato.broker;

import java.net.InetSocketAddress;
import java.util.Optional;

import org.mqtt4j.hato.MqttVersion;

/// Receives the notices of a broker, apart from the broker's I/O and the handling of its
/// clients: a listener that takes its time delays only its own later notices. The calls come
/// one at a time, except [#onFailed(Throwable)], which waits for no other call and can run at
/// the same time as one; a listener whose methods share state must allow for that. The default
/// methods do nothing, so a listener overrides the ones that it needs.
///
/// The notices of the connections of one client identifier come in the order in which the
/// connections came and went: that one connected, that it disconnected, that the next one
/// connected, and so on, also when a connection takes the session over from another. So a
/// listener can follow which clients are connected. While the listener falls behind, the broker
/// holds these notices up to an internal limit, beyond which it drops the oldest one that the
/// listener has yet to hear of. The broker gives no notice of the connections that its own end
/// closes: those that [MqttBroker#close()] closes, those that a shutdown closes as it ends (see
/// [MqttBroker#shutdown(java.time.Duration)]), and those that an internal failure ends.
///
/// Listener exceptions are logged and do not affect the broker. Interrupt status left by a call
/// is cleared before the next call.
public interface BrokerListener {

    /// Called when the broker has accepted a client, after the [Authenticator], if any, has
    /// accepted it, once the broker has queued its CONNACK. A connection that the broker refuses
    /// is not reported.
    ///
    /// @param connection
    ///   the client's connection, which the notice of its end carries again
    default void onConnected(Connection connection) {
    }

    /// Called when a connection that [#onConnected(Connection)] reported has ended.
    ///
    /// @param connection
    ///   the object that [#onConnected(Connection)] received for the connection
    /// @param reason
    ///   why the connection ended
    default void onDisconnected(Connection connection, DisconnectReason reason) {
    }

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
    /// well.
    ///
    /// This call waits for no other call of the listener: it can run while another one, such
    /// as a report of dropped deliveries, is still in progress or just beginning as the broker
    /// stops. No other notice begins once the broker has ended.
    ///
    /// The broker goes on ending without waiting for this call, so its listening port may still
    /// be in use and its connections open while the call runs. [MqttBroker#close()], whether
    /// called here or later, returns once the port is free and every connection is closed, and
    /// [MqttBroker#terminated()] completes only after this call and any other call in progress
    /// have returned.
    ///
    /// @param failure
    ///   what stopped the broker
    default void onFailed(Throwable failure) {
    }

    /// A connection of a client, as the broker reports it. The broker passes the same object to
    /// both notices of a connection, and the object has no equality of its own. Later versions
    /// may add methods with default implementations, so that a listener's tests can implement
    /// this interface.
    interface Connection {

        /// {@return the identifier of the client, which the broker assigns to an MQTT 5.0 client
        /// that connected without one}
        String clientId();

        /// {@return the address that the connection comes from, or empty for a client in the
        /// same JVM}
        Optional<InetSocketAddress> remoteAddress();

        /// {@return the MQTT version of the connection}
        MqttVersion version();
    }

    /// Why a connection ended. Later versions may add reasons.
    enum DisconnectReason {

        /// The client sent DISCONNECT, whatever its MQTT 5.0 reason code.
        CLIENT_DISCONNECT,

        /// The broker received nothing from the client for one and a half times its keep-alive.
        KEEP_ALIVE_TIMEOUT,

        /// The network connection closed without a DISCONNECT, or reading from it or writing to
        /// it failed.
        CONNECTION_LOST,

        /// Another connection with the same client identifier took the session over.
        TAKEN_OVER,

        /// The application closed the connection with [MqttBroker#disconnect(String)].
        APPLICATION,

        /// The client broke the protocol, or a limit that the broker set for it, such as the
        /// largest packet that the broker takes.
        PROTOCOL_ERROR,

        /// The broker failed to handle the connection, and ended it alone.
        BROKER_ERROR
    }
}
