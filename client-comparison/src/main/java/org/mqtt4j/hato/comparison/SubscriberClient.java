package org.mqtt4j.hato.comparison;

/// One client library under comparison, reduced to what a subscriber needs: a connection made when
/// the client is created, subscriptions, and an orderly disconnect.
///
/// Each implementation uses its library the way an application that receives at full speed would,
/// and differs from the library's defaults only where the comparison needs the clients to agree:
/// MQTT 3.1.1, a clean session, session state in memory, and a QoS 1 message acknowledged once its
/// handler has returned.
sealed interface SubscriberClient extends AutoCloseable
        permits HatoSubscriber, HiveMqSubscriber, PahoBlockingSubscriber, PahoSubscriber {

    /// Subscribes `filter` at `qos` and returns once the broker has granted it. The handler
    /// receives each message's payload header, read the way the library offers the payload.
    void subscribe(String filter, int qos, Handler handler) throws Exception;

    /// The connections that have ended so far other than through [#close()], such as those that
    /// the client ended because it was overloaded. Each ending is also reported on standard
    /// error, with its cause.
    long connectionsLost();

    /// Disconnects and releases the client.
    @Override
    void close();

    /// Receives each message of a subscription, one at a time.
    @FunctionalInterface
    interface Handler {

        void handle(long sequence, long sentMicros);
    }
}
