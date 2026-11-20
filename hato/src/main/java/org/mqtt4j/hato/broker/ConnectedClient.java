package org.mqtt4j.hato.broker;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.internal.BrokerServer;
import org.mqtt4j.hato.internal.protocol.Packet;

/// A client that a broker serves, as [MqttBroker#clients()] read it: the connection that the
/// broker accepted, the subscriptions of the client's session, and the deliveries that wait for
/// the client there. The broker reads each value at a moment of its own while the connection has
/// the client's session, without stopping its work, so the values need not agree with each
/// other.
public final class ConnectedClient {

    private final BrokerServer.ClientValues values;

    private final List<Subscription> subscriptions;

    ConnectedClient(BrokerServer.ClientValues values) {
        this.values = values;
        subscriptions = values.subscriptions().stream().map(Subscription::new).toList();
    }

    /// The client's connection, as the broker reports it. It is the object that
    /// [BrokerListener#onConnected(BrokerListener.Connection)] received for the connection, if
    /// the broker has a listener, and
    /// [MqttBroker#disconnect(BrokerListener.Connection)] closes the connection that it stands
    /// for, and never a later one of the same client.
    ///
    /// @return
    ///   the client's connection
    public BrokerListener.Connection connection() {
        return values.connection();
    }

    /// {@return how long ago the broker accepted the connection}
    public Duration connectedFor() {
        return Duration.ofNanos(values.connectedNanos());
    }

    /// {@return the subscriptions of the client's session, in the order that the client made
    /// them} A session that the connection resumed keeps those that earlier connections made.
    public List<Subscription> subscriptions() {
        return subscriptions;
    }

    /// {@return the deliveries that wait in the client's session, as
    /// [BrokerStats#queuedDeliveries()] counts them for all the sessions}
    public long queuedDeliveries() {
        return values.queuedDeliveries();
    }

    @Override
    public String toString() {
        return "ConnectedClient[connection=" + connection()
                + ", connectedFor=" + connectedFor()
                + ", subscriptions=" + subscriptions
                + ", queuedDeliveries=" + queuedDeliveries() + "]";
    }

    /// A subscription of a client: its filter, and the QoS that the broker granted.
    public static final class Subscription {

        private final TopicFilter filter;

        private final Qos qos;

        private Subscription(Packet.Subscription subscription) {
            filter = subscription.filter();
            qos = subscription.qos();
        }

        /// {@return the filter of the subscription}
        public TopicFilter filter() {
            return filter;
        }

        /// {@return the QoS that the broker granted, at most the one that the client asked for}
        public Qos qos() {
            return qos;
        }

        @Override
        @SuppressWarnings("exports") // Nullness annotations are compile-time only.
        public boolean equals(@Nullable Object other) {
            return other instanceof Subscription that
                    && filter.equals(that.filter)
                    && qos == that.qos;
        }

        @Override
        public int hashCode() {
            return Objects.hash(filter, qos);
        }

        @Override
        public String toString() {
            return "Subscription[filter=" + filter.pattern() + ", qos=" + qos + "]";
        }
    }
}
