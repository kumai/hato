package org.mqtt4j.hato.broker;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.internal.BrokerServer;
import org.mqtt4j.hato.internal.protocol.Packet;

/// A client that a broker serves, as [MqttBroker#clients()] read it: the connection that the
/// broker accepted, the subscriptions of the client's session, the deliveries that wait for the
/// client there, and, on a broker that records the activity of its clients, what the client has
/// done on the connection. The broker reads each value at a moment of its own while the
/// connection has the client's session, without stopping its work, so the values need not agree
/// with each other.
public final class ConnectedClient {

    private final BrokerServer.ClientValues values;

    private final List<Subscription> subscriptions;

    private final @Nullable Activity activity;

    ConnectedClient(BrokerServer.ClientValues values) {
        this.values = values;
        subscriptions = values.subscriptions().stream().map(Subscription::new).toList();
        BrokerServer.ActivityValues done = values.activity();
        activity = (done == null) ? null : new Activity(done);
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

    /// {@return what the client has done on the connection, on a broker that records the
    /// activity of its clients; empty on a broker that does not} See
    /// [MqttBrokerBuilder#recordClientActivity()].
    public Optional<Activity> activity() {
        return Optional.ofNullable(activity);
    }

    @Override
    public String toString() {
        return "ConnectedClient[connection=" + connection()
                + ", connectedFor=" + connectedFor()
                + ", subscriptions=" + subscriptions
                + ", queuedDeliveries=" + queuedDeliveries()
                + ", activity=" + activity + "]";
    }

    /// What a client has done on its connection since the broker accepted it: the PUBLISH
    /// packets that it sent, with the topic of the last one, the PUBLISH packets that the broker
    /// sent it, and the publications and subscriptions that its scope refused, with the topic and
    /// the filter of the last ones. A client that connects again starts with nothing, whatever it
    /// did on an earlier connection. The broker keeps a topic or a filter whole up to a length
    /// that it sets, and a longer one as its beginning, which the activity marks as truncated.
    public static final class Activity {

        private final BrokerServer.ActivityValues values;

        private Activity(BrokerServer.ActivityValues values) {
            this.values = values;
        }

        /// {@return the PUBLISH packets that the broker took from the client on the connection,
        /// retransmissions and those outside the client's scope included}
        public long received() {
            return values.received();
        }

        /// {@return the topic of the last PUBLISH packet that the broker took from the client,
        /// as a Topic Alias stands for it; empty before the first} It is only the beginning of
        /// the topic if [#lastTopicTruncated()] says so.
        public Optional<String> lastTopic() {
            return Optional.ofNullable(values.lastTopic());
        }

        /// {@return whether [#lastTopic()] is only the beginning of a topic longer than the
        /// broker keeps}
        public boolean lastTopicTruncated() {
            return values.lastTopicTruncated();
        }

        /// {@return how long ago the broker received the last PUBLISH packet from the client;
        /// empty before the first}
        public Optional<Duration> sinceLastReceived() {
            return (values.lastTopic() == null)
                    ? Optional.empty()
                    : Optional.of(Duration.ofNanos(values.sinceLastReceivedNanos()));
        }

        /// {@return the PUBLISH packets that the broker sent to the client on the connection,
        /// retransmissions included}
        public long delivered() {
            return values.delivered();
        }

        /// {@return the publications of the client that the broker refused because their topics
        /// are outside the scope that the [Authenticator] gave the client}
        public long refusedPublications() {
            return values.refusedPublications();
        }

        /// {@return the topic of the last publication that the client's scope refused; empty
        /// before the first} It is only the beginning of the topic if
        /// [#lastRefusedTopicTruncated()] says so.
        public Optional<String> lastRefusedTopic() {
            return Optional.ofNullable(values.lastRefusedTopic());
        }

        /// {@return whether [#lastRefusedTopic()] is only the beginning of a topic longer than
        /// the broker keeps}
        public boolean lastRefusedTopicTruncated() {
            return values.lastRefusedTopicTruncated();
        }

        /// {@return the subscriptions of the client that the broker refused because their
        /// filters are outside the scope that the [Authenticator] gave the client}
        public long refusedSubscriptions() {
            return values.refusedSubscriptions();
        }

        /// {@return the filter of the last subscription that the client's scope refused; empty
        /// before the first} It is only the beginning of the filter if
        /// [#lastRefusedFilterTruncated()] says so.
        public Optional<String> lastRefusedFilter() {
            return Optional.ofNullable(values.lastRefusedFilter());
        }

        /// {@return whether [#lastRefusedFilter()] is only the beginning of a filter longer than
        /// the broker keeps}
        public boolean lastRefusedFilterTruncated() {
            return values.lastRefusedFilterTruncated();
        }

        @Override
        public String toString() {
            return "Activity[received=" + received()
                    + ", lastTopic=" + values.lastTopic()
                    + ", lastTopicTruncated=" + lastTopicTruncated()
                    + ", sinceLastReceived=" + sinceLastReceived().orElse(null)
                    + ", delivered=" + delivered()
                    + ", refusedPublications=" + refusedPublications()
                    + ", lastRefusedTopic=" + values.lastRefusedTopic()
                    + ", lastRefusedTopicTruncated=" + lastRefusedTopicTruncated()
                    + ", refusedSubscriptions=" + refusedSubscriptions()
                    + ", lastRefusedFilter=" + values.lastRefusedFilter()
                    + ", lastRefusedFilterTruncated=" + lastRefusedFilterTruncated() + "]";
        }
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
