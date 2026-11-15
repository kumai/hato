package org.mqtt4j.hato.broker;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

import static java.util.Objects.requireNonNull;

/// Decides whether the broker accepts a client that connects, and what the client may do then.
///
/// The broker asks once for each CONNECT that follows the protocol, before it touches the
/// session of the client's identifier: an earlier connection of the same identifier, its
/// session, and its Will stay as they are until a decision accepts the new one. The broker asks
/// apart from its I/O and from the handling of other clients, so a decision may take its time,
/// as for a database lookup, and decisions for different clients can run at the same time. At
/// most a bounded number of them run at once.
///
/// A client is refused, and its connection closed, when the decision refuses it, in MQTT 3.1.1
/// with return code 4 for bad credentials and 5 for a client that is not authorized, and in MQTT
/// 5.0 with Bad user name or password and Not authorized. It is refused with Server unavailable,
/// return code 3 in MQTT 3.1.1, when the decision throws, returns null, or takes longer than
/// [MqttBrokerBuilder#authenticationTimeout(java.time.Duration)]; a decision that returns after
/// that is ignored. A CONNECT that comes while as many decisions run as the broker allows is
/// refused at once, in MQTT 5.0 with Server busy and in MQTT 3.1.1 with Server unavailable.
///
/// The broker keeps the client's credentials out of its logs. [MqttBroker#close()] interrupts
/// the decisions that are running, and [MqttBroker#terminated()] completes only once they have
/// returned.
@FunctionalInterface
public interface Authenticator {

    /// Decides on the client that `request` describes.
    ///
    /// @param request
    ///   what the broker knows of the client
    /// @return
    ///   the decision
    /// @throws Exception
    ///   if the decision cannot be made, which refuses the client
    Decision authenticate(Request request) throws Exception;

    /// What the broker knows of a client that connects.
    interface Request {

        /// {@return the identifier that the client has once accepted: the one in its CONNECT, or
        /// for an empty one, the one that the broker assigned}
        String clientId();

        /// {@return the user name of the CONNECT, if it has one}
        Optional<String> username();

        /// {@return a copy of the password of the CONNECT, if it has one}
        Optional<byte[]> password();

        /// {@return the address that the client connects from, if it connects over a network}
        Optional<InetSocketAddress> remoteAddress();

        /// {@return the protocol version of the CONNECT}
        MqttVersion version();
    }

    /// A decision on a client: to accept it, which lets it do what a [Scope] allows, or to
    /// refuse it.
    sealed interface Decision {

        /// {@return a decision that accepts the client, which may do what `scope` allows}
        ///
        /// @param scope
        ///   what the client may do
        static Decision accept(Scope scope) {
            return new Accepted(scope);
        }

        /// {@return a decision that refuses the client for its credentials}
        static Decision refuseBadCredentials() {
            return Refused.BAD_CREDENTIALS;
        }

        /// {@return a decision that refuses the client, which is not authorized to connect}
        static Decision refuseNotAuthorized() {
            return Refused.NOT_AUTHORIZED;
        }

        /// A decision that accepts the client.
        ///
        /// @param scope
        ///   what the client may do
        record Accepted(Scope scope) implements Decision {

            public Accepted {
                requireNonNull(scope, "scope");
            }
        }

        /// A decision that refuses the client, for its credentials or as not authorized.
        enum Refused implements Decision {

            /// Refuses the client for its credentials.
            BAD_CREDENTIALS,

            /// Refuses the client, which is not authorized to connect.
            NOT_AUTHORIZED
        }
    }

    /// What an accepted client may do: the topics that it may publish to, and the topic filters
    /// that it may subscribe to. The broker holds the client to its scope for as long as its
    /// connection lasts, without asking the application again.
    ///
    /// - A publication to a topic outside the scope reaches no subscriber and is not retained. An
    ///   MQTT 5.0 client learns of it from Not authorized in PUBACK or PUBREC. A QoS 0
    ///   publication, and an MQTT 3.1.1 client, which cannot be told, get their usual
    ///   acknowledgements, and the connection goes on.
    /// - A subscription is made only if every topic that its filter matches is one that a filter
    ///   of the scope matches; a filter that only overlaps the scope subscribes to nothing.
    ///   SUBACK refuses it, in MQTT 5.0 with Not authorized and in MQTT 3.1.1 with failure 0x80.
    /// - A CONNECT whose Will goes to a topic outside the scope is refused as not authorized.
    /// - When a client resumes a persistent session, the broker holds the session to the
    ///   client's scope as well: the session's subscriptions outside the scope end, and the
    ///   messages that wait for the client, those that it has not acknowledged included, go
    ///   unless the scope would let it subscribe to their topics.
    final class Scope {

        private static final Scope ALL = new Scope(true, List.of(), List.of());

        // Whether the scope has no limits, which its filters cannot say: a filter that starts
        // with a wildcard matches no topic that starts with '$'.
        private final boolean all;

        private final List<TopicFilter> publish;

        private final List<TopicFilter> subscribe;

        private Scope(boolean all, List<TopicFilter> publish, List<TopicFilter> subscribe) {
            this.all = all;
            this.publish = publish;
            this.subscribe = subscribe;
        }

        /// {@return a scope that lets the client publish to every topic and subscribe to every
        /// filter, as a broker without an authenticator lets every client}
        public static Scope all() {
            return ALL;
        }

        /// {@return a scope that lets the client publish to the topics that a filter of `publish`
        /// matches, and subscribe to the filters that lie within a filter of `subscribe`} Either
        /// may be empty, which allows no publication or no subscription.
        ///
        /// @param publish
        ///   the filters of the topics that the client may publish to
        /// @param subscribe
        ///   the filters within which the client may subscribe
        public static Scope of(Collection<TopicFilter> publish, Collection<TopicFilter> subscribe) {
            return new Scope(false, List.copyOf(publish), List.copyOf(subscribe));
        }

        /// {@return whether the scope lets the client publish to `topic`}
        ///
        /// @param topic
        ///   the topic of a publication
        public boolean permitsPublish(Topic topic) {
            requireNonNull(topic, "topic");
            if (all) {
                return true;
            }
            // Ceiling: the topic is matched against the filters in turn, as a session matches
            // its subscriptions: about 40 ns a filter for each publication on an M1 Max. A scope
            // of hundreds of filters would want them in a tree.
            for (TopicFilter filter : publish) {
                if (filter.matches(topic)) {
                    return true;
                }
            }
            return false;
        }

        /// {@return whether the scope lets the client subscribe to `filter`: whether every topic
        /// that `filter` matches is one that a filter of the scope matches}
        ///
        /// @param filter
        ///   the filter of a subscription
        public boolean permitsSubscribe(TopicFilter filter) {
            requireNonNull(filter, "filter");
            if (all) {
                return true;
            }
            for (TopicFilter outer : subscribe) {
                if (contains(outer, filter)) {
                    return true;
                }
            }
            return false;
        }

        // Whether every topic that `inner` matches is one that `outer` matches. The levels are
        // compared in turn: a '#' of `outer` covers the rest of `inner`, and the level above it
        // as well; a '+' covers any one level, a '+' of `inner` included; any other level covers
        // itself alone. A '#' of `inner` needs more, as below. A wildcard at the first level of
        // `outer` covers no first level that starts with '$', which it does not match
        // [MQTT-4.7.2-1].
        private static boolean contains(TopicFilter outer, TopicFilter inner) {
            String[] outerLevels = outer.pattern().split("/", -1);
            String[] innerLevels = inner.pattern().split("/", -1);
            boolean outerWildcardFirst = outerLevels[0].equals("#") || outerLevels[0].equals("+");
            if (outerWildcardFirst && innerLevels[0].startsWith("$")) {
                return false;
            }
            for (int level = 0; level < outerLevels.length; level++) {
                String covering = outerLevels[level];
                if (covering.equals("#")) {
                    return true;
                }
                if (level >= innerLevels.length) {
                    // `inner` matches a topic with fewer levels than `outer` needs.
                    return false;
                }
                String covered = innerLevels[level];
                if (covered.equals("#")) {
                    // The '#' matches any further levels, and the topic of the levels above it
                    // as well, if they make one. A '+' with a '#' after it covers the further
                    // levels but not that topic, so it covers the '#' only where the levels
                    // above make none: at the first level, which has none above, and below an
                    // empty first level, as the empty name is no topic.
                    boolean noTopicAbove = level == 0 || (level == 1 && innerLevels[0].isEmpty());
                    return noTopicAbove
                            && covering.equals("+")
                            && outerLevels.length == level + 2
                            && outerLevels[level + 1].equals("#");
                }
                if (!covering.equals("+") && !covering.equals(covered)) {
                    return false;
                }
            }
            return outerLevels.length == innerLevels.length;
        }

        @Override
        public String toString() {
            if (all) {
                return "Scope[all]";
            }
            return "Scope[publish=" + publish + ", subscribe=" + subscribe + "]";
        }
    }
}
