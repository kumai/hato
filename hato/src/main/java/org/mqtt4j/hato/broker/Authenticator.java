package org.mqtt4j.hato.broker;

import java.net.InetSocketAddress;
import java.util.Optional;

import org.mqtt4j.hato.MqttVersion;

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

    /// What an accepted client may do.
    final class Scope {

        private static final Scope ALL = new Scope();

        private Scope() {
        }

        /// {@return a scope that lets the client publish to every topic and subscribe to every
        /// filter, as a broker without an authenticator lets every client}
        public static Scope all() {
            return ALL;
        }
    }
}
