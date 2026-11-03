package org.mqtt4j.hato.client.internal;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.protocol.SharedTopicFilter;

/// The local state of a persistent session that a later client object can continue: the filters
/// that the broker may hold for the session, where the numbering of Subscription Identifiers goes
/// on, and the receptions whose acknowledgement condition the application met. It carries no
/// message, so outgoing exchanges stay with the client object that started them.
///
/// @param registrations the filters that the broker may hold for the session, in registration
///     order
/// @param nextIdentifier where the search for the next Subscription Identifier starts. Numbering
///     goes on from it, so that a message that the broker still holds for a removed subscription
///     reaches no newer one
/// @param receptions the receptions whose resend a later client object acknowledges without a
///     delivery
record SessionState(
        List<Registration> registrations,
        int nextIdentifier,
        List<Reception> receptions
) {

    SessionState {
        registrations = List.copyOf(registrations);
        receptions = List.copyOf(receptions);
        checkIdentifier(nextIdentifier);
        var filters = new HashSet<TopicFilter>();
        var identifiers = new HashSet<Integer>();
        for (Registration registration : registrations) {
            if (!filters.add(registration.filter())
                    || !identifiers.add(registration.identifier())) {
                throw new IllegalArgumentException(
                        "Repeated registration: " + registration.filter().pattern()
                );
            }
        }
        var packetIds = new HashSet<Integer>();
        for (Reception reception : receptions) {
            if (!packetIds.add(reception.packetId())) {
                throw new IllegalArgumentException(
                        "Repeated reception: " + reception.packetId()
                );
            }
        }
    }

    /// A filter that the broker may hold for the session.
    ///
    /// @param filter the filter, with the prefix of a shared subscription, which names a share and
    ///     a filter after it as subscribing requires
    /// @param identifier the Subscription Identifier that SUBSCRIBE carries for the filter on a
    ///     connection with Subscription Identifiers
    /// @param heldWithoutIdentifier whether the filter's last SUBSCRIBE went on a connection
    ///     without Subscription Identifiers, so that the broker's messages for it carry none
    record Registration(TopicFilter filter, int identifier, boolean heldWithoutIdentifier) {

        Registration {
            checkIdentifier(identifier);
            // Restoring the registration parses a shared subscription again, which must not fail
            // once the restore has begun.
            Optional<SharedTopicFilter> _ = SharedTopicFilter.parse(filter);
        }
    }

    /// A QoS 1 or QoS 2 reception whose acknowledgement condition the application met. The broker
    /// may still resend it, and a QoS 2 one may still await PUBREL.
    ///
    /// @param packetId the broker's packet identifier of the reception
    /// @param qos the reception's QoS
    record Reception(int packetId, Qos qos) {

        Reception {
            if (packetId < 1 || packetId > 65_535) {
                throw new IllegalArgumentException("Packet identifier out of range: " + packetId);
            }
            if (qos == Qos.AT_MOST_ONCE) {
                throw new IllegalArgumentException("QoS 0 receptions are not acknowledged");
            }
        }
    }

    private static void checkIdentifier(int identifier) {
        if (identifier < 1 || identifier > Subscriptions.MAX_SUBSCRIPTION_IDENTIFIER) {
            throw new IllegalArgumentException(
                    "Subscription Identifier out of range: " + identifier
            );
        }
    }
}
