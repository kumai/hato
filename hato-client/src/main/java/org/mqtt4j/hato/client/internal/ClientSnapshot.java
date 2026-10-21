package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.ClientStats.CapacityLimit;
import org.mqtt4j.hato.ClientStats.ConnectionState;
import org.mqtt4j.hato.ClientStats.DropReason;
import org.mqtt4j.hato.ClientStats.OverloadCause;
import org.mqtt4j.hato.ClientStats.Usage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.TopicFilter;

/// What a client holds and has counted, collected by its actor at one point: the values that
/// [org.mqtt4j.hato.ClientStats] shows to applications. The actor reads the core's values and
/// then the delivery lanes' values under their lock, so lane values can include completions that
/// the actor has not processed yet.
///
/// @param connection the connection's state and attempt numbers
/// @param receiving incoming publications and their exchanges
/// @param sending outgoing writes and exchanges
/// @param subscribing the subscription registrations and delivery lanes
/// @param subscriptions the values of each subscription that the client holds
/// @param refusals the operations refused for capacity, by limit, over the client's lifetime
/// @param overloads the connections ended for overload, by cause, over the client's lifetime
/// @param droppedMetricEvents the metrics listener events dropped because the listener fell
///     behind, over the client's lifetime
public record ClientSnapshot(
        Connection connection,
        Receiving receiving,
        Sending sending,
        Subscribing subscribing,
        List<SubscriptionValues> subscriptions,
        Map<CapacityLimit, Long> refusals,
        Map<OverloadCause, Long> overloads,
        long droppedMetricEvents
) {

    public ClientSnapshot {
        subscriptions = List.copyOf(subscriptions);
        refusals = copy(refusals, CapacityLimit.class);
        overloads = copy(overloads, OverloadCause.class);
    }

    /// Counts kept at the ordinals of `type`, as a map with every key of `type`.
    static <E extends Enum<E>> Map<E, Long> counts(long[] counted, Class<E> type) {
        var counts = new EnumMap<E, Long>(type);
        for (E key : type.getEnumConstants()) {
            counts.put(key, counted[key.ordinal()]);
        }
        return counts;
    }

    /// An unmodifiable copy in the order of `type`, so that a snapshot prints its counts in the
    /// same order every time.
    private static <E extends Enum<E>> Map<E, Long> copy(Map<E, Long> counts, Class<E> type) {
        var copy = new EnumMap<E, Long>(type);
        copy.putAll(counts);
        return Collections.unmodifiableMap(copy);
    }

    /// @param state the connection's state
    /// @param attempt the number of the latest connection attempt, or 0 before the first
    /// @param establishedAttempt the number of the attempt that established the latest
    ///     connection, or 0 if none has
    /// @param droppedNotifications the listener notifications dropped because the listener fell
    ///     behind, over the client's lifetime
    public record Connection(
            ConnectionState state,
            long attempt,
            long establishedAttempt,
            long droppedNotifications
    ) {}

    /// @param publishes the PUBLISH packets received, by QoS, over the client's lifetime
    /// @param unmatched the received publications that matched no subscription
    /// @param deliveries the deliveries that subscriptions took
    /// @param dropped the deliveries that left their subscription unprocessed, by reason
    /// @param heldDeliveries the deliveries that the client holds, against its limit
    /// @param heldBytes the bytes of those deliveries, against the client's limit
    /// @param awaitingConditions QoS 1 and QoS 2 receptions whose acknowledgement waits for a
    ///     handler, an acceptor, or `ack()`
    /// @param awaitingTurn receptions whose acknowledgement is due but waits for an
    ///     earlier reception or for send capacity
    /// @param awaitingAcknowledgementWrite receptions whose PUBACK or PUBREC the send queue
    ///     accepted and has not written yet
    /// @param awaitingRelease QoS 2 receptions that wait for the broker's PUBREL
    /// @param awaitingCompletionWrite QoS 2 receptions whose PUBCOMP waits to be written
    /// @param receptions all of those receptions, against the receive table's limit
    /// @param oldestAwaitingConditions how long the oldest reception has awaited its conditions,
    ///     or null if none does
    /// @param oldestAwaitingRelease how long the oldest reception has awaited PUBREL, or null if
    ///     none does
    public record Receiving(
            Map<Qos, Long> publishes,
            long unmatched,
            long deliveries,
            Map<DropReason, Long> dropped,
            Usage heldDeliveries,
            Usage heldBytes,
            long awaitingConditions,
            long awaitingTurn,
            long awaitingAcknowledgementWrite,
            long awaitingRelease,
            long awaitingCompletionWrite,
            Usage receptions,
            @Nullable Duration oldestAwaitingConditions,
            @Nullable Duration oldestAwaitingRelease
    ) {

        public Receiving {
            publishes = copy(publishes, Qos.class);
            dropped = copy(dropped, DropReason.class);
        }
    }

    /// @param operations the operation slots that publications hold, against their limit
    /// @param queuedWrites the writes that the send queue accepted and has not written yet
    /// @param queuedBytes the bytes of those writes
    /// @param awaitingPubAck QoS 1 publications that await PUBACK, including those not sent yet
    /// @param awaitingPubRec QoS 2 publications that await PUBREC, including those not sent yet
    /// @param awaitingPubComp QoS 2 exchanges that await PUBCOMP
    /// @param publications all unfinished QoS 1 and QoS 2 publications, against their limit
    /// @param keptBytes the bytes that unfinished publications keep, against their limit
    /// @param quota the MQTT 5.0 send quota left on the established connection, or null without
    ///     one
    /// @param oldestResponseWait how long the oldest response that the client awaits has been
    ///     awaited since its write completed, or null if none is awaited
    public record Sending(
            Usage operations,
            Usage queuedWrites,
            Usage queuedBytes,
            long awaitingPubAck,
            long awaitingPubRec,
            long awaitingPubComp,
            Usage publications,
            Usage keptBytes,
            @Nullable Integer quota,
            @Nullable Duration oldestResponseWait
    ) {}

    /// @param registrations the subscription registrations, against their limit
    /// @param established registrations that the broker has granted
    /// @param pending registrations that wait for a connection or for SUBACK
    /// @param removing registrations whose removal waits for the broker
    /// @param lanes the delivery lanes, including stopped lanes whose handlers have not
    ///     returned, against their limit
    public record Subscribing(
            Usage registrations,
            long established,
            long pending,
            long removing,
            Usage lanes
    ) {}

    /// @param subscription the subscription's handle
    /// @param filter the subscription's filter
    /// @param waiting deliveries that wait to be handled or received
    /// @param running deliveries that a handler or a hand-off acceptor is processing
    /// @param awaitingAck processed deliveries whose `ack()` has not been called yet
    /// @param bufferCapacity the subscription's buffer capacity
    /// @param dropped the subscription's deliveries that left it unprocessed, by reason
    public record SubscriptionValues(
            Subscription subscription,
            TopicFilter filter,
            long waiting,
            long running,
            long awaitingAck,
            int bufferCapacity,
            Map<DropReason, Long> dropped
    ) {

        public SubscriptionValues {
            dropped = copy(dropped, DropReason.class);
        }
    }
}
