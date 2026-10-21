package org.mqtt4j.hato;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.ClientSnapshot;

/// What a client holds and has counted, as only the client can know it, collected at one point
/// after [MqttClient#stats()] was called. The values of one snapshot agree with each other: each
/// unfinished QoS 1 or QoS 2 exchange counts in exactly one of its stages, and the registrations
/// add up to their total. The stages move when the client processes an event, such as the
/// completion of a write. Values that other threads change are read right after: those of the
/// send queue, of the operation capacity, and of deliveries and delivery lanes. They can already
/// show a completed write, a handler that has returned, or an [MqttDelivery#ack()] that has been
/// called before the client has processed it.
///
/// Counts described as lifetime counts cover the client's whole life and only grow. Durations
/// are measured with the client's [org.mqtt4j.hato.spi.TimerScheduler]. Maps contain every
/// key of their enum, in its order, with zero for what never happened. A limit shown with a
/// value is the one in force when the snapshot was taken; the client may change the internal
/// ones in later versions.
public final class ClientStats {

    private final ClientSnapshot snapshot;

    private final Connection connection;

    private final Receiving receiving;

    private final Sending sending;

    private final Subscribing subscribing;

    private final List<SubscriptionStats> subscriptions;

    private ClientStats(ClientSnapshot snapshot) {
        this.snapshot = snapshot;
        connection = new Connection(snapshot.connection());
        receiving = new Receiving(snapshot.receiving());
        sending = new Sending(snapshot.sending());
        subscribing = new Subscribing(snapshot.subscribing());
        subscriptions = snapshot.subscriptions().stream().map(SubscriptionStats::new).toList();
    }

    /// Shows a snapshot that the client's actor collected; the builder hands this to the client.
    static ClientStats of(ClientSnapshot snapshot) {
        return new ClientStats(snapshot);
    }

    /// The connection's state and attempt numbers.
    public Connection connection() {
        return connection;
    }

    /// Incoming publications, their deliveries, and their acknowledgement exchanges.
    public Receiving receiving() {
        return receiving;
    }

    /// Outgoing writes and publication exchanges.
    public Sending sending() {
        return sending;
    }

    /// The subscription registrations and delivery lanes, in total.
    public Subscribing subscribing() {
        return subscribing;
    }

    /// The values of each subscription that the client holds, in no particular order. A
    /// subscription joins the list when its handle is returned, before the client processes its
    /// registration, and leaves it once its removal completes or it is closed; a handler that
    /// still runs then keeps its lane counted in [Subscribing#lanes()].
    public List<SubscriptionStats> subscriptions() {
        return subscriptions;
    }

    /// Lifetime counts of operations that the client refused for capacity, by the limit that
    /// refused them. The refused operations failed with [MqttCapacityException].
    public Map<CapacityLimit, Long> refusals() {
        return snapshot.refusals();
    }

    /// Lifetime counts of connections that the client ended because it was overloaded, by
    /// cause. The ending of each such connection carries a [MqttCapacityException].
    public Map<OverloadCause, Long> overloads() {
        return snapshot.overloads();
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof ClientStats stats && snapshot.equals(stats.snapshot);
    }

    @Override
    public int hashCode() {
        return snapshot.hashCode();
    }

    @Override
    public String toString() {
        return snapshot.toString();
    }

    /// How much of a limit is in use.
    ///
    /// @param used how much is in use
    /// @param limit the most that may be in use
    public record Usage(long used, long limit) {}

    /// The state of a client's connection.
    public enum ConnectionState {

        /// No connection is requested.
        DISCONNECTED,

        /// An attempt is establishing a connection.
        CONNECTING,

        /// A connection is established.
        CONNECTED,

        /// A requested disconnection is in progress.
        DISCONNECTING,

        /// A connection ended, and the client waits to try again.
        WAITING_TO_RECONNECT,

        /// The client is closed.
        CLOSED
    }

    /// A limit that refuses operations with [MqttCapacityException] when it is full.
    public enum CapacityLimit {

        /// The operation capacity that publications share with concurrent [MqttClient#connect()]
        /// and [MqttClient#disconnect()] calls beyond the first of each; see
        /// [Sending#operations()].
        OPERATIONS,

        /// The operations waiting for the client to process them.
        OPERATION_QUEUE,

        /// [MqttClientBuilder#maxInflightPublishes(int)].
        INFLIGHT_PUBLISHES,

        /// The bytes that unfinished publications keep.
        PUBLICATION_BYTES,

        /// The packet identifiers that publications and subscription requests share.
        PACKET_IDENTIFIERS,

        /// The send queue, or a backlog in it that waits for send capacity.
        SEND_QUEUE,

        /// [MqttClientBuilder#maxSubscriptions(int)], for registrations and delivery lanes.
        SUBSCRIPTIONS,

        /// The registrations waiting for the client to process them.
        REGISTRATION_QUEUE,

        /// The pending observations, such as [MqttClient#whenConnected()],
        /// [Subscription#established()], [MqttClient#terminated()], and
        /// [MqttClient#stats()].
        OBSERVATIONS
    }

    /// Why the client ended a connection as overloaded.
    public enum OverloadCause {

        /// The unfinished QoS 1 and QoS 2 receptions filled the client's receive table.
        RECEPTIONS,

        /// A QoS 1 or QoS 2 delivery did not fit a subscription's buffer or the client's limits
        /// on deliveries and their bytes.
        DELIVERIES,

        /// A hand-off acceptor refused a QoS 1 or QoS 2 delivery.
        HAND_OFF,

        /// The client fell behind on incoming packets, control events, or keep-alive events.
        EVENTS,

        /// CONNECT, PINGREQ, or DISCONNECT found no room in the send capacity reserved for them.
        RESERVED_SENDS
    }

    /// Why a delivery left its subscription unprocessed.
    public enum DropReason {

        /// Its handler threw, the stripe of its striped handler could not be chosen, or its
        /// executor refused it.
        FAILED,

        /// The end of the connection that delivered it discarded it while it waited.
        CONNECTION_ENDED,

        /// Its subscription stopped while it waited.
        SUBSCRIPTION_STOPPED,

        /// A QoS 0 delivery found its subscription's buffer full.
        SUBSCRIPTION_FULL,

        /// A QoS 0 delivery did not fit the client's limits on deliveries and their bytes.
        CLIENT_FULL,

        /// Its hand-off acceptor refused it.
        REFUSED
    }

    /// The connection's state and attempt numbers.
    public static final class Connection {

        private final ClientSnapshot.Connection values;

        private Connection(ClientSnapshot.Connection values) {
            this.values = values;
        }

        public ConnectionState state() {
            return values.state();
        }

        /// The number of the latest connection attempt, as [ConnAck#connectionAttempt()] and
        /// [Disconnection#connectionAttempt()] number them, or 0 before the first attempt.
        public long attempt() {
            return values.attempt();
        }

        /// The number of the attempt that established the latest connection, or 0 if none has.
        /// The difference from [#attempt()] counts the attempts since then.
        public long establishedAttempt() {
            return values.establishedAttempt();
        }

        /// A lifetime count of [ConnectionListener] notifications dropped because the listener
        /// fell behind.
        public long droppedNotifications() {
            return values.droppedNotifications();
        }
    }

    /// Incoming publications, their deliveries, and their acknowledgement exchanges.
    ///
    /// Each unfinished QoS 1 or QoS 2 reception counts in one stage: awaiting its conditions
    /// (a handler, a hand-off acceptor, or `ack()`, as its [AckMode] requires), awaiting its turn
    /// (its acknowledgement is due but follows an earlier reception, in receive order, or waits
    /// for send capacity), awaiting the write of its PUBACK or PUBREC, awaiting PUBREL, or
    /// awaiting the write of its PUBCOMP.
    public static final class Receiving {

        private final ClientSnapshot.Receiving values;

        private Receiving(ClientSnapshot.Receiving values) {
            this.values = values;
        }

        /// A lifetime count of the PUBLISH packets received at `qos`, retransmissions included.
        public long publishes(Qos qos) {
            return values.publishes().getOrDefault(qos, 0L);
        }

        /// A lifetime count of received publications that matched no subscription.
        public long unmatched() {
            return values.unmatched();
        }

        /// A lifetime count of the deliveries that subscriptions took: one for each copy of a
        /// received publication that a subscription's buffer took. A retransmission that joins
        /// a copy still held adds none, and copies that did not fit, counted under
        /// [DropReason#SUBSCRIPTION_FULL] and [DropReason#CLIENT_FULL], are not included.
        public long deliveries() {
            return values.deliveries();
        }

        /// Lifetime counts of deliveries that left their subscription unprocessed, by reason,
        /// across all subscriptions, including those that have ended.
        public Map<DropReason, Long> dropped() {
            return values.dropped();
        }

        /// The deliveries that the client holds, against its limit for all subscriptions.
        public Usage heldDeliveries() {
            return values.heldDeliveries();
        }

        /// The payload and property bytes of the deliveries that the client holds, against its
        /// limit for all subscriptions.
        public Usage heldBytes() {
            return values.heldBytes();
        }

        /// Unfinished receptions whose acknowledgement awaits its conditions.
        public long awaitingConditions() {
            return values.awaitingConditions();
        }

        /// Unfinished receptions whose acknowledgement is due but awaits its turn.
        public long awaitingTurn() {
            return values.awaitingTurn();
        }

        /// Unfinished receptions whose PUBACK or PUBREC awaits its write.
        public long awaitingAcknowledgementWrite() {
            return values.awaitingAcknowledgementWrite();
        }

        /// QoS 2 receptions that await the broker's PUBREL, which has no deadline.
        public long awaitingRelease() {
            return values.awaitingRelease();
        }

        /// QoS 2 receptions whose PUBCOMP awaits its write.
        public long awaitingCompletionWrite() {
            return values.awaitingCompletionWrite();
        }

        /// All unfinished QoS 1 and QoS 2 receptions, against the client's limit, which includes
        /// receptions that a persistent session carries across connections.
        public Usage receptions() {
            return values.receptions();
        }

        /// How long the oldest reception that awaits its conditions has done so, which also
        /// holds back the acknowledgements counted as awaiting their turn; empty if none does.
        public Optional<Duration> oldestAwaitingConditions() {
            return Optional.ofNullable(values.oldestAwaitingConditions());
        }

        /// How long the oldest reception that awaits PUBREL has done so; empty if none does.
        public Optional<Duration> oldestAwaitingRelease() {
            return Optional.ofNullable(values.oldestAwaitingRelease());
        }
    }

    /// Outgoing writes and publication exchanges.
    public static final class Sending {

        private final ClientSnapshot.Sending values;

        private Sending(ClientSnapshot.Sending values) {
            this.values = values;
        }

        /// The operation capacity in use, against its limit. A publication holds a slot until its
        /// future has completed and the future's synchronous continuations have returned, and so
        /// does each concurrent [MqttClient#connect()] or [MqttClient#disconnect()] call beyond
        /// the first of each.
        public Usage operations() {
            return values.operations();
        }

        /// The writes of any packet that the send queue accepted and has not written yet,
        /// against its limit.
        public Usage queuedWrites() {
            return values.queuedWrites();
        }

        /// The bytes of the writes counted by [#queuedWrites()], against the send queue's limit.
        public Usage queuedBytes() {
            return values.queuedBytes();
        }

        /// QoS 1 publications that await PUBACK, including those that wait to be sent.
        public long awaitingPubAck() {
            return values.awaitingPubAck();
        }

        /// QoS 2 publications that await PUBREC, including those that wait to be sent.
        public long awaitingPubRec() {
            return values.awaitingPubRec();
        }

        /// QoS 2 exchanges that await PUBCOMP.
        public long awaitingPubComp() {
            return values.awaitingPubComp();
        }

        /// All unfinished QoS 1 and QoS 2 publications, against
        /// [MqttClientBuilder#maxInflightPublishes(int)].
        public Usage publications() {
            return values.publications();
        }

        /// The bytes that unfinished publications keep, against their limit: a persistent
        /// session keeps a publication until PUBACK or PUBREC, and a nonpersistent one only
        /// while it waits to be sent.
        public Usage keptBytes() {
            return values.keptBytes();
        }

        /// How many more QoS 1 and QoS 2 publications the established MQTT 5.0 connection lets
        /// the client have in progress, within the broker's Receive Maximum; empty without an
        /// established MQTT 5.0 connection.
        public OptionalInt quota() {
            Integer quota = values.quota();
            return quota == null ? OptionalInt.empty() : OptionalInt.of(quota);
        }

        /// How long the oldest response that the client awaits, a PUBACK, PUBREC, PUBCOMP,
        /// SUBACK, or UNSUBACK, has been awaited since the write that it answers completed;
        /// empty if none is awaited. The response timeout of
        /// [MqttClientBuilder#responseTimeout(Duration)] counts from the same time.
        public Optional<Duration> oldestResponseWait() {
            return Optional.ofNullable(values.oldestResponseWait());
        }
    }

    /// The subscription registrations and delivery lanes, in total.
    public static final class Subscribing {

        private final ClientSnapshot.Subscribing values;

        private Subscribing(ClientSnapshot.Subscribing values) {
            this.values = values;
        }

        /// The registrations that the client has processed, against
        /// [MqttClientBuilder#maxSubscriptions(int)]: the sum of [#established()],
        /// [#pending()], and [#removing()].
        public Usage registrations() {
            return values.registrations();
        }

        /// Registrations that the broker has granted.
        public long established() {
            return values.established();
        }

        /// Registrations that wait for a connection or for the broker's SUBACK.
        public long pending() {
            return values.pending();
        }

        /// Registrations whose removal waits for the broker's UNSUBACK or for a connection that
        /// discards the session.
        public long removing() {
            return values.removing();
        }

        /// The delivery lanes, including the lanes of stopped subscriptions whose handlers have
        /// not returned, against [MqttClientBuilder#maxSubscriptions(int)].
        public Usage lanes() {
            return values.lanes();
        }
    }

    /// The values of one subscription.
    public static final class SubscriptionStats {

        private final ClientSnapshot.SubscriptionValues values;

        private SubscriptionStats(ClientSnapshot.SubscriptionValues values) {
            this.values = values;
        }

        /// The subscription's handle.
        public Subscription subscription() {
            return values.subscription();
        }

        public TopicFilter filter() {
            return values.filter();
        }

        /// Deliveries that wait for a handler, an acceptor, or a receiver.
        public long waiting() {
            return values.waiting();
        }

        /// Deliveries that a handler or a hand-off acceptor is processing.
        public long running() {
            return values.running();
        }

        /// Deliveries that a handler, an acceptor, or a receiver has processed, and whose
        /// [MqttDelivery#ack()] has not been called yet.
        public long awaitingAck() {
            return values.awaitingAck();
        }

        /// [Subscribe#bufferCapacity()].
        public int bufferCapacity() {
            return values.bufferCapacity();
        }

        /// Lifetime counts of the subscription's deliveries that left it unprocessed, by reason.
        public Map<DropReason, Long> dropped() {
            return values.dropped();
        }
    }
}
