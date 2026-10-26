package org.mqtt4j.hato.client.internal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.IntPredicate;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.CapacityLimit;
import org.mqtt4j.hato.ClientStats.Usage;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttSubscriptionRefusedException;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.RetainHandling;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.SessionCore.State;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.client.internal.common.protocol.SharedTopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mqtt4j.hato.client.internal.SessionCore.isFailure;
import static org.mqtt4j.hato.client.internal.SessionCore.protocolError;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// The session core's part for subscription registrations. It keeps the client's registry,
/// sends SUBSCRIBE and UNSUBSCRIBE in bounded windows, and reconciles the registry with the
/// broker on each connection. The core decides when registrations may use send capacity; this
/// part reaches the rest of the core only through [SessionCore.Context].
final class Subscriptions {

    private static final Log log = Log.of(Subscriptions.class);

    // Removals use no operation slots. Bound their exchanges so a batch of closures cannot
    // flood the actor with send completions and UNSUBACKs.
    private static final int CLOSE_INFLIGHT_LIMIT = 16;

    // Registrations use no operation slots either. Bound their pending SUBACKs apart from
    // removals, so many retained subscriptions cannot flood the actor.
    private static final int SUBSCRIBE_INFLIGHT_LIMIT = 16;

    private static final Comparator<LocalSubscription> REGISTRATION_ORDER =
            Comparator.comparingLong(local -> local.id);

    // The most topics whose registrations the client keeps for publications to come.
    private static final int ROUTE_LIMIT = 64;

    // The largest Subscription Identifier, a variable byte integer of at most four bytes.
    static final int MAX_SUBSCRIPTION_IDENTIFIER = 268_435_455;

    private final SessionCore.Port port;

    private final SessionCore.Context core;

    private final String clientId;

    private final boolean cleanStart;

    // The connection's codec, which sizes SUBSCRIBE and UNSUBSCRIBE against the largest packet
    // that the broker announced.
    private final PacketCodec codec;

    // Broker registrations outlive locally closed handles until UNSUBACK or a CONNACK without
    // Session Present, so they are bounded apart from the delivery lanes that local closure
    // releases. The mailbox bounds registrations still queued for the actor by the same limit.
    private final int registrationLimit;

    // The client's registrations by id, in registration order. The indexes below hold the same
    // registrations, so that no single registration or publication scans them all.
    private final Map<Long, LocalSubscription> registry = new LinkedHashMap<>();

    private final Map<TopicFilter, LocalSubscription> byFilter = new HashMap<>();

    private final Map<Integer, LocalSubscription> byIdentifier = new HashMap<>();

    // Shared subscriptions, which no other subscription may overlap, in registration order.
    private final Set<LocalSubscription> sharedRegistrations = new LinkedHashSet<>();

    // The registrations by the filter that they match topics with.
    private final FilterTree<LocalSubscription> byTopic = new FilterTree<>();

    // The registrations that publications without Subscription Identifiers went to, by topic
    // name, as matching() returned them. Registering, unregistering, and starting or ending a
    // removal empty it. A deliberate simplification: once it holds ROUTE_LIMIT topics it is
    // emptied and refilled, so a stream of ever new topics, such as one per device, pays a map
    // insertion per publication and never hits; a bounded LRU would keep the hot ones.
    private final Map<String, List<Long>> routes = new HashMap<>();

    // Where the search for the next registration's Subscription Identifier starts.
    private int nextIdentifier = 1;

    // The current connection's exchanges; a connection end replaces them.
    private Window window = new Window();

    Subscriptions(
            SessionCore.Port port,
            SessionCore.Context core,
            String clientId,
            boolean cleanStart,
            PacketCodec codec,
            int registrationLimit
    ) {
        this.port = port;
        this.core = core;
        this.clientId = clientId;
        this.cleanStart = cleanStart;
        this.codec = codec;
        this.registrationLimit = registrationLimit;
    }

    void subscribe(long operation, Subscribe request) {
        if (core.state() == State.CLOSED) {
            port.complete(operation, null, new IllegalStateException("Client is closed"));
            return;
        }
        if (byFilter.containsKey(request.filter())) {
            port.complete(operation, null, new IllegalStateException(
                    "Subscription already exists: " + request.filter().pattern()
            ));
            return;
        }
        Optional<SharedTopicFilter> sharedFilter = SharedTopicFilter.parse(request.filter());
        boolean shared = sharedFilter.isPresent();
        TopicFilter matchingFilter = sharedFilter.map(SharedTopicFilter::filter)
                .orElse(request.filter());
        // A shared subscription is compared with every registration, in time that grows with
        // them, and any other one only with the shared subscriptions, which are few.
        Collection<LocalSubscription> candidates = shared ? registry.values() : sharedRegistrations;
        Optional<LocalSubscription> conflict = candidates.stream()
                .filter(existing -> filtersOverlap(matchingFilter, existing.matchingFilter))
                .findFirst();
        if (conflict.isPresent()) {
            port.complete(operation, null, new IllegalStateException(
                    "Subscription " + request.filter().pattern() + " overlaps "
                            + conflict.orElseThrow().filter.pattern()
                            + "; a shared subscription cannot overlap another subscription"
            ));
            return;
        }
        if (registry.size() >= registrationLimit) {
            core.refused(CapacityLimit.SUBSCRIPTIONS);
            port.complete(
                    operation,
                    null,
                    new MqttCapacityException("Subscription capacity", CapacityLimit.SUBSCRIPTIONS)
            );
            return;
        }

        // Registration precedes send: a broker may publish before sending SUBACK.
        Optional<Subscription> registered = port.registerSubscription(operation);
        if (registered.isEmpty()) {
            core.refused(CapacityLimit.SUBSCRIPTIONS);
            port.complete(
                    operation,
                    null,
                    new MqttCapacityException("Subscription capacity", CapacityLimit.SUBSCRIPTIONS)
            );
            return;
        }
        var local = new LocalSubscription(
                operation,
                request,
                shared,
                matchingFilter,
                registered.orElseThrow(),
                allocateIdentifier()
        );
        register(local);
        if (core.state() == State.CONNECTED) {
            window.registrations.addLast(local);
            core.drain();
        }
    }

    void unsubscribe(long operation, long subscriptionId) {
        LocalSubscription local = registry.get(subscriptionId);
        if (local == null) {
            port.complete(operation, null, null);
            return;
        }
        if (local.removal != null) {
            // The shell starts one durable removal per registration, observed by all its callers.
            return;
        }
        // A registration still queued for SUBSCRIBE stays in the queue, which passes over it.
        local.removal = new UnsubscribeOperation(operation, local);
        routes.clear();
        if (core.state() != State.CONNECTED) {
            if (cleanStart) {
                removeSubscription(local, null);
                finishRemoval(local);
            }
            return;
        }
        if (window.sessionPresent && !window.subscribing.containsValue(local)) {
            // An unsent restoration cannot prove that a persistent broker has no registration.
            local.established = true;
        }
        if (!local.established && !window.subscribing.containsValue(local)) {
            removeSubscription(local, null);
            finishRemoval(local);
        } else {
            window.queuedClosures.addLast(local.removal);
            drainClosures();
        }
        core.drain();
    }

    /// The registrations to which `publish` goes, except those being removed. On a connection
    /// with Subscription Identifiers, a publication that carries identifiers goes to the
    /// registrations that they name, without matching their filters, and to those that the
    /// broker holds without an identifier and whose filter matches its topic. Any other
    /// publication goes to the registrations whose filter matches its topic.
    List<Long> matching(Packet.Publish publish) {
        List<Integer> identifiers = core.subscriptionIdentifiers()
                ? identifiers(publish.properties())
                : List.of();
        if (!identifiers.isEmpty()) {
            return matching(publish.topic(), identifiers);
        }
        // Without identifiers, the answer depends on the topic and the registrations alone, and
        // most streams repeat a few topics, so the answer for a topic is kept until the
        // registrations change.
        String name = publish.topic().name();
        List<Long> route = routes.get(name);
        if (route == null) {
            if (routes.size() == ROUTE_LIMIT) {
                routes.clear();
            }
            route = List.copyOf(matching(publish.topic(), identifiers));
            routes.put(name, route);
        }
        return route;
    }

    /// The registrations to which a publication to `topic` that carries `identifiers` goes, as
    /// [#matching(Packet.Publish)] describes.
    private List<Long> matching(Topic topic, List<Integer> identifiers) {
        var matches = new ArrayList<LocalSubscription>();
        if (identifiers.isEmpty()) {
            byTopic.match(topic, matches::add);
        } else {
            for (int identifier : identifiers) {
                LocalSubscription named = byIdentifier.get(identifier);
                if (named != null) {
                    matches.add(named);
                }
            }
            // A broker that sends one copy for overlapping subscriptions names only those that
            // it holds with an identifier, so the others still need their filters matched.
            byTopic.match(topic, local -> {
                if (local.heldWithoutIdentifier) {
                    matches.add(local);
                }
            });
        }
        // Most publications match one registration, which needs no ordering.
        if (matches.size() == 1) {
            LocalSubscription only = matches.getFirst();
            return only.removal == null ? List.of(only.id) : List.of();
        }
        // Registration order, which is the order of the ids, decides the order in which the
        // lanes take their copies; a registration named twice, or named and matched, goes once.
        matches.sort(REGISTRATION_ORDER);
        var ids = new ArrayList<Long>(matches.size());
        for (LocalSubscription local : matches) {
            if (local.removal == null && (ids.isEmpty() || ids.getLast() != local.id)) {
                ids.add(local.id);
            }
        }
        return ids;
    }

    /// Reconciles the registry with the broker after a successful CONNACK and before the core
    /// announces the connection, so connect() and onConnected already find continued
    /// subscriptions established on it.
    void connectionEstablished(boolean sessionPresent) {
        window.sessionPresent = sessionPresent;
        for (LocalSubscription local : List.copyOf(registry.values())) {
            if (!sessionPresent) {
                // A new session holds none of the client's subscriptions yet.
                local.heldWithoutIdentifier = false;
            }
            if (local.removal != null) {
                if (sessionPresent) {
                    local.established = true;
                    window.queuedClosures.addLast(local.removal);
                } else {
                    removeSubscription(local, null);
                    finishRemoval(local);
                }
            } else if (!sessionPresent || !local.established) {
                local.established = false;
                window.registrations.addLast(local);
            } else {
                port.subscribed(local.handle, local.granted);
            }
        }
    }

    /// Settles the registry when a connection or attempt ends. A permanent end removes every
    /// registration with `cause`; a nonpersistent session forgets broker registrations and
    /// completes pending removals. Identifiers borrowed by the ended connection return to the
    /// session.
    void connectionEnded(boolean permanent, @Nullable Throwable cause) {
        for (LocalSubscription local : List.copyOf(registry.values())) {
            if (permanent) {
                // The shell completes pending removals after shutdown, because only it can tell
                // an ordinary close from an internal failure.
                removeSubscription(local, cause);
            } else if (cleanStart) {
                local.established = false;
                if (local.removal != null) {
                    removeSubscription(local, null);
                    finishRemoval(local);
                }
            }
        }
        // SUBSCRIBE and UNSUBSCRIBE return their borrowed identifiers. The rest of the ended
        // connection, such as its queues and windows, goes with it.
        window.subscribing.keySet().forEach(core::releasePacketId);
        window.unsubscribing.keySet().forEach(core::releasePacketId);
        window = new Window();
    }

    void drainClosures() {
        // DISCONNECT stays last, even when a PUBACK arrives while it is pending.
        if (core.state() != State.CONNECTED) {
            return;
        }
        // A removal waits only for its own SUBACK, so the drain passes over those still awaiting
        // one; at most 16 SUBACKs are pending, which bounds the scan. A capacity refusal stops
        // the drain, so the refused removal stays ahead of later ones.
        Iterator<UnsubscribeOperation> removals = window.queuedClosures.iterator();
        while (window.closingInFlight < CLOSE_INFLIGHT_LIMIT && removals.hasNext()) {
            UnsubscribeOperation removal = removals.next();
            // A removal that finished otherwise leaves the queue when the drain reaches it.
            if (removal.subscription.removal != removal) {
                removals.remove();
                continue;
            }
            if (!removal.subscription.established) {
                continue;
            }
            LocalSubscription local = removal.subscription;
            if (exceedsLimit(unsubscribePacket(local, 1))) {
                // The UNSUBSCRIBE for a filter is smaller than its SUBSCRIBE, which an earlier
                // connection sent, so only a continued session whose broker announced a smaller
                // limit gets here. As after a refusing UNSUBACK, the broker may still hold the
                // subscription, and its messages then match no registration.
                log.warn("Removing the subscription to {} for {} locally: its UNSUBSCRIBE exceeds "
                        + "the broker's Maximum Packet Size, and the broker may still hold it",
                        local.filter.pattern(), clientId);
                removals.remove();
                removeSubscription(local, null);
                finishRemoval(local);
                continue;
            }
            if (!sendUnsubscribe(removal)) {
                return;
            }
            removals.remove();
            window.closingInFlight++;
        }
    }

    /// Sends queued registrations within the SUBACK window. The core calls this after replays
    /// and PUBREC responses, so registrations never delay a session's replay.
    void sendRegistrations() {
        while (!window.registrations.isEmpty()) {
            if (window.subscribingInFlight >= SUBSCRIBE_INFLIGHT_LIMIT) {
                return;
            }
            LocalSubscription local = window.registrations.getFirst();
            // A removed registration, or one whose removal has begun, leaves the queue here.
            if (!registry.containsKey(local.id) || local.removal != null) {
                window.registrations.removeFirst();
                continue;
            }
            Optional<ReasonCode> refusal = refusal(local);
            if (refusal.isPresent()) {
                window.registrations.removeFirst();
                refuse(local, refusal.orElseThrow(), null);
                continue;
            }
            OptionalInt allocated = core.allocatePacketId();
            if (allocated.isEmpty()) {
                return;
            }
            int id = allocated.getAsInt();
            window.subscribing.put(id, local);
            long ticket = core.nextTicket();
            SendQueue.OfferResult result = port.send(
                    core.generation(),
                    ticket,
                    subscriptionPacket(local, id),
                    null
            );
            if (result == SendQueue.OfferResult.FULL) {
                window.subscribing.remove(id);
                core.releasePacketId(id);
                return;
            }
            if (result == SendQueue.OfferResult.ACCEPTED) {
                core.responses().add(ticket, ResponseDeadlines.Kind.SUBACK, id);
            }
            local.heldWithoutIdentifier = !core.subscriptionIdentifiers();
            window.registrations.removeFirst();
            window.subscribingInFlight++;
        }
    }

    void subscribed(Packet.SubAck ack) {
        LocalSubscription local = window.subscribing.get(ack.packetId());
        if (local == null) {
            core.fail(protocolError("Unexpected SUBACK"));
            return;
        }
        if (ack.reasonCodes().size() != 1) {
            abandon(local, "Unexpected SUBACK for " + local.filter.pattern());
            return;
        }
        ReasonCode reason = ack.reasonCodes().getFirst();
        boolean refused = isFailure(reason);
        if (!refused && reason.value() > local.requestedQos.level()) {
            abandon(local, "SUBACK grants an unsupported QoS for " + local.filter.pattern());
            return;
        }
        window.subscribing.remove(ack.packetId());
        core.responses().answered(ResponseDeadlines.Kind.SUBACK, ack.packetId());
        core.releasePacketId(ack.packetId());
        window.subscribingInFlight--;
        if (refused) {
            refuse(
                    local,
                    reason,
                    ConnectionEstablishment.text(ack.properties(), PropertyId.REASON_STRING)
                            .orElse(null)
            );
        } else {
            local.confirmed = true;
            local.established = true;
            local.granted = Qos.of(reason.value());
            port.subscribed(local.handle, local.granted);
        }
    }

    void unsubscribed(Packet.UnsubAck ack) {
        UnsubscribeOperation pending = window.unsubscribing.get(ack.packetId());
        // MQTT 3.1.1 carries no reason code, and MQTT 5.0 one for the single filter sent.
        if (pending == null || ack.reasonCodes().size() > 1) {
            core.fail(protocolError("Unexpected UNSUBACK"));
            return;
        }
        // Any answer confirms the removal, as a refused SUBACK confirms that there is nothing
        // to remove. After a refusal the broker may still hold the subscription.
        for (ReasonCode reason : ack.reasonCodes()) {
            if (isFailure(reason)) {
                log.warn("Broker refused to remove the subscription to {} for {}: {} ({})",
                        pending.subscription.filter.pattern(), clientId, reason,
                        ConnectionEstablishment.text(ack.properties(), PropertyId.REASON_STRING)
                                .orElse("no reason string"));
            }
        }
        window.unsubscribing.remove(ack.packetId());
        core.responses().answered(ResponseDeadlines.Kind.UNSUBACK, ack.packetId());
        core.releasePacketId(ack.packetId());
        window.closingInFlight--;
        removeSubscription(pending.subscription, null);
        finishRemoval(pending.subscription);
    }

    /// Ends a registration that the broker refused, or would refuse by the limits that it
    /// announced for the connection. A first registration fails with the refusal; one that a
    /// continued or new session re-creates is reported as a failed re-creation.
    private void refuse(
            LocalSubscription local,
            ReasonCode reason,
            @Nullable String reasonString
    ) {
        var refusal = new MqttSubscriptionRefusedException(reason, reasonString);
        if (!local.confirmed) {
            removeSubscription(local, refusal);
        } else {
            unregister(local);
            port.resubscribeFailed(local.handle, refusal);
        }
        finishRemoval(local);
    }

    /// The reason code with which the broker would refuse the subscription `local`, by the limits
    /// that it announced for the connection, or empty when it would accept it. A broker that
    /// receives such a SUBSCRIBE ends the connection with the same reason code (MQTT 5.0 sections
    /// 3.2.2.3.6, 3.2.2.3.11, and 3.2.2.3.13).
    private Optional<ReasonCode> refusal(LocalSubscription local) {
        ConnAck limits = core.limits();
        Optional<SharedTopicFilter> shared = SharedTopicFilter.parse(local.filter);
        if (shared.isPresent() && !limits.sharedSubscriptionAvailable()) {
            return Optional.of(ReasonCode.SHARED_SUBSCRIPTIONS_NOT_SUPPORTED);
        }
        String pattern = shared.map(subscription -> subscription.filter().pattern())
                .orElse(local.filter.pattern());
        if (!limits.wildcardSubscriptionAvailable()
                && (pattern.contains("+") || pattern.contains("#"))) {
            return Optional.of(ReasonCode.WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED);
        }
        // The packet identifier takes two bytes whatever its value.
        if (exceedsLimit(subscriptionPacket(local, 1))) {
            return Optional.of(ReasonCode.PACKET_TOO_LARGE);
        }
        return Optional.empty();
    }

    /// Whether `packet` is larger than the largest packet that the broker announced for the
    /// connection; MQTT 3.1.1 brokers announce none. Only the packets whose size the application
    /// decides are compared with the limit: PUBLISH in [Publications], and SUBSCRIBE and
    /// UNSUBSCRIBE here. The others take a few bytes, and the client could not refuse them.
    private boolean exceedsLimit(Packet packet) {
        int limit = core.limits().maximumPacketSize();
        return limit < ConnectionEstablishment.LARGEST_PACKET_SIZE
                && codec.encode(packet).length > limit;
    }

    /// A SUBACK that violates the protocol ends its registration as well as the connection, so
    /// a later connection does not repeat the same SUBSCRIBE. A persistent broker may still hold
    /// the subscription, so its removal stays pending, as after a local close.
    private void abandon(LocalSubscription local, String message) {
        var violation = new MqttProtocolException(message, ReasonCode.PROTOCOL_ERROR);
        if (cleanStart) {
            // Ending this connection also ends its nonpersistent session.
            removeSubscription(local, violation);
            finishRemoval(local);
        } else {
            port.removeSubscription(local.id, local.handle, violation);
            if (local.removal == null) {
                // No caller observes this removal; it only keeps the filter until confirmed.
                local.removal = new UnsubscribeOperation(0, local);
                routes.clear();
            }
        }
        core.fail(protocolError(message));
    }

    /// Returns whether the send queue admitted the packet. A refusal for packet identifier or
    /// send queue capacity releases the identifier and leaves no pending UNSUBACK entry. CLOSED
    /// counts as admitted and waits for the connection's terminal cause, like other admitted
    /// protocol operations.
    private boolean sendUnsubscribe(UnsubscribeOperation removal) {
        OptionalInt allocated = core.allocatePacketId();
        if (allocated.isEmpty()) {
            return false;
        }
        int id = allocated.getAsInt();
        window.unsubscribing.put(id, removal);
        long ticket = core.nextTicket();
        SendQueue.OfferResult result = port.send(
                core.generation(),
                ticket,
                unsubscribePacket(removal.subscription, id),
                null
        );
        if (result == SendQueue.OfferResult.FULL) {
            window.unsubscribing.remove(id);
            core.releasePacketId(id);
            return false;
        }
        if (result == SendQueue.OfferResult.ACCEPTED) {
            core.responses().add(ticket, ResponseDeadlines.Kind.UNSUBACK, id);
        }
        return true;
    }

    private Packet.Subscribe subscriptionPacket(LocalSubscription local, int id) {
        int retainHandling = switch (local.retainHandling) {
            case SEND -> 0;
            case SEND_IF_NEW_SUBSCRIPTION -> 1;
            case DO_NOT_SEND -> 2;
        };
        // The broker returns the identifier with each message for the subscription, which tells
        // apart the copies that it sends for overlapping subscriptions.
        Properties properties = core.subscriptionIdentifiers()
                ? new Properties(List.of(new Property.VariableByteValue(
                        PropertyId.SUBSCRIPTION_IDENTIFIER,
                        local.identifier
                )))
                : EMPTY;
        return new Packet.Subscribe(id, List.of(new Packet.Subscription(
                local.filter,
                local.requestedQos,
                local.noLocal,
                local.retainAsPublished,
                retainHandling
        )), properties);
    }

    private static Packet.Unsubscribe unsubscribePacket(LocalSubscription local, int id) {
        return new Packet.Unsubscribe(id, List.of(local.filter), EMPTY);
    }

    /// The Subscription Identifiers that `properties` carry, in their order.
    private static List<Integer> identifiers(Properties properties) {
        var identifiers = new ArrayList<Integer>();
        for (Property property : properties.entries()) {
            if (property instanceof Property.VariableByteValue(var id, var value)
                    && id == PropertyId.SUBSCRIPTION_IDENTIFIER) {
                identifiers.add(value);
            }
        }
        return identifiers;
    }

    /// Takes the next Subscription Identifier that no registration uses. Identifiers only
    /// increase, so a message that the broker still holds for a removed registration does not
    /// reach a newer one.
    private int allocateIdentifier() {
        int identifier = freeIdentifier(nextIdentifier, byIdentifier::containsKey);
        nextIdentifier = identifier == MAX_SUBSCRIPTION_IDENTIFIER ? 1 : identifier + 1;
        return identifier;
    }

    /// The first identifier that is not `inUse`, searching from `start` on and continuing from 1
    /// past the largest. The registration limit stays within the identifiers, so one is free;
    /// identifiers only increase, so the search usually ends at `start`.
    static int freeIdentifier(int start, IntPredicate inUse) {
        int candidate = start;
        while (inUse.test(candidate)) {
            candidate = candidate == MAX_SUBSCRIPTION_IDENTIFIER ? 1 : candidate + 1;
        }
        return candidate;
    }

    /// Records `local` in the registry and in the indexes that find it by filter, identifier,
    /// and topic.
    private void register(LocalSubscription local) {
        registry.put(local.id, local);
        byFilter.put(local.filter, local);
        byIdentifier.put(local.identifier, local);
        if (local.shared) {
            sharedRegistrations.add(local);
        }
        byTopic.add(local.matchingFilter, local);
        routes.clear();
    }

    /// Removes `local` from the registry and its indexes, if it is still there. The queues of
    /// the connection's window pass over it.
    private void unregister(LocalSubscription local) {
        if (registry.remove(local.id) == null) {
            return;
        }
        byFilter.remove(local.filter);
        byIdentifier.remove(local.identifier);
        sharedRegistrations.remove(local);
        byTopic.remove(local.matchingFilter, local);
        routes.clear();
    }

    private void removeSubscription(LocalSubscription local, @Nullable Throwable cause) {
        unregister(local);
        port.removeSubscription(local.id, local.handle, cause);
    }

    /// Completes the pending removal of `local`, if any. A queued removal leaves the queue when
    /// the drain reaches it.
    private void finishRemoval(LocalSubscription local) {
        UnsubscribeOperation removal = local.removal;
        if (removal != null && removal.operation != 0) {
            port.complete(removal.operation, null, null);
        }
        local.removal = null;
        routes.clear();
    }

    /// Whether both filters can match at least one valid, nonempty MQTT topic.
    private static boolean filtersOverlap(TopicFilter first, TopicFilter second) {
        String[] left = first.pattern().split("/", -1);
        String[] right = second.pattern().split("/", -1);
        boolean leftTail = left[left.length - 1].equals("#");
        boolean rightTail = right[right.length - 1].equals("#");
        int leftMinimum = left.length - (leftTail ? 1 : 0);
        int rightMinimum = right.length - (rightTail ? 1 : 0);
        int levels = Math.max(1, Math.max(leftMinimum, rightMinimum));
        if ((!leftTail && leftMinimum != levels) || (!rightTail && rightMinimum != levels)) {
            return false;
        }

        // Build the shortest common topic's byte count. A trailing # supplies unrestricted
        // remaining levels; an unconstrained + level can be empty, including the first one.
        int bytes = levels - 1;
        boolean firstLevelFlexible = false;
        for (int i = 0; i < levels; i++) {
            String a = i < leftMinimum ? left[i] : "+";
            String b = i < rightMinimum ? right[i] : "+";
            if (i == 0 && ((a.startsWith("$") && b.equals("+"))
                    || (b.startsWith("$") && a.equals("+")))) {
                return false;
            }
            if (!a.equals("+") && !b.equals("+") && !a.equals(b)) {
                return false;
            }
            String literal = a.equals("+") ? b : a;
            if (literal.equals("+")) {
                firstLevelFlexible |= i == 0;
            } else {
                bytes += literal.getBytes(UTF_8).length;
            }
        }

        // An empty candidate is valid only if its first level can become nonempty, or both
        // trailing # wildcards allow adding another empty level to form the topic "/".
        return bytes <= 65535
                && (bytes > 0 || firstLevelFlexible || (leftTail && rightTail));
    }

    // The exchanges of one connection.
    private static final class Window {

        // Session Present of the connection's CONNACK. Broker registrations can survive even
        // when their last local operation was unconfirmed.
        boolean sessionPresent;
        final Map<Integer, LocalSubscription> subscribing = new HashMap<>();
        final Map<Integer, UnsubscribeOperation> unsubscribing = new HashMap<>();
        // At most one removal per registered subscription; the registry supplies the total bound.
        // A removal that finished otherwise stays until the drain passes it.
        final ArrayDeque<UnsubscribeOperation> queuedClosures = new ArrayDeque<>();
        // Registrations that await queue admission for SUBSCRIBE, at most the registration limit.
        // A removed one stays until the sending passes it.
        final ArrayDeque<LocalSubscription> registrations = new ArrayDeque<>();
        int subscribingInFlight;
        int closingInFlight;
    }

    /// The registrations' values in [ClientSnapshot.Subscribing] that the core knows.
    Values values() {
        long established = 0;
        long pending = 0;
        long removing = 0;
        for (LocalSubscription local : registry.values()) {
            if (local.removal != null) {
                removing++;
            } else if (local.established) {
                established++;
            } else {
                pending++;
            }
        }
        return new Values(
                new Usage(registry.size(), registrationLimit),
                established,
                pending,
                removing
        );
    }

    /// @param registrations the registrations, against their limit
    /// @param established registrations that the broker has granted
    /// @param pending registrations that wait for a connection or for SUBACK
    /// @param removing registrations whose removal waits for the broker
    record Values(Usage registrations, long established, long pending, long removing) {}

    private static final class LocalSubscription {

        // A client-lifetime registration id shared with the shell and its delivery lane.
        final long id;
        final TopicFilter filter;
        final TopicFilter matchingFilter;
        final Qos requestedQos;
        // The subscription options, which every SUBSCRIBE for this registration carries.
        final boolean noLocal;
        final boolean retainAsPublished;
        final RetainHandling retainHandling;
        final boolean shared;
        final Subscription handle;
        // Sent in every SUBSCRIBE for this registration on a connection with Subscription
        // Identifiers, and unique among the registrations.
        final int identifier;
        boolean confirmed;
        boolean established;
        // Whether the session's broker holds the subscription without the identifier: its last
        // SUBSCRIBE went on a connection without Subscription Identifiers. A later connection
        // that accepts them can continue the session with it.
        boolean heldWithoutIdentifier;
        Qos granted = Qos.AT_MOST_ONCE;
        @Nullable UnsubscribeOperation removal;

        LocalSubscription(
                long id,
                Subscribe request,
                boolean shared,
                TopicFilter matchingFilter,
                Subscription handle,
                int identifier
        ) {
            this.id = id;
            filter = request.filter();
            requestedQos = request.qos();
            noLocal = request.noLocal();
            retainAsPublished = request.retainAsPublished();
            retainHandling = request.retainHandling();
            this.shared = shared;
            this.matchingFilter = matchingFilter;
            this.handle = handle;
            this.identifier = identifier;
        }
    }

    /// `operation` is zero when no caller observes the removal.
    private record UnsubscribeOperation(
            long operation,
            LocalSubscription subscription
    ) {
    }
}
