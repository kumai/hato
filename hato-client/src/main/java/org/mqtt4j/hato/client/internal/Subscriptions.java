package org.mqtt4j.hato.client.internal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttSubscribeException;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.SessionCore.State;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
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

    // Broker registrations outlive locally closed handles until UNSUBACK or a CONNACK without
    // Session Present, so they are bounded apart from the delivery lanes that local closure
    // releases. The mailbox bounds registrations still queued for the actor by the same limit.
    static final int REGISTRATION_LIMIT = 256;

    // Removals use no operation slots. Bound their exchanges so a batch of closures cannot
    // flood the actor with send completions and UNSUBACKs.
    private static final int CLOSE_INFLIGHT_LIMIT = 16;

    // Registrations use no operation slots either. Bound their pending SUBACKs apart from
    // removals, so many retained subscriptions cannot flood the actor.
    private static final int SUBSCRIBE_INFLIGHT_LIMIT = 16;

    private final SessionCore.Port port;

    private final SessionCore.Context core;

    private final String clientId;

    private final boolean cleanStart;

    // The client's registrations by id, in registration order.
    private final Map<Long, LocalSubscription> registry = new LinkedHashMap<>();

    // The current connection's exchanges; a connection end replaces them.
    private Window window = new Window();

    Subscriptions(
            SessionCore.Port port,
            SessionCore.Context core,
            String clientId,
            boolean cleanStart
    ) {
        this.port = port;
        this.core = core;
        this.clientId = clientId;
        this.cleanStart = cleanStart;
    }

    void subscribe(long operation, Subscribe request) {
        if (core.state() == State.CLOSED) {
            port.complete(operation, null, new IllegalStateException("Client is closed"));
            return;
        }
        if (registry.values().stream()
                .anyMatch(local -> local.filter.equals(request.filter()))) {
            port.complete(operation, null, new IllegalStateException(
                    "Subscription already exists: " + request.filter().pattern()
            ));
            return;
        }
        Optional<SharedTopicFilter> sharedFilter = SharedTopicFilter.parse(request.filter());
        boolean shared = sharedFilter.isPresent();
        TopicFilter matchingFilter = sharedFilter.map(SharedTopicFilter::filter)
                .orElse(request.filter());
        Optional<LocalSubscription> conflict = registry.values().stream()
                .filter(existing -> (shared || existing.shared)
                        && filtersOverlap(matchingFilter, existing.matchingFilter))
                .findFirst();
        if (conflict.isPresent()) {
            port.complete(operation, null, new IllegalStateException(
                    "Subscription " + request.filter().pattern() + " overlaps "
                            + conflict.orElseThrow().filter.pattern()
                            + "; a shared subscription cannot overlap another subscription"
            ));
            return;
        }
        if (registry.size() >= REGISTRATION_LIMIT) {
            port.complete(operation, null, new MqttCapacityException("Subscription capacity"));
            return;
        }

        // Registration precedes send: a broker may publish before sending SUBACK.
        Optional<Subscription> registered = port.registerSubscription(operation);
        if (registered.isEmpty()) {
            port.complete(operation, null, new MqttCapacityException("Subscription capacity"));
            return;
        }
        var local = new LocalSubscription(
                operation,
                request,
                shared,
                matchingFilter,
                registered.orElseThrow()
        );
        registry.put(operation, local);
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
        local.removal = new UnsubscribeOperation(operation, local);
        window.registrations.remove(local);
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

    /// The registrations whose filter matches `topic`, except those being removed.
    List<Long> matching(Topic topic) {
        var matches = new ArrayList<Long>();
        // Matching is O(subscriptions). Replace with a topic tree if 0.4 measurements show
        // this scan dominates, keeping TopicFilter.matches as the reference implementation.
        for (var local : registry.values()) {
            if (local.removal == null && local.matchingFilter.matches(topic)) {
                matches.add(local.id);
            }
        }
        return matches;
    }

    /// Reconciles the registry with the broker after a successful CONNACK and before the core
    /// announces the connection, so connect() and onConnected already find continued
    /// subscriptions established on it.
    void connectionEstablished(boolean sessionPresent) {
        window.sessionPresent = sessionPresent;
        for (LocalSubscription local : List.copyOf(registry.values())) {
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
            if (!removal.subscription.established) {
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
            if (!registry.containsKey(local.id)) {
                window.registrations.removeFirst();
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
        boolean restoring = local.confirmed;
        window.subscribingInFlight--;
        if (refused) {
            var refusal = new MqttSubscribeException(reason);
            if (!restoring) {
                removeSubscription(local, refusal);
            } else {
                registry.remove(local.id);
                port.resubscribeFailed(local.handle, refusal);
            }
            finishRemoval(local);
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
                log.warn("Broker refused to remove the subscription to {} for {}: {}",
                        pending.subscription.filter.pattern(), clientId, reason);
            }
        }
        window.unsubscribing.remove(ack.packetId());
        core.responses().answered(ResponseDeadlines.Kind.UNSUBACK, ack.packetId());
        core.releasePacketId(ack.packetId());
        window.closingInFlight--;
        removeSubscription(pending.subscription, null);
        finishRemoval(pending.subscription);
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
        var packet = new Packet.Unsubscribe(id, List.of(removal.subscription.filter), EMPTY);
        long ticket = core.nextTicket();
        SendQueue.OfferResult result = port.send(core.generation(), ticket, packet, null);
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

    private static Packet.Subscribe subscriptionPacket(LocalSubscription local, int id) {
        return new Packet.Subscribe(id, List.of(new Packet.Subscription(
                local.filter,
                local.requestedQos,
                false,
                false,
                0
        )), EMPTY);
    }

    private void removeSubscription(LocalSubscription local, @Nullable Throwable cause) {
        registry.remove(local.id);
        window.registrations.remove(local);
        port.removeSubscription(local.id, local.handle, cause);
    }

    private void finishRemoval(LocalSubscription local) {
        UnsubscribeOperation removal = local.removal;
        if (removal != null) {
            window.queuedClosures.remove(removal);
            if (removal.operation != 0) {
                port.complete(removal.operation, null, null);
            }
        }
        local.removal = null;
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
        final ArrayDeque<UnsubscribeOperation> queuedClosures = new ArrayDeque<>();
        // At most 256 broker registrations await queue admission.
        final ArrayDeque<LocalSubscription> registrations = new ArrayDeque<>();
        int subscribingInFlight;
        int closingInFlight;
    }

    private static final class LocalSubscription {

        // A client-lifetime registration id shared with the shell and its delivery lane.
        final long id;
        final TopicFilter filter;
        final TopicFilter matchingFilter;
        final Qos requestedQos;
        final boolean shared;
        final Subscription handle;
        boolean confirmed;
        boolean established;
        Qos granted = Qos.AT_MOST_ONCE;
        @Nullable UnsubscribeOperation removal;

        LocalSubscription(
                long id,
                Subscribe request,
                boolean shared,
                TopicFilter matchingFilter,
                Subscription handle
        ) {
            this.id = id;
            filter = request.filter();
            requestedQos = request.qos();
            this.shared = shared;
            this.matchingFilter = matchingFilter;
            this.handle = handle;
        }
    }

    /// `operation` is zero when no caller observes the removal.
    private record UnsubscribeOperation(
            long operation,
            LocalSubscription subscription
    ) {
    }
}
