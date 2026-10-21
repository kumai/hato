package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.FailurePolicy;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttSubscriptionRefusedException;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.client.internal.common.log.Log;

import static java.util.Objects.requireNonNull;

/// Each push subscription has a dispatcher, an internal virtual thread that takes its deliveries
/// in receive order. For serial delivery it executes each handler in place. For application
/// executors it moves each delivery to its executor's sublane, asking the application's selector
/// for the executor when the subscription uses lanes. From each sublane, one drain task at a
/// time, run by the executor, claims and executes the head and then asks the executor again.
/// Pull subscriptions use their callers' threads. Hand-off subscriptions share one hand-off
/// worker, an internal virtual thread that offers each delivery to its subscription's acceptor,
/// oldest first across all of them. Reservations include executing handlers and survive
/// connection changes until they return.
final class DeliveryLanes {

    private static final Log log = Log.of(DeliveryLanes.class);

    // Bounds per-delivery overhead across all subscriptions (about 240 bytes each when measured);
    // the byte limit bounds payloads. Keep it high enough not to override a subscription's own
    // buffer capacity in ordinary use.
    private static final int DELIVERY_LIMIT = 65_536;

    // One lock keeps fan-out reservations atomic; each lane's condition wakes only that lane's
    // dispatcher or receivers.
    private final ReentrantLock lock = new ReentrantLock();

    private final Map<Long, Lane> lanes = new HashMap<>();

    private final Map<Long, RetainedDelivery> acknowledging = new HashMap<>();

    // Each core-admitted QoS 1 or QoS 2 receipt enters once. The core's 4096-receipt limit also
    // bounds this metadata-only queue, including receipts with no local recipient.
    private final Set<Long> readyTokens = new LinkedHashSet<>();

    private final String clientId;

    private final long byteLimit;

    // The lanes that may be retained, including stopped lanes whose handlers have not returned.
    private final int laneLimit;

    // True when the failure ends the current connection or the client; the lane then runs
    // nothing more until connection cleanup or removal. An old receipt returns false.
    private final BiPredicate<DeliveryToken, Throwable> onFailure;

    // Ends, as overload, the connection that delivered a QoS 1 or 2 delivery whose acceptor
    // refused it. Called without the lock.
    private final Consumer<DeliveryToken> onOverload;

    private final Runnable onReady;

    // Called once per lane, without the lock, when the lane can run and hand off nothing more: a
    // push lane's dispatcher has ended it, or a pull lane has stopped.
    private final LongConsumer onTerminated;

    // Lanes that ended under the lock. Their terminations run after it is released.
    private final List<Lane> terminatedLanes = new ArrayList<>();

    // Set before the client's close() returns: no lane claims a delivery afterwards.
    private boolean handOffsClosed;

    // The hand-off worker waits here. Every hand-off lane uses it as its own condition, so
    // whatever wakes a lane wakes the worker.
    private final Condition handOffReady = lock.newCondition();

    // Whether the hand-off worker runs. A hand-off lane starts it, and it ends once no hand-off
    // lane is left or the client's close has ended hand-off; the next hand-off lane starts a
    // new one.
    private boolean handOffStarted;

    // Numbers queued deliveries in receive order across all lanes, so the hand-off worker can
    // take the oldest head.
    private long enqueued;

    private int retainedLanes;

    private int retainedDeliveries;

    private long retainedBytes;

    // Deliveries that left their subscriptions unprocessed, by reason, across all lanes.
    private final long[] dropped = new long[Dropped.values().length];

    private long handledDeliveries;

    private long reportedFailures;

    private long placedDeliveries;

    DeliveryLanes(
            String clientId,
            long byteLimit,
            int laneLimit,
            BiPredicate<DeliveryToken, Throwable> onFailure,
            Consumer<DeliveryToken> onOverload,
            Runnable onReady,
            LongConsumer onTerminated
    ) {
        this.clientId = clientId;
        this.byteLimit = byteLimit;
        this.laneLimit = laneLimit;
        this.onFailure = onFailure;
        this.onOverload = onOverload;
        this.onReady = onReady;
        this.onTerminated = onTerminated;
    }

    /// A serial push lane whose dispatcher executes each handler in place.
    boolean register(long id, Subscribe request, Consumer<MqttDelivery> handler) {
        return register(id, request, new HandlerExecution.Internal(), handler);
    }

    boolean register(
            long id,
            Subscribe request,
            HandlerExecution execution,
            Consumer<MqttDelivery> handler
    ) {
        return registerLane(new Lane(
                id,
                request.bufferCapacity(),
                request.ackMode().orElse(AckMode.AFTER_HANDLER),
                request.failurePolicy(),
                execution,
                handler,
                null,
                lock.newCondition()
        ));
    }

    /// A hand-off lane, whose deliveries the client's hand-off worker offers to `acceptor`.
    boolean registerHandOff(long id, Subscribe request, Predicate<MqttDelivery> acceptor) {
        return registerLane(new Lane(
                id,
                request.bufferCapacity(),
                request.ackMode().orElse(AckMode.ON_RECEIVE),
                request.failurePolicy(),
                new HandlerExecution.Internal(),
                null,
                acceptor,
                handOffReady
        ));
    }

    /// A pull handle can wait before the actor admits its routing lane.
    Receiver receiver(long id, Subscribe request) {
        if (request.ackMode().orElse(AckMode.ON_RECEIVE) == AckMode.AFTER_HANDLER) {
            throw new IllegalArgumentException("A pull subscription has no handler to acknowledge");
        }
        return new Receiver(new Lane(
                id,
                request.bufferCapacity(),
                request.ackMode().orElse(AckMode.ON_RECEIVE),
                FailurePolicy.STOP,
                new HandlerExecution.Internal(),
                null,
                null,
                lock.newCondition()
        ));
    }

    boolean register(Receiver receiver) {
        return registerLane(receiver.lane);
    }

    /// Returns false only when the lane limit is full. A push lane starts its dispatcher, and a
    /// hand-off lane starts the hand-off worker unless it runs.
    private boolean registerLane(Lane lane) {
        lock.lock();
        try {
            if (lane.stopped) {
                // A receiver ended before the actor admitted it stays ended, without a lane slot.
                return true;
            }
            if (retainedLanes >= laneLimit) {
                return false;
            }
            if (lanes.containsKey(lane.id)) {
                throw new IllegalStateException(
                        "Subscription lane is already registered: " + lane.id
                );
            }
            lanes.put(lane.id, lane);
            retainedLanes++;
            try {
                if (lane.handler != null) {
                    Thread.ofVirtual()
                            .name("hato-" + clientId + "-delivery-" + lane.id)
                            .start(() -> dispatch(lane));
                } else if (lane.acceptor != null && !handOffStarted) {
                    Thread.ofVirtual()
                            .name("hato-" + clientId + "-hand-off")
                            .start(this::handOff);
                    handOffStarted = true;
                }
            } catch (Throwable failure) {
                lanes.remove(lane.id);
                retainedLanes--;
                throw failure;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /// Stops new local delivery while removal is pending. Queued deliveries still run or are
    /// received; later deliveries are not admitted, so this lane no longer holds back their
    /// acknowledgement.
    void pause(long id) {
        lock.lock();
        try {
            Lane lane = lanes.get(id);
            if (lane != null) {
                lane.paused = true;
            }
        } finally {
            lock.unlock();
        }
    }

    /// Ends a push or hand-off lane whose removal is confirmed once the deliveries it accepted
    /// have run or been handed off: those queued before removal was requested, because a paused
    /// lane admits nothing new. A later [#remove] still discards what remains.
    void finish(long id) {
        lock.lock();
        try {
            Lane lane = lanes.get(id);
            if (lane != null) {
                lane.paused = true;
                lane.finishing = true;
                // A hand-off lane with nothing left to hand off ends here; the worker would not
                // pick it again.
                settle(lane);
                lane.ready.signalAll();
            }
        } finally {
            lock.unlock();
        }
        runTerminations();
    }

    /// Closes hand-off for every lane before the client's close() returns. No dispatcher or
    /// receiver claims a delivery afterwards. Queued deliveries wait for [#close] to discard
    /// them, and handlers that already claimed one may finish.
    void closeHandOffs() {
        lock.lock();
        try {
            handOffsClosed = true;
            for (Lane lane : lanes.values()) {
                lane.ready.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    void remove(long id) {
        remove(id, null);
    }

    void remove(long id, @Nullable Throwable failure) {
        boolean ready = false;
        lock.lock();
        try {
            Lane lane = lanes.get(id);
            if (lane != null) {
                lane.failure = failure;
                ready = stop(lane);
            }
        } finally {
            lock.unlock();
        }
        if (ready) {
            onReady.run();
        }
        runTerminations();
    }

    /// The discarded session's receipt conditions end. A lane paused by one of its handler
    /// failures resumes, because that failure no longer ends a connection.
    void sessionDiscarded() {
        lock.lock();
        try {
            retireAcknowledgements();
            for (Lane lane : lanes.values()) {
                lane.failed = false;
                lane.ready.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /// A withdrawn connection request discards a stop admitted while waiting to reconnect, and no
    /// connection end follows to settle it. Its failed views are released as if the failure had
    /// arrived without a request. Without a connection nothing is queued, so resuming the lanes
    /// hands over no delivery that the stop would have discarded.
    void stopDiscarded() {
        lock.lock();
        try {
            for (RetainedDelivery delivery : acknowledging.values()) {
                for (DeliveryView view : List.copyOf(delivery.views)) {
                    if (!view.valid) {
                        view.released = true;
                        releaseView(view);
                    }
                }
            }
            for (Lane lane : lanes.values()) {
                lane.failed = false;
                lane.ready.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /// Pending views are discarded; a continuing session keeps handed-off receipt conditions.
    Map<Long, Boolean> connectionEnded(boolean preserveSession, boolean stopped) {
        Map<Long, Boolean> result;
        lock.lock();
        try {
            if (!preserveSession) {
                retireAcknowledgements();
            }
            // A finishing pull lane can stop here, which removes it from the map.
            for (Lane lane : List.copyOf(lanes.values())) {
                discard(lane, lane.queue);
                for (Sublane sublane : lane.sublanes.values()) {
                    discard(lane, sublane.queue);
                }
                // A sublane keeps the drain task on its way, so the next connection's deliveries
                // there wait for a handler still running, as on an internal thread.
                lane.sublanes.values().removeIf(sublane -> sublane.scheduled == 0);
                lane.queuedInSublanes = 0;
                lane.failed = false;
                settle(lane);
                lane.ready.signalAll();
            }
            if (preserveSession) {
                for (RetainedDelivery delivery : acknowledging.values()) {
                    for (DeliveryView view : trackedViews(delivery)) {
                        if (!view.valid || (stopped && !view.recipient.ready)) {
                            view.released = true;
                            invalidate(view);
                        }
                    }
                }
            }
            result = receiveStateLocked();
            for (long token : List.copyOf(acknowledging.keySet())) {
                if (!result.containsKey(token)) {
                    retire(requireNonNull(acknowledging.remove(token)));
                }
            }
            readyTokens.clear();
        } finally {
            lock.unlock();
        }
        runTerminations();
        return result;
    }

    /// Includes authoritative readiness after handlers may have finished while disconnected.
    Map<Long, Boolean> receiveState() {
        lock.lock();
        try {
            return receiveStateLocked();
        } finally {
            lock.unlock();
        }
    }

    boolean isReady(long token) {
        lock.lock();
        try {
            RetainedDelivery delivery = acknowledging.get(token);
            return delivery != null && delivery.pending == 0;
        } finally {
            lock.unlock();
        }
    }

    private Map<Long, Boolean> receiveStateLocked() {
        var result = new HashMap<Long, Boolean>();
        for (var entry : acknowledging.entrySet()) {
            RetainedDelivery delivery = entry.getValue();
            if (delivery.recipients.values().stream().anyMatch(recipient -> recipient.handed)) {
                result.put(entry.getKey(), delivery.pending == 0);
            }
        }
        return result;
    }

    /// Ends every local subscription; application handlers already executing may finish.
    void close(@Nullable Throwable failure) {
        lock.lock();
        try {
            retireAcknowledgements();
            for (Lane lane : List.copyOf(lanes.values())) {
                lane.failure = failure == null ? null : new MqttConnectionLostException(failure);
                // Retired receipts leave stopping no acknowledgement condition to complete.
                boolean _ = stop(lane);
            }
        } finally {
            lock.unlock();
        }
        runTerminations();
    }

    /// Caller holds lock. Running handlers retain their payload until they return.
    private void retireAcknowledgements() {
        for (RetainedDelivery delivery : List.copyOf(acknowledging.values())) {
            retire(delivery);
        }
        acknowledging.clear();
        readyTokens.clear();
    }

    /// Caller holds lock. A retired receipt meets no acknowledgement condition, so a late ack()
    /// completes nothing. Every view's ack() is still detached, including accepted hand-off
    /// views that returned their capacity already: a delivery the application keeps after its
    /// receipt ends must not keep the client's lanes reachable through it.
    private void retire(RetainedDelivery delivery) {
        delivery.retired = true;
        for (DeliveryView view : trackedViews(delivery)) {
            view.acknowledgement = null;
            view.released = true;
            releaseView(view);
        }
        delivery.accepted.clear();
    }

    /// Caller holds lock. A copy of the views whose acknowledgement condition the receipt still
    /// tracks: those that hold capacity, and accepted hand-off views, which returned theirs.
    private static List<DeliveryView> trackedViews(RetainedDelivery delivery) {
        var tracked = new ArrayList<DeliveryView>(delivery.views);
        tracked.addAll(delivery.accepted);
        return tracked;
    }

    /// Admits a received publication to its matching lanes. Returns false when it does not fit
    /// and overload must end the connection, which only a QoS 1 or 2 publication does: it
    /// reserves every matching lane together, so overload cannot deliver only part of a fan-out.
    /// A QoS 0 publication carries no acknowledgement token, and overflow drops it instead; see
    /// [#deliverAtMostOnce].
    boolean deliver(
            DeliveryToken token,
            List<Long> ids,
            MqttMessage message,
            boolean dup,
            int bytes
    ) {
        if (token.acknowledgement() == 0) {
            deliverAtMostOnce(token, ids, message, dup, bytes);
            return true;
        }
        boolean ready;
        lock.lock();
        try {
            var recipients = new ArrayList<Lane>();
            var states = new ArrayList<Recipient>();
            var abandoned = new ArrayList<Recipient>();
            RetainedDelivery delivery = acknowledging.get(token.acknowledgement());
            boolean first = delivery == null;
            if (first) {
                for (long id : ids) {
                    Lane lane = lanes.get(id);
                    // A lane awaiting removal admits nothing new, so it does not hold back the
                    // acknowledgement of QoS 1 and 2.
                    if (lane != null && !lane.paused) {
                        recipients.add(lane);
                        states.add(new Recipient(id));
                    }
                }
                delivery = new RetainedDelivery(token, bytes);
            } else {
                for (Recipient recipient : requireNonNull(delivery).recipients.values()) {
                    if (!recipient.handed && !recipient.ready && !recipient.queued) {
                        Lane lane = lanes.get(recipient.id);
                        // A removed lane, or one awaiting removal, takes no retransmission.
                        if (lane == null || lane.paused) {
                            abandoned.add(recipient);
                        } else {
                            recipients.add(lane);
                            states.add(recipient);
                        }
                    }
                }
            }
            for (Lane lane : recipients) {
                // A pending failure owns termination; don't replace it with a capacity failure.
                if (lane.retained >= lane.capacity && !lane.failed) {
                    return false;
                }
            }
            RetainedDelivery retained = requireNonNull(delivery);
            long addedBytes = recipients.isEmpty() || !retained.views.isEmpty()
                    ? 0 : retained.bytes;
            if (!fits(recipients.size(), addedBytes)) {
                return false;
            }
            if (first) {
                acknowledging.put(token.acknowledgement(), retained);
            }
            for (Recipient recipient : abandoned) {
                recipient.ready = true;
                retained.pending--;
            }
            if (!recipients.isEmpty()) {
                if (retained.message == null) {
                    retained.message = message;
                }
                retainedBytes += addedBytes;
                retainedDeliveries += recipients.size();
                for (int index = 0; index < recipients.size(); index++) {
                    Lane lane = recipients.get(index);
                    Recipient recipient = states.get(index);
                    if (first) {
                        retained.recipients.put(recipient.id, recipient);
                        retained.pending++;
                    }
                    var view = new DeliveryView(
                            retained,
                            recipient,
                            lane,
                            requireNonNull(retained.message),
                            dup,
                            token
                    );
                    if (lane.ackMode == AckMode.MANUAL) {
                        view.acknowledgement = () -> acknowledge(view);
                    }
                    enqueue(lane, view);
                }
            }
            ready = recipients.isEmpty() && retained.pending == 0;
            if (ready) {
                readyTokens.add(token.acknowledgement());
            }
        } finally {
            lock.unlock();
        }
        if (ready) {
            onReady.run();
        }
        return true;
    }

    /// Admits a QoS 0 publication. It holds back no acknowledgement, so overflow drops it rather
    /// than ending the connection: a full lane misses this arrival while the other lanes take it,
    /// and an arrival that does not fit the client's limits reaches no lane, rather than only the
    /// lanes that would fit. Deliveries already waiting are never evicted.
    private void deliverAtMostOnce(
            DeliveryToken token,
            List<Long> ids,
            MqttMessage message,
            boolean dup,
            int bytes
    ) {
        var overflows = new ArrayList<Overflow>();
        lock.lock();
        try {
            var recipients = new ArrayList<Lane>();
            for (long id : ids) {
                Lane lane = lanes.get(id);
                // A lane awaiting removal admits nothing new.
                if (lane == null || lane.paused) {
                    continue;
                }
                if (lane.retained < lane.capacity) {
                    recipients.add(lane);
                } else {
                    overflows.add(overflow(lane, Dropped.SUBSCRIPTION_FULL));
                }
            }
            if (!recipients.isEmpty()) {
                if (fits(recipients.size(), bytes)) {
                    var delivery = new RetainedDelivery(token, bytes);
                    delivery.message = message;
                    retainedBytes += bytes;
                    retainedDeliveries += recipients.size();
                    for (Lane lane : recipients) {
                        var view = new DeliveryView(
                                delivery,
                                new Recipient(lane.id),
                                lane,
                                message,
                                dup,
                                token
                        );
                        enqueue(lane, view);
                    }
                } else {
                    for (Lane lane : recipients) {
                        overflows.add(overflow(lane, Dropped.CLIENT_FULL));
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        for (Overflow overflow : overflows) {
            logOverflow(overflow);
        }
    }

    /// Caller holds lock. Whether the client's limits leave room for `deliveries` more deliveries
    /// that add `bytes` more bytes.
    private boolean fits(int deliveries, long bytes) {
        return deliveries <= DELIVERY_LIMIT - retainedDeliveries
                && bytes <= byteLimit - retainedBytes;
    }

    /// Caller holds lock. Queues a new view on `lane` and wakes the lane's dispatcher, receiver,
    /// or the hand-off worker.
    private void enqueue(Lane lane, DeliveryView view) {
        view.sequence = ++enqueued;
        view.delivery.views.add(view);
        view.recipient.queued = true;
        lane.retained++;
        lane.queue.addLast(view);
        lane.ready.signal();
    }

    /// Caller holds lock. Counts a QoS 0 delivery that overflow kept from `lane`. The lane's first
    /// one for `reason` is logged at warning level.
    private Overflow overflow(Lane lane, Dropped reason) {
        drop(lane, reason);
        return new Overflow(lane.id, reason, lane.warned.add(reason));
    }

    private void logOverflow(Overflow overflow) {
        String cause = switch (overflow.reason()) {
            case SUBSCRIPTION_FULL -> "the subscription's buffer is full";
            case CLIENT_FULL -> "the client's delivery buffers are full";
            case REFUSED -> "its acceptor refused it";
            case FAILED, CONNECTION_ENDED, SUBSCRIPTION_STOPPED ->
                    throw new IllegalArgumentException("Not an overflow: " + overflow.reason());
        };
        if (overflow.first()) {
            log.warn("Dropped a QoS 0 delivery for {} (subscription {}): {}; later ones are "
                    + "logged at debug level", clientId, overflow.id(), cause);
        } else {
            log.debug("Dropped a QoS 0 delivery for {} (subscription {}): {}",
                    clientId, overflow.id(), cause);
        }
    }

    /// Takes one finite snapshot. A concurrent completion belongs to this or the next wake;
    /// the mailbox rearms its coalesced wake before the actor calls this method.
    List<Long> takeReady() {
        lock.lock();
        try {
            var result = new ArrayList<Long>();
            for (long token : readyTokens) {
                RetainedDelivery delivery = acknowledging.get(token);
                if (delivery != null && delivery.pending == 0) {
                    result.add(token);
                }
            }
            readyTokens.clear();
            return result;
        } finally {
            lock.unlock();
        }
    }

    /// PUBACK or PUBREC entered the send queue; running handlers retain their capacity.
    void release(long token) {
        lock.lock();
        try {
            RetainedDelivery delivery = acknowledging.get(token);
            if (delivery != null) {
                for (DeliveryView view : List.copyOf(delivery.views)) {
                    view.released = true;
                    releaseView(view);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /// The receive table has retired this receipt; old handlers may still retain local capacity.
    void forget(long token) {
        lock.lock();
        try {
            readyTokens.remove(token);
            RetainedDelivery delivery = acknowledging.remove(token);
            if (delivery != null) {
                retire(delivery);
            }
        } finally {
            lock.unlock();
        }
    }

    int retainedLanes() {
        lock.lock();
        try {
            return retainedLanes;
        } finally {
            lock.unlock();
        }
    }

    int retainedDeliveries() {
        lock.lock();
        try {
            return retainedDeliveries;
        } finally {
            lock.unlock();
        }
    }

    long retainedBytes() {
        lock.lock();
        try {
            return retainedBytes;
        } finally {
            lock.unlock();
        }
    }

    int acknowledgingDeliveries() {
        lock.lock();
        try {
            return acknowledging.size();
        } finally {
            lock.unlock();
        }
    }

    /// Counts deliveries that left their subscriptions unprocessed for `reason`, for tests until
    /// applications get statistics. The count changes under the same lock as the delivery's
    /// acknowledgement condition and capacity.
    long dropped(Dropped reason) {
        lock.lock();
        try {
            return dropped[reason.ordinal()];
        } finally {
            lock.unlock();
        }
    }

    /// Counts deliveries that left the lane `id` unprocessed for `reason`, as [#dropped(Dropped)]
    /// does for all lanes. A lane that has left the routing map counts nothing.
    long dropped(long id, Dropped reason) {
        lock.lock();
        try {
            Lane lane = lanes.get(id);
            return lane == null ? 0 : lane.dropped[reason.ordinal()];
        } finally {
            lock.unlock();
        }
    }

    /// Counts handler runs that returned normally, including MANUAL deliveries not yet acked.
    /// The count changes under the same lock as the acknowledgement condition and capacity that
    /// the return updates, so a caller that sees it sees them too; `onReady` may still follow.
    long handledDeliveries() {
        lock.lock();
        try {
            return handledDeliveries;
        } finally {
            lock.unlock();
        }
    }

    /// Counts handler failures whose report has returned. A failure that stops the subscription
    /// is reported to the client: the count changes after the report has offered its termination
    /// and the lane has paused or released the failed delivery. A skipped failure is reported to
    /// the log: the count changes after its delivery was released and its acknowledgement
    /// announced. Either way, a caller that sees the count can order later events after it.
    long reportedFailures() {
        lock.lock();
        try {
            return reportedFailures;
        } finally {
            lock.unlock();
        }
    }

    /// Counts deliveries that dispatchers moved to an executor's sublane. The count changes under
    /// the same lock as the move, so a caller that sees it can order later events after it.
    long placedDeliveries() {
        lock.lock();
        try {
            return placedDeliveries;
        } finally {
            lock.unlock();
        }
    }

    /// The lane's dispatcher: its internal virtual thread, which takes the lane's deliveries in
    /// receive order. Serial delivery executes each claim here, in place; execution on an
    /// application executor moves each delivery to its sublane instead. The dispatcher alone ends
    /// its lane, once no drain task of it runs and no delivery of it waits on an executor, so
    /// neither its handler nor its executor is called afterwards. A lane that drained after a
    /// confirmed removal then leaves the routing map, as a stopped lane does.
    private void dispatch(Lane lane) {
        try {
            while (dispatchNext(lane)) {
                // Each call drops its delivery reference before the next wait.
            }
            awaitExecutors(lane);
        } finally {
            lock.lock();
            try {
                if (lane.finishing && lane.queue.isEmpty()) {
                    // Nothing waits any more, so stopping completes no acknowledgement condition.
                    boolean _ = stop(lane);
                }
                retainedLanes--;
                terminatedLanes.add(lane);
            } finally {
                lock.unlock();
            }
            runTerminations();
        }
    }

    /// Waits until no drain task of the lane runs and no delivery of it waits on an executor. A
    /// stop or a connection's end discards the waiting deliveries; after a confirmed removal they
    /// run first. Serial delivery has none of either.
    private void awaitExecutors(Lane lane) {
        lock.lock();
        try {
            while (lane.activeTasks > 0 || lane.queuedInSublanes > 0) {
                // Returning drain tasks, stop(), and connectionEnded() signal this condition.
                lane.ready.awaitUninterruptibly();
            }
        } finally {
            lock.unlock();
        }
    }

    /// Waits for the lane's next delivery and hands it on as the lane's handler execution directs.
    /// Returns false once the lane has stopped, its queue has emptied after a confirmed removal,
    /// or the client's close has ended hand-off.
    private boolean dispatchNext(Lane lane) {
        return switch (lane.execution) {
            case HandlerExecution.Internal _ ->
                    executeNext(lane);
            case HandlerExecution.OnExecutor(Executor executor) ->
                    placeNext(lane, executor);
            case HandlerExecution.Lanes(Function<MqttMessage, Executor> selector) ->
                    placeNextOnLane(lane, selector);
            case HandlerExecution.Striped striped ->
                    placeNextOnStripe(lane, striped);
            case HandlerExecution.Parallel parallel ->
                    placeNextShared(lane, parallel);
        };
    }

    /// Caller holds lock. Waits until the lane has a delivery to hand on, and returns false
    /// instead once the lane has stopped, its queue has emptied after a confirmed removal, or the
    /// client's close has ended hand-off.
    private boolean awaitNext(Lane lane) {
        while (!lane.stopped && !handOffsClosed && (lane.failed || lane.queue.isEmpty())) {
            if (lane.finishing && lane.queue.isEmpty()) {
                return false;
            }
            // stop(), finish(), and closeHandOffs() signal this condition.
            lane.ready.awaitUninterruptibly();
        }
        return !lane.stopped && !handOffsClosed;
    }

    /// Serial delivery: claims the head and executes its handler here, in place.
    private boolean executeNext(Lane lane) {
        Claim claim;
        lock.lock();
        try {
            if (!awaitNext(lane)) {
                return false;
            }
            claim = claim(lane, lane.queue);
        } finally {
            lock.unlock();
        }
        execute(lane, claim);
        return true;
    }

    /// An application executor: moves the head to the executor's sublane, then submits a drain
    /// task unless one is on its way there.
    private boolean placeNext(Lane lane, Executor executor) {
        Sublane sublane;
        Optional<DrainTask> submitting;
        lock.lock();
        try {
            if (!awaitNext(lane)) {
                return false;
            }
            sublane = sublane(lane, executor);
            submitting = place(lane, lane.queue.removeFirst(), sublane);
        } finally {
            lock.unlock();
        }
        submitting.ifPresent(task -> submit(lane, sublane, task));
        return true;
    }

    /// Parallel: moves the head to the lane's one shared sublane, then submits another drain task
    /// unless as many as the parallelism are on their way there.
    private boolean placeNextShared(Lane lane, HandlerExecution.Parallel parallel) {
        Sublane sublane;
        Optional<DrainTask> submitting;
        lock.lock();
        try {
            if (!awaitNext(lane)) {
                return false;
            }
            sublane = shared(lane, parallel);
            submitting = place(lane, lane.queue.removeFirst(), sublane);
        } finally {
            lock.unlock();
        }
        submitting.ifPresent(task -> submit(lane, sublane, task));
        return true;
    }

    /// Lanes: places the head on the executor that the application's selector returns for it.
    private boolean placeNextOnLane(Lane lane, Function<MqttMessage, Executor> selector) {
        return placeNextChosen(
                lane,
                message -> requireNonNull(selector.apply(message), "The selector returned null"),
                executor -> sublane(lane, executor)
        );
    }

    /// Striped: places the head on the stripe that its key's hash chooses. The key's hashCode()
    /// is the application's code too, so the stripe's number is found without the lock as well.
    private boolean placeNextOnStripe(Lane lane, HandlerExecution.Striped striped) {
        return placeNextChosen(
                lane,
                message -> stripeOf(
                        requireNonNull(
                                striped.key().apply(message),
                                "The key function returned null"
                        ),
                        striped.stripes()
                ),
                number -> stripe(lane, striped, number)
        );
    }

    /// Lanes and Striped: asks the application's code about the head, without the lock, then
    /// places the head on the sublane that `sublaneFor` returns for the answer, as [#placeNext]
    /// does. The head stays queued meanwhile, so a stop or a connection's end still settles it,
    /// and the answer is then dropped. Code that throws, or returns null, ends the head as a
    /// handler failure.
    private <T> boolean placeNextChosen(
            Lane lane,
            Function<MqttMessage, T> choose,
            Function<T, Sublane> sublaneFor
    ) {
        DeliveryView head;
        lock.lock();
        try {
            if (!awaitNext(lane)) {
                return false;
            }
            head = lane.queue.getFirst();
        } finally {
            lock.unlock();
        }
        T choice;
        try {
            choice = choose.apply(head.message);
        } catch (Throwable failure) {
            boolean _ = Thread.interrupted();
            failSelected(lane, head, failure);
            return true;
        }
        // Like a handler, the application's code leaves no interrupt state on this thread.
        boolean _ = Thread.interrupted();
        Sublane sublane;
        Optional<DrainTask> submitting;
        lock.lock();
        try {
            if (!canTake(lane, head)) {
                return true;
            }
            sublane = sublaneFor.apply(choice);
            submitting = place(lane, lane.queue.removeFirst(), sublane);
        } finally {
            lock.unlock();
        }
        submitting.ifPresent(task -> submit(lane, sublane, task));
        return true;
    }

    /// Spreads keys over `stripes` stripes, so that equal keys share one. Fibonacci hashing mixes
    /// the key's hash code, so codes that differ only in their low bits, such as identifiers in
    /// steps of the stripe count, still spread; the mixed code's high bits then pick the stripe.
    static int stripeOf(Object key, int stripes) {
        long mixed = (key.hashCode() * 0x9E3779B9) & 0xFFFFFFFFL;
        return (int) ((mixed * stripes) >>> 32);
    }

    /// Ends the head as a handler failure after the application's code failed for it, unless a
    /// stop, a connection's end, or the client's close has taken it meanwhile. Reporting a
    /// delivery that one of those settled could stop the client for a subscription that no longer
    /// has it.
    private void failSelected(Lane lane, DeliveryView head, Throwable failure) {
        lock.lock();
        try {
            if (!canTake(lane, head)) {
                return;
            }
            lane.queue.removeFirst();
            head.recipient.queued = false;
            head.recipient.handed = true;
        } finally {
            lock.unlock();
        }
        fail(lane, head, failure, false);
    }

    /// Caller holds lock. Whether the dispatcher can still take `head`, which the application's
    /// code answered for: only the dispatcher takes the head, anything else that takes it clears
    /// the queue, and after the client's close the head stays queued for the close to settle.
    private boolean canTake(Lane lane, DeliveryView head) {
        return lane.queue.peekFirst() == head && !handOffsClosed;
    }

    /// Caller holds lock; the lane is not stopped, no failure holds it, and the queue is not
    /// empty. Taking the head fixes its hand-off. Claiming and stopping share the lock, so a stop
    /// either discards the delivery first or leaves its handler to run, possibly after the stop
    /// returns.
    private Claim claim(Lane lane, ArrayDeque<DeliveryView> queue) {
        DeliveryView view = queue.removeFirst();
        view.recipient.queued = false;
        view.recipient.handed = true;
        return new Claim(view, lane.ackMode == AckMode.ON_RECEIVE && markReady(view));
    }

    /// Caller holds lock. The sublane of an application's executor, told apart by identity.
    private Sublane sublane(Lane lane, Executor executor) {
        return lane.sublanes.computeIfAbsent(
                executor,
                _ -> new Sublane(lane, executor, executor, false, 1)
        );
    }

    /// Caller holds lock. The sublane of the stripe numbered `number`. It runs on the options'
    /// executor, or, without one, on an internal virtual thread that drains it until it is empty.
    private Sublane stripe(Lane lane, HandlerExecution.Striped striped, int number) {
        return lane.sublanes.computeIfAbsent(number, _ -> {
            Executor executor = striped.executor();
            if (executor != null) {
                return new Sublane(lane, number, executor, false, 1);
            }
            String name = "hato-" + clientId + "-delivery-" + lane.id + "-stripe-" + number;
            return new Sublane(
                    lane,
                    number,
                    task -> Thread.ofVirtual().name(name).start(task),
                    true,
                    1
            );
        });
    }

    /// Caller holds lock. A parallel lane's one sublane, from which up to `parallelism` drain
    /// tasks take deliveries at once. They run on the options' executor, or, without one, each
    /// on an internal virtual thread that drains the sublane until it is empty.
    private Sublane shared(Lane lane, HandlerExecution.Parallel parallel) {
        return lane.sublanes.computeIfAbsent(0, _ -> {
            Executor executor = parallel.executor();
            if (executor != null) {
                return new Sublane(lane, 0, executor, false, parallel.parallelism());
            }
            String name = "hato-" + clientId + "-delivery-" + lane.id + "-parallel";
            return new Sublane(
                    lane,
                    0,
                    task -> Thread.ofVirtual().name(name).start(task),
                    true,
                    parallel.parallelism()
            );
        });
    }

    /// Caller holds lock. Moves a delivery, still queued, to `sublane`. Returns a drain task while
    /// fewer than the sublane's limit are on their way there; the caller then submits it.
    private Optional<DrainTask> place(Lane lane, DeliveryView view, Sublane sublane) {
        sublane.queue.addLast(view);
        lane.queuedInSublanes++;
        placedDeliveries++;
        if (sublane.scheduled == sublane.limit) {
            return Optional.empty();
        }
        sublane.scheduled++;
        return Optional.of(sublane.idle.removeFirst());
    }

    /// Hands a drain task of the sublane to its executor, without the lock. An executor that
    /// throws ends the head delivery as a handler failure; while the lane goes on, the next head
    /// is tried, so a refusing executor leaves no delivery waiting for a task.
    private void submit(Lane lane, Sublane sublane, DrainTask task) {
        while (true) {
            try {
                sublane.executor.execute(task);
                return;
            } catch (Throwable failure) {
                if (!failHead(lane, sublane, task, failure)) {
                    return;
                }
            }
        }
    }

    /// Ends the sublane's head as a handler failure, after its executor refused `task`, which
    /// would have run it. Returns whether to ask the executor again for the rest.
    private boolean failHead(Lane lane, Sublane sublane, DrainTask task, Throwable failure) {
        DeliveryView view;
        lock.lock();
        try {
            if (!canClaim(lane, sublane)) {
                unschedule(lane, sublane, task);
                return false;
            }
            view = sublane.queue.removeFirst();
            lane.queuedInSublanes--;
            view.recipient.queued = false;
            view.recipient.handed = true;
        } finally {
            lock.unlock();
        }
        fail(lane, view, failure, true);
        lock.lock();
        try {
            if (!canClaim(lane, sublane)) {
                unschedule(lane, sublane, task);
                return false;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /// A drain task, run by the sublane's executor: claims and executes the head delivery. On an
    /// application's executor, it then asks the executor again while deliveries remain. When the
    /// executor runs that request before its execute() returns, as a synchronous executor does,
    /// the outer frame takes it, so the stack does not grow with the queue. A task on an internal
    /// thread instead goes on until the sublane is empty. The dispatcher does not end the lane
    /// while the task runs, because it may still call the handler or the executor.
    private void drain(Sublane sublane, DrainTask task) {
        Lane lane = sublane.lane;
        lock.lock();
        try {
            if (task.inline == Thread.currentThread()) {
                task.again = true;
                return;
            }
            lane.activeTasks++;
        } finally {
            lock.unlock();
        }
        try {
            while (runHead(lane, sublane, task)
                    && (sublane.drainsUntilEmpty || askAgain(lane, sublane, task))) {
                // An internal thread goes on, or the executor ran the next request inline and
                // this frame runs it.
            }
        } finally {
            lock.lock();
            try {
                if (--lane.activeTasks == 0) {
                    lane.ready.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /// Claims and executes the sublane's head on this thread. False when none can be claimed,
    /// after which `task` is no longer on its way to the sublane.
    private boolean runHead(Lane lane, Sublane sublane, DrainTask task) {
        Claim claim;
        lock.lock();
        try {
            if (!canClaim(lane, sublane)) {
                unschedule(lane, sublane, task);
                return false;
            }
            claim = claim(lane, sublane.queue);
            lane.queuedInSublanes--;
        } finally {
            lock.unlock();
        }
        execute(lane, claim);
        return true;
    }

    /// After one delivery, asks the executor to run `task` again while deliveries remain.
    /// Returns whether that request ran inline, for this frame to continue.
    private boolean askAgain(Lane lane, Sublane sublane, DrainTask task) {
        Thread current = Thread.currentThread();
        lock.lock();
        try {
            if (!canClaim(lane, sublane)) {
                unschedule(lane, sublane, task);
                return false;
            }
            task.inline = current;
            task.again = false;
        } finally {
            lock.unlock();
        }
        submit(lane, sublane, task);
        lock.lock();
        try {
            // A request that went to another thread may already be asking again from there.
            if (task.inline != current) {
                return false;
            }
            task.inline = null;
            boolean again = task.again;
            task.again = false;
            return again;
        } finally {
            lock.unlock();
        }
    }

    /// Caller holds lock. Whether a drain task may claim the sublane's head now. A failure holds
    /// every sublane of its lane; their deliveries are discarded before the failure clears.
    private boolean canClaim(Lane lane, Sublane sublane) {
        return !lane.stopped && !handOffsClosed && !lane.failed && !sublane.queue.isEmpty();
    }

    /// Caller holds lock. `task` is no longer on its way to the sublane and waits for the next
    /// placement there. A sublane that is empty, with no task on its way, leaves its lane.
    private void unschedule(Lane lane, Sublane sublane, DrainTask task) {
        sublane.scheduled--;
        sublane.idle.addLast(task);
        if (sublane.scheduled == 0 && sublane.queue.isEmpty()) {
            lane.sublanes.remove(sublane.key, sublane);
        }
    }

    /// The client's hand-off worker: one internal virtual thread that offers deliveries to the
    /// acceptors of every hand-off lane, oldest first across them, so the application sees one
    /// receive order. It ends once no hand-off lane is left or the client's close has ended
    /// hand-off, so it never waits for lanes that no longer exist.
    private void handOff() {
        while (true) {
            Lane lane;
            DeliveryView view;
            lock.lock();
            try {
                Optional<Lane> next = awaitHandOff();
                if (next.isEmpty()) {
                    return;
                }
                lane = next.orElseThrow();
                // Taking the head fixes its hand-off; a stop from here on leaves the lane's end
                // to the worker.
                view = lane.queue.removeFirst();
                view.recipient.queued = false;
                view.recipient.handed = true;
                view.offered = true;
                lane.accepting = true;
            } finally {
                lock.unlock();
            }
            offer(lane, view);
        }
    }

    /// Caller holds lock. Waits for the hand-off lane whose head arrived first. Returns empty, and
    /// lets the next hand-off lane start a new worker, once no hand-off lane is left or the
    /// client's close has ended hand-off.
    private Optional<Lane> awaitHandOff() {
        while (!handOffsClosed && hasHandOffLanes()) {
            Optional<Lane> oldest = oldestHandOff();
            if (oldest.isPresent()) {
                return oldest;
            }
            // Queuing, a stop, a cleared failure, and closeHandOffs() signal this condition.
            handOffReady.awaitUninterruptibly();
        }
        handOffStarted = false;
        return Optional.empty();
    }

    /// Caller holds lock. Stopped lanes have left the routing map.
    private boolean hasHandOffLanes() {
        return lanes.values().stream().anyMatch(lane -> lane.acceptor != null);
    }

    /// Caller holds lock. Among the hand-off lanes that can hand off now, the one whose head
    /// arrived first. A failure holds its lane until it clears. This scans every lane, O(lanes)
    /// per delivery with at most 256 lanes; ordering the lanes by their heads' sequence would make
    /// it O(log lanes) if measurements show the scan.
    private Optional<Lane> oldestHandOff() {
        return lanes.values().stream()
                .filter(lane -> lane.acceptor != null && !lane.failed && !lane.queue.isEmpty())
                .min(Comparator.comparingLong(lane -> lane.queue.getFirst().sequence));
    }

    /// Offers a claimed delivery to its lane's acceptor, without the lock. True accepts it, false
    /// refuses it, and an exception fails it as a handler failure would. Like a handler, the
    /// acceptor leaves no interrupt state on the worker.
    private void offer(Lane lane, DeliveryView view) {
        boolean accepted;
        try {
            accepted = requireNonNull(lane.acceptor).test(view);
        } catch (Throwable failure) {
            boolean _ = Thread.interrupted();
            fail(lane, view, failure, false);
            lock.lock();
            try {
                view.offered = false;
                finishAccepting(lane);
            } finally {
                lock.unlock();
            }
            runTerminations();
            return;
        }
        boolean _ = Thread.interrupted();
        if (accepted) {
            accept(lane, view);
        } else {
            refuse(lane, view);
        }
    }

    /// The acceptor took the delivery, and the application owns it from now on. An ON_RECEIVE
    /// condition is met, and so is a MANUAL one that ack() met while the acceptor ran. The
    /// delivery's capacity returns; its payload stays counted while another subscription still
    /// holds the same message, and a MANUAL condition still open stays tracked.
    private void accept(Lane lane, DeliveryView view) {
        boolean ready;
        lock.lock();
        try {
            view.offered = false;
            view.localDone = true;
            ready = (lane.ackMode == AckMode.ON_RECEIVE || view.ackedWhileOffered)
                    && markReady(view);
            if (holdsAcknowledgement(view)) {
                keepAccepted(view);
            } else {
                releaseView(view);
            }
            finishAccepting(lane);
        } finally {
            lock.unlock();
        }
        if (ready) {
            onReady.run();
        }
        runTerminations();
    }

    /// The acceptor refused the delivery: an overflow of its lane, as when the lane's buffer is
    /// full, and the acceptor is not asked again. A QoS 0 delivery is dropped for this lane
    /// only. A QoS 1 or 2 delivery goes back to not handed off, so a continued session's
    /// retransmission reaches this lane again, and the connection that delivered it ends as
    /// overloaded.
    private void refuse(Lane lane, DeliveryView view) {
        if (view.delivery.token.acknowledgement() == 0) {
            Overflow overflow;
            lock.lock();
            try {
                view.offered = false;
                view.localDone = true;
                overflow = overflow(lane, Dropped.REFUSED);
                releaseView(view);
                finishAccepting(lane);
            } finally {
                lock.unlock();
            }
            logOverflow(overflow);
        } else {
            lock.lock();
            try {
                view.offered = false;
                view.localDone = true;
                drop(lane, Dropped.REFUSED);
                view.released = true;
                invalidate(view);
                finishAccepting(lane);
            } finally {
                lock.unlock();
            }
            onOverload.accept(view.arrival);
        }
        runTerminations();
    }

    /// Caller holds lock. The worker has returned from the lane's acceptor. A stop that came
    /// meanwhile left the lane's end to now, and a finishing lane ends once nothing is left to
    /// hand off.
    private void finishAccepting(Lane lane) {
        lane.accepting = false;
        if (lane.stopped) {
            retainedLanes--;
            terminatedLanes.add(lane);
        } else {
            settle(lane);
        }
    }

    /// Runs a claimed delivery's handler without the lock, then settles the delivery. A normal
    /// return ends its local processing; a failure invalidates its acknowledgement condition and
    /// is reported. Either way the handler's interrupt state is cleared.
    private void execute(Lane lane, Claim claim) {
        DeliveryView view = claim.view();
        try {
            if (claim.ready()) {
                onReady.run();
            }
            requireNonNull(lane.handler).accept(view);
            // Clear handler-owned interrupt state before the next delivery or queue wait.
            boolean _ = Thread.interrupted();
            boolean ready;
            lock.lock();
            try {
                view.localDone = true;
                ready = lane.ackMode == AckMode.AFTER_HANDLER && markReady(view);
                releaseView(view);
                handledDeliveries++;
            } finally {
                lock.unlock();
            }
            if (ready) {
                onReady.run();
            }
        } catch (Throwable failure) {
            boolean _ = Thread.interrupted();
            fail(lane, view, failure, false);
        }
    }

    /// A claimed delivery failed: its handler threw, the lane selector failed for it, or its
    /// executor refused the task that would have run it. A delivery that still holds back its
    /// acknowledgement stops the subscription under [FailurePolicy#STOP], and always when it was
    /// `refused`, since a refusing executor processes nothing: the failure invalidates its
    /// acknowledgement condition and is reported. Any other failure is skipped: the delivery
    /// counts as done, acknowledged unprocessed if it held back its acknowledgement, and the
    /// subscription goes on.
    private void fail(Lane lane, DeliveryView view, Throwable failure, boolean refused) {
        boolean stops;
        boolean ready = false;
        boolean firstSkip = false;
        lock.lock();
        try {
            drop(lane, Dropped.FAILED);
            view.localDone = true;
            stops = holdsAcknowledgement(view)
                    && (refused || lane.failurePolicy == FailurePolicy.STOP);
            if (stops) {
                invalidate(view);
            } else {
                ready = markReady(view);
                releaseView(view);
                firstSkip = lane.warned.add(Dropped.FAILED);
            }
        } finally {
            lock.unlock();
        }
        if (!stops) {
            if (ready) {
                onReady.run();
            }
            if (firstSkip) {
                log.warn("Skipped a failed delivery for {} (subscription {}); later ones are "
                        + "logged at debug level", clientId, lane.id, failure);
            } else {
                log.debug("Skipped a failed delivery for {} (subscription {})",
                        clientId, lane.id, failure);
            }
            lock.lock();
            try {
                reportedFailures++;
            } finally {
                lock.unlock();
            }
            return;
        }
        boolean paused = reportFailure(lane, view.delivery.token, failure);
        lock.lock();
        try {
            if (!paused) {
                view.released = true;
                releaseView(view);
            }
            reportedFailures++;
        } finally {
            lock.unlock();
        }
    }

    /// Caller holds lock. Whether the view still holds back its receipt's acknowledgement: a QoS 1
    /// or 2 delivery whose condition is unmet, as with AFTER_HANDLER, or MANUAL before ack(). A
    /// retired receipt, such as one withdrawn for a reused packet identifier, holds back nothing.
    private boolean holdsAcknowledgement(DeliveryView view) {
        return view.delivery.token.acknowledgement() != 0 && !view.delivery.retired
                && view.valid && !view.recipient.ready;
    }

    /// Caller holds lock. Counts a delivery that left `lane` unprocessed, for the subscription and
    /// for the client.
    private void drop(Lane lane, Dropped reason) {
        lane.dropped[reason.ordinal()]++;
        dropped[reason.ordinal()]++;
    }

    private void acknowledge(DeliveryView view) {
        boolean ready;
        lock.lock();
        try {
            if (view.offered) {
                // The acceptor has not returned yet; its answer decides whether this ack counts.
                view.ackedWhileOffered = true;
                return;
            }
            ready = markReady(view);
            view.delivery.accepted.remove(view);
            releaseView(view);
        } finally {
            lock.unlock();
        }
        if (ready) {
            onReady.run();
        }
    }

    /// Caller holds lock. A recipient's condition is independent from its handler lifetime.
    private boolean markReady(DeliveryView view) {
        RetainedDelivery delivery = view.delivery;
        if (delivery.token.acknowledgement() != 0
                && view.valid && !view.recipient.ready && !delivery.retired) {
            view.recipient.ready = true;
            view.acknowledgement = null;
            if (--delivery.pending == 0) {
                readyTokens.add(delivery.token.acknowledgement());
                return true;
            }
        }
        return false;
    }

    /// Caller holds lock. A failed or stopped view cannot complete its replacement's condition.
    private void invalidate(DeliveryView view) {
        if (view.valid) {
            view.valid = false;
            if (!view.delivery.retired) {
                view.recipient.handed = false;
                if (view.recipient.ready) {
                    view.recipient.ready = false;
                    view.delivery.pending++;
                }
            }
            view.acknowledgement = null;
        }
        view.delivery.accepted.remove(view);
        releaseView(view);
    }

    private boolean reportFailure(Lane lane, DeliveryToken token, Throwable failure) {
        log.warn("Delivery lane failed for {} (subscription {}, generation {})",
                clientId, lane.id, token.connection(), failure);
        Throwable callbackFailure = null;
        boolean paused;
        lock.lock();
        try {
            // Admission and connection cleanup see either no decision or the final one. The
            // callback only offers a termination to the mailbox, which never calls these lanes.
            paused = onFailure.test(token, failure);
            // A failure that is only logged, such as one on another lane while the first
            // failure stops the subscription, must not lift that stop.
            if (paused) {
                lane.failed = true;
            }
        } catch (Throwable cause) {
            callbackFailure = cause;
            paused = false;
        } finally {
            lock.unlock();
        }
        if (callbackFailure != null) {
            log.warn("Delivery failure notification failed for {}", clientId, callbackFailure);
        }
        return paused;
    }

    /// Caller holds lock; never interrupts an executing application handler. Drain tasks that an
    /// executor still holds are detached: they do nothing when they run, and the lane does not
    /// wait for them.
    private boolean stop(Lane lane) {
        if (lane.stopped) {
            return false;
        }
        lanes.remove(lane.id, lane);
        lane.stopped = true;
        boolean ready = abandon(lane, lane.queue);
        for (Sublane sublane : lane.sublanes.values()) {
            ready |= abandon(lane, sublane.queue);
            for (DrainTask task : sublane.tasks) {
                task.detach();
            }
        }
        lane.sublanes.clear();
        lane.queuedInSublanes = 0;
        // Pull and hand-off lanes have no dispatcher to return this slot or report termination
        // when it exits; the stopped guard above keeps both single. A hand-off lane whose
        // acceptor runs ends when the worker returns from it.
        if (lane.handler == null && !lane.accepting) {
            retainedLanes--;
            terminatedLanes.add(lane);
        }
        lane.ready.signalAll();
        return ready;
    }

    /// Caller holds lock. A stopped lane's queued deliveries end without their handler, and their
    /// recipients no longer hold back acknowledgement. Returns whether that completed a receipt.
    private boolean abandon(Lane lane, ArrayDeque<DeliveryView> queue) {
        boolean ready = false;
        while (!queue.isEmpty()) {
            DeliveryView view = queue.removeFirst();
            view.recipient.queued = false;
            drop(lane, Dropped.SUBSCRIPTION_STOPPED);
            view.localDone = true;
            ready |= markReady(view);
            releaseView(view);
        }
        return ready;
    }

    /// Caller holds lock. A connection's end discards its queued deliveries; a later delivery
    /// of the same receipt can replace them.
    private void discard(Lane lane, ArrayDeque<DeliveryView> queue) {
        while (!queue.isEmpty()) {
            DeliveryView view = queue.removeFirst();
            view.recipient.queued = false;
            drop(lane, Dropped.CONNECTION_ENDED);
            view.localDone = true;
            view.released = true;
            invalidate(view);
        }
    }

    /// Caller holds lock. A finishing pull or hand-off lane stops once its accepted deliveries
    /// have been received or handed off; a finishing push lane's dispatcher stops it. Every
    /// operation that can empty a finishing pull or hand-off lane, or end its acceptor call,
    /// calls this, so such a lane never waits empty.
    private void settle(Lane lane) {
        if (lane.finishing && lane.handler == null && lane.queue.isEmpty() && !lane.accepting) {
            // An empty lane completes no acknowledgement condition by stopping.
            boolean _ = stop(lane);
        }
    }

    /// Reports the lanes that ended under the lock. Call it without the lock: a report completes
    /// subscription observations, which take the client's own lock. Callers announce
    /// acknowledgement readiness first, as a push lane's dispatcher does before it ends the lane.
    private void runTerminations() {
        List<Lane> ended;
        lock.lock();
        try {
            if (terminatedLanes.isEmpty()) {
                return;
            }
            ended = List.copyOf(terminatedLanes);
            terminatedLanes.clear();
        } finally {
            lock.unlock();
        }
        for (Lane lane : ended) {
            onTerminated.accept(lane.id);
        }
    }

    /// Pull capacity ends at handoff (or manual ack); the sequencer separately bounds wire ACKs.
    /// Push views retain capacity until both the handler and wire acknowledgement are released.
    private void releaseView(DeliveryView view) {
        Lane lane = view.lane;
        if (lane != null && view.localDone
                && (view.released || (lane.handler == null && view.recipient.ready))) {
            view.acknowledgement = null;
            returnCapacity(lane, view);
        }
    }

    /// Caller holds lock. An accepted MANUAL hand-off delivery returns its capacity like any
    /// accepted delivery, while its acknowledgement condition stays open: stops and retirement
    /// find the view in the receipt's accepted list until ack() meets the condition.
    private void keepAccepted(DeliveryView view) {
        returnCapacity(requireNonNull(view.lane), view);
        view.delivery.accepted.add(view);
    }

    /// Caller holds lock. The view leaves its lane's slot and the client's delivery slot, and the
    /// payload once no other view holds it.
    private void returnCapacity(Lane lane, DeliveryView view) {
        RetainedDelivery delivery = view.delivery;
        view.lane = null;
        lane.retained--;
        retainedDeliveries--;
        // A fast pull recipient must not remain referenced by a slower overlapping one.
        delivery.views.remove(view);
        if (delivery.views.isEmpty()) {
            retainedBytes -= delivery.bytes;
            delivery.message = null;
        }
    }

    /// A stable handle preserves the receiver's end state after removal from the routing map.
    final class Receiver {

        private final Lane lane;

        private Receiver(Lane lane) {
            this.lane = lane;
        }

        void end(@Nullable Throwable failure) {
            boolean ready = false;
            lock.lock();
            try {
                if (!lane.stopped) {
                    lane.failure = failure;
                    if (lanes.get(lane.id) == lane) {
                        ready = stop(lane);
                    } else {
                        lane.stopped = true;
                        terminatedLanes.add(lane);
                        lane.ready.signalAll();
                    }
                }
            } finally {
                lock.unlock();
            }
            if (ready) {
                onReady.run();
            }
            runTerminations();
        }

        /// Ends this pull subscription after a confirmed removal, once every delivery it
        /// accepted has been received. A receiver the actor never admitted holds none and ends
        /// at once.
        void finish() {
            lock.lock();
            try {
                if (!lane.stopped) {
                    if (lanes.get(lane.id) == lane) {
                        lane.paused = true;
                        lane.finishing = true;
                        settle(lane);
                    } else {
                        lane.stopped = true;
                        terminatedLanes.add(lane);
                    }
                    lane.ready.signalAll();
                }
            } finally {
                lock.unlock();
            }
            runTerminations();
        }

        ReceiveResult receive() throws InterruptedException {
            return receiveNanos(-1);
        }

        ReceiveResult receive(Duration timeout) throws InterruptedException {
            requireNonNull(timeout, "timeout");
            if (timeout.isNegative()) {
                throw new IllegalArgumentException("Receive timeout must not be negative");
            }
            // Saturation accepts large Durations without overflowing the relative wait budget.
            return receiveNanos(TimeUnit.NANOSECONDS.convert(timeout));
        }

        private ReceiveResult receiveNanos(long remaining) throws InterruptedException {
            boolean unlimited = remaining < 0;
            ReceiveResult result;
            boolean ready = false;
            lock.lockInterruptibly();
            try {
                while (!lane.stopped && !handOffsClosed && lane.queue.isEmpty()) {
                    if (unlimited) {
                        lane.ready.await();
                    } else {
                        if (remaining <= 0) {
                            return new ReceiveResult.TimedOut();
                        }
                        remaining = lane.ready.awaitNanos(remaining);
                    }
                }
                if (lane.stopped || handOffsClosed) {
                    checkFailure();
                    result = new ReceiveResult.Ended();
                } else {
                    // Claim, close, and acknowledgement share the lock. No other receiver can
                    // claim this view, and an interrupt cannot discard it after handoff commits.
                    DeliveryView view = lane.queue.removeFirst();
                    view.recipient.queued = false;
                    view.recipient.handed = true;
                    view.localDone = true;
                    ready = lane.ackMode == AckMode.ON_RECEIVE && markReady(view);
                    releaseView(view);
                    // Receiving the last delivery accepted before a confirmed removal ends it.
                    settle(lane);
                    result = new ReceiveResult.Received(view);
                }
            } finally {
                lock.unlock();
            }
            if (ready) {
                onReady.run();
            }
            runTerminations();
            return result;
        }

        private void checkFailure() {
            if (lane.failure instanceof MqttConnectionLostException ending) {
                Throwable cause = ending.getCause();
                throw cause == null ? new MqttConnectionLostException()
                        : new MqttConnectionLostException(cause);
            }
            if (lane.failure instanceof IllegalStateException rejection) {
                throw new IllegalStateException(rejection.getMessage(), rejection);
            }
            if (lane.failure instanceof MqttCapacityException rejection) {
                throw new MqttCapacityException(requireNonNull(rejection.getMessage()));
            }
            if (lane.failure instanceof MqttSubscriptionRefusedException rejection) {
                // Concurrent receivers must not share one throwable's trace and suppressions.
                throw new MqttSubscriptionRefusedException(rejection.reasonCode());
            }
            if (lane.failure instanceof MqttProtocolException violation) {
                throw new MqttProtocolException(
                        requireNonNull(violation.getMessage()),
                        violation.reasonCode()
                );
            }
            if (lane.failure != null) {
                throw new MqttConnectionLostException(lane.failure);
            }
        }
    }

    private static final class Lane {

        final long id;
        final int capacity;
        final AckMode ackMode;
        final FailurePolicy failurePolicy;
        final HandlerExecution execution;
        final @Nullable Consumer<MqttDelivery> handler;
        // A hand-off lane's acceptor; push and pull lanes have none.
        final @Nullable Predicate<MqttDelivery> acceptor;
        // A hand-off lane shares the hand-off worker's condition.
        final Condition ready;
        final ArrayDeque<DeliveryView> queue = new ArrayDeque<>();
        // A sublane exists while deliveries wait there or a drain task is on its way. Executors
        // are told apart by identity, so no application's equals() runs under the lock. Stripes,
        // and the one sublane of a parallel lane, are told apart by the equality of their
        // numbers, because Java shares the boxed objects only for numbers from -128 to 127.
        final Map<Object, Sublane> sublanes;
        // Deliveries that left this lane unprocessed, by reason.
        final long[] dropped = new long[Dropped.values().length];
        int retained;
        int queuedInSublanes;
        // Drain tasks running on an executor's thread, from their start until they return.
        int activeTasks;
        boolean paused;
        // A confirmed removal: the lane stops once its accepted deliveries are done.
        boolean finishing;
        boolean stopped;
        boolean failed;
        // The hand-off worker is in this lane's acceptor; a stop then leaves the lane's end to
        // the worker.
        boolean accepting;
        // Reasons whose first drop from this lane has been logged at warning level; later drops
        // for the same reason are logged at debug level.
        final Set<Dropped> warned = EnumSet.noneOf(Dropped.class);
        @Nullable Throwable failure;

        Lane(
                long id,
                int capacity,
                AckMode ackMode,
                FailurePolicy failurePolicy,
                HandlerExecution execution,
                @Nullable Consumer<MqttDelivery> handler,
                @Nullable Predicate<MqttDelivery> acceptor,
                Condition ready
        ) {
            this.id = id;
            this.capacity = capacity;
            this.ackMode = ackMode;
            this.failurePolicy = failurePolicy;
            this.execution = execution;
            this.handler = handler;
            this.acceptor = acceptor;
            this.ready = ready;
            sublanes = switch (execution) {
                case HandlerExecution.Striped _,
                     HandlerExecution.Parallel _ -> new HashMap<>();
                case HandlerExecution.Internal _,
                     HandlerExecution.OnExecutor _,
                     HandlerExecution.Lanes _ -> new IdentityHashMap<>();
            };
        }
    }

    /// One lane's deliveries waiting for one executor, one stripe, or the handlers of a parallel
    /// lane, in receive order. At most `limit` drain tasks are on their way at once: submitted,
    /// running, or about to ask the executor again.
    private final class Sublane {

        final Lane lane;
        // The sublane's key in its lane: its executor, or its number.
        final Object key;
        final Executor executor;
        // A drain task on an internal thread runs until the sublane is empty; one on an
        // application's executor runs one delivery and asks the executor again.
        final boolean drainsUntilEmpty;
        // A parallel lane's parallelism, and otherwise one, which keeps the receive order.
        final int limit;
        final ArrayDeque<DeliveryView> queue = new ArrayDeque<>();
        // The sublane's `limit` drain tasks; those not on their way wait in `idle`, so
        // `scheduled` plus the size of `idle` is always `limit`.
        final List<DrainTask> tasks;
        final ArrayDeque<DrainTask> idle;
        int scheduled;

        Sublane(
                Lane lane,
                Object key,
                Executor executor,
                boolean drainsUntilEmpty,
                int limit
        ) {
            this.lane = lane;
            this.key = key;
            this.executor = executor;
            this.drainsUntilEmpty = drainsUntilEmpty;
            this.limit = limit;
            tasks = new ArrayList<>(limit);
            for (int index = 0; index < limit; index++) {
                tasks.add(new DrainTask(this));
            }
            idle = new ArrayDeque<>(tasks);
        }

        void drain(DrainTask task) {
            DeliveryLanes.this.drain(this, task);
        }
    }

    /// The task that an executor holds. Detached when its lane stops, so a task held or run late
    /// does nothing and retains no client state.
    private static final class DrainTask implements Runnable {

        // Cleared once, under the lock, and read once per run without it. Whether a handler runs
        // is decided under the lock, so a run that read the sublane just before it was cleared
        // only finds the lane stopped; volatile alone lets later runs see the clearing at once.
        private volatile @Nullable Sublane sublane;
        // Read and written under the lock. The thread asking the executor to run this task again;
        // `again` records that the executor ran that request before returning, for the asking
        // frame to run instead.
        @Nullable Thread inline;
        boolean again;

        DrainTask(Sublane sublane) {
            this.sublane = sublane;
        }

        @Override
        public void run() {
            Sublane target = sublane;
            if (target != null) {
                target.drain(this);
            }
        }

        void detach() {
            sublane = null;
        }
    }

    /// Why a delivery left its subscription unprocessed. The counts are internal until
    /// applications get statistics.
    enum Dropped {

        /// Its handler threw, the lane selector failed for it, or its executor refused it.
        FAILED,

        /// A connection's end discarded it while it waited.
        CONNECTION_ENDED,

        /// A stop of its subscription discarded it while it waited.
        SUBSCRIPTION_STOPPED,

        /// A QoS 0 delivery found its subscription's buffer full.
        SUBSCRIPTION_FULL,

        /// A QoS 0 delivery did not fit the client's limits on deliveries and bytes.
        CLIENT_FULL,

        /// Its hand-off acceptor refused it.
        REFUSED
    }

    /// A QoS 0 delivery that overflow kept from the lane `id`, logged after the lock is released.
    private record Overflow(long id, Dropped reason, boolean first) {}

    /// A delivery taken from its lane's queue. `ready` says whether taking it met the last
    /// acknowledgement condition of its receipt, which `execute` announces without the lock.
    private record Claim(DeliveryView view, boolean ready) {}


    private static final class RetainedDelivery {

        final DeliveryToken token;
        final int bytes;
        // Receipt metadata is bounded by the core's receipt limit and the 256 subscriptions.
        // Completed pull views leave only these IDs and conditions, without payload or lane.
        final Map<Long, Recipient> recipients = new LinkedHashMap<>();
        // Views that hold capacity; the payload stays counted until the last of them leaves.
        final List<DeliveryView> views = new ArrayList<>(1);
        // Accepted MANUAL hand-off views. They returned their capacity at acceptance, but their
        // acknowledgement condition stays open until ack(), a stop, or retirement.
        final List<DeliveryView> accepted = new ArrayList<>(0);
        @Nullable MqttMessage message;
        int pending;
        boolean retired;

        RetainedDelivery(DeliveryToken token, int bytes) {
            this.token = token;
            this.bytes = bytes;
        }
    }

    private static final class Recipient {

        final long id;
        boolean handed;
        boolean ready;
        boolean queued;

        Recipient(long id) {
            this.id = id;
        }
    }

    /// Only MANUAL views hold an acknowledgement action, detached as soon as it is satisfied or
    /// retired. Released views also drop their lane so application-held messages retain no work.
    private static final class DeliveryView implements MqttDelivery {

        final RetainedDelivery delivery;
        final Recipient recipient;
        final MqttMessage message;
        final boolean dup;
        // The token of the arrival that delivered this copy. For a retransmission, its
        // connection and attempt can be later than the receipt's first.
        final DeliveryToken arrival;
        // Receive order across lanes, numbered when the copy is queued.
        long sequence;
        // The hand-off worker is offering this view to its acceptor, so an ack() waits for the
        // answer: acceptance meets the condition, and a refusal or failure drops the ack.
        boolean offered;
        boolean ackedWhileOffered;
        @Nullable Lane lane;
        volatile @Nullable Runnable acknowledgement;
        boolean localDone;
        boolean released;
        boolean valid = true;

        DeliveryView(
                RetainedDelivery delivery,
                Recipient recipient,
                Lane lane,
                MqttMessage message,
                boolean dup,
                DeliveryToken arrival
        ) {
            this.delivery = delivery;
            this.recipient = recipient;
            this.lane = lane;
            this.message = message;
            this.dup = dup;
            this.arrival = arrival;
            released = delivery.token.acknowledgement() == 0;
        }

        @Override
        public MqttMessage message() {
            return message;
        }

        @Override
        public boolean dup() {
            return dup;
        }

        @Override
        public long connectionAttempt() {
            return arrival.attempt();
        }

        @Override
        public void ack() {
            Runnable action = acknowledgement;
            if (action != null) {
                action.run();
            }
        }
    }
}
