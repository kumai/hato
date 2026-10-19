package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttSubscribeException;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.client.internal.common.log.Log;

import static java.util.Objects.requireNonNull;

/// Each push subscription has a dispatcher, an internal virtual thread that takes its deliveries
/// in receive order. For serial delivery it executes each handler in place. For an application
/// executor it moves each delivery to that executor's sublane, from which one drain task at a
/// time, run by the executor, claims and executes the head and then asks the executor again.
/// Pull subscriptions use their callers' threads. Reservations include executing handlers and
/// survive connection changes until they return.
final class DeliveryLanes {

    private static final Log log = Log.of(DeliveryLanes.class);

    private static final int LANE_LIMIT = 256;

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

    // True when the failure ends the current connection or the client; the lane then runs
    // nothing more until connection cleanup or removal. An old receipt returns false.
    private final BiPredicate<DeliveryToken, Throwable> onFailure;

    private final Runnable onReady;

    // Called once per lane, without the lock, when the lane can run and hand off nothing more: a
    // push lane's dispatcher has ended it, or a pull lane has stopped.
    private final LongConsumer onTerminated;

    // Lanes that ended under the lock. Their terminations run after it is released.
    private final List<Lane> terminatedLanes = new ArrayList<>();

    // Set before the client's close() returns: no lane claims a delivery afterwards.
    private boolean handOffsClosed;

    private int retainedLanes;

    private int retainedDeliveries;

    private long retainedBytes;

    private long abandonedDeliveries;

    private long discardedDeliveries;

    private long handledDeliveries;

    private long reportedFailures;

    private long placedDeliveries;

    DeliveryLanes(
            String clientId,
            long byteLimit,
            BiPredicate<DeliveryToken, Throwable> onFailure,
            Runnable onReady,
            LongConsumer onTerminated
    ) {
        this.clientId = clientId;
        this.byteLimit = byteLimit;
        this.onFailure = onFailure;
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
                execution,
                handler,
                lock.newCondition()
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
                new HandlerExecution.Internal(),
                null,
                lock.newCondition()
        ));
    }

    boolean register(Receiver receiver) {
        return registerLane(receiver.lane);
    }

    /// Returns false only when the lane limit is full.
    private boolean registerLane(Lane lane) {
        lock.lock();
        try {
            if (lane.stopped) {
                // A receiver ended before the actor admitted it stays ended, without a lane slot.
                return true;
            }
            if (retainedLanes >= LANE_LIMIT) {
                return false;
            }
            if (lanes.containsKey(lane.id)) {
                throw new IllegalStateException(
                        "Subscription lane is already registered: " + lane.id
                );
            }
            lanes.put(lane.id, lane);
            retainedLanes++;
            if (lane.handler == null) {
                return true;
            }
            try {
                Thread.ofVirtual().name("hato-" + clientId + "-delivery-" + lane.id)
                        .start(() -> dispatch(lane));
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

    /// Ends a push lane whose removal is confirmed once the deliveries it accepted have run: those
    /// queued before removal was requested, because a paused lane admits nothing new. A later
    /// [#remove] still discards what remains.
    void finish(long id) {
        lock.lock();
        try {
            Lane lane = lanes.get(id);
            if (lane != null) {
                lane.paused = true;
                lane.finishing = true;
                lane.ready.signalAll();
            }
        } finally {
            lock.unlock();
        }
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
                discard(lane.queue);
                for (Sublane sublane : lane.sublanes.values()) {
                    discard(sublane.queue);
                }
                // A sublane keeps the drain task on its way, so the next connection's deliveries
                // there wait for a handler still running, as on an internal thread.
                lane.sublanes.values().removeIf(sublane -> !sublane.scheduled);
                lane.queuedInSublanes = 0;
                lane.failed = false;
                settle(lane);
                lane.ready.signalAll();
            }
            if (preserveSession) {
                for (RetainedDelivery delivery : acknowledging.values()) {
                    for (DeliveryView view : List.copyOf(delivery.views)) {
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
                stop(lane);
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

    private void retire(RetainedDelivery delivery) {
        delivery.retired = true;
        for (DeliveryView view : List.copyOf(delivery.views)) {
            view.acknowledgement = null;
            view.released = true;
            releaseView(view);
        }
    }

    /// Reserves every matching lane together so overload cannot deliver only part of a fan-out.
    boolean deliver(
            DeliveryToken token,
            List<Long> ids,
            MqttMessage message,
            boolean dup,
            int bytes
    ) {
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
            if (recipients.size() > DELIVERY_LIMIT - retainedDeliveries
                    || addedBytes > byteLimit - retainedBytes) {
                return false;
            }
            if (first && token.acknowledgement() != 0) {
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
                    if (first && token.acknowledgement() != 0) {
                        retained.recipients.put(recipient.id, recipient);
                        retained.pending++;
                    }
                    var view = new DeliveryView(
                            retained,
                            recipient,
                            lane,
                            requireNonNull(retained.message),
                            dup
                    );
                    if (token.acknowledgement() != 0 && lane.ackMode == AckMode.MANUAL) {
                        view.acknowledgement = () -> acknowledge(view);
                    }
                    retained.views.add(view);
                    recipient.queued = true;
                    lane.retained++;
                    lane.queue.addLast(view);
                    lane.ready.signal();
                }
            }
            ready = recipients.isEmpty() && retained.pending == 0;
            if (ready && token.acknowledgement() != 0) {
                readyTokens.add(token.acknowledgement());
            }
        } finally {
            lock.unlock();
        }
        if (ready && token.acknowledgement() != 0) {
            onReady.run();
        }
        return true;
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

    long abandonedDeliveries() {
        lock.lock();
        try {
            return abandonedDeliveries;
        } finally {
            lock.unlock();
        }
    }

    long discardedDeliveries() {
        lock.lock();
        try {
            return discardedDeliveries;
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

    /// Counts handler failures whose report has returned. The count changes after the report has
    /// offered its termination and the lane has paused or released the failed delivery, so a
    /// caller that sees it can order later events after the report.
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
                    stop(lane);
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

    /// Waits for the next delivery and executes it, or places it on its executor. Returns false
    /// once the lane has stopped, its queue has emptied after a confirmed removal, or the client's
    /// close has ended hand-off.
    private boolean dispatchNext(Lane lane) {
        Claim claim = null;
        Sublane submitting = null;
        lock.lock();
        try {
            while (!lane.stopped && !handOffsClosed && (lane.failed || lane.queue.isEmpty())) {
                if (lane.finishing && lane.queue.isEmpty()) {
                    return false;
                }
                // stop(), finish(), and closeHandOffs() signal this condition.
                lane.ready.awaitUninterruptibly();
            }
            if (lane.stopped || handOffsClosed) {
                return false;
            }
            switch (lane.execution) {
                case HandlerExecution.Internal _ -> claim = claim(lane, lane.queue);
                case HandlerExecution.OnExecutor(Executor executor) ->
                        submitting = place(lane, lane.queue.removeFirst(), executor);
            }
        } finally {
            lock.unlock();
        }
        if (claim != null) {
            execute(lane, claim);
        }
        if (submitting != null) {
            submit(lane, submitting);
        }
        return true;
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

    /// Caller holds lock. Moves a delivery, still queued, to its executor's sublane. Returns the
    /// sublane when no drain task is on its way there yet; the caller then submits one.
    private @Nullable Sublane place(Lane lane, DeliveryView view, Executor executor) {
        Sublane sublane = lane.sublanes.computeIfAbsent(executor, key -> new Sublane(lane, key));
        sublane.queue.addLast(view);
        lane.queuedInSublanes++;
        placedDeliveries++;
        if (sublane.scheduled) {
            return null;
        }
        sublane.scheduled = true;
        return sublane;
    }

    /// Hands the sublane's drain task to its executor, without the lock. An executor that throws
    /// ends the head delivery as a handler failure; while the lane goes on, the next head is
    /// tried, so a refusing executor leaves no delivery waiting for a task.
    private void submit(Lane lane, Sublane sublane) {
        while (true) {
            try {
                sublane.executor.execute(sublane.task);
                return;
            } catch (Throwable failure) {
                if (!failHead(lane, sublane, failure)) {
                    return;
                }
            }
        }
    }

    /// Ends the sublane's head as a handler failure, after its executor refused the task that
    /// would have run it. Returns whether to ask the executor again for the rest.
    private boolean failHead(Lane lane, Sublane sublane, Throwable failure) {
        DeliveryView view;
        lock.lock();
        try {
            if (!canClaim(lane, sublane)) {
                unschedule(lane, sublane);
                return false;
            }
            view = sublane.queue.removeFirst();
            lane.queuedInSublanes--;
            view.recipient.queued = false;
            view.recipient.handed = true;
        } finally {
            lock.unlock();
        }
        fail(lane, view, failure);
        lock.lock();
        try {
            if (!canClaim(lane, sublane)) {
                unschedule(lane, sublane);
                return false;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /// A drain task, run by the sublane's executor: claims and executes the head delivery, then
    /// asks the executor again while deliveries remain. When the executor runs that request before
    /// its execute() returns, as a synchronous executor does, the outer frame takes it, so the
    /// stack does not grow with the queue. The dispatcher does not end the lane while the task
    /// runs, because it may still call the handler or the executor.
    private void drain(Sublane sublane) {
        Lane lane = sublane.lane;
        lock.lock();
        try {
            if (sublane.inline == Thread.currentThread()) {
                sublane.again = true;
                return;
            }
            lane.activeTasks++;
        } finally {
            lock.unlock();
        }
        try {
            while (runHead(lane, sublane) && askAgain(lane, sublane)) {
                // The executor ran the next request inline; this frame runs it.
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
    /// after which no drain task is on its way to the sublane.
    private boolean runHead(Lane lane, Sublane sublane) {
        Claim claim;
        lock.lock();
        try {
            if (!canClaim(lane, sublane)) {
                unschedule(lane, sublane);
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

    /// After one delivery, asks the executor for the next request while deliveries remain.
    /// Returns whether that request ran inline, for this frame to continue.
    private boolean askAgain(Lane lane, Sublane sublane) {
        Thread current = Thread.currentThread();
        lock.lock();
        try {
            if (!canClaim(lane, sublane)) {
                unschedule(lane, sublane);
                return false;
            }
            sublane.inline = current;
            sublane.again = false;
        } finally {
            lock.unlock();
        }
        submit(lane, sublane);
        lock.lock();
        try {
            // A request that went to another thread may already be asking again from there.
            if (sublane.inline != current) {
                return false;
            }
            sublane.inline = null;
            boolean again = sublane.again;
            sublane.again = false;
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

    /// Caller holds lock. No drain task is on its way to the sublane any more, and an empty
    /// sublane leaves its lane.
    private void unschedule(Lane lane, Sublane sublane) {
        sublane.scheduled = false;
        if (sublane.queue.isEmpty()) {
            lane.sublanes.remove(sublane.executor, sublane);
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
            Thread.interrupted();
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
            Thread.interrupted();
            fail(lane, view, failure);
        }
    }

    /// A claimed delivery failed: its handler threw, or its executor refused the task that would
    /// have run it. The failure invalidates its acknowledgement condition and is reported.
    private void fail(Lane lane, DeliveryView view, Throwable failure) {
        lock.lock();
        try {
            abandonedDeliveries++;
            view.localDone = true;
            invalidate(view);
        } finally {
            lock.unlock();
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

    private void acknowledge(DeliveryView view) {
        boolean ready;
        lock.lock();
        try {
            ready = markReady(view);
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
            lane.failed = onFailure.test(token, failure);
            paused = lane.failed;
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
        boolean ready = abandon(lane.queue);
        for (Sublane sublane : lane.sublanes.values()) {
            ready |= abandon(sublane.queue);
            sublane.task.detach();
        }
        lane.sublanes.clear();
        lane.queuedInSublanes = 0;
        // Pull lanes have no dispatcher to return this slot or report termination when it exits;
        // the stopped guard above keeps both single.
        if (lane.handler == null) {
            retainedLanes--;
            terminatedLanes.add(lane);
        }
        lane.ready.signalAll();
        return ready;
    }

    /// Caller holds lock. A stopped lane's queued deliveries end without their handler, and their
    /// recipients no longer hold back acknowledgement. Returns whether that completed a receipt.
    private boolean abandon(ArrayDeque<DeliveryView> queue) {
        boolean ready = false;
        while (!queue.isEmpty()) {
            DeliveryView view = queue.removeFirst();
            view.recipient.queued = false;
            abandonedDeliveries++;
            view.localDone = true;
            ready |= markReady(view);
            releaseView(view);
        }
        return ready;
    }

    /// Caller holds lock. A connection's end discards its queued deliveries; a later delivery
    /// of the same receipt can replace them.
    private void discard(ArrayDeque<DeliveryView> queue) {
        while (!queue.isEmpty()) {
            DeliveryView view = queue.removeFirst();
            view.recipient.queued = false;
            discardedDeliveries++;
            view.localDone = true;
            view.released = true;
            invalidate(view);
        }
    }

    /// Caller holds lock. A finishing pull lane stops once its accepted deliveries have been
    /// received; a finishing push lane's dispatcher stops it. Every operation that can empty a
    /// finishing pull lane calls this, so such a lane never waits empty.
    private void settle(Lane lane) {
        if (lane.finishing && lane.handler == null && lane.queue.isEmpty()) {
            stop(lane);
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
        RetainedDelivery delivery = view.delivery;
        if (lane != null && view.localDone
                && (view.released || (lane.handler == null && view.recipient.ready))) {
            view.lane = null;
            view.acknowledgement = null;
            lane.retained--;
            retainedDeliveries--;
            // A fast pull recipient must not remain referenced by a slower overlapping one.
            delivery.views.remove(view);
            if (delivery.views.isEmpty()) {
                retainedBytes -= delivery.bytes;
                delivery.message = null;
            }
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
            if (lane.failure instanceof MqttSubscribeException rejection) {
                // Concurrent receivers must not share one throwable's trace and suppressions.
                throw new MqttSubscribeException(rejection.reasonCode());
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
        final HandlerExecution execution;
        final @Nullable Consumer<MqttDelivery> handler;
        final Condition ready;
        final ArrayDeque<DeliveryView> queue = new ArrayDeque<>();
        // An executor's sublane exists while deliveries wait there or a drain task is on its way.
        // Executors are told apart by identity.
        final Map<Executor, Sublane> sublanes = new IdentityHashMap<>();
        int retained;
        int queuedInSublanes;
        // Drain tasks running on an executor's thread, from their start until they return.
        int activeTasks;
        boolean paused;
        // A confirmed removal: the lane stops once its accepted deliveries are done.
        boolean finishing;
        boolean stopped;
        boolean failed;
        @Nullable Throwable failure;

        Lane(
                long id,
                int capacity,
                AckMode ackMode,
                HandlerExecution execution,
                @Nullable Consumer<MqttDelivery> handler,
                Condition ready
        ) {
            this.id = id;
            this.capacity = capacity;
            this.ackMode = ackMode;
            this.execution = execution;
            this.handler = handler;
            this.ready = ready;
        }
    }

    /// One lane's deliveries waiting for one executor, in receive order. While `scheduled` is set,
    /// one drain task is on its way: submitted, running, or about to ask the executor again.
    private final class Sublane {

        final Lane lane;
        final Executor executor;
        final ArrayDeque<DeliveryView> queue = new ArrayDeque<>();
        final DrainTask task;
        boolean scheduled;
        // The thread asking the executor again; `again` records that the executor ran that
        // request before returning, for the asking frame to run instead.
        @Nullable Thread inline;
        boolean again;

        Sublane(Lane lane, Executor executor) {
            this.lane = lane;
            this.executor = executor;
            task = new DrainTask(this);
        }

        void drain() {
            DeliveryLanes.this.drain(this);
        }
    }

    /// The task that an executor holds. Detached when its lane stops, so a task held or run late
    /// does nothing and retains no client state.
    private static final class DrainTask implements Runnable {

        // Cleared once, under the lock, and read once per run without it. Whether a handler runs
        // is decided under the lock, so a run that read the sublane just before it was cleared
        // only finds the lane stopped; volatile alone lets later runs see the clearing at once.
        private volatile @Nullable Sublane sublane;

        DrainTask(Sublane sublane) {
            this.sublane = sublane;
        }

        @Override
        public void run() {
            Sublane target = sublane;
            if (target != null) {
                target.drain();
            }
        }

        void detach() {
            sublane = null;
        }
    }

    /// A delivery taken from its lane's queue. `ready` says whether taking it met the last
    /// acknowledgement condition of its receipt, which `execute` announces without the lock.
    private record Claim(DeliveryView view, boolean ready) {}

    private static final class RetainedDelivery {

        final DeliveryToken token;
        final int bytes;
        // Receipt metadata is bounded by the core's receipt limit and the 256 subscriptions.
        // Completed pull views leave only these IDs and conditions, without payload or lane.
        final Map<Long, Recipient> recipients = new LinkedHashMap<>();
        final List<DeliveryView> views = new ArrayList<>(1);
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
                boolean dup
        ) {
            this.delivery = delivery;
            this.recipient = recipient;
            this.lane = lane;
            this.message = message;
            this.dup = dup;
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
        public void ack() {
            Runnable action = acknowledgement;
            if (action != null) {
                action.run();
            }
        }
    }
}
