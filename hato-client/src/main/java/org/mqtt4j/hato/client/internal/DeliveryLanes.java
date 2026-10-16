package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

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

/// Push subscriptions have one SERIAL worker; pull subscriptions use their callers' threads.
/// Reservations include executing handlers and survive connection changes until they return.
final class DeliveryLanes {

    private static final Log log = Log.of(DeliveryLanes.class);

    private static final int LANE_LIMIT = 256;

    // Bounds per-delivery overhead across all subscriptions (about 240 bytes each when measured);
    // the byte limit bounds payloads. Keep it high enough not to override a subscription's own
    // buffer capacity in ordinary use.
    private static final int DELIVERY_LIMIT = 65_536;

    // One lock keeps fan-out reservations atomic; each lane's condition wakes only that lane's
    // worker or receivers.
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

    private int retainedLanes;

    private int retainedDeliveries;

    private long retainedBytes;

    private long abandonedDeliveries;

    private long discardedDeliveries;

    DeliveryLanes(
            String clientId,
            long byteLimit,
            BiPredicate<DeliveryToken, Throwable> onFailure,
            Runnable onReady
    ) {
        this.clientId = clientId;
        this.byteLimit = byteLimit;
        this.onFailure = onFailure;
        this.onReady = onReady;
    }

    boolean register(long id, Subscribe request, Consumer<MqttDelivery> handler) {
        return registerLane(new Lane(
                id,
                request.bufferCapacity(),
                request.ackMode().orElse(AckMode.AFTER_HANDLER),
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
                        .start(() -> run(lane));
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
    /// received; later QoS 0 is dropped, and later QoS 1 and 2 is held until removal stops the
    /// lane, which releases its acknowledgement condition without running it.
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

    /// Pending views are discarded; a continuing session keeps handed-off receipt conditions.
    Map<Long, Boolean> connectionEnded(boolean preserveSession, boolean stopped) {
        lock.lock();
        try {
            if (!preserveSession) {
                retireAcknowledgements();
            }
            for (Lane lane : lanes.values()) {
                while (!lane.queue.isEmpty()) {
                    DeliveryView view = lane.queue.removeFirst();
                    view.recipient.queued = false;
                    discardedDeliveries++;
                    view.localDone = true;
                    view.released = true;
                    invalidate(view);
                }
                lane.failed = false;
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
            Map<Long, Boolean> result = receiveStateLocked();
            for (long token : List.copyOf(acknowledging.keySet())) {
                if (!result.containsKey(token)) {
                    retire(requireNonNull(acknowledging.remove(token)));
                }
            }
            readyTokens.clear();
            return result;
        } finally {
            lock.unlock();
        }
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
                    // A pending removal drops QoS 0 and holds QoS 1/2 until it stops the lane.
                    if (lane != null && (!lane.paused || token.acknowledgement() != 0)) {
                        recipients.add(lane);
                        states.add(new Recipient(id));
                    }
                }
                delivery = new RetainedDelivery(token, bytes);
            } else {
                for (Recipient recipient : requireNonNull(delivery).recipients.values()) {
                    if (!recipient.handed && !recipient.ready && !recipient.queued) {
                        Lane lane = lanes.get(recipient.id);
                        if (lane == null) {
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
                    view.held = lane.paused;
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

    private void run(Lane lane) {
        try {
            while (runNext(lane)) {
                // Each call drops its delivery reference before the next wait.
            }
        } finally {
            lock.lock();
            try {
                retainedLanes--;
            } finally {
                lock.unlock();
            }
        }
    }

    private boolean runNext(Lane lane) {
        DeliveryView view;
        boolean ready;
        lock.lock();
        try {
            while (!lane.stopped && (lane.failed || !lane.hasDelivery())) {
                // The private worker ends through stop(), which signals this condition.
                lane.ready.awaitUninterruptibly();
            }
            if (lane.stopped) {
                return false;
            }
            // Claiming and stopping share the lock. A claimed handler may finish.
            view = lane.queue.removeFirst();
            view.recipient.queued = false;
            view.recipient.handed = true;
            ready = lane.ackMode == AckMode.ON_RECEIVE && markReady(view);
        } finally {
            lock.unlock();
        }
        try {
            if (ready) {
                onReady.run();
            }
            requireNonNull(lane.handler).accept(view);
            // Clear handler-owned interrupt state before the next delivery or queue wait.
            Thread.interrupted();
            lock.lock();
            try {
                view.localDone = true;
                ready = lane.ackMode == AckMode.AFTER_HANDLER && markReady(view);
                releaseView(view);
            } finally {
                lock.unlock();
            }
            if (ready) {
                onReady.run();
            }
            return true;
        } catch (Throwable failure) {
            lock.lock();
            try {
                abandonedDeliveries++;
                view.localDone = true;
                invalidate(view);
            } finally {
                lock.unlock();
            }
            Thread.interrupted();
            if (!reportFailure(lane, view.delivery.token, failure)) {
                lock.lock();
                try {
                    view.released = true;
                    releaseView(view);
                } finally {
                    lock.unlock();
                }
            }
            return true;
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

    /// Caller holds lock; never interrupts an executing application handler.
    private boolean stop(Lane lane) {
        if (lane.stopped) {
            return false;
        }
        boolean ready = false;
        lanes.remove(lane.id, lane);
        lane.stopped = true;
        while (!lane.queue.isEmpty()) {
            DeliveryView view = lane.queue.removeFirst();
            view.recipient.queued = false;
            abandonedDeliveries++;
            view.localDone = true;
            ready |= markReady(view);
            releaseView(view);
        }
        // Pull lanes have no worker to return this slot when it exits; the stopped guard above
        // keeps this release single.
        if (lane.handler == null) {
            retainedLanes--;
        }
        lane.ready.signalAll();
        return ready;
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
                        lane.ready.signalAll();
                    }
                }
            } finally {
                lock.unlock();
            }
            if (ready) {
                onReady.run();
            }
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
            DeliveryView view;
            boolean ready;
            lock.lockInterruptibly();
            try {
                while (!lane.stopped && !lane.hasDelivery()) {
                    if (unlimited) {
                        lane.ready.await();
                    } else {
                        if (remaining <= 0) {
                            return new ReceiveResult.TimedOut();
                        }
                        remaining = lane.ready.awaitNanos(remaining);
                    }
                }
                if (lane.stopped) {
                    checkFailure();
                    return new ReceiveResult.Ended();
                }
                // Claim, close, and acknowledgement share the lock. No other receiver can
                // claim this view, and an interrupt cannot discard it after handoff commits.
                view = lane.queue.removeFirst();
                view.recipient.queued = false;
                view.recipient.handed = true;
                view.localDone = true;
                ready = lane.ackMode == AckMode.ON_RECEIVE && markReady(view);
                releaseView(view);
            } finally {
                lock.unlock();
            }
            if (ready) {
                onReady.run();
            }
            return new ReceiveResult.Received(view);
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
        final @Nullable Consumer<MqttDelivery> handler;
        final Condition ready;
        final ArrayDeque<DeliveryView> queue = new ArrayDeque<>();
        int retained;
        boolean paused;
        boolean stopped;
        boolean failed;
        @Nullable Throwable failure;

        Lane(
                long id,
                int capacity,
                AckMode ackMode,
                @Nullable Consumer<MqttDelivery> handler,
                Condition ready
        ) {
            this.id = id;
            this.capacity = capacity;
            this.ackMode = ackMode;
            this.handler = handler;
            this.ready = ready;
        }

        /// Caller holds lock. Held deliveries wait in both modes until removal stops the lane.
        boolean hasDelivery() {
            return !queue.isEmpty() && !(paused && requireNonNull(queue.peekFirst()).held);
        }
    }

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
        // Admitted while the lane awaited removal; it is never run or received, and removal
        // discards it.
        boolean held;

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
