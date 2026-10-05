package org.mqtt4j.mqtj.client.internal;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.AckMode;
import org.mqtt4j.mqtj.MqttConnectionLostException;
import org.mqtt4j.mqtj.MqttDelivery;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.ReceiveResult;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.client.internal.common.log.Log;

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

    // Each core-admitted QoS 1 receipt enters once. The core's 4096-receipt limit also bounds
    // this metadata-only queue, including receipts with no local recipient.
    private final ArrayDeque<Long> readyTokens = new ArrayDeque<>();

    private final String clientId;

    private final long byteLimit;

    private final BiConsumer<Long, Throwable> onFailure;

    private final Runnable onReady;

    private int retainedLanes;

    private int retainedDeliveries;

    private long retainedBytes;

    private long abandonedDeliveries;

    DeliveryLanes(
            String clientId,
            long byteLimit,
            BiConsumer<Long, Throwable> onFailure,
            Runnable onReady
    ) {
        this.clientId = clientId;
        this.byteLimit = byteLimit;
        this.onFailure = onFailure;
        this.onReady = onReady;
    }

    boolean register(long generation, long id, Subscribe request, Consumer<MqttDelivery> handler) {
        return registerLane(generation, id, request, handler).isPresent();
    }

    Optional<Receiver> registerPull(long generation, long id, Subscribe request) {
        if (request.ackMode().orElse(AckMode.ON_RECEIVE) == AckMode.AFTER_HANDLER) {
            throw new IllegalArgumentException("A pull subscription has no handler to acknowledge");
        }
        return registerLane(generation, id, request, null).map(Receiver::new);
    }

    private Optional<Lane> registerLane(
            long generation,
            long id,
            Subscribe request,
            @Nullable Consumer<MqttDelivery> handler
    ) {
        lock.lock();
        try {
            if (retainedLanes >= LANE_LIMIT) {
                return Optional.empty();
            }
            if (lanes.containsKey(id)) {
                throw new IllegalStateException("Subscription lane is already registered: " + id);
            }
            var lane = new Lane(
                    generation,
                    id,
                    request.bufferCapacity(),
                    request.ackMode().orElse(
                            handler == null ? AckMode.ON_RECEIVE : AckMode.AFTER_HANDLER
                    ),
                    handler,
                    lock.newCondition()
            );
            lanes.put(id, lane);
            retainedLanes++;
            if (handler == null) {
                return Optional.of(lane);
            }
            try {
                Thread.ofVirtual().name("mqtj-" + clientId + "-delivery-" + id)
                        .start(() -> run(lane));
            } catch (Throwable failure) {
                lanes.remove(id);
                retainedLanes--;
                throw failure;
            }
            return Optional.of(lane);
        } finally {
            lock.unlock();
        }
    }

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

    /// A refused removal releases the QoS 1 deliveries held during the attempt, so none is
    /// acknowledged without reaching the application of a subscription that stays active.
    void resume(long id) {
        lock.lock();
        try {
            Lane lane = lanes.get(id);
            if (lane != null) {
                lane.paused = false;
                for (DeliveryView view : lane.queue) {
                    view.held = false;
                }
                lane.ready.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    void remove(long id) {
        boolean ready = false;
        lock.lock();
        try {
            Lane lane = lanes.get(id);
            if (lane != null && stop(lane)) {
                ready = true;
            }
        } finally {
            lock.unlock();
        }
        if (ready) {
            onReady.run();
        }
    }

    /// Retires acknowledgements on connection end, keeping only running handler reservations.
    /// The same manager admits new lanes after a reconnect. Pull receivers end normally without
    /// a failure; otherwise they throw [MqttConnectionLostException] with it.
    void close(@Nullable Throwable failure) {
        lock.lock();
        try {
            for (RetainedDelivery delivery : List.copyOf(acknowledging.values())) {
                delivery.retired = true;
                delivery.released = true;
                for (DeliveryView view : List.copyOf(delivery.views)) {
                    view.acknowledgement = null;
                    releaseView(view);
                }
            }
            acknowledging.clear();
            readyTokens.clear();
            for (Lane lane : List.copyOf(lanes.values())) {
                lane.failure = failure;
                stop(lane);
            }
        } finally {
            lock.unlock();
        }
    }

    /// Reserves every matching lane together so overload cannot deliver only part of a fan-out.
    /// Token zero is QoS 0; other tokens are unique for the client's lifetime.
    boolean deliver(
            long generation,
            long token,
            List<Long> ids,
            MqttMessage message,
            boolean dup,
            int bytes
    ) {
        boolean empty;
        lock.lock();
        try {
            var recipients = new ArrayList<Lane>();
            for (long id : ids) {
                Lane lane = lanes.get(id);
                // A paused lane awaits removal. It holds QoS 1 until UNSUBACK abandons it or a
                // refused removal resumes it; QoS 0 is dropped as at-most-once delivery allows.
                if (lane != null && lane.generation == generation
                        && (!lane.paused || token != 0)) {
                    if (lane.retained >= lane.capacity) {
                        return false;
                    }
                    recipients.add(lane);
                }
            }
            empty = recipients.isEmpty();
            if (!empty) {
                if (recipients.size() > DELIVERY_LIMIT - retainedDeliveries
                        || bytes > byteLimit - retainedBytes) {
                    return false;
                }
                var delivery = new RetainedDelivery(
                        token, message, dup, bytes, recipients.size()
                );
                if (token != 0) {
                    acknowledging.put(token, delivery);
                }
                retainedBytes += bytes;
                retainedDeliveries += recipients.size();
                for (Lane lane : recipients) {
                    var view = new DeliveryView(delivery, lane);
                    view.held = lane.paused;
                    if (token != 0 && lane.ackMode == AckMode.MANUAL) {
                        view.acknowledgement = () -> acknowledge(view);
                    }
                    delivery.views.add(view);
                    lane.retained++;
                    lane.queue.addLast(view);
                    lane.ready.signal();
                }
            } else if (token != 0) {
                readyTokens.addLast(token);
            }
        } finally {
            lock.unlock();
        }
        if (empty && token != 0) {
            onReady.run();
        }
        return true;
    }

    /// Takes one finite snapshot. A concurrent completion belongs to this or the next wake;
    /// the mailbox rearms its coalesced wake before the actor calls this method.
    List<Long> takeReady() {
        lock.lock();
        try {
            var result = new ArrayList<>(readyTokens);
            readyTokens.clear();
            return result;
        } finally {
            lock.unlock();
        }
    }

    /// The sequencer has admitted this PUBACK; handlers still executing retain their capacity.
    void release(long token) {
        lock.lock();
        try {
            RetainedDelivery delivery = acknowledging.remove(token);
            if (delivery != null) {
                delivery.released = true;
                for (DeliveryView view : List.copyOf(delivery.views)) {
                    releaseView(view);
                }
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

    private void run(Lane lane) {
        try {
            while (runNext(lane)) {
                // Each call drops its delivery reference before the next wait.
            }
        } catch (Throwable failure) {
            lock.lock();
            try {
                stopFailed(lane);
            } finally {
                lock.unlock();
            }
            reportFailure(lane, failure);
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
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

    private boolean runNext(Lane lane) throws InterruptedException {
        DeliveryView view;
        boolean ready;
        lock.lock();
        try {
            while (!lane.stopped && !lane.hasDelivery()) {
                lane.ready.await();
            }
            if (lane.stopped) {
                return false;
            }
            // Claiming and stopping share the lock. A claimed handler may finish.
            view = lane.queue.removeFirst();
            ready = lane.ackMode == AckMode.ON_RECEIVE && markReady(view);
        } finally {
            lock.unlock();
        }
        try {
            if (ready) {
                onReady.run();
            }
            requireNonNull(lane.handler).accept(view);
        } catch (Throwable failure) {
            lock.lock();
            try {
                abandonedDeliveries++;
                view.localDone = true;
                view.delivery.retired = true;
                view.acknowledgement = null;
                releaseView(view);
                stopFailed(lane);
            } finally {
                lock.unlock();
            }
            reportFailure(lane, failure);
            return false;
        }
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
        if (delivery.token != 0 && !view.ready && !delivery.retired && !delivery.released) {
            view.ready = true;
            view.acknowledgement = null;
            if (--delivery.pending == 0) {
                readyTokens.addLast(delivery.token);
                return true;
            }
        }
        return false;
    }

    private void reportFailure(Lane lane, Throwable failure) {
        try {
            log.warn("Delivery lane failed for {} (subscription {}, generation {})",
                    clientId, lane.id, lane.generation, failure);
        } catch (Throwable _) {
            // A logging backend must not replace the original failure or suppress notification.
        }
        try {
            onFailure.accept(lane.generation, failure);
        } catch (Throwable callbackFailure) {
            try {
                log.warn("Delivery failure notification failed for {}", clientId, callbackFailure);
            } catch (Throwable _) {
                // Both reporting paths failed; preserve lane cleanup without notifying twice.
            }
        }
    }

    /// Caller holds lock. A failure stops the connection, so deliveries this lane never processed
    /// withhold their PUBACK even if other matching subscriptions finish them. Removal instead
    /// abandons them and releases this lane's acknowledgement condition.
    private void stopFailed(Lane lane) {
        for (DeliveryView view : lane.queue) {
            view.delivery.retired = true;
        }
        stop(lane);
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
                && (delivery.released || (lane.handler == null && view.ready))) {
            view.lane = null;
            view.acknowledgement = null;
            lane.retained--;
            retainedDeliveries--;
            // A fast pull recipient must not remain referenced by a slower overlapping one.
            delivery.views.remove(view);
            if (delivery.views.isEmpty()) {
                retainedBytes -= delivery.bytes;
                acknowledging.remove(delivery.token, delivery);
            }
        }
    }

    /// A stable handle preserves the receiver's end state after removal from the routing map.
    final class Receiver {

        private final Lane lane;

        private Receiver(Lane lane) {
            this.lane = lane;
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
            if (lane.failure != null) {
                throw new MqttConnectionLostException(lane.failure);
            }
        }
    }

    private static final class Lane {

        final long generation;
        final long id;
        final int capacity;
        final AckMode ackMode;
        final @Nullable Consumer<MqttDelivery> handler;
        final Condition ready;
        final ArrayDeque<DeliveryView> queue = new ArrayDeque<>();
        int retained;
        boolean paused;
        boolean stopped;
        @Nullable Throwable failure;

        Lane(
                long generation,
                long id,
                int capacity,
                AckMode ackMode,
                @Nullable Consumer<MqttDelivery> handler,
                Condition ready
        ) {
            this.generation = generation;
            this.id = id;
            this.capacity = capacity;
            this.ackMode = ackMode;
            this.handler = handler;
            this.ready = ready;
        }

        /// Caller holds lock. Held deliveries wait for a resumed lane in both modes.
        boolean hasDelivery() {
            return !queue.isEmpty() && !(paused && requireNonNull(queue.peekFirst()).held);
        }
    }

    private static final class RetainedDelivery {

        final long token;
        final MqttMessage message;
        final boolean dup;
        final int bytes;
        final List<DeliveryView> views = new ArrayList<>(1);
        int pending;
        boolean released;
        boolean retired;

        RetainedDelivery(
                long token,
                MqttMessage message,
                boolean dup,
                int bytes,
                int recipients
        ) {
            this.token = token;
            this.message = message;
            this.dup = dup;
            this.bytes = bytes;
            pending = recipients;
            released = token == 0;
        }
    }

    /// Only MANUAL views hold an acknowledgement action, detached as soon as it is satisfied or
    /// retired. Released views also drop their lane so application-held messages retain no work.
    private static final class DeliveryView implements MqttDelivery {

        final RetainedDelivery delivery;
        @Nullable Lane lane;
        volatile @Nullable Runnable acknowledgement;
        boolean localDone;
        boolean ready;
        // Admitted while the lane awaited removal; it is not run or received until the lane
        // resumes.
        boolean held;

        DeliveryView(RetainedDelivery delivery, Lane lane) {
            this.delivery = delivery;
            this.lane = lane;
        }

        @Override
        public MqttMessage message() {
            return delivery.message;
        }

        @Override
        public boolean dup() {
            return delivery.dup;
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
