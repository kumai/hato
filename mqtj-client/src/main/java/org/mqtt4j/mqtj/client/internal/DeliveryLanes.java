package org.mqtt4j.mqtj.client.internal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.AckMode;
import org.mqtt4j.mqtj.MqttDelivery;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.client.internal.common.log.Log;

import static java.util.Objects.requireNonNull;

/// One virtual thread per subscription preserves SERIAL order without blocking the actor.
/// Reservations include executing handlers and survive connection changes until they return.
final class DeliveryLanes {

    private static final Log log = Log.of(DeliveryLanes.class);

    private static final int LANE_LIMIT = 256;

    private static final int DELIVERY_LIMIT = 4096;

    // One lock keeps fan-out reservations atomic; each lane wakes only its own worker.
    private final ReentrantLock lock = new ReentrantLock();

    private final Map<Long, Lane> lanes = new HashMap<>();

    private final Map<Long, RetainedDelivery> acknowledging = new HashMap<>();

    // Each core-admitted QoS 1 receipt enters once. The core's 4096-receipt limit also bounds
    // this metadata-only queue, including receipts with no local recipient.
    private final ArrayDeque<Ready> readyTokens = new ArrayDeque<>();

    private final String clientId;

    private final long byteLimit;

    private final BiConsumer<Long, Throwable> onFailure;

    private final LongConsumer onReady;

    private int retainedLanes;

    private int retainedDeliveries;

    private long retainedBytes;

    private long abandonedDeliveries;

    DeliveryLanes(
            String clientId,
            long byteLimit,
            BiConsumer<Long, Throwable> onFailure,
            LongConsumer onReady
    ) {
        this.clientId = clientId;
        this.byteLimit = byteLimit;
        this.onFailure = onFailure;
        this.onReady = onReady;
    }

    boolean register(long generation, long id, Subscribe request, Consumer<MqttDelivery> handler) {
        lock.lock();
        try {
            if (retainedLanes >= LANE_LIMIT) {
                return false;
            }
            if (lanes.containsKey(id)) {
                throw new IllegalStateException("Subscription lane is already registered: " + id);
            }
            var lane = new Lane(
                    generation,
                    id,
                    request.bufferCapacity(),
                    // A handler acknowledges after it returns unless the request chooses a mode.
                    request.ackMode().orElse(AckMode.AFTER_HANDLER),
                    handler,
                    lock.newCondition()
            );
            lanes.put(id, lane);
            retainedLanes++;
            try {
                Thread.ofVirtual().name("mqtj-" + clientId + "-delivery-" + id)
                        .start(() -> run(lane));
            } catch (Throwable failure) {
                lanes.remove(id);
                retainedLanes--;
                throw failure;
            }
            return true;
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

    /// A refused removal runs the QoS 1 deliveries held during the attempt, so none is
    /// acknowledged without reaching the handler of a subscription that stays active.
    void resume(long id) {
        lock.lock();
        try {
            Lane lane = lanes.get(id);
            if (lane != null) {
                lane.paused = false;
                for (DeliveryView view : lane.queue) {
                    view.held = false;
                }
                lane.ready.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    void remove(long id) {
        long readyGeneration = 0;
        lock.lock();
        try {
            Lane lane = lanes.get(id);
            if (lane != null && stop(lane)) {
                readyGeneration = lane.generation;
            }
        } finally {
            lock.unlock();
        }
        if (readyGeneration != 0) {
            onReady.accept(readyGeneration);
        }
    }

    /// Retires acknowledgements on connection end, keeping only running handler reservations.
    /// The same manager admits new lanes after a reconnect.
    void close() {
        lock.lock();
        try {
            for (RetainedDelivery delivery : acknowledging.values()) {
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
                        generation, token, message, dup, bytes, recipients.size()
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
                readyTokens.addLast(new Ready(generation, token));
            }
        } finally {
            lock.unlock();
        }
        if (empty && token != 0) {
            onReady.accept(generation);
        }
        return true;
    }

    /// Takes one finite snapshot. A concurrent completion belongs to this or the next wake;
    /// the mailbox rearms its coalesced wake before the actor calls this method.
    List<Long> takeReady(long generation) {
        lock.lock();
        try {
            var result = new ArrayList<Long>();
            readyTokens.removeIf(ready -> {
                if (ready.generation() == generation) {
                    result.add(ready.token());
                    return true;
                }
                return false;
            });
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
            while (!lane.stopped && (lane.queue.isEmpty()
                    || (lane.paused && requireNonNull(lane.queue.peekFirst()).held))) {
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
                onReady.accept(lane.generation);
            }
            lane.handler.accept(view);
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
            onReady.accept(lane.generation);
        }
        return true;
    }

    private void acknowledge(DeliveryView view) {
        boolean ready;
        lock.lock();
        try {
            ready = markReady(view);
        } finally {
            lock.unlock();
        }
        if (ready) {
            onReady.accept(view.delivery.generation);
        }
    }

    /// Caller holds lock. A recipient's condition is independent from its handler lifetime.
    private boolean markReady(DeliveryView view) {
        RetainedDelivery delivery = view.delivery;
        if (delivery.token != 0 && !view.ready && !delivery.retired && !delivery.released) {
            view.ready = true;
            view.acknowledgement = null;
            if (--delivery.pending == 0) {
                readyTokens.addLast(new Ready(delivery.generation, delivery.token));
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
        lane.ready.signal();
        return ready;
    }

    /// Caller holds lock. Counts remain charged until both local work and wire ack are released.
    private void releaseView(DeliveryView view) {
        Lane lane = view.lane;
        RetainedDelivery delivery = view.delivery;
        if (lane != null && view.localDone && delivery.released) {
            view.lane = null;
            view.acknowledgement = null;
            lane.retained--;
            retainedDeliveries--;
            if (--delivery.remaining == 0) {
                retainedBytes -= delivery.bytes;
                delivery.views.clear();
            }
        }
    }

    private record Ready(long generation, long token) {
    }

    private static final class Lane {

        final long generation;
        final long id;
        final int capacity;
        final AckMode ackMode;
        final Consumer<MqttDelivery> handler;
        final Condition ready;
        final ArrayDeque<DeliveryView> queue = new ArrayDeque<>();
        int retained;
        boolean paused;
        boolean stopped;

        Lane(
                long generation,
                long id,
                int capacity,
                AckMode ackMode,
                Consumer<MqttDelivery> handler,
                Condition ready
        ) {
            this.generation = generation;
            this.id = id;
            this.capacity = capacity;
            this.ackMode = ackMode;
            this.handler = handler;
            this.ready = ready;
        }
    }

    private static final class RetainedDelivery {

        final long generation;
        final long token;
        final MqttMessage message;
        final boolean dup;
        final int bytes;
        final List<DeliveryView> views = new ArrayList<>();
        int remaining;
        int pending;
        boolean released;
        boolean retired;

        RetainedDelivery(
                long generation,
                long token,
                MqttMessage message,
                boolean dup,
                int bytes,
                int recipients
        ) {
            this.generation = generation;
            this.token = token;
            this.message = message;
            this.dup = dup;
            this.bytes = bytes;
            remaining = recipients;
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
        // Admitted while the lane awaited removal; it does not run until the lane resumes.
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
