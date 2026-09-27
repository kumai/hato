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

import org.mqtt4j.mqtj.MqttDelivery;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.client.internal.common.log.Log;

/// One virtual thread per subscription preserves SERIAL order without blocking the actor.
/// Reservations include executing handlers and survive connection changes until they return.
final class DeliveryLanes {

    private static final Log log = Log.of(DeliveryLanes.class);

    private static final int LANE_LIMIT = 256;

    private static final int DELIVERY_LIMIT = 4096;

    // One lock keeps fan-out reservations atomic; each lane wakes only its own worker.
    private final ReentrantLock lock = new ReentrantLock();

    private final Map<Long, Lane> lanes = new HashMap<>();

    private final String clientId;

    private final long byteLimit;

    private final BiConsumer<Long, Throwable> onFailure;

    private int retainedLanes;

    private int retainedDeliveries;

    private long retainedBytes;

    private long abandonedDeliveries;

    DeliveryLanes(String clientId, long byteLimit, BiConsumer<Long, Throwable> onFailure) {
        this.clientId = clientId;
        this.byteLimit = byteLimit;
        this.onFailure = onFailure;
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
            var lane = new Lane(generation, id, request.bufferCapacity(), handler,
                    lock.newCondition());
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

    void resume(long id) {
        lock.lock();
        try {
            Lane lane = lanes.get(id);
            if (lane != null) {
                lane.paused = false;
            }
        } finally {
            lock.unlock();
        }
    }

    void remove(long id) {
        lock.lock();
        try {
            Lane lane = lanes.get(id);
            if (lane != null) {
                stop(lane);
            }
        } finally {
            lock.unlock();
        }
    }

    /// Stops existing lanes; the same manager admits new lanes after a reconnect.
    void close() {
        lock.lock();
        try {
            for (Lane lane : List.copyOf(lanes.values())) {
                stop(lane);
            }
        } finally {
            lock.unlock();
        }
    }

    /// Reserves every matching lane together so overload cannot deliver only part of a fan-out.
    boolean deliver(long generation, List<Long> ids, MqttMessage message, boolean dup, int bytes) {
        lock.lock();
        try {
            var recipients = new ArrayList<Lane>();
            for (long id : ids) {
                Lane lane = lanes.get(id);
                if (lane != null && lane.generation == generation && !lane.paused) {
                    if (lane.retained >= lane.capacity) {
                        return false;
                    }
                    recipients.add(lane);
                }
            }
            if (recipients.isEmpty()) {
                return true;
            }
            if (recipients.size() > DELIVERY_LIMIT - retainedDeliveries
                    || bytes > byteLimit - retainedBytes) {
                return false;
            }
            var delivery = new RetainedDelivery(message, dup, bytes, recipients.size());
            retainedBytes += bytes;
            retainedDeliveries += recipients.size();
            for (Lane lane : recipients) {
                lane.retained++;
                lane.queue.addLast(delivery);
                lane.ready.signal();
            }
            return true;
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
                stop(lane);
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
        RetainedDelivery delivery;
        lock.lock();
        try {
            while (!lane.stopped && lane.queue.isEmpty()) {
                lane.ready.await();
            }
            if (lane.stopped) {
                return false;
            }
            // Claiming and stopping share the lock. A claimed handler may finish.
            delivery = lane.queue.removeFirst();
        } finally {
            lock.unlock();
        }
        try {
            lane.handler.accept(delivery);
        } catch (Throwable failure) {
            lock.lock();
            try {
                abandonedDeliveries++;
                release(lane, delivery);
                stop(lane);
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
            release(lane, delivery);
        } finally {
            lock.unlock();
        }
        return true;
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

    /// Caller holds lock; never interrupts an executing application handler.
    private void stop(Lane lane) {
        lanes.remove(lane.id, lane);
        lane.stopped = true;
        while (!lane.queue.isEmpty()) {
            abandonedDeliveries++;
            release(lane, lane.queue.removeFirst());
        }
        lane.ready.signal();
    }

    private void release(Lane lane, RetainedDelivery delivery) {
        lane.retained--;
        retainedDeliveries--;
        if (--delivery.remaining == 0) {
            retainedBytes -= delivery.bytes;
        }
    }

    private static final class Lane {

        final long generation;
        final long id;
        final int capacity;
        final Consumer<MqttDelivery> handler;
        final Condition ready;
        final ArrayDeque<RetainedDelivery> queue = new ArrayDeque<>();
        int retained;
        boolean paused;
        boolean stopped;

        Lane(
                long generation,
                long id,
                int capacity,
                Consumer<MqttDelivery> handler,
                Condition ready
        ) {
            this.generation = generation;
            this.id = id;
            this.capacity = capacity;
            this.handler = handler;
            this.ready = ready;
        }
    }

    private static final class RetainedDelivery implements MqttDelivery {

        private final MqttMessage message;
        private final boolean dup;
        private final int bytes;
        private int remaining;

        RetainedDelivery(MqttMessage message, boolean dup, int bytes, int remaining) {
            this.message = message;
            this.dup = dup;
            this.bytes = bytes;
            this.remaining = remaining;
        }

        @Override
        public MqttMessage message() {
            return message;
        }

        @Override
        public boolean dup() {
            return dup;
        }
    }
}
