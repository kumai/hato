package org.mqtt4j.mqtj.client.internal;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import org.jspecify.annotations.Nullable;

/// FIFO order is shared by both admission classes. Termination has its own slot.
/// Generation changes and admission share the monitor so old callbacks cannot consume capacity.
final class Mailbox {

    enum OfferResult {
        ACCEPTED,
        FULL,
        STALE
    }

    record Event(long generation, Runnable action, boolean control, int bytes) {
    }

    private final ArrayDeque<Event> events = new ArrayDeque<>();

    private final int dataLimit;

    private final int controlLimit;

    private final long byteLimit;

    private long activeGeneration;

    private int dataCount;

    private int controlCount;

    private long bytes;

    private @Nullable Event termination;

    Mailbox(int dataLimit, int controlLimit, long byteLimit) {
        this.dataLimit = dataLimit;
        this.controlLimit = controlLimit;
        this.byteLimit = byteLimit;
    }

    /// Called by the actor before scheduling timers or opening I/O for the new connection.
    synchronized void activate(long generation) {
        activeGeneration = generation;
    }

    synchronized OfferResult offer(Event event) {
        // API commands use generation zero and survive connection changes.
        if (event.generation() != 0 && event.generation() != activeGeneration) {
            return OfferResult.STALE;
        }
        if (event.control()
                ? controlCount >= controlLimit
                : dataCount >= dataLimit || event.bytes() > byteLimit - bytes
        ) {
            return OfferResult.FULL;
        }
        events.addLast(event);
        if (event.control()) {
            controlCount++;
        } else {
            dataCount++;
            bytes += event.bytes();
        }
        notifyAll();
        return OfferResult.ACCEPTED;
    }

    synchronized void terminate(Event event) {
        if (event.generation() == 0 || event.generation() != activeGeneration) {
            return;
        }
        if (termination == null) {
            termination = event;
        }
        notifyAll();
    }

    /// Preserves preceding input when possible; a full FIFO must not hide the terminal cause.
    synchronized void offerTermination(Event event) {
        if (offer(event) == OfferResult.FULL) {
            terminate(event);
        }
    }

    /// Waits for the next event; returns empty when the actor is stopping.
    synchronized Optional<Event> take(BooleanSupplier stopping) throws InterruptedException {
        while (events.isEmpty() && termination == null && !stopping.getAsBoolean()) {
            wait();
        }
        if (stopping.getAsBoolean()) {
            return Optional.empty();
        }
        Event end = termination;
        if (end != null) {
            termination = null;
            return Optional.of(end);
        }
        Event event = events.removeFirst();
        release(event);
        return Optional.of(event);
    }

    /// Stops admission and removes queued work for this connection; zero clears all work.
    /// Only the actor switches generations, so a taken event cannot become stale before execution.
    synchronized void discard(long generation) {
        if (generation == 0 || generation == activeGeneration) {
            activeGeneration = 0;
        }
        if (termination != null && (generation == 0 || termination.generation() == generation)) {
            termination = null;
        }
        events.removeIf(event -> {
            if (generation == 0 || event.generation() == generation) {
                release(event);
                return true;
            }
            return false;
        });
    }

    synchronized void wake() {
        notifyAll();
    }

    private void release(Event event) {
        if (event.control()) {
            controlCount--;
        } else {
            dataCount--;
            bytes -= event.bytes();
        }
    }
}
