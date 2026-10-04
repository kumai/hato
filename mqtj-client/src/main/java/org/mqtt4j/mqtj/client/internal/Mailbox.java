package org.mqtt4j.mqtj.client.internal;

import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import org.jspecify.annotations.Nullable;

/// FIFO order is shared by all admission classes. Termination has its own slot.
/// Generation changes and admission share the monitor so old callbacks cannot consume capacity.
final class Mailbox {

    enum OfferResult {
        ACCEPTED,
        FULL,
        STALE
    }

    enum Kind {
        COMMAND,
        INCOMING,
        CONTROL,
        DELIVERY_READY,
        SEND_CAPACITY
    }

    record Event(long generation, Runnable action, Kind kind, int bytes) {
    }

    private final ArrayDeque<Event> events = new ArrayDeque<>();

    // Each wake has one independent FIFO slot. Its state lives at the producer, not in events.
    private final Map<Kind, Event> wakes = new EnumMap<>(Kind.class);

    private final int commandLimit;

    private final long commandByteLimit;

    private final int incomingLimit;

    private final long incomingByteLimit;

    private final int controlLimit;

    private long activeGeneration;

    private int commandCount;

    private long commandBytes;

    private int incomingCount;

    private long incomingBytes;

    private int controlCount;

    private @Nullable Event termination;

    Mailbox(
            int commandLimit,
            long commandByteLimit,
            int incomingLimit,
            long incomingByteLimit,
            int controlLimit
    ) {
        this.commandLimit = commandLimit;
        this.commandByteLimit = commandByteLimit;
        this.incomingLimit = incomingLimit;
        this.incomingByteLimit = incomingByteLimit;
        this.controlLimit = controlLimit;
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
        if (event.kind() == Kind.DELIVERY_READY || event.kind() == Kind.SEND_CAPACITY) {
            if (event.generation() == 0) {
                return OfferResult.STALE;
            }
            Event pending = wakes.get(event.kind());
            if (pending != null && pending.generation() == event.generation()) {
                return OfferResult.ACCEPTED;
            }
        }
        boolean full = switch (event.kind()) {
            case COMMAND -> commandCount >= commandLimit
                    || event.bytes() > commandByteLimit - commandBytes;
            case INCOMING -> incomingCount >= incomingLimit
                    || event.bytes() > incomingByteLimit - incomingBytes;
            case CONTROL -> controlCount >= controlLimit;
            case DELIVERY_READY, SEND_CAPACITY -> false;
        };
        if (full) {
            return OfferResult.FULL;
        }
        events.addLast(event);
        switch (event.kind()) {
            case COMMAND -> {
                commandCount++;
                commandBytes += event.bytes();
            }
            case INCOMING -> {
                incomingCount++;
                incomingBytes += event.bytes();
            }
            case CONTROL -> controlCount++;
            case DELIVERY_READY, SEND_CAPACITY -> wakes.put(event.kind(), event);
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
        switch (event.kind()) {
            case COMMAND -> {
                commandCount--;
                commandBytes -= event.bytes();
            }
            case INCOMING -> {
                incomingCount--;
                incomingBytes -= event.bytes();
            }
            case CONTROL -> controlCount--;
            case DELIVERY_READY, SEND_CAPACITY -> wakes.remove(event.kind(), event);
        }
    }
}
