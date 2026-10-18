package org.mqtt4j.hato.client.internal;

import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import org.jspecify.annotations.Nullable;

/// FIFO order is shared by every admission class except keep-alive. Keep-alive events go ahead
/// of the FIFO, so a backlog of received packets cannot hold back the PINGREQ that the keep-alive
/// interval requires [MQTT-3.1.2-23]. They are the keep-alive and PINGRESP deadline timers and the
/// PINGRESPs that answer admitted PINGREQs. Their outcome does not depend on the events they
/// overtake, except that a send time not yet recorded can bring a PINGREQ forward. Termination has
/// its own slot ahead of both, and a session's stop request precedes termination. Generation and
/// session changes share the admission monitor so old callbacks cannot consume capacity.
final class Mailbox {

    enum OfferResult {
        ACCEPTED,
        FULL,
        STALE
    }

    enum Kind {
        COMMAND,
        REGISTRATION,
        INCOMING,
        CONTROL,
        KEEP_ALIVE,
        DELIVERY_READY,
        SEND_CAPACITY,
        CREDENTIALS_READY,
        SUBSCRIPTION_CLOSE,
        RECONNECT;

        /// A wake keeps at most one queued event per generation and is never full. Admission,
        /// queueing, and release all ask this, so a new kind is classified in one place.
        boolean wake() {
            return switch (this) {
                case COMMAND,
                     REGISTRATION,
                     INCOMING,
                     CONTROL,
                     KEEP_ALIVE -> false;
                case DELIVERY_READY,
                     SEND_CAPACITY,
                     CREDENTIALS_READY,
                     SUBSCRIPTION_CLOSE,
                     RECONNECT -> true;
            };
        }
    }

    record Event(long generation, Runnable action, Kind kind, int bytes) {
    }

    // Two timers and the answers to admitted PINGREQs, a few at a time. A full class ends the
    // connection as overload, like the control class.
    private static final int KEEP_ALIVE_LIMIT = 16;

    private final ArrayDeque<Event> events = new ArrayDeque<>();

    private final ArrayDeque<Event> keepAlive = new ArrayDeque<>();

    // Each wake has one independent FIFO slot. Its state lives at the producer, not in events.
    private final Map<Kind, Event> wakes = new EnumMap<>(Kind.class);

    private final int commandLimit;

    private final long commandByteLimit;

    private final int incomingLimit;

    private final long incomingByteLimit;

    private final int controlLimit;

    private long activeGeneration;

    // Session numbers start at one; zero means none.
    private long activeSession;

    private boolean connectionRequested;

    private int commandCount;

    private int registrationCount;

    private long commandBytes;

    private int incomingCount;

    private long incomingBytes;

    private int controlCount;

    private @Nullable Event termination;

    // A stop belongs to the session and connection request, so it survives connection changes.
    private @Nullable Event stop;

    private long stopSession;

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

    /// Session replacement and stop admission share the monitor. An admitted old handler
    /// must not occupy the new session's stop slot while its CONNACK is being processed.
    synchronized void activateSession(long session) {
        activeSession = session;
        if (stopSession != session) {
            stop = null;
            stopSession = 0;
        }
    }

    /// Closes stop admission before the actor releases failed lanes at the end of a request.
    synchronized void connectionRequested(boolean requested) {
        connectionRequested = requested;
        if (!requested) {
            stop = null;
            stopSession = 0;
        }
    }

    synchronized OfferResult offer(Event event) {
        // Client-owned commands and wakes use generation zero and survive connection changes.
        if (event.generation() != 0 && event.generation() != activeGeneration) {
            return OfferResult.STALE;
        }
        if (event.kind().wake()) {
            Event pending = wakes.get(event.kind());
            if (pending != null && pending.generation() == event.generation()) {
                return OfferResult.ACCEPTED;
            }
        }
        boolean full = switch (event.kind()) {
            case COMMAND -> commandCount >= commandLimit
                    || event.bytes() > commandByteLimit - commandBytes;
            // One burst can queue the core's whole registration limit without operation capacity.
            case REGISTRATION -> registrationCount >= SessionCore.REGISTRATION_LIMIT;
            case INCOMING -> incomingCount >= incomingLimit
                    || event.bytes() > incomingByteLimit - incomingBytes;
            case CONTROL -> controlCount >= controlLimit;
            case KEEP_ALIVE -> keepAlive.size() >= KEEP_ALIVE_LIMIT;
            case DELIVERY_READY, SEND_CAPACITY, SUBSCRIPTION_CLOSE, CREDENTIALS_READY,
                    RECONNECT -> false;
        };
        if (full) {
            return OfferResult.FULL;
        }
        (event.kind() == Kind.KEEP_ALIVE ? keepAlive : events).addLast(event);
        if (event.kind().wake()) {
            wakes.put(event.kind(), event);
        }
        switch (event.kind()) {
            case COMMAND -> {
                commandCount++;
                commandBytes += event.bytes();
            }
            case REGISTRATION -> registrationCount++;
            case INCOMING -> {
                incomingCount++;
                incomingBytes += event.bytes();
            }
            case CONTROL -> controlCount++;
            case KEEP_ALIVE -> {
                // The keep-alive queue's size is its count.
            }
        }
        notifyAll();
        return OfferResult.ACCEPTED;
    }

    /// Returns whether the ending belongs to the active connection, including when an earlier
    /// valid termination already owns the slot.
    synchronized boolean terminate(Event event) {
        if (event.generation() == 0 || event.generation() != activeGeneration) {
            return false;
        }
        if (termination == null) {
            termination = event;
        }
        notifyAll();
        return true;
    }

    /// Admits a current session's stop while connecting, connected, or awaiting reconnection.
    /// Later failures share the first stop and keep their lanes paused until it is processed.
    synchronized boolean stopSession(long session, Runnable action) {
        if (session != activeSession || !connectionRequested) {
            return false;
        }
        if (stop == null) {
            stop = new Event(0, action, Kind.CONTROL, 0);
            stopSession = session;
        }
        notifyAll();
        return true;
    }

    /// Preserves preceding input when possible; a full FIFO must not hide the terminal cause.
    synchronized void offerTermination(Event event) {
        if (offer(event) == OfferResult.FULL) {
            terminate(event);
        }
    }

    /// Waits for the next event; returns empty when the actor is stopping.
    synchronized Optional<Event> take(BooleanSupplier stopping) throws InterruptedException {
        while (events.isEmpty() && keepAlive.isEmpty() && termination == null && stop == null
                && !stopping.getAsBoolean()) {
            wait();
        }
        if (stopping.getAsBoolean()) {
            return Optional.empty();
        }
        Event request = stop;
        if (request != null) {
            stop = null;
            stopSession = 0;
            termination = null;
            return Optional.of(request);
        }
        Event end = termination;
        if (end != null) {
            termination = null;
            return Optional.of(end);
        }
        Event event = keepAlive.isEmpty() ? events.removeFirst() : keepAlive.removeFirst();
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
        if (generation == 0) {
            connectionRequested(false);
        }
        for (ArrayDeque<Event> queue : List.of(keepAlive, events)) {
            queue.removeIf(event -> {
                if (generation == 0 || event.generation() == generation) {
                    release(event);
                    return true;
                }
                return false;
            });
        }
    }

    synchronized void wake() {
        notifyAll();
    }

    /// Queued events, excluding pending stop and termination requests.
    synchronized int size() {
        return keepAlive.size() + events.size();
    }

    /// Bytes of queued commands and incoming packets.
    synchronized long bytes() {
        return commandBytes + incomingBytes;
    }

    private void release(Event event) {
        if (event.kind().wake()) {
            wakes.remove(event.kind(), event);
        }
        switch (event.kind()) {
            case COMMAND -> {
                commandCount--;
                commandBytes -= event.bytes();
            }
            case REGISTRATION -> registrationCount--;
            case INCOMING -> {
                incomingCount--;
                incomingBytes -= event.bytes();
            }
            case CONTROL -> controlCount--;
            case KEEP_ALIVE -> {
                // Leaving the keep-alive queue releases its slot.
            }
        }
    }
}
