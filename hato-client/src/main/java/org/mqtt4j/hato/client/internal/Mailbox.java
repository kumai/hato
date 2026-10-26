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
/// session changes share the admission monitor so old callbacks cannot consume capacity. QoS 0
/// publications have an incoming allowance of their own, as large as that of the other incoming
/// packets: the client drops the ones that do not fit, and they cannot take the room of a packet
/// that the client must not drop.
///
/// The mailbox also paces the transport that reads incoming packets. Its reader pauses once they
/// fill half of an allowance and resumes once the actor has taken them back to a quarter, so that
/// flow control rather than the allowances holds back a broker that sends faster than the actor
/// routes. While a PINGRESP is due the reader goes on instead: it overtakes the queue only once
/// it has been read, and what does not fit meanwhile meets the allowances.
///
/// One thread at a time runs the events, in the order described above; this class calls it the
/// actor. It is the client's actor thread, or another thread of the client that posts an event
/// while none runs them: that thread then runs them itself, in place, rather than waking the actor
/// thread, which costs more than a small event. It lets go of them once none is left, or hands
/// them over to the actor thread.
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
        // A received QoS 0 publication, which the client may drop.
        AT_MOST_ONCE,
        CONTROL,
        KEEP_ALIVE,
        DELIVERY_READY,
        SEND_CAPACITY,
        CREDENTIALS_READY,
        SUBSCRIPTION_CLOSE,
        RECONNECT,
        STATS;

        /// Whether the event carries a packet that the transport received, which paces it.
        boolean received() {
            return switch (this) {
                case INCOMING,
                     AT_MOST_ONCE -> true;
                case COMMAND,
                     REGISTRATION,
                     CONTROL,
                     KEEP_ALIVE,
                     DELIVERY_READY,
                     SEND_CAPACITY,
                     CREDENTIALS_READY,
                     SUBSCRIPTION_CLOSE,
                     RECONNECT,
                     STATS -> false;
            };
        }

        /// A wake keeps at most one queued event per generation and is never full. Admission,
        /// queueing, and release all ask this, so a new kind is classified in one place.
        boolean wake() {
            return switch (this) {
                case COMMAND,
                     REGISTRATION,
                     INCOMING,
                     AT_MOST_ONCE,
                     CONTROL,
                     KEEP_ALIVE -> false;
                case DELIVERY_READY,
                     SEND_CAPACITY,
                     CREDENTIALS_READY,
                     SUBSCRIPTION_CLOSE,
                     RECONNECT,
                     STATS -> true;
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

    private final int registrationLimit;

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

    private int atMostOnceCount;

    private long atMostOnceBytes;

    // Written under the monitor; the actor reads it without, to skip resumeDue() when unpaused.
    private volatile boolean readingPaused;

    private boolean pingResponseDue;

    private int controlCount;

    private @Nullable Event termination;

    // A stop belongs to the session and connection request, so it survives connection changes.
    private @Nullable Event stop;

    private long stopSession;

    // The thread that runs the events now, or null while none does. Written under the monitor; a
    // thread reads it without, to learn whether it runs them itself, which only it changes.
    private volatile @Nullable Thread holder;

    // Set once the actor ends: the actor thread then waits for the thread that runs the events to
    // let go.
    private boolean ending;

    Mailbox(
            int commandLimit,
            long commandByteLimit,
            int registrationLimit,
            int incomingLimit,
            long incomingByteLimit,
            int controlLimit
    ) {
        this.commandLimit = commandLimit;
        this.commandByteLimit = commandByteLimit;
        this.registrationLimit = registrationLimit;
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
        return offer(event, false);
    }

    /// Admits `event` as [#offer(Event)] does. When `here` and no thread runs the events, the
    /// calling thread takes them over instead of waking the actor: it must then run them with
    /// [#poll()] as soon as it holds no lock that their actions take, and before it waits for
    /// anything.
    synchronized OfferResult offer(Event event, boolean here) {
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
            case REGISTRATION -> registrationCount >= registrationLimit;
            case INCOMING -> incomingCount >= incomingLimit
                    || event.bytes() > incomingByteLimit - incomingBytes;
            case AT_MOST_ONCE -> atMostOnceCount >= incomingLimit
                    || event.bytes() > incomingByteLimit - atMostOnceBytes;
            case CONTROL -> controlCount >= controlLimit;
            case KEEP_ALIVE -> keepAlive.size() >= KEEP_ALIVE_LIMIT;
            case DELIVERY_READY, SEND_CAPACITY, SUBSCRIPTION_CLOSE, CREDENTIALS_READY,
                    RECONNECT, STATS -> false;
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
            case AT_MOST_ONCE -> {
                atMostOnceCount++;
                atMostOnceBytes += event.bytes();
            }
            case CONTROL -> controlCount++;
            case KEEP_ALIVE -> {
                // The keep-alive queue's size is its count.
            }
        }
        if (here && holder == null) {
            holder = Thread.currentThread();
        } else {
            wakeActor();
        }
        return OfferResult.ACCEPTED;
    }

    /// Whether the reader of `generation` should pause, because received packets fill half of an
    /// allowance while no PINGRESP is due. True once, until [#resumeDue()] or the end of the
    /// connection. Called by the reader after posting a received packet.
    synchronized boolean pauseDue(long generation) {
        if (readingPaused || pingResponseDue || generation != activeGeneration
                || (incomingCount + atMostOnceCount < incomingLimit / 2
                        && incomingBytes + atMostOnceBytes < incomingByteLimit / 2)) {
            return false;
        }
        readingPaused = true;
        return true;
    }

    /// Whether a paused reader should resume, because the actor has taken received packets back
    /// to a quarter of an allowance. True once for each pause. Called by the actor after taking a
    /// received packet.
    boolean resumeDue() {
        if (!readingPaused) {
            return false;
        }
        synchronized (this) {
            if (!readingPaused
                    || incomingCount + atMostOnceCount > incomingLimit / 4
                    || incomingBytes + atMostOnceBytes > incomingByteLimit / 4) {
                return false;
            }
            readingPaused = false;
            return true;
        }
    }

    /// Records whether a PINGRESP is due on the active connection, and returns whether a paused
    /// reader must resume at once to reach it.
    synchronized boolean pingResponseDue(boolean due) {
        pingResponseDue = due;
        if (due && readingPaused) {
            readingPaused = false;
            return true;
        }
        return false;
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
        wakeActor();
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
        wakeActor();
        return true;
    }

    /// Preserves preceding input when possible; a full FIFO must not hide the terminal cause.
    synchronized void offerTermination(Event event) {
        if (offer(event) == OfferResult.FULL) {
            terminate(event);
        }
    }

    /// For the actor thread: waits until an event is queued and no other thread runs the events,
    /// then runs them and returns the next; returns empty when the actor is stopping. While
    /// nothing is queued, the actor thread lets go of the events, so that a thread that posts one
    /// can run it.
    synchronized Optional<Event> take(BooleanSupplier stopping) throws InterruptedException {
        Thread current = Thread.currentThread();
        while (!stopping.getAsBoolean()) {
            if (holder == null || holder == current) {
                Event next = next();
                if (next != null) {
                    holder = current;
                    return Optional.of(remove(next));
                }
                holder = null;
            }
            wait();
        }
        return Optional.empty();
    }

    /// Whether the calling thread runs the events now.
    boolean holds() {
        return holder == Thread.currentThread();
    }

    /// For a thread that runs the events: takes the next one, or lets go of them and returns null
    /// when none is queued.
    synchronized @Nullable Event poll() {
        if (holder != Thread.currentThread()) {
            throw new IllegalStateException("The calling thread does not run the events");
        }
        Event next = next();
        if (next != null) {
            return remove(next);
        }
        holder = null;
        if (ending) {
            notifyAll();
        }
        return null;
    }

    /// For a thread that runs the events: lets go of them, and wakes the actor thread to run those
    /// that are still queued. Does nothing for a thread that does not run them.
    void handOver() {
        if (holds()) {
            synchronized (this) {
                holder = null;
                notifyAll();
            }
        }
    }

    /// For the actor thread once the actor ends: waits until no other thread runs the events, then
    /// runs them for good, so that none runs beside the actor's cleanup.
    synchronized void holdForGood() {
        Thread current = Thread.currentThread();
        ending = true;
        boolean interrupted = false;
        while (holder != null && holder != current) {
            try {
                wait();
            } catch (InterruptedException _) {
                // The thread that runs the events lets go after its current one; keep waiting.
                interrupted = true;
            }
        }
        holder = current;
        if (interrupted) {
            current.interrupt();
        }
    }

    // A thread that runs the events takes those posted meanwhile before it lets go, so only an
    // event queued while none runs them wakes the actor thread.
    private void wakeActor() {
        if (holder == null) {
            notifyAll();
        }
    }

    /// Takes, without waiting, the received QoS 0 publications that come next, up to `limit`, and
    /// adds them to `into` in order, so that the actor can go on with a run of them. It stops at
    /// the first event of another kind, which stays queued; an event that goes ahead of the FIFO
    /// therefore ends the run, as stopping does. The run leaves under one hold of the monitor,
    /// which the reader takes for each packet it posts: taking the publications one at a time
    /// made the two contend for it on every one when they ran at once.
    synchronized void takeAtMostOnce(BooleanSupplier stopping, int limit, List<Event> into) {
        for (int taken = 0; taken < limit; taken++) {
            Event next = next();
            if (next == null || next.kind() != Kind.AT_MOST_ONCE || stopping.getAsBoolean()) {
                return;
            }
            into.add(remove(next));
        }
    }

    // The event to take next: a stop request, then a termination, then keep-alive events, then
    // the FIFO.
    private @Nullable Event next() {
        if (stop != null) {
            return stop;
        }
        if (termination != null) {
            return termination;
        }
        Event alive = keepAlive.peekFirst();
        return alive != null ? alive : events.peekFirst();
    }

    // Removes `next`, which next() has just returned.
    private Event remove(Event next) {
        if (next == stop) {
            stop = null;
            stopSession = 0;
            termination = null;
        } else if (next == termination) {
            termination = null;
        } else {
            (keepAlive.isEmpty() ? events : keepAlive).removeFirst();
            release(next);
        }
        return next;
    }

    /// Stops admission and removes queued work for this connection; zero clears all work.
    /// Only the actor switches generations, so a taken event cannot become stale before execution.
    synchronized void discard(long generation) {
        if (generation == 0 || generation == activeGeneration) {
            activeGeneration = 0;
            // The next connection's transport starts reading unpaused.
            readingPaused = false;
            pingResponseDue = false;
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
        return commandBytes + incomingBytes + atMostOnceBytes;
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
            case AT_MOST_ONCE -> {
                atMostOnceCount--;
                atMostOnceBytes -= event.bytes();
            }
            case CONTROL -> controlCount--;
            case KEEP_ALIVE -> {
                // Leaving the keep-alive queue releases its slot.
            }
        }
    }
}
