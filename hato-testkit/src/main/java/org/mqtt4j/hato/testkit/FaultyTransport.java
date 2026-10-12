package org.mqtt4j.hato.testkit;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.spi.TimerScheduler;
import org.mqtt4j.hato.spi.Transport;
import org.mqtt4j.hato.testkit.internal.common.log.Log;

import static java.util.Objects.requireNonNull;

/// An experimental wrapper that adds ordered receive delays, send delays, and disconnection.
/// Use a long delay in either direction to simulate stopped traffic. A delay must fit in
/// nanoseconds, and the scheduler must accept the resulting deadline. At most one send is
/// outstanding; admission retains its array until `onSent` or the final `onClosed`.
/// Incoming data is bounded to 256 chunks and 16 MiB; overload closes the transport with a
/// capacity failure. The wrapper owns and closes the delegate, but borrows the scheduler.
public final class FaultyTransport implements Transport {

    private static final int RECEIVE_CHUNK_LIMIT = 256;
    private static final int RECEIVE_BYTE_LIMIT = 16 * 1024 * 1024;

    private static final Log log = Log.of(FaultyTransport.class);

    private final Transport delegate;
    private final TimerScheduler scheduler;
    private final ArrayDeque<Incoming> incoming = new ArrayDeque<>();

    private @Nullable Listener listener;
    private long receiveDelayNanos;
    private long sendDelayNanos;
    private @Nullable Throwable ended;
    private boolean delegateEnded;
    private boolean closing;
    private boolean started;
    private boolean connected;
    private boolean connectedPending;
    private boolean notifying;
    private int sendCalls;
    private long retainedBytes;
    private @Nullable Alarm alarm;
    private @Nullable Outgoing outgoing;

    public FaultyTransport(Transport delegate, TimerScheduler scheduler) {
        this.delegate = requireNonNull(delegate, "delegate");
        this.scheduler = requireNonNull(scheduler, "scheduler");
    }

    /// Changes the delay of subsequently received chunks. Earlier chunks keep their deadlines;
    /// later chunks never overtake them even when the delay is reduced.
    ///
    /// @throws IllegalArgumentException if `value` is negative
    /// @throws ArithmeticException if `value` does not fit in nanoseconds, about 292 years
    public synchronized FaultyTransport receiveDelay(Duration value) {
        receiveDelayNanos = delayNanos(value);
        return this;
    }

    /// Changes the delay of subsequently admitted sends. An admitted send keeps its deadline.
    /// Zero delegates immediately; a positive delay postpones delegation, not write completion.
    /// Closing before delegation discards the send; the final `onClosed` releases its array.
    ///
    /// @throws IllegalArgumentException if `value` is negative
    /// @throws ArithmeticException if `value` does not fit in nanoseconds, about 292 years
    public synchronized FaultyTransport sendDelay(Duration value) {
        sendDelayNanos = delayNanos(value);
        return this;
    }

    private static long delayNanos(Duration value) {
        if (value.isNegative()) {
            throw new IllegalArgumentException("Delay must not be negative");
        }
        return value.toNanos();
    }

    @Override
    public void start(URI endpoint, Duration timeout, Listener receiver) {
        boolean alreadyEnded;
        synchronized (this) {
            if (started) {
                throw new IllegalStateException("Transport already started");
            }
            started = true;
            listener = requireNonNull(receiver, "listener");
            alreadyEnded = ended != null;
            if (alreadyEnded) {
                delegateEnded = true;
            }
        }
        if (alreadyEnded) {
            drain();
            return;
        }
        delegate.start(endpoint, timeout, new Listener() {
            @Override
            public void onConnected() {
                synchronized (FaultyTransport.this) {
                    if (ended == null) {
                        connected = true;
                        connectedPending = true;
                    }
                }
                drain();
            }

            @Override
            public void onBytes(byte[] bytes) {
                enqueue(bytes, null);
            }

            @Override
            public void onSent(long ticket) {
                synchronized (FaultyTransport.this) {
                    Outgoing pending = outgoing;
                    if (pending != null && pending.ticket == ticket) {
                        pending.completed = true;
                    }
                }
                drain();
            }

            @Override
            public void onClosed(Throwable cause) {
                Alarm cancelled;
                synchronized (FaultyTransport.this) {
                    delegateEnded = true;
                    closing = true;
                    cancelled = stopSending();
                }
                cancel(cancelled);
                enqueue(null, cause);
                drain();
            }
        });
    }

    private void enqueue(byte @Nullable [] bytes, @Nullable Throwable cause) {
        try {
            synchronized (this) {
                if (ended != null) {
                    return;
                }
                int size = bytes == null ? 0 : bytes.length;
                if (incoming.size() == RECEIVE_CHUNK_LIMIT
                        || size > RECEIVE_BYTE_LIMIT - retainedBytes) {
                    throw new MqttCapacityException("Delayed receive capacity");
                }
                long now = scheduler.nanoTime();
                long deadline = now + receiveDelayNanos;
                Incoming last = incoming.peekLast();
                if (last != null && last.deadline() - now > deadline - now) {
                    deadline = last.deadline();
                }
                retainedBytes += size;
                incoming.addLast(new Incoming(deadline, bytes, cause));
            }
            drain();
        } catch (Throwable failure) {
            disconnect(failure);
        }
    }

    private void drain() {
        synchronized (this) {
            if (notifying) {
                return;
            }
            notifying = true;
        }
        for (;;) {
            Runnable notification;
            synchronized (this) {
                try {
                    notification = nextNotification();
                } catch (Throwable failure) {
                    notification = () -> disconnect(failure);
                }
                if (notification == null) {
                    notifying = false;
                    return;
                }
            }
            try {
                notification.run();
            } catch (Throwable failure) {
                disconnect(failure);
            }
        }
    }

    // Select one notification without moving received bytes into another, unbounded queue.
    private @Nullable Runnable nextNotification() {
        Listener receiver = listener;
        if (receiver == null) {
            return null;
        }
        if (connectedPending) {
            connectedPending = false;
            return receiver::onConnected;
        }
        Outgoing pending = outgoing;
        if (pending != null && pending.completed) {
            // Free the slot immediately before onSent, which may send again synchronously.
            outgoing = null;
            return () -> receiver.onSent(pending.ticket);
        }
        if (ended == null) {
            Incoming item = incoming.peekFirst();
            if (item != null) {
                long remaining = item.deadline() - scheduler.nanoTime();
                if (remaining > 0) {
                    if (alarm == null) {
                        var next = new Alarm();
                        alarm = next;
                        return () -> scheduleReceive(next, item.deadline());
                    }
                } else {
                    incoming.removeFirst();
                    byte[] bytes = item.bytes();
                    if (bytes != null) {
                        retainedBytes -= bytes.length;
                        return () -> receiver.onBytes(bytes);
                    }
                    return () -> disconnect(requireNonNull(item.failure()));
                }
            }
        } else if (delegateEnded && sendCalls == 0) {
            Throwable cause = ended;
            listener = null;
            outgoing = null;
            return () -> {
                try {
                    // Only delegate termination releases a buffer handed to the delegate.
                    receiver.onClosed(cause);
                } catch (Throwable failure) {
                    log.warn("Transport termination listener failed", failure);
                }
            };
        }
        return null;
    }

    private void scheduleReceive(Alarm next, long deadline) {
        TimerScheduler.Cancellable cancellation = scheduler.schedule(
                Duration.ofNanos(Math.max(0, deadline - scheduler.nanoTime())),
                () -> {
                    synchronized (this) {
                        if (alarm != next) {
                            return;
                        }
                        alarm = null;
                    }
                    drain();
                }
        );
        synchronized (this) {
            next.cancellation = cancellation;
            if (alarm == next) {
                return;
            }
        }
        cancel(next);
    }

    @Override
    public SendResult send(long ticket, byte[] bytes) {
        Outgoing pending;
        boolean delayed;
        synchronized (this) {
            if (ended != null || closing) {
                return SendResult.CLOSED;
            }
            if (!connected) {
                throw new IllegalStateException("Transport not connected");
            }
            if (outgoing != null) {
                throw new IllegalStateException("Send already outstanding");
            }
            delayed = sendDelayNanos > 0;
            pending = new Outgoing(ticket, requireNonNull(bytes, "bytes"),
                    delayed ? scheduler.nanoTime() + sendDelayNanos : 0);
            outgoing = pending;
        }
        if (!delayed) {
            return forward(pending);
        }
        try {
            TimerScheduler.Cancellable cancellation = scheduler.schedule(
                    Duration.ofNanos(Math.max(0, pending.deadline - scheduler.nanoTime())),
                    () -> {
                        try {
                            forward(pending);
                        } catch (Throwable failure) {
                            disconnect(failure);
                        }
                    }
            );
            synchronized (this) {
                pending.alarm.cancellation = cancellation;
                if (outgoing == pending && !pending.forwarded && ended == null && !closing) {
                    return SendResult.ACCEPTED;
                }
            }
            cancel(pending.alarm);
            return SendResult.ACCEPTED;
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                if (outgoing == pending) {
                    outgoing = null;
                }
            }
            throw failure;
        }
    }

    private SendResult forward(Outgoing pending) {
        synchronized (this) {
            if (outgoing != pending || pending.forwarded || ended != null || closing) {
                return SendResult.CLOSED;
            }
            pending.forwarded = true;
            // Inline completion may reenter send before this delegate call returns.
            sendCalls++;
        }
        try {
            SendResult result = delegate.send(pending.ticket, pending.bytes);
            if (result == SendResult.CLOSED) {
                synchronized (this) {
                    closing = true;
                }
            }
            return result;
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                if (outgoing == pending) {
                    outgoing = null;
                }
            }
            throw failure;
        } finally {
            synchronized (this) {
                sendCalls--;
            }
            drain();
        }
    }

    /// Ends the connection with `cause`: later sends are rejected, delayed and later input is
    /// discarded, and the delegate is closed. A write the delegate has already completed is
    /// still reported through `onSent` before the final `onClosed`, which carries `cause`.
    public void disconnect(Throwable cause) {
        Alarm cancelledReceive;
        Alarm cancelledSend;
        boolean closeDelegate;
        synchronized (this) {
            if (ended != null) {
                return;
            }
            ended = requireNonNull(cause, "cause");
            connectedPending = false;
            cancelledReceive = alarm;
            alarm = null;
            cancelledSend = stopSending();
            incoming.clear();
            retainedBytes = 0;
            closeDelegate = !delegateEnded;
        }
        cancel(cancelledReceive);
        cancel(cancelledSend);
        if (closeDelegate) {
            // Closing can originate inside a delegate callback. Avoid its callback-lock order.
            Thread.ofVirtual().name("hato-faulty-transport-close").start(() -> {
                try {
                    delegate.close();
                } catch (Throwable failure) {
                    synchronized (FaultyTransport.this) {
                        if (failure != cause) {
                            cause.addSuppressed(failure);
                        }
                        delegateEnded = true;
                    }
                    drain();
                }
            });
        }
        drain();
    }

    private @Nullable Alarm stopSending() {
        Outgoing pending = outgoing;
        return pending == null || pending.forwarded ? null : pending.alarm;
    }

    private static void cancel(@Nullable Alarm pending) {
        if (pending != null && pending.cancellation != null) {
            try {
                pending.cancellation.cancel();
            } catch (Throwable failure) {
                log.warn("Transport delay cancellation failed", failure);
            }
        }
    }

    @Override
    public void close() {
        disconnect(new IOException("Faulty transport closed"));
    }

    private record Incoming(long deadline, byte @Nullable [] bytes, @Nullable Throwable failure) {
    }

    private static final class Outgoing {

        final long ticket;
        final byte[] bytes;
        final long deadline;
        final Alarm alarm = new Alarm();
        boolean forwarded;
        boolean completed;

        Outgoing(long ticket, byte[] bytes, long deadline) {
            this.ticket = ticket;
            this.bytes = bytes;
            this.deadline = deadline;
        }
    }

    private static final class Alarm {

        volatile TimerScheduler.@Nullable Cancellable cancellation;
    }
}
