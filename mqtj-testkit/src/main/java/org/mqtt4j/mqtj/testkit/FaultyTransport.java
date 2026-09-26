package org.mqtt4j.mqtj.testkit;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.MqttCapacityException;
import org.mqtt4j.mqtj.spi.TimerScheduler;
import org.mqtt4j.mqtj.spi.Transport;
import org.mqtt4j.mqtj.testkit.internal.common.log.Log;

import static java.util.Objects.requireNonNull;

/// Adds ordered receive delays and explicit disconnection to another transport.
/// Outgoing sends retain the delegate's admission and completion contract. Incoming data is
/// bounded to 256 chunks and 16 MiB; overload closes the transport with a capacity failure.
/// The wrapper owns and closes the delegate, but borrows the scheduler.
public final class FaultyTransport implements Transport {

    private static final int RECEIVE_CHUNK_LIMIT = 256;
    private static final int RECEIVE_BYTE_LIMIT = 16 * 1024 * 1024;

    private static final Log log = Log.of(FaultyTransport.class);

    private final Transport delegate;
    private final TimerScheduler scheduler;
    private final ArrayDeque<Incoming> incoming = new ArrayDeque<>();

    private @Nullable Listener listener;
    private Duration delay = Duration.ZERO;
    private @Nullable Throwable ended;
    private boolean delegateEnded;
    private boolean started;
    private long retainedBytes;
    private @Nullable Alarm alarm;

    public FaultyTransport(Transport delegate, TimerScheduler scheduler) {
        this.delegate = requireNonNull(delegate, "delegate");
        this.scheduler = requireNonNull(scheduler, "scheduler");
    }

    /// Changes the delay of subsequently received chunks. Earlier chunks keep their deadlines;
    /// later chunks never overtake them even when the delay is reduced.
    public synchronized FaultyTransport receiveDelay(Duration value) {
        if (value.isNegative()) {
            throw new IllegalArgumentException("Receive delay must not be negative");
        }
        value.toNanos();
        delay = value;
        return this;
    }

    @Override
    public synchronized void start(URI endpoint, Duration timeout, Listener receiver) {
        if (started) {
            throw new IllegalStateException("Transport already started");
        }
        started = true;
        listener = requireNonNull(receiver, "listener");
        if (ended != null) {
            notifyClosed();
            return;
        }
        delegate.start(endpoint, timeout, new Listener() {
            @Override
            public void onConnected() {
                synchronized (FaultyTransport.this) {
                    if (ended == null) {
                        receiver.onConnected();
                    }
                }
            }

            @Override
            public void onBytes(byte[] bytes) {
                enqueue(bytes, null);
            }

            @Override
            public void onSent(long ticket) {
                synchronized (FaultyTransport.this) {
                    if (ended == null) {
                        receiver.onSent(ticket);
                    }
                }
            }

            @Override
            public void onClosed(Throwable cause) {
                synchronized (FaultyTransport.this) {
                    delegateEnded = true;
                    if (ended != null) {
                        notifyClosed();
                        return;
                    }
                }
                enqueue(null, cause);
            }
        });
    }

    private synchronized void enqueue(byte @Nullable [] bytes, @Nullable Throwable cause) {
        if (ended != null) {
            return;
        }
        int size = bytes == null ? 0 : bytes.length;
        if (incoming.size() == RECEIVE_CHUNK_LIMIT || size > RECEIVE_BYTE_LIMIT - retainedBytes) {
            disconnect(new MqttCapacityException("Delayed receive capacity"));
            return;
        }
        long now = scheduler.nanoTime();
        long deadline = now + delay.toNanos();
        Incoming last = incoming.peekLast();
        if (last != null && last.deadline() - now > deadline - now) {
            deadline = last.deadline();
        }
        retainedBytes += size;
        incoming.addLast(new Incoming(deadline, bytes, cause));
        if (alarm == null) {
            deliver();
        }
    }

    private synchronized void deliver() {
        try {
            Listener receiver = listener;
            if (ended != null || receiver == null) {
                return;
            }
            Incoming item;
            while ((item = incoming.peekFirst()) != null) {
                long remaining = item.deadline() - scheduler.nanoTime();
                if (remaining > 0) {
                    var next = new Alarm();
                    alarm = next;
                    TimerScheduler.Cancellable cancellation = scheduler.schedule(
                            Duration.ofNanos(remaining),
                            () -> {
                                synchronized (FaultyTransport.this) {
                                    if (alarm == next) {
                                        alarm = null;
                                        deliver();
                                    }
                                }
                            }
                    );
                    next.cancellation = cancellation;
                    if (alarm != next) {
                        cancellation.cancel();
                    }
                    return;
                }
                incoming.removeFirst();
                byte[] bytes = item.bytes();
                if (bytes != null) {
                    retainedBytes -= bytes.length;
                    receiver.onBytes(bytes);
                } else {
                    disconnect(requireNonNull(item.failure()));
                    return;
                }
            }
        } catch (Throwable failure) {
            disconnect(failure);
        }
    }

    @Override
    public SendResult send(long ticket, byte[] bytes) {
        synchronized (this) {
            if (ended != null) {
                return SendResult.CLOSED;
            }
        }
        return delegate.send(ticket, bytes);
    }

    /// Ends the connection immediately and discards delayed input.
    public void disconnect(Throwable cause) {
        synchronized (this) {
            if (ended != null) {
                return;
            }
            ended = requireNonNull(cause, "cause");
            Alarm pending = alarm;
            alarm = null;
            if (pending != null && pending.cancellation != null) {
                pending.cancellation.cancel();
            }
            incoming.clear();
            retainedBytes = 0;
            if (delegateEnded) {
                notifyClosed();
                return;
            }
        }
        // Closing can originate inside a delegate callback or while the timer owns our monitor.
        // A separate worker avoids reversing the delegate's callback-lock order.
        Thread.ofVirtual().name("mqtj-faulty-transport-close").start(() -> {
            try {
                delegate.close();
            } catch (Throwable failure) {
                synchronized (FaultyTransport.this) {
                    if (failure != cause) {
                        cause.addSuppressed(failure);
                    }
                    notifyClosed();
                }
            }
        });
    }

    private void notifyClosed() {
        Listener receiver = listener;
        Throwable cause = ended;
        listener = null;
        if (receiver != null && cause != null) {
            try {
                // The delegate's terminal callback, not close() returning, releases send buffers.
                receiver.onClosed(cause);
            } catch (Throwable failure) {
                log.warn("Transport termination listener failed", failure);
            }
        }
    }

    @Override
    public void close() {
        disconnect(new IOException("Faulty transport closed"));
    }

    private record Incoming(long deadline, byte @Nullable [] bytes, @Nullable Throwable failure) {
    }

    private static final class Alarm {

        TimerScheduler.@Nullable Cancellable cancellation;
    }
}
