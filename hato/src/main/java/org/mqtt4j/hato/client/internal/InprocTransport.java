package org.mqtt4j.hato.client.internal;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Transport;
import org.mqtt4j.hato.internal.concurrent.CarrierShare;
import org.mqtt4j.hato.internal.inproc.InprocBrokers;
import org.mqtt4j.hato.internal.inproc.InprocLink;

import static java.util.Objects.requireNonNull;

/// The client's end of a connection to a broker in the same JVM, which the URI `inproc://<name>`
/// names. To the client it is a transport as TCP is, whose unit is the packet: a thread of the
/// client's core connects it and then reads it, the client's sender writes to it on its own
/// thread, and the client can pause its reading. Packets pass as they are, with the bytes that
/// they take on the wire, so neither side encodes or decodes them.
///
/// A read takes the packets that wait, up to [#READ_LIMIT]. A read that takes fewer has emptied
/// the queue, so the client runs its events on the reading thread, as after a TCP read that does
/// not fill its buffer; one that takes the limit leaves them to the client's actor. The reading
/// thread follows the rules of a TCP transport's: a client whose core runs on platform threads
/// reads on one of its own, and one on virtual threads takes a platform reader while the JVM has
/// one to spare, and moves between the two kinds as [PlatformReaders] says.
public final class InprocTransport implements Transport, PausableTransport {

    // The most packets that one read takes.
    static final int READ_LIMIT = 64;

    private final String threadName;

    private final CoreThreads cores;

    private final PlatformReaders platformReaders;

    // Guards the state below and serializes the listener's calls. Only the client's threads take
    // it; the link to the broker takes no lock.
    private final Object callbacks = new Object();

    private @Nullable PacketListener listener;

    private @Nullable InprocLink link;

    private boolean started;

    private boolean connected;

    // Written under callbacks; read without it to stop waiting and reading.
    private volatile boolean closed;

    private volatile boolean readingPaused;

    private @Nullable Throwable terminalCause;

    // The reader once the link exists, and each write that runs: onClosed waits for them.
    private int ioWorkers;

    /// A transport for the client `clientId`, whose reading thread is of the kind of `cores`.
    public InprocTransport(String clientId, CoreThreads cores) {
        this(clientId, cores, PlatformReaders.shared);
    }

    InprocTransport(String clientId, CoreThreads cores, PlatformReaders platformReaders) {
        threadName = "hato-" + requireNonNull(clientId, "clientId");
        this.cores = requireNonNull(cores, "cores");
        this.platformReaders = requireNonNull(platformReaders, "platformReaders");
    }

    @Override
    public void start(URI endpoint, Duration timeout, Listener receiver) {
        requireNonNull(endpoint, "endpoint");
        requireNonNull(timeout, "timeout");
        requireNonNull(receiver, "listener");
        if (!(receiver instanceof PacketListener packets)) {
            throw new IllegalArgumentException("An in-process transport passes packets");
        }
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("Connect timeout must be positive");
        }
        if (!InprocBrokers.SCHEME.equals(endpoint.getScheme()) || endpoint.getHost() == null) {
            throw new IllegalArgumentException(
                    "Transport requires " + InprocBrokers.SCHEME + " scheme"
            );
        }
        String name = endpoint.getHost();
        long nanos = timeout.toNanos();
        synchronized (callbacks) {
            if (started) {
                throw new IllegalStateException("Transport already started");
            }
            started = true;
            listener = packets;
            if (closed) {
                notifyClosed();
                return;
            }
            cores.builder()
                    .name(threadName + "-reader")
                    .start(() -> connectAndRead(name, nanos, packets));
        }
    }

    private void connectAndRead(String name, long nanos, PacketListener receiver) {
        boolean attached = false;
        try {
            InprocBrokers.Endpoint broker = InprocBrokers.find(name);
            if (broker == null) {
                throw new ConnectException("No broker named " + name + " runs in this JVM");
            }
            var joined = new InprocLink();
            synchronized (callbacks) {
                if (closed) {
                    return;
                }
                link = joined;
                ioWorkers++;
                attached = true;
            }
            broker.connect(joined);
            if (!joined.awaitAnswer(nanos)) {
                if (closed) {
                    return;
                }
                if (joined.brokerEnded()) {
                    throw new ConnectException("Broker " + name + " refused the connection");
                }
                throw new SocketTimeoutException("Transport connection deadline expired");
            }
            synchronized (callbacks) {
                if (closed) {
                    return;
                }
                connected = true;
                receiver.onConnected();
                if (closed) {
                    return;
                }
                // A thread of a core on platform threads reads on itself.
                if (!cores.platform() && readOnPlatformThread(joined, receiver)) {
                    // That thread ends this worker when it stops reading.
                    attached = false;
                    return;
                }
            }
            if (read(joined, receiver)) {
                // The reading moved to a thread that ends this worker when it stops.
                attached = false;
            }
        } catch (Throwable failure) {
            terminate(failure);
        } finally {
            if (attached) {
                workerEnded();
            }
        }
    }

    /// Caller holds callbacks, once connected and still open. Hands the reading to a platform
    /// thread of its own while the JVM has one to spare, and returns whether it did; otherwise
    /// the caller reads on.
    private boolean readOnPlatformThread(InprocLink joined, PacketListener receiver) {
        if (!platformReaders.take()) {
            return false;
        }
        Thread platform = Thread.ofPlatform()
                .daemon()
                .name(threadName + "-reader")
                .unstarted(() -> readAfterMove(joined, receiver, true));
        try {
            platform.start();
        } catch (OutOfMemoryError _) {
            // The system refused another thread; the caller reads instead.
            platformReaders.giveBack();
            return false;
        }
        return true;
    }

    /// Caller holds callbacks, on a platform reader of a connection that is still open. Hands
    /// the reading to a virtual thread and gives the platform thread back, and returns whether it
    /// did; otherwise the caller reads on.
    private boolean readOnVirtualThread(InprocLink joined, PacketListener receiver) {
        Thread virtual = Thread.ofVirtual()
                .name(threadName + "-reader")
                .unstarted(() -> readAfterMove(joined, receiver, false));
        try {
            virtual.start();
        } catch (OutOfMemoryError _) {
            // The system refused another thread; the caller reads instead.
            return false;
        }
        platformReaders.giveBack();
        return true;
    }

    /// Runs on a thread that the reading moved to: reads until the reading ends or moves on
    /// again, and ends the worker unless it moved.
    private void readAfterMove(InprocLink joined, PacketListener receiver, boolean platform) {
        boolean moved = false;
        try {
            moved = read(joined, receiver);
        } catch (Throwable failure) {
            terminate(failure);
        } finally {
            if (!moved) {
                if (platform) {
                    // Before onClosed, so that a connection that replaces this one can take the
                    // slot.
                    platformReaders.giveBack();
                }
                workerEnded();
            }
        }
    }

    /// Called by the reading thread between reads, with no lock held: moves the reading to a
    /// platform thread of its own when `toPlatform`, and otherwise to a virtual thread, unless
    /// the transport has closed; returns whether it did.
    private boolean moveReading(InprocLink joined, PacketListener receiver, boolean toPlatform) {
        synchronized (callbacks) {
            if (closed) {
                return false;
            }
            return toPlatform
                    ? readOnPlatformThread(joined, receiver)
                    : readOnVirtualThread(joined, receiver);
        }
    }

    // Reads until the broker ends its side or the transport closes, and returns false; or moves
    // the reading, between reads, to a thread of the other kind and returns true.
    private boolean read(InprocLink joined, PacketListener receiver) throws InterruptedException {
        var packets = new ArrayList<InprocLink.Item>(READ_LIMIT);
        // A platform reader holds no carrier that others wait for.
        boolean sharesCarrier = Thread.currentThread().isVirtual();
        // Tells this thread when the reading should move to a thread of the other kind; null
        // where the platform readers stay with the connections that took them, and for a core on
        // platform threads, whose reading stays on its thread and holds no platform reader.
        PlatformReaders.Watch watch = cores.platform()
                ? null
                : platformReaders.watch(!sharesCarrier, receiver, System.nanoTime());
        try {
            return read(joined, receiver, packets, sharesCarrier, watch);
        } finally {
            if (watch != null) {
                watch.stop();
            }
        }
    }

    private boolean read(
            InprocLink joined,
            PacketListener receiver,
            List<InprocLink.Item> packets,
            boolean sharesCarrier,
            PlatformReaders.@Nullable Watch watch
    ) throws InterruptedException {
        // Only a platform reader whose thread may move bounds its waits, to look up from a
        // connection that receives nothing.
        long timeout = watch == null ? 0 : watch.readTimeout().toNanos();
        // The time spent on what arrived since the reader last let others run. A wait for the
        // broker frees the carrier, so only the work on the packets counts.
        long worked = 0;
        while (!closed) {
            // Packets come before the end of the broker's side, so an end seen before the read
            // that finds none leaves none behind.
            boolean ended = joined.brokerEnded();
            InprocLink.Item item;
            while (packets.size() < READ_LIMIT && (item = joined.receive()) != null) {
                packets.add(item);
            }
            if (packets.isEmpty()) {
                if (ended) {
                    terminate(new EOFException("Broker closed the connection"));
                    return false;
                }
                joined.awaitPackets(timeout);
                worked = 0;
                if (timeout > 0) {
                    long now = System.nanoTime();
                    if (watch != null && watch.afterTimeout(now)) {
                        if (moveReading(joined, receiver, false)) {
                            return true;
                        }
                        watch.stay(now);
                    }
                }
                continue;
            }
            long started = System.nanoTime();
            synchronized (callbacks) {
                if (closed) {
                    return false;
                }
                receiver.onPackets(packets, packets.size() == READ_LIMIT);
            }
            packets.clear();
            // Without the lock, where what the listener does may call this transport.
            receiver.afterBorrowedBytes();
            worked += System.nanoTime() - started;
            // A pause, which the listener may have asked for, leaves the broker's packets in the
            // link, which then holds the broker back, until the client resumes.
            if (readingPaused) {
                synchronized (callbacks) {
                    while (readingPaused && !closed) {
                        callbacks.wait();
                    }
                }
                worked = 0;
                if (watch != null) {
                    watch.restart(System.nanoTime());
                }
            }
            // The client is done with what this read returned, so another thread can read next.
            if (watch != null && watch.afterRead(started)) {
                if (moveReading(joined, receiver, sharesCarrier)) {
                    return true;
                }
                watch.stay(started);
            }
            // A broker that keeps sending keeps the reader from waiting, and a virtual reader from
            // freeing its carrier for the client's other threads, so the reader parks once its
            // work fills a slice.
            if (sharesCarrier && CarrierShare.spent(worked)) {
                CarrierShare.share();
                worked = 0;
            }
        }
        return false;
    }

    /// Hands `packets` to the broker on the calling thread, which may wait for room, and reports
    /// the write through `onSent` before it returns. A write that fails ends the connection,
    /// whose `onClosed` releases it. Returns as [Transport#send] does.
    SendResult write(long ticket, List<InprocLink.Item> packets) {
        InprocLink joined;
        synchronized (callbacks) {
            if (closed) {
                return SendResult.CLOSED;
            }
            joined = link;
            if (joined == null || !connected) {
                throw new IllegalStateException("Transport not connected");
            }
            // onClosed waits for this write.
            ioWorkers++;
        }
        try {
            for (InprocLink.Item packet : packets) {
                while (!joined.send(packet)) {
                    if (closed) {
                        return SendResult.ACCEPTED;
                    }
                    if (joined.brokerEnded()) {
                        throw new IOException("Broker closed the connection");
                    }
                    joined.awaitRoom(packet.size());
                }
            }
            synchronized (callbacks) {
                PacketListener receiver = listener;
                // Report a finished write even while closing, as a TCP transport does.
                if (receiver != null) {
                    receiver.onSent(ticket);
                }
            }
        } catch (Throwable failure) {
            terminate(failure);
        } finally {
            workerEnded();
        }
        return SendResult.ACCEPTED;
    }

    /// Not used: the client hands this transport packets through [#write(long, List)].
    @Override
    public SendResult send(long ticket, byte[] bytes) {
        throw new UnsupportedOperationException("An in-process transport passes packets");
    }

    @Override
    public void pauseReading() {
        synchronized (callbacks) {
            readingPaused = true;
        }
    }

    @Override
    public void resumeReading() {
        synchronized (callbacks) {
            readingPaused = false;
            callbacks.notifyAll();
        }
    }

    @Override
    public void close() {
        terminate(new IOException("Transport closed"));
    }

    private void terminate(Throwable cause) {
        InprocLink joined;
        synchronized (callbacks) {
            if (closed) {
                return;
            }
            closed = true;
            terminalCause = cause;
            callbacks.notifyAll();
            joined = link;
            notifyClosed();
        }
        // Wakes the broker's side and this client's threads that wait on the link.
        if (joined != null) {
            joined.endClient();
        }
    }

    private void workerEnded() {
        synchronized (callbacks) {
            ioWorkers--;
            notifyClosed();
        }
    }

    // Caller holds callbacks.
    private void notifyClosed() {
        PacketListener receiver = listener;
        Throwable cause = terminalCause;
        if (closed && ioWorkers == 0 && receiver != null && cause != null) {
            listener = null;
            receiver.onClosed(cause);
        }
    }
}
