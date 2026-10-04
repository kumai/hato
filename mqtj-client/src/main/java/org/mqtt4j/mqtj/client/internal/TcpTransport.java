package org.mqtt4j.mqtj.client.internal;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Semaphore;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.mqtj.client.internal.common.io.FrameWriter;
import org.mqtt4j.mqtj.client.internal.common.io.TcpConnection;
import org.mqtt4j.mqtj.spi.Transport;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/// Socket ownership and named reader/writer threads. Closing never waits for either worker.
public final class TcpTransport implements Transport {

    private static final int DEFAULT_PORT = 1883;

    private static final int READ_BUFFER_SIZE = 8192;

    private static final int RESOLUTION_LIMIT = 64;

    @FunctionalInterface
    interface Resolver {

        InetAddress resolve(String host) throws IOException;
    }

    @FunctionalInterface
    interface Connector {

        TcpConnection connect(InetSocketAddress address, Duration timeout) throws IOException;
    }

    // DNS resolution is not reliably cancellable. Bound active lookups across the JVM;
    // replace the resolver if applications need a stronger per-host cancellation contract.
    private static final Semaphore resolving = new Semaphore(RESOLUTION_LIMIT, true);

    private final String threadName;
    private final Semaphore resolutionSlots;
    private final Resolver resolver;
    private final Connector connector;
    private final Object callbacks = new Object();

    private @Nullable Listener listener;
    private @Nullable TcpConnection connection;
    private @Nullable Thread reader;
    private @Nullable Throwable terminalCause;
    private @Nullable Write pending;
    private int ioWorkers;
    private boolean closed;
    private boolean started;

    public TcpTransport(String clientId) {
        this(clientId, resolving, InetAddress::getByName, TcpConnection::connect);
    }

    TcpTransport(
            String clientId,
            Semaphore resolutionSlots,
            Resolver resolver,
            Connector connector
    ) {
        threadName = "mqtj-" + requireNonNull(clientId, "clientId");
        this.resolutionSlots = resolutionSlots;
        this.resolver = resolver;
        this.connector = connector;
    }

    @Override
    public void start(URI endpoint, Duration timeout, Listener receiver) {
        requireNonNull(endpoint, "endpoint");
        requireNonNull(timeout, "timeout");
        requireNonNull(receiver, "listener");
        if (timeout.isZero() || timeout.isNegative()
                || timeout.compareTo(Duration.ofMillis(Integer.MAX_VALUE)) > 0) {
            throw new IllegalArgumentException(
                    "Connect timeout must be positive and at most 2147483647 milliseconds"
            );
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (callbacks) {
            if (started) {
                throw new IllegalStateException("Transport already started");
            }
            started = true;
            listener = receiver;
            if (closed) {
                notifyClosed();
                return;
            }
            reader = Thread.ofVirtual().name(threadName + "-reader").unstarted(() -> {
                boolean attached = false;
                try {
                    InetAddress host = resolve(endpoint.getHost(), deadline);
                    var address = new InetSocketAddress(
                            host,
                            endpoint.getPort() == -1 ? DEFAULT_PORT : endpoint.getPort()
                    );
                    TcpConnection socket = connector.connect(address, remaining(deadline));
                    synchronized (callbacks) {
                        if (closed || deadline - System.nanoTime() <= 0) {
                            socket.close();
                            if (!closed) {
                                throw new SocketTimeoutException("TCP connection deadline expired");
                            }
                            return;
                        }
                        connection = socket;
                        ioWorkers++;
                        attached = true;
                        receiver.onConnected();
                        if (closed) {
                            return;
                        }
                        ioWorkers++;
                        Thread.ofVirtual().name(threadName + "-writer").start(() -> write(socket));
                    }
                    read(socket, receiver);
                } catch (Throwable failure) {
                    terminate(failure);
                } finally {
                    if (attached) {
                        workerEnded();
                    }
                }
            });
            reader.start();
        }
    }

    private InetAddress resolve(String host, long deadline)
            throws IOException, InterruptedException {
        try {
            return InetAddress.ofLiteral(host);
        } catch (IllegalArgumentException _) {
            // Only host names enter the resolver; literals do not consume a DNS slot.
        }
        if (!resolutionSlots.tryAcquire(remaining(deadline).toNanos(), NANOSECONDS)) {
            throw new SocketTimeoutException("Name resolution capacity wait expired");
        }
        try {
            remaining(deadline);
            return resolver.resolve(host);
        } finally {
            // Closing or expiring the client cannot free a lookup that is still running.
            resolutionSlots.release();
        }
    }

    private Duration remaining(long deadline) throws IOException {
        synchronized (callbacks) {
            if (closed || Thread.currentThread().isInterrupted()) {
                throw new IOException("Transport closed");
            }
        }
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new SocketTimeoutException("TCP connection deadline expired");
        }
        return Duration.ofNanos(nanos);
    }

    private void read(TcpConnection socket, Listener receiver) throws IOException {
        var buffer = new byte[READ_BUFFER_SIZE];
        int count;
        while ((count = socket.input().read(buffer)) != -1) {
            synchronized (callbacks) {
                if (closed) {
                    return;
                }
                receiver.onBytes(Arrays.copyOf(buffer, count));
            }
        }
        terminate(new EOFException("Peer closed the byte stream"));
    }

    private void write(TcpConnection socket) {
        try {
            var writer = new FrameWriter(socket.output());
            while (true) {
                Write send;
                synchronized (callbacks) {
                    while (pending == null && !closed) {
                        callbacks.wait();
                    }
                    if (closed) {
                        return;
                    }
                    send = requireNonNull(pending);
                }
                writer.write(send.bytes());
                synchronized (callbacks) {
                    pending = null;
                    Listener receiver = listener;
                    // Report a finished write even while closing, as when the peer closes after
                    // reading DISCONNECT. onClosed waits for this worker, so it still comes last.
                    if (receiver != null) {
                        receiver.onSent(send.ticket());
                    }
                }
            }
        } catch (Throwable failure) {
            terminate(failure);
        } finally {
            workerEnded();
        }
    }

    @Override
    public SendResult send(long ticket, byte[] bytes) {
        requireNonNull(bytes, "bytes");
        synchronized (callbacks) {
            if (closed) {
                return SendResult.CLOSED;
            }
            if (connection == null) {
                throw new IllegalStateException("Transport is not connected");
            }
            if (pending != null) {
                throw new IllegalStateException("A send is already outstanding");
            }
            pending = new Write(ticket, bytes);
            callbacks.notifyAll();
            return SendResult.ACCEPTED;
        }
    }

    @Override
    public void close() {
        terminate(new IOException("Transport closed"));
    }

    private void terminate(Throwable cause) {
        synchronized (callbacks) {
            if (closed) {
                return;
            }
            closed = true;
            terminalCause = cause;
            callbacks.notifyAll();
            TcpConnection socket = connection;
            connection = null;
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException failure) {
                    cause.addSuppressed(failure);
                }
            }
            Thread connectingThread = reader;
            if (connectingThread != null && socket == null) {
                connectingThread.interrupt();
            }
            notifyClosed();
        }
    }

    private void workerEnded() {
        synchronized (callbacks) {
            ioWorkers--;
            notifyClosed();
        }
    }

    private void notifyClosed() {
        Listener receiver = listener;
        Throwable cause = terminalCause;
        if (closed && ioWorkers == 0 && receiver != null && cause != null) {
            listener = null;
            pending = null;
            // An active write no longer retains or reads the admitted array at this point.
            receiver.onClosed(cause);
        }
    }

    private record Write(long ticket, byte[] bytes) {
    }
}
