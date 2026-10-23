package org.mqtt4j.hato.client.internal;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.TimerScheduler;
import org.mqtt4j.hato.Transport;
import org.mqtt4j.hato.client.internal.common.io.FrameWriter;
import org.mqtt4j.hato.client.internal.common.io.TcpConnection;
import org.mqtt4j.hato.client.internal.common.log.Log;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/// Socket ownership and named reader/writer threads. Closing never waits for either worker.
public final class TcpTransport implements Transport, PausableTransport {

    private static final Log log = Log.of(TcpTransport.class);

    private static final Duration CLOSE_GRACE = Duration.ofSeconds(1);

    private static final int DEFAULT_PORT = 1883;

    private static final int DEFAULT_TLS_PORT = 8883;

    private static final int READ_BUFFER_SIZE = 8192;

    private static final int RESOLUTION_LIMIT = 64;

    @FunctionalInterface
    interface Resolver {

        InetAddress resolve(String host) throws IOException;
    }

    @FunctionalInterface
    interface Connector {

        // The address is already resolved; TLS identifies the server by the URI's host.
        Connection connect(String host, InetSocketAddress address, Duration timeout)
                throws IOException;
    }

    interface Connection extends Closeable {

        InputStream input() throws IOException;

        OutputStream output() throws IOException;

        default void handshake() throws IOException {
        }

        // Only a connection that can end its output while its input stays readable, as TCP and
        // TLS can, closes gracefully, on a separate closing worker within a grace period: it
        // ends the output, the reader reads to the end of the peer's stream, and abort then
        // releases the socket.
        default boolean closesGracefully() {
            return false;
        }

        // Ends the output after what was written, with a TLS close_notify where TLS applies.
        default void shutdownOutput() throws IOException {
        }

        // Abort must release blocking I/O without taking a TLS read/write lock.
        default void abort() throws IOException {
            close();
        }
    }

    // DNS resolution is not reliably cancellable. Bound active lookups across the JVM;
    // replace the resolver if applications need a stronger per-host cancellation contract.
    private static final Semaphore resolving = new Semaphore(RESOLUTION_LIMIT, true);

    private final String threadName;
    private final String scheme;
    private final Semaphore resolutionSlots;
    private final Resolver resolver;
    private final Connector connector;
    private final TimerScheduler timer;
    private final Object callbacks = new Object();

    private @Nullable Listener listener;
    private @Nullable Connection connection;
    private @Nullable Thread reader;
    private @Nullable Throwable terminalCause;
    private @Nullable Write pending;
    private int ioWorkers;
    private boolean closed;
    private boolean started;
    private boolean connected;
    // The reader waits before its next read while set, and a close ends the wait.
    private boolean readingPaused;

    public TcpTransport(String clientId, TimerScheduler timer) {
        this(
                clientId,
                "mqtt",
                resolving,
                InetAddress::getByName,
                (_, address, timeout) -> plain(TcpConnection.connect(address, timeout)),
                timer
        );
    }

    TcpTransport(
            String clientId,
            Semaphore resolutionSlots,
            Resolver resolver,
            Connector connector
    ) {
        this(
                clientId,
                "mqtt",
                resolutionSlots,
                resolver,
                connector,
                DefaultTimerScheduler.instance
        );
    }

    public TcpTransport(
            String clientId,
            SSLContext context,
            SSLParameters parameters,
            TimerScheduler timer
    ) {
        this(
                clientId,
                "mqtts",
                resolving,
                InetAddress::getByName,
                (host, address, timeout) -> tls(host, address, timeout, context, parameters),
                timer
        );
    }

    TcpTransport(
            String clientId,
            String scheme,
            Semaphore resolutionSlots,
            Resolver resolver,
            Connector connector,
            TimerScheduler timer
    ) {
        threadName = "hato-" + requireNonNull(clientId, "clientId");
        this.scheme = scheme;
        this.resolutionSlots = resolutionSlots;
        this.resolver = resolver;
        this.connector = connector;
        this.timer = timer;
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
        if (!scheme.equals(endpoint.getScheme())) {
            throw new IllegalArgumentException("Transport requires " + scheme + " scheme");
        }
        long deadline = timer.nanoTime() + timeout.toNanos();
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
                    String host = endpoint.getHost();
                    // Connect to the resolved address itself, which keeps an IPv6 zone.
                    var address = new InetSocketAddress(
                            resolve(host, deadline),
                            (endpoint.getPort() == -1)
                                    ? (scheme.equals("mqtts") ? DEFAULT_TLS_PORT : DEFAULT_PORT)
                                    : endpoint.getPort()
                    );
                    Connection socket = connector.connect(host, address, remaining(deadline));
                    synchronized (callbacks) {
                        if (closed) {
                            abort(socket, requireNonNull(terminalCause));
                            return;
                        }
                        // Own the socket before consulting the injected clock again, so a
                        // failed time read follows the same cleanup as a failed handshake.
                        connection = socket;
                        ioWorkers++;
                        attached = true;
                    }
                    // SO_TIMEOUT only bounds individual reads. Close the underlying TCP socket
                    // at the total deadline even if the peer keeps making partial progress.
                    TimerScheduler.Cancellable handshakeDeadline = timer.schedule(
                            remaining(deadline),
                            () -> {
                                synchronized (callbacks) {
                                    if (!connected) {
                                        terminate(new SocketTimeoutException(
                                                "Transport connection deadline expired"
                                        ));
                                    }
                                }
                            }
                    );
                    try {
                        socket.handshake();
                        synchronized (callbacks) {
                            remaining(deadline);
                            connected = true;
                        }
                    } finally {
                        cancel(handshakeDeadline);
                    }
                    synchronized (callbacks) {
                        if (closed) {
                            return;
                        }
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
        long nanos = deadline - timer.nanoTime();
        if (nanos <= 0) {
            throw new SocketTimeoutException("TCP connection deadline expired");
        }
        return Duration.ofNanos(nanos);
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

    private void read(Connection socket, Listener receiver)
            throws IOException, InterruptedException {
        var buffer = new byte[READ_BUFFER_SIZE];
        InputStream input = socket.input();
        long busySince = System.nanoTime();
        int count;
        while ((count = input.read(buffer)) != -1) {
            synchronized (callbacks) {
                // After a close, keep reading and drop what arrives until the peer ends its
                // stream, so that closeGracefully can release a socket that receives nothing.
                if (!closed) {
                    receiver.onBytes(Arrays.copyOf(buffer, count));
                }
                // A pause, which onBytes may have asked for, leaves the peer to TCP flow control
                // until the client resumes. Waiting frees the carrier.
                if (readingPaused && !closed) {
                    while (readingPaused && !closed) {
                        callbacks.wait();
                    }
                    busySince = System.nanoTime();
                }
            }
            // A peer that never pauses keeps every read from blocking, and the reader from
            // freeing its carrier for the writer, the closing worker, and the client. Whether
            // the next read blocks is unknown, so the reader parks once a slice is over.
            if (CarrierShare.due(busySince)) {
                busySince = CarrierShare.share();
            }
        }
        terminate(new EOFException("Peer closed the byte stream"));
    }

    private void write(Connection socket) {
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
            if (!connected) {
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
        terminate(new IOException("Transport closed"), true);
    }

    private void terminate(Throwable cause) {
        terminate(cause, false);
    }

    private void terminate(Throwable cause, boolean graceful) {
        synchronized (callbacks) {
            if (closed) {
                return;
            }
            closed = true;
            terminalCause = cause;
            callbacks.notifyAll();
            Connection socket = connection;
            connection = null;
            if (socket != null) {
                if (graceful && connected && pending == null && socket.closesGracefully()) {
                    ioWorkers++;
                    Thread.ofVirtual().name(threadName + "-close").start(() -> {
                        try {
                            closeGracefully(socket, cause);
                        } finally {
                            workerEnded();
                        }
                    });
                } else {
                    abort(socket, cause);
                }
            }
            Thread connectingThread = reader;
            if (connectingThread != null && socket == null) {
                connectingThread.interrupt();
            }
            notifyClosed();
        }
    }

    private void closeGracefully(Connection socket, Throwable cause) {
        // Ending TLS output can wait on a peer or TLS I/O lock (JDK-8241239). Never run it
        // under the callback lock; the independent grace timer closes the plain socket.
        TimerScheduler.Cancellable deadline = null;
        try {
            deadline = timer.schedule(CLOSE_GRACE, () -> abort(socket, cause));
            socket.shutdownOutput();
            // Closing a socket that still receives resets the connection, and a peer that sees
            // the reset can drop what it received but had not read yet, such as a DISCONNECT.
            // So abort only once the reader has read to the end of the peer's stream, which
            // ending the output lets the peer close. For a peer that keeps the connection open,
            // the grace timer aborts, which ends the reader.
            requireNonNull(reader).join();
        } catch (Throwable failure) {
            cause.addSuppressed(failure);
        } finally {
            if (deadline != null) {
                cancel(deadline);
            }
            abort(socket, cause);
        }
    }

    private static void cancel(TimerScheduler.Cancellable deadline) {
        try {
            deadline.cancel();
        } catch (Throwable failure) {
            log.warn("Transport timer cancellation failed", failure);
        }
    }

    private static void abort(Connection socket, Throwable cause) {
        try {
            socket.abort();
        } catch (IOException failure) {
            cause.addSuppressed(failure);
        }
    }

    static Connection plain(TcpConnection socket) {
        return new Connection() {
            @Override
            public InputStream input() {
                return socket.input();
            }

            @Override
            public OutputStream output() {
                return socket.output();
            }

            @Override
            public boolean closesGracefully() {
                return true;
            }

            @Override
            public void shutdownOutput() throws IOException {
                socket.shutdownOutput();
            }

            @Override
            public void close() throws IOException {
                socket.close();
            }
        };
    }

    static Connection tls(
            String host,
            InetSocketAddress address,
            Duration timeout,
            SSLContext context,
            SSLParameters parameters
    ) throws IOException {
        var raw = new Socket();
        TcpConnection tcp = TcpConnection.connect(raw, address, timeout);
        try {
            // Identify an IPv6 literal without the brackets that URI syntax requires.
            String peerHost = host.startsWith("[") ? host.substring(1, host.length() - 1) : host;
            var ssl = (SSLSocket) context.getSocketFactory().createSocket(
                    raw,
                    peerHost,
                    address.getPort(),
                    true
            );
            // Apply only once to a fresh socket, so an explicit null identification
            // algorithm can disable the builder's HTTPS default.
            ssl.setSSLParameters(parameters);
            return new Connection() {
                @Override
                public InputStream input() throws IOException {
                    return ssl.getInputStream();
                }

                @Override
                public OutputStream output() throws IOException {
                    return ssl.getOutputStream();
                }

                @Override
                public void handshake() throws IOException {
                    ssl.startHandshake();
                }

                @Override
                public boolean closesGracefully() {
                    return true;
                }

                // Sends close_notify and ends the TCP output, while the input stays readable.
                @Override
                public void shutdownOutput() throws IOException {
                    ssl.shutdownOutput();
                }

                @Override
                public void close() throws IOException {
                    ssl.close();
                }

                @Override
                public void abort() throws IOException {
                    tcp.close();
                }
            };
        } catch (IOException | RuntimeException | Error failure) {
            abort(plain(tcp), failure);
            throw failure;
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
