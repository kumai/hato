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
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
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

/// Socket ownership, a named reader thread, and writes: on the caller's thread through
/// [#write], or on a named writer thread through [#send]. Closing waits for none of them.
public final class TcpTransport implements Transport, PausableTransport, BlockingWriteTransport {

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

        // The channel that output() writes to, which writes several buffers with one gathering
        // write, or null.
        default @Nullable GatheringByteChannel channel() {
            return null;
        }
    }

    // DNS resolution is not reliably cancellable. Bound active lookups across the JVM;
    // replace the resolver if applications need a stronger per-host cancellation contract.
    private static final Semaphore resolving = new Semaphore(RESOLUTION_LIMIT, true);

    // A virtual thread that waits for its socket is woken by the JDK's poller, a hand-off that
    // each read pays when it arrives after a pause. On Linux, where the poller runs on virtual
    // threads too, a connection receiving 20,000 messages a second spent 40 to 60 percent less
    // CPU per message when a platform thread read it, blocking in the read itself. A platform
    // thread per connection does not scale, though: with a thousand of them, a message to every
    // connection took 14 to 50 times as long to reach them all. So, once connected, as many
    // connections as the JVM has processors read on platform threads of their own, and the
    // others go on reading on the virtual thread that connected them. The connections that come
    // up first take them, busy or not; a shared event loop would serve every connection without
    // either cost. The build's single-carrier suite sets hato.client.platformReaders to 0, so
    // that it tests virtual readers on one carrier; the property is not a supported setting.
    private static final Semaphore platformReading = new Semaphore(Integer.getInteger(
            "hato.client.platformReaders",
            Runtime.getRuntime().availableProcessors()
    ));

    private final String threadName;
    private final String scheme;
    private final Semaphore resolutionSlots;
    private final Resolver resolver;
    private final Connector connector;
    private final TimerScheduler timer;
    private final Semaphore platformReadingSlots;
    private final Object callbacks = new Object();

    private @Nullable Listener listener;
    private @Nullable Connection connection;
    // The thread that connects and then reads, or the platform thread that it hands the reading
    // to once connected.
    private @Nullable Thread reader;
    private @Nullable Throwable terminalCause;
    private @Nullable Write pending;
    // Writes the connection's frames, for the writer thread and for writes that the client makes.
    private @Nullable FrameWriter frames;
    // A write that the client makes through write() is under way.
    private boolean writing;
    // Whether the connection writes several parts with one gathering write; set before
    // onConnected, so before any write.
    private volatile boolean gathering;
    private boolean writerStarted;
    private int ioWorkers;
    private boolean closed;
    private boolean started;
    private boolean connected;
    // The reader waits before its next read while set, and a close ends the wait. Written under
    // callbacks; the reader reads it without first.
    private volatile boolean readingPaused;

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
        this(
                clientId,
                scheme,
                resolutionSlots,
                resolver,
                connector,
                timer,
                platformReading
        );
    }

    TcpTransport(
            String clientId,
            String scheme,
            Semaphore resolutionSlots,
            Resolver resolver,
            Connector connector,
            TimerScheduler timer,
            Semaphore platformReadingSlots
    ) {
        threadName = "hato-" + requireNonNull(clientId, "clientId");
        this.scheme = scheme;
        this.resolutionSlots = resolutionSlots;
        this.resolver = resolver;
        this.connector = connector;
        this.timer = timer;
        this.platformReadingSlots = platformReadingSlots;
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
                        // Ready before onConnected, which may send at once. The writer thread
                        // starts with the first send; a client that writes itself needs none.
                        frames = new FrameWriter(socket.output(), socket.channel());
                        gathering = socket.channel() != null;
                        receiver.onConnected();
                        if (closed) {
                            return;
                        }
                        if (readOnPlatformThread(socket, receiver)) {
                            // That thread ends this worker when it stops reading.
                            attached = false;
                            return;
                        }
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

    /// Caller holds callbacks, once connected. Hands the reading to a platform thread of its own
    /// while the JVM has one to spare, and returns whether it did; otherwise the caller reads.
    private boolean readOnPlatformThread(Connection socket, Listener receiver) {
        if (!platformReadingSlots.tryAcquire()) {
            return false;
        }
        Thread platform = Thread.ofPlatform()
                .daemon()
                .name(threadName + "-reader")
                .unstarted(() -> {
                    try {
                        read(socket, receiver);
                    } catch (Throwable failure) {
                        terminate(failure);
                    } finally {
                        // Before onClosed, so that a connection that replaces this one can take
                        // the slot.
                        platformReadingSlots.release();
                        workerEnded();
                    }
                });
        try {
            platform.start();
        } catch (OutOfMemoryError _) {
            // The system refused another thread; the caller reads instead.
            platformReadingSlots.release();
            return false;
        }
        reader = platform;
        return true;
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
        // A listener that borrows takes the bytes where they lie, which spares a copy of every
        // read.
        BorrowingListener borrower = receiver instanceof BorrowingListener lender ? lender : null;
        // A platform reader holds no carrier that others wait for.
        boolean sharesCarrier = Thread.currentThread().isVirtual();
        // The time spent on what arrived since the reader last let others run. A read that waits
        // for the peer frees the carrier, so only the work on the bytes counts.
        long worked = 0;
        int count;
        while ((count = input.read(buffer)) != -1) {
            long started = System.nanoTime();
            BorrowingListener lent = null;
            try {
                synchronized (callbacks) {
                    // After a close, keep reading and drop what arrives until the peer ends its
                    // stream, so that closeGracefully can release a socket that receives nothing.
                    if (!closed) {
                        if (borrower != null) {
                            lent = borrower;
                            borrower.onBorrowedBytes(buffer, 0, count);
                        } else {
                            receiver.onBytes(Arrays.copyOf(buffer, count));
                        }
                    }
                }
            } finally {
                if (lent != null) {
                    // Without the lock, where what the listener does may call this transport.
                    lent.afterBorrowedBytes();
                }
            }
            worked += System.nanoTime() - started;
            // A pause, which the listener may have asked for, leaves the peer to TCP flow control
            // until the client resumes. It waits after afterBorrowedBytes, whose work may resume
            // the reader. Waiting frees the carrier.
            if (readingPaused) {
                synchronized (callbacks) {
                    while (readingPaused && !closed) {
                        callbacks.wait();
                    }
                }
                worked = 0;
            }
            // A peer that never pauses keeps every read from blocking, and a virtual reader from
            // freeing its carrier for the writer, the closing worker, and the client, so the
            // reader parks once its work fills a slice.
            if (sharesCarrier && CarrierShare.spent(worked)) {
                CarrierShare.share();
                worked = 0;
            }
        }
        terminate(new EOFException("Peer closed the byte stream"));
    }

    private void writeSends(FrameWriter writer) {
        try {
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
            FrameWriter writer = admit();
            if (writer == null) {
                return SendResult.CLOSED;
            }
            if (!writerStarted) {
                writerStarted = true;
                ioWorkers++;
                Thread.ofVirtual().name(threadName + "-writer").start(() -> writeSends(writer));
            }
            pending = new Write(ticket, bytes);
            callbacks.notifyAll();
            return SendResult.ACCEPTED;
        }
    }

    @Override
    public SendResult write(long ticket, byte[] bytes) {
        requireNonNull(bytes, "bytes");
        return writeHere(ticket, writer -> writer.write(bytes));
    }

    @Override
    public SendResult write(long ticket, byte[] header, byte[] payload) {
        requireNonNull(header, "header");
        requireNonNull(payload, "payload");
        return writeHere(ticket, writer -> writer.write(header, payload));
    }

    @Override
    public boolean gathers() {
        return gathering;
    }

    @Override
    public SendResult writeParts(long ticket, ByteBuffer[] parts) {
        requireNonNull(parts, "parts");
        return writeHere(ticket, writer -> writer.writeParts(parts));
    }

    @FunctionalInterface
    private interface FrameWrite {

        void writeTo(FrameWriter writer) throws IOException;
    }

    private SendResult writeHere(long ticket, FrameWrite write) {
        FrameWriter writer;
        synchronized (callbacks) {
            writer = admit();
            if (writer == null) {
                return SendResult.CLOSED;
            }
            writing = true;
            // onClosed waits for this write, as it waits for the writer thread.
            ioWorkers++;
        }
        try {
            write.writeTo(writer);
            synchronized (callbacks) {
                writing = false;
                Listener receiver = listener;
                // Report a finished write even while closing, as the writer thread does.
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

    /// Caller holds callbacks. The frame writer for one more write, or null once closed.
    private @Nullable FrameWriter admit() {
        if (closed) {
            return null;
        }
        FrameWriter writer = frames;
        if (!connected || writer == null) {
            throw new IllegalStateException("Transport is not connected");
        }
        if (pending != null || writing) {
            throw new IllegalStateException("A send is already outstanding");
        }
        return writer;
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
                if (graceful && connected && pending == null && !writing
                        && socket.closesGracefully()) {
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

            @Override
            public @Nullable GatheringByteChannel channel() {
                return socket.channel();
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
