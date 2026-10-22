package org.mqtt4j.hato.common.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/// Owns one connected TCP socket and its blocking input and output streams.
///
/// One reader and one writer may use the connection concurrently. The owner serializes reads
/// with other reads and writes with other writes; it may close the connection from any thread.
/// This class creates no threads and leaves MQTT framing and packet encoding to its callers.
///
/// The streams are borrowed from this connection. Close the connection to release both sides;
/// closing either stream also closes the underlying socket. No additional buffering is added.
///
/// See [JDK 25 Socket](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/net/Socket.html).
public final class TcpConnection implements Closeable {

    private static final Duration MAX_CONNECT_TIMEOUT = Duration.ofMillis(Integer.MAX_VALUE);

    private final Socket socket;

    private final InputStream input;

    private final OutputStream output;

    TcpConnection(Socket socket) throws IOException {
        socket.setTcpNoDelay(true);
        this.socket = socket;
        input = socket.getInputStream();
        output = socket.getOutputStream();
    }

    /// Resolves `host` and establishes a TCP connection to `port`.
    ///
    /// The positive timeout applies to TCP connection establishment, after DNS resolution.
    /// It does not bound DNS resolution or later reads and writes. Fractional milliseconds
    /// are rounded up, so a positive duration never becomes the JDK's zero (infinite) timeout.
    /// The maximum is [Integer#MAX_VALUE] milliseconds. Any socket created during a failed
    /// attempt is closed before the failure is propagated.
    ///
    /// @throws IllegalArgumentException if the host is blank, the port is outside 1 to 65535,
    ///         or the timeout is not positive or exceeds the maximum
    /// @throws java.net.SocketTimeoutException if the TCP connect timeout expires
    /// @throws IOException if name resolution, connection, or stream initialization fails
    public static TcpConnection connect(
            String host,
            int port,
            Duration connectTimeout
    ) throws IOException {
        requireNonNull(host, "host must not be null");
        if (host.isBlank()) {
            throw new IllegalArgumentException("Host must not be blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        int timeoutMillis = timeoutMillis(connectTimeout);
        var address = new InetSocketAddress(host, port);
        return connect(new Socket(), address, Duration.ofMillis(timeoutMillis));
    }

    /// Establishes a TCP connection without resolving a host name.
    ///
    /// Timeout bounds and socket ownership are the same as [#connect(String, int, Duration)].
    ///
    /// @throws IllegalArgumentException if the port is zero, or the timeout is not positive
    ///         or exceeds the maximum
    /// @throws java.net.UnknownHostException if the address is unresolved
    /// @throws java.net.SocketTimeoutException if the TCP connect timeout expires
    /// @throws IOException if connection or stream initialization fails
    public static TcpConnection connect(
            InetSocketAddress address,
            Duration connectTimeout
    ) throws IOException {
        return connect(new Socket(), address, connectTimeout);
    }

    /// Connects a caller-created plain socket, taking ownership of it on success or failure.
    /// This allows a TLS layer to retain the socket for cancellation while sharing TCP setup.
    /// Address and timeout constraints are the same as [#connect(InetSocketAddress, Duration)].
    public static TcpConnection connect(
            Socket socket,
            InetSocketAddress address,
            Duration connectTimeout
    ) throws IOException {
        requireNonNull(socket, "socket must not be null");
        try {
            requireNonNull(address, "address must not be null");
            if (address.getPort() == 0) {
                throw new IllegalArgumentException("Port must be between 1 and 65535");
            }
            int timeoutMillis = timeoutMillis(connectTimeout);
            socket.connect(address, timeoutMillis);
            return new TcpConnection(socket);
        } catch (IOException | RuntimeException | Error failure) {
            // Ownership transfers only after both streams are available. Keep the original
            // failure if releasing the partially initialized connection also fails.
            closeAfterFailure(socket, failure);
            throw failure;
        }
    }

    private static void closeAfterFailure(Socket socket, Throwable failure) {
        try {
            socket.close();
        } catch (IOException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static int timeoutMillis(Duration connectTimeout) {
        requireNonNull(connectTimeout, "connectTimeout must not be null");
        if (connectTimeout.isZero() || connectTimeout.isNegative()
                || connectTimeout.compareTo(MAX_CONNECT_TIMEOUT) > 0) {
            throw new IllegalArgumentException(
                    "Connect timeout must be positive and at most 2147483647 milliseconds"
            );
        }
        // The range check above also makes conversion to nanoseconds safe from overflow.
        return (int) Math.ceilDiv(connectTimeout.toNanos(), 1_000_000L);
    }

    /// Returns the borrowed input stream, for one reader at a time.
    ///
    /// Reads block until data, EOF, or failure; [#close] can cancel a pending read.
    public InputStream input() {
        return input;
    }

    /// Returns the borrowed output stream, for one writer at a time.
    public OutputStream output() {
        return output;
    }

    /// Ends the output direction after the bytes already written, so the peer reads the end of
    /// the stream, while the input stays readable until the peer ends its direction too.
    /// Repeated calls before [#close] are harmless.
    ///
    /// Closing a socket whose input still holds unread bytes, or receives more, resets the
    /// connection, and a peer that sees the reset can discard what it received but had not yet
    /// read. Ending the output first and reading to the end of the stream avoids that.
    ///
    /// @throws IOException if the connection is closed or ending the output fails
    public void shutdownOutput() throws IOException {
        if (!socket.isOutputShutdown()) {
            socket.shutdownOutput();
        }
    }

    /// Closes both directions and releases the socket. Repeated calls are harmless.
    ///
    /// May be called from any thread. A thread blocked in socket I/O is released with a
    /// [java.net.SocketException], including when that thread is virtual. The connection
    /// cannot be reopened and this operation does not send an MQTT DISCONNECT packet.
    ///
    /// @throws IOException if closing the socket fails
    @Override
    public void close() throws IOException {
        socket.close();
    }
}
