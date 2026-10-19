package org.mqtt4j.hato.common.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TcpConnectionTest {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

    @Test
    void a_connection_exchanges_bytes_in_both_directions() throws Exception {
        // Arrange: close sockets before the executor, so failed assertions cannot strand a read.
        try (
                var executor = Executors.newVirtualThreadPerTaskExecutor();
                var server = listen();
                var sut = connectTo(server);
                var peer = server.accept()
        ) {
            peer.setSoTimeout(5000);
            var request = new byte[]{0, 1, (byte) 0xFF};
            var response = new byte[]{(byte) 0x80, 2, 0};
            Future<byte[]> received = executor.submit(
                    () -> sut.input().readNBytes(response.length)
            );

            // Act: writing must work while another thread waits for incoming bytes.
            sut.output().write(request);
            sut.output().flush();
            assertThat(peer.getInputStream().readNBytes(request.length)).isEqualTo(request);
            peer.getOutputStream().write(response);
            peer.getOutputStream().flush();

            // Assert
            assertThat(received.get(5, SECONDS)).isEqualTo(response);
        }
    }

    @ParameterizedTest(name = "virtual reader: {0}")
    @ValueSource(booleans = {false, true})
    void closing_from_another_thread_releases_a_pending_read(boolean virtual) throws Exception {
        try (var server = listen()) {
            var sut = connectTo(server);
            var started = new CountDownLatch(1);
            var read = new FutureTask<Integer>(() -> {
                started.countDown();
                return sut.input().read();
            });
            Thread.Builder builder = virtual ? Thread.ofVirtual() : Thread.ofPlatform().daemon();
            Thread reader = builder.unstarted(read);
            try (var peer = server.accept()) {
                peer.setSoTimeout(5000);
                // Arrange: the peer sends nothing, so the read must remain pending.
                reader.start();
                assertThat(started.await(5, SECONDS)).isTrue();
                assertThatExceptionOfType(TimeoutException.class)
                        .isThrownBy(() -> read.get(100, MILLISECONDS));

                // Act
                sut.close();

                // Assert: local cancellation is an I/O failure, not remote EOF.
                assertThatExceptionOfType(ExecutionException.class)
                        .isThrownBy(() -> read.get(5, SECONDS))
                        .withCauseInstanceOf(SocketException.class);
                assertThat(peer.getInputStream().read()).isEqualTo(-1);
            } finally {
                sut.close();
                // The assertion after this block checks that the reader ended.
                boolean _ = reader.join(Duration.ofSeconds(5));
            }
            assertThat(reader.isAlive()).isFalse();
        }
    }

    @Test
    void repeated_close_is_harmless_and_both_streams_stop_working() throws Exception {
        try (var server = listen()) {
            var sut = connectTo(server);
            try (var peer = server.accept()) {
                peer.setSoTimeout(5000);

                sut.close();
                sut.close();

                assertThatThrownBy(() -> sut.input().read()).isInstanceOf(SocketException.class);
                assertThatThrownBy(() -> sut.output().write(1)).isInstanceOf(SocketException.class);
                assertThat(peer.getInputStream().read()).isEqualTo(-1);
            } finally {
                sut.close();
            }
        }
    }

    @Test
    void peer_shutdown_is_reported_as_end_of_stream() throws Exception {
        try (
                var executor = Executors.newVirtualThreadPerTaskExecutor();
                var server = listen();
                var sut = connectTo(server);
                var peer = server.accept()
        ) {
            Future<Integer> received = executor.submit(() -> sut.input().read());

            peer.shutdownOutput();

            assertThat(received.get(5, SECONDS)).isEqualTo(-1);
        }
    }

    @Test
    void a_refused_connection_is_reported_as_an_io_failure() throws IOException {
        // Bind without listening, reserving the port so another process cannot take it.
        try (var unlistening = new Socket()) {
            unlistening.bind(new InetSocketAddress(LOOPBACK, 0));

            assertThatThrownBy(() -> TcpConnection.connect(
                    LOOPBACK.getHostAddress(), unlistening.getLocalPort(), CONNECT_TIMEOUT
            )).isInstanceOf(IOException.class);
        }
    }

    @Test
    void a_resolved_address_connects_without_looking_up_its_host_name() throws IOException {
        // Arrange: the address carries an intentionally unresolvable host name.
        InetAddress address = InetAddress.getByAddress("broker.invalid", LOOPBACK.getAddress());
        try (
                ServerSocket server = listen();
                var sut = TcpConnection.connect(
                        new InetSocketAddress(address, server.getLocalPort()),
                        CONNECT_TIMEOUT
                );
                Socket peer = server.accept()
        ) {
            peer.setSoTimeout(5000);

            // Act
            sut.output().write(0x7F);

            // Assert
            assertThat(peer.getInputStream().read()).isEqualTo(0x7F);
        }
    }

    @Test
    void a_caller_created_socket_is_configured_and_owned_by_the_connection() throws IOException {
        // Arrange
        try (ServerSocket server = listen(); var socket = new Socket()) {
            assertThat(socket.getTcpNoDelay()).isFalse();

            // Act
            try (
                    var sut = TcpConnection.connect(
                            socket,
                            new InetSocketAddress(LOOPBACK, server.getLocalPort()),
                            CONNECT_TIMEOUT
                    );
                    Socket peer = server.accept()
            ) {
                // Assert: setup reuses the caller's socket and exposes its borrowed streams.
                assertThat(socket.getTcpNoDelay()).isTrue();
                assertThat(sut.input()).isSameAs(socket.getInputStream());
                assertThat(sut.output()).isSameAs(socket.getOutputStream());
                assertThat(socket.isClosed()).isFalse();
                peer.setSoTimeout(5000);
                sut.output().write(0x7F);
                assertThat(peer.getInputStream().read()).isEqualTo(0x7F);
            }
            assertThat(socket.isClosed()).isTrue();
        }
    }

    @ParameterizedTest(name = "input stream fails: {0}")
    @ValueSource(booleans = {false, true})
    void stream_initialization_failure_closes_the_owned_socket_once(boolean inputFails)
            throws IOException {
        // Arrange: cleanup also fails, but must preserve the stream initialization failure.
        var failure = new IOException("stream initialization failed");
        var closeFailure = new IOException("socket close failed");
        var closes = new AtomicInteger();
        var socket = new Socket() {
            @Override
            public InputStream getInputStream() throws IOException {
                if (inputFails) {
                    throw failure;
                }
                return super.getInputStream();
            }

            @Override
            public OutputStream getOutputStream() throws IOException {
                throw failure;
            }

            @Override
            public void close() throws IOException {
                closes.incrementAndGet();
                super.close();
                throw closeFailure;
            }
        };
        try (ServerSocket server = listen()) {
            // Act & Assert
            assertThatThrownBy(() -> TcpConnection.connect(
                    socket,
                    new InetSocketAddress(LOOPBACK, server.getLocalPort()),
                    CONNECT_TIMEOUT
            )).isSameAs(failure).hasSuppressedException(closeFailure);
            assertThat(closes).hasValue(1);
            assertThat(socket.isClosed()).isTrue();
            try (Socket peer = server.accept()) {
                peer.setSoTimeout(5000);
                assertThat(peer.getInputStream().read()).isEqualTo(-1);
            }
        }
    }

    @Test
    void an_unresolved_address_is_rejected_without_resolving_it() {
        var address = InetSocketAddress.createUnresolved("localhost", 1883);

        assertThatThrownBy(() -> TcpConnection.connect(address, CONNECT_TIMEOUT))
                .isInstanceOf(UnknownHostException.class);
    }

    @Test
    void a_resolved_address_must_have_a_nonzero_port() throws IOException {
        var address = new InetSocketAddress(LOOPBACK, 0);

        assertThatThrownBy(() -> TcpConnection.connect(address, CONNECT_TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
        try (var socket = new Socket()) {
            assertThatThrownBy(() -> TcpConnection.connect(socket, address, CONNECT_TIMEOUT))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(socket.isClosed()).isTrue();
        }
    }

    @Test
    void the_largest_supported_connect_timeout_is_accepted() throws IOException {
        try (
                var server = listen();
                var sut = TcpConnection.connect(
                        LOOPBACK.getHostAddress(), server.getLocalPort(),
                        Duration.ofMillis(Integer.MAX_VALUE)
                );
                var peer = server.accept()
        ) {
            peer.setSoTimeout(5000);
            sut.output().write(0x7F);

            assertThat(peer.getInputStream().read()).isEqualTo(0x7F);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t\n"})
    void a_blank_host_is_rejected_instead_of_connecting_to_loopback(String host) {
        assertThatThrownBy(() -> TcpConnection.connect(host, 1883, CONNECT_TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 65536, Integer.MAX_VALUE})
    void invalid_remote_ports_are_rejected(int port) {
        assertThatThrownBy(() -> TcpConnection.connect("127.0.0.1", port, CONNECT_TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @MethodSource("invalidTimeouts")
    void unbounded_or_unrepresentable_timeouts_are_rejected(Duration timeout) throws IOException {
        assertThatThrownBy(() -> TcpConnection.connect("127.0.0.1", 1883, timeout))
                .isInstanceOf(IllegalArgumentException.class);
        try (var socket = new Socket()) {
            assertThatThrownBy(() -> TcpConnection.connect(
                    socket,
                    new InetSocketAddress(LOOPBACK, 1883),
                    timeout
            )).isInstanceOf(IllegalArgumentException.class);
            assertThat(socket.isClosed()).isTrue();
        }
    }

    static Stream<Duration> invalidTimeouts() {
        return Stream.of(
                Duration.ZERO,
                Duration.ofNanos(-1),
                Duration.ofSeconds(-1),
                Duration.ofMillis(Integer.MAX_VALUE).plusNanos(1),
                Duration.ofSeconds(Long.MAX_VALUE)
        );
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the factory's non-null contract.
    void host_and_connect_timeout_are_required() {
        assertThatThrownBy(() -> TcpConnection.connect(null, 1883, CONNECT_TIMEOUT))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> TcpConnection.connect("127.0.0.1", 1883, null))
                .isInstanceOf(NullPointerException.class);
    }

    private static ServerSocket listen() throws IOException {
        var server = new ServerSocket(0, 1, LOOPBACK);
        server.setSoTimeout(5000);
        return server;
    }

    private static TcpConnection connectTo(ServerSocket server) throws IOException {
        return TcpConnection.connect(
                LOOPBACK.getHostAddress(), server.getLocalPort(), CONNECT_TIMEOUT
        );
    }
}
