package org.mqtt4j.hato.client.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import javax.net.ssl.SSLContext;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.spi.TimerScheduler;
import org.mqtt4j.hato.spi.Transport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SuppressWarnings("try") // Explicit close exercises cancellation before resource cleanup.
class TcpTransportTest {

    private static final URI SERVER = URI.create("mqtt://broker.test:1883");
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final InetAddress ADDRESS = InetAddress.ofLiteral("127.0.0.1");

    private final Semaphore resolutionSlots = new Semaphore(1, true);
    private final AtomicInteger lookups = new AtomicInteger();
    private final AtomicInteger connections = new AtomicInteger();
    private final IOException connectFailure = new IOException("Test connector stopped");
    private final RecordingListener listener = new RecordingListener();
    private final ManualTimerScheduler clock = new ManualTimerScheduler();

    @ParameterizedTest(name = "{0} transport rejects {1}")
    @CsvSource({
            "mqtt, mqtts", "mqtts, mqtt", "mqtt, tcp", "mqtts, ssl", "mqtts, tls",
            "mqtt, ws", "mqtts, wss"
    })
    void unsupported_schemes_are_rejected_before_starting_a_connection(
            String transportScheme,
            String endpointScheme
    ) throws Exception {
        // Arrange
        SSLContext context = SSLContext.getDefault();
        Transport sut = transportScheme.equals("mqtt")
                ? Transport.tcp("wrong-scheme")
                : new TcpTransport(
                        "wrong-scheme",
                        context,
                        context.getDefaultSSLParameters(),
                        DefaultTimerScheduler.instance
                );
        try (sut) {
            // Act & Assert
            assertThatThrownBy(() -> sut.start(
                    URI.create(endpointScheme + "://127.0.0.1"),
                    TIMEOUT,
                    listener
            )).isInstanceOf(IllegalArgumentException.class);
            assertThat(listener.closed).isNotDone();
        }
    }

    @ParameterizedTest(name = "{0} defaults to port {1}")
    @CsvSource({"mqtt, 1883", "mqtts, 8883"})
    void omitted_ports_use_the_scheme_default(String scheme, int port) throws Exception {
        // Arrange
        var hostUsed = new CompletableFuture<String>();
        var addressUsed = new CompletableFuture<InetSocketAddress>();
        try (var sut = new TcpTransport(
                "default-port",
                scheme,
                resolutionSlots,
                this::resolve,
                (host, address, timeout) -> {
                    hostUsed.complete(host);
                    addressUsed.complete(address);
                    return connect(host, address, timeout);
                },
                clock
        )) {
            // Act
            sut.start(URI.create(scheme + "://broker.test"), TIMEOUT, listener);

            // Assert: the connector also receives the URI host, for TLS identification and SNI.
            assertThat(addressUsed.get(5, SECONDS).getPort()).isEqualTo(port);
            assertThat(hostUsed.get(5, SECONDS)).isEqualTo("broker.test");
            assertThat(listener.closed.get(5, SECONDS)).isSameAs(connectFailure);
        }
    }

    @Test
    void a_scoped_ipv6_literal_keeps_its_zone_for_the_connection() throws Exception {
        // Arrange: a numeric zone parses without depending on this host's interface names.
        var addressUsed = new CompletableFuture<InetSocketAddress>();
        try (var sut = new TcpTransport(
                "scoped",
                resolutionSlots,
                this::resolve,
                (host, address, timeout) -> {
                    addressUsed.complete(address);
                    return connect(host, address, timeout);
                }
        )) {
            // Act
            sut.start(URI.create("mqtt://[fe80::1%1]:1883"), TIMEOUT, listener);

            // Assert: equality of addresses ignores the zone, so compare the scope itself.
            var address = (Inet6Address) addressUsed.get(5, SECONDS).getAddress();
            assertThat(address.getScopeId()).isEqualTo(1);
            assertThat(listener.closed.get(5, SECONDS)).isSameAs(connectFailure);
        }
    }

    @Test
    void connection_and_bytes_are_not_reported_until_the_handshake_completes() throws Exception {
        // Arrange
        var handshaking = new CompletableFuture<Void>();
        var finishHandshake = new CompletableFuture<Void>();
        var connected = new CompletableFuture<Void>();
        var received = new CompletableFuture<byte[]>();
        var ended = new CompletableFuture<Throwable>();
        var events = new CopyOnWriteArrayList<String>();
        var input = new ByteArrayInputStream(new byte[]{1, 2, 3});
        var socket = new TcpTransport.Connection() {
            @Override
            public void handshake() {
                handshaking.complete(null);
                finishHandshake.join();
            }

            @Override
            public InputStream input() {
                return input;
            }

            @Override
            public OutputStream output() {
                return OutputStream.nullOutputStream();
            }

            @Override
            public void close() {
                finishHandshake.complete(null);
            }
        };
        try (var sut = new TcpTransport(
                "handshake",
                "mqtts",
                resolutionSlots,
                this::resolve,
                (_, _, _) -> socket,
                clock
        )) {
            sut.start(URI.create("mqtts://broker.test"), TIMEOUT, new Transport.Listener() {
                @Override
                public void onConnected() {
                    events.add("connected");
                    connected.complete(null);
                }

                @Override
                public void onBytes(byte[] bytes) {
                    events.add("bytes");
                    received.complete(bytes);
                }

                @Override
                public void onSent(long ticket) {
                    throw new AssertionError("Unexpected send completion");
                }

                @Override
                public void onClosed(Throwable cause) {
                    events.add("closed");
                    ended.complete(cause);
                }
            });
            handshaking.get(5, SECONDS);

            // Assert: a connected TCP socket alone cannot admit MQTT traffic.
            assertThat(connected).isNotDone();
            assertThat(received).isNotDone();
            assertThatThrownBy(() -> sut.send(1, new byte[]{1}))
                    .isInstanceOf(IllegalStateException.class);

            // Act
            finishHandshake.complete(null);

            // Assert
            ended.get(5, SECONDS);
            assertThat(received.get(5, SECONDS)).containsExactly(1, 2, 3);
            assertThat(events).containsExactly("connected", "bytes", "closed");
        } finally {
            finishHandshake.complete(null);
        }
    }

    @Test
    void handshake_progress_does_not_extend_the_total_connection_deadline() throws Exception {
        // Arrange: repeatedly model progress without ever completing the handshake.
        var handshaking = new CompletableFuture<Void>();
        var aborted = new CompletableFuture<Void>();
        var progress = new AtomicInteger();
        var gracefulCloses = new AtomicInteger();
        var socket = new TcpTransport.Connection() {
            @Override
            public void handshake() throws IOException {
                handshaking.complete(null);
                while (!aborted.isDone()) {
                    progress.incrementAndGet();
                    LockSupport.parkNanos(MILLISECONDS.toNanos(5));
                }
                throw new IOException("Handshake aborted");
            }

            @Override
            public InputStream input() {
                throw new AssertionError("Input is unavailable before the handshake");
            }

            @Override
            public OutputStream output() {
                throw new AssertionError("Output is unavailable before the handshake");
            }

            @Override
            public void close() {
                gracefulCloses.incrementAndGet();
            }

            @Override
            public void abort() {
                aborted.complete(null);
            }
        };
        try (var sut = new TcpTransport(
                "handshake-deadline",
                "mqtts",
                resolutionSlots,
                this::resolve,
                (_, _, _) -> socket,
                clock
        )) {
            sut.start(URI.create("mqtts://broker.test"), Duration.ofMillis(500), listener);
            handshaking.get(5, SECONDS);
            await().until(() -> progress.get() > 1);

            // Act: progress does not move the deadline, which counts from the start.
            clock.advance(Duration.ofMillis(500).minusNanos(1));
            assertThat(aborted).isNotDone();
            clock.advance(Duration.ofNanos(1));

            // Assert: expiry aborts the raw connection and never announces an MQTT connection.
            assertThat(aborted).isDone();
            assertThat(listener.closed.get(5, SECONDS)).isInstanceOf(SocketTimeoutException.class);
            assertThat(gracefulCloses).hasValue(0);
            assertThat(listener.endings).hasValue(1);
        } finally {
            aborted.complete(null);
        }
    }

    @ParameterizedTest(name = "literal {0}")
    @ValueSource(strings = {"127.0.0.1", "[::1]", "[::ffff:127.0.0.1]"})
    void ip_literals_connect_even_when_name_resolution_is_full(String host) throws Exception {
        // Arrange
        resolutionSlots.acquire();
        try (var sut = new TcpTransport(
                "literal",
                resolutionSlots,
                this::resolve,
                (name, address, timeout) -> {
                    assertThat(address.getAddress()).isEqualTo(InetAddress.ofLiteral(host));
                    return connect(name, address, timeout);
                }
        )) {
            // Act
            sut.start(URI.create("mqtt://" + host + ":1883"), TIMEOUT, listener);

            // Assert
            assertThat(listener.closed.get(5, SECONDS)).isSameAs(connectFailure);
            assertThat(lookups).hasValue(0);
            assertThat(connections).hasValue(1);
            assertThat(resolutionSlots.availablePermits()).isZero();
        } finally {
            resolutionSlots.release();
        }
    }

    @Test
    void waiting_and_resolution_share_the_deadline_and_tcp_does_not_hold_a_dns_slot()
            throws Exception {
        // Arrange: separately hold the capacity wait, lookup, and TCP connection attempt.
        resolutionSlots.acquire();
        var resolving = new CompletableFuture<Void>();
        var resolved = new CompletableFuture<InetAddress>();
        var connecting = new CompletableFuture<Duration>();
        var finishConnect = new CompletableFuture<Void>();
        try (var sut = new TcpTransport(
                "waiting",
                resolutionSlots,
                _ -> {
                    resolving.complete(null);
                    return resolved.join();
                },
                (host, address, timeout) -> {
                    assertThat(address.getAddress()).isEqualTo(ADDRESS);
                    connecting.complete(timeout);
                    finishConnect.join();
                    return connect(host, address, timeout);
                }
        )) {
            // Act: wait for capacity before starting name resolution.
            sut.start(SERVER, TIMEOUT, listener);
            await().until(resolutionSlots::hasQueuedThreads);
            assertThatThrownBy(() -> resolving.get(100, MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            resolutionSlots.release();
            resolving.get(5, SECONDS);

            // Act: name resolution also consumes the original connection budget.
            assertThatThrownBy(() -> connecting.get(100, MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            resolved.complete(ADDRESS);

            // Assert: TCP receives the remaining time and no longer owns the DNS slot.
            assertThat(connecting.get(5, SECONDS))
                    .isPositive()
                    .isLessThan(TIMEOUT.minusMillis(200));
            assertThat(resolutionSlots.availablePermits()).isEqualTo(1);
            finishConnect.complete(null);
            assertThat(listener.closed.get(5, SECONDS)).isSameAs(connectFailure);
        } finally {
            resolved.completeExceptionally(new IOException("Test cleanup"));
            finishConnect.complete(null);
        }
    }

    @Test
    void a_full_resolver_waits_until_the_connection_deadline() throws Exception {
        // Arrange
        resolutionSlots.acquire();
        try (var sut = new TcpTransport(
                "deadline",
                resolutionSlots,
                this::resolve,
                this::connect
        )) {
            // Act
            sut.start(SERVER, Duration.ofMillis(100), listener);

            // Assert
            assertThat(listener.closed.get(5, SECONDS)).isInstanceOf(SocketTimeoutException.class);
            assertThat(lookups).hasValue(0);
            assertThat(connections).hasValue(0);
            assertThat(resolutionSlots.availablePermits()).isZero();
            assertThat(resolutionSlots.hasQueuedThreads()).isFalse();
        } finally {
            resolutionSlots.release();
        }
    }

    @Test
    void close_cancels_a_capacity_wait_without_starting_name_resolution() throws Exception {
        // Arrange
        resolutionSlots.acquire();
        try (var sut = new TcpTransport(
                "closed-wait",
                resolutionSlots,
                this::resolve,
                this::connect
        )) {
            sut.start(SERVER, TIMEOUT, listener);
            await().until(resolutionSlots::hasQueuedThreads);

            // Act
            sut.close();

            // Assert
            assertThat(listener.closed.get(5, SECONDS)).hasMessage("Transport closed");
            await().until(() -> !resolutionSlots.hasQueuedThreads());
            assertThat(lookups).hasValue(0);
            assertThat(connections).hasValue(0);
            assertThat(resolutionSlots.availablePermits()).isZero();
        } finally {
            resolutionSlots.release();
        }
    }

    @Test
    void close_retains_the_dns_slot_until_the_lookup_returns_and_discards_its_result()
            throws Exception {
        // Arrange: the resolver deliberately ignores interruption, like a stalled native lookup.
        var resolving = new CompletableFuture<Thread>();
        var resolved = new CompletableFuture<InetAddress>();
        try (var sut = new TcpTransport(
                "closed-lookup",
                resolutionSlots,
                _ -> {
                    resolving.complete(Thread.currentThread());
                    return resolved.join();
                },
                this::connect
        )) {
            sut.start(SERVER, TIMEOUT, listener);
            Thread worker = resolving.get(5, SECONDS);

            // Act
            sut.close();

            // Assert: notifying shutdown does not make a still-running DNS slot available.
            assertThat(listener.closed.get(5, SECONDS)).hasMessage("Transport closed");
            assertThat(resolutionSlots.availablePermits()).isZero();

            // Act: release a result after the client has ended the connection attempt.
            resolved.complete(ADDRESS);
            worker.join(TIMEOUT);

            // Assert
            assertThat(worker.isAlive()).isFalse();
            assertThat(resolutionSlots.availablePermits()).isEqualTo(1);
            assertThat(connections).hasValue(0);
            assertThat(listener.endings).hasValue(1);
        } finally {
            resolved.completeExceptionally(new IOException("Test cleanup"));
        }
    }

    @Test
    void a_lookup_returning_after_the_deadline_does_not_start_tcp() throws Exception {
        // Arrange
        var resolving = new CompletableFuture<Void>();
        var resolved = new CompletableFuture<InetAddress>();
        try (var sut = new TcpTransport(
                "expired-lookup",
                resolutionSlots,
                _ -> {
                    resolving.complete(null);
                    return resolved.join();
                },
                this::connect
        )) {
            sut.start(SERVER, Duration.ofMillis(250), listener);
            resolving.get(5, SECONDS);

            // Act: keep the lookup running beyond the deadline, then return an address.
            assertThatThrownBy(() -> listener.closed.get(300, MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            resolved.complete(ADDRESS);

            // Assert
            assertThat(listener.closed.get(5, SECONDS)).isInstanceOf(SocketTimeoutException.class);
            assertThat(resolutionSlots.availablePermits()).isEqualTo(1);
            assertThat(connections).hasValue(0);
        } finally {
            resolved.completeExceptionally(new IOException("Test cleanup"));
        }
    }

    @Test
    void a_failed_lookup_returns_its_slot_without_attempting_tcp() throws Exception {
        // Arrange
        var failure = new UnknownHostException("broker.test");
        try (var sut = new TcpTransport(
                "failed-lookup",
                resolutionSlots,
                _ -> {
                    throw failure;
                },
                this::connect
        )) {
            // Act
            sut.start(SERVER, TIMEOUT, listener);

            // Assert
            assertThat(listener.closed.get(5, SECONDS)).isSameAs(failure);
            assertThat(resolutionSlots.availablePermits()).isEqualTo(1);
            assertThat(connections).hasValue(0);
        }
    }

    @Test
    void a_stalled_graceful_close_is_aborted_after_its_grace_period() throws Exception {
        // Arrange: model a provider whose close cannot finish before its TCP socket is closed.
        var connected = new CompletableFuture<Void>();
        var closing = new CompletableFuture<Void>();
        var aborted = new CompletableFuture<Void>();
        var ended = new CompletableFuture<Throwable>();
        var socket = new TcpTransport.Connection() {
            @Override
            public InputStream input() {
                return new InputStream() {
                    @Override
                    public int read() {
                        aborted.join();
                        return -1;
                    }
                };
            }

            @Override
            public OutputStream output() {
                return OutputStream.nullOutputStream();
            }

            @Override
            public boolean closesGracefully() {
                return true;
            }

            @Override
            public void close() {
                closing.complete(null);
                aborted.join();
            }

            @Override
            public void abort() {
                aborted.complete(null);
            }
        };
        try (var sut = new TcpTransport(
                "grace",
                "mqtt",
                resolutionSlots,
                this::resolve,
                (_, _, _) -> socket,
                clock
        )) {
            sut.start(SERVER, TIMEOUT, new Transport.Listener() {
                @Override
                public void onConnected() {
                    connected.complete(null);
                }

                @Override
                public void onBytes(byte[] bytes) {
                    throw new AssertionError("Unexpected bytes");
                }

                @Override
                public void onSent(long ticket) {
                    throw new AssertionError("Unexpected send completion");
                }

                @Override
                public void onClosed(Throwable cause) {
                    assertThat(aborted).isDone();
                    ended.complete(cause);
                }
            });
            connected.get(5, SECONDS);

            // Act: close itself must return before the provider finishes closing.
            var returned = new CompletableFuture<Void>();
            Thread.ofVirtual().start(() -> {
                sut.close();
                returned.complete(null);
            });
            returned.get(5, SECONDS);
            closing.get(5, SECONDS);

            // Assert: keep waiting through the grace period, then abort and notify after cleanup.
            clock.advance(Duration.ofSeconds(1).minusNanos(1));
            assertThat(aborted).isNotDone();
            assertThat(ended).isNotDone();
            clock.advance(Duration.ofNanos(1));
            assertThat(aborted).isDone();
            assertThat(ended.get(5, SECONDS)).isInstanceOf(IOException.class);
        } finally {
            aborted.complete(null);
        }
    }

    @ParameterizedTest(name = "clock failure after handshake: {0}")
    @ValueSource(booleans = {false, true})
    void clock_failure_after_creating_a_socket_aborts_it(boolean afterHandshake) throws Exception {
        // Arrange
        var failure = new IllegalStateException("Clock failed");
        var timer = new FailingTimer();
        var socket = new GracefulConnection();
        socket.handshakeAction = () -> timer.timeFailure = failure;
        try (var sut = new TcpTransport(
                "failed-clock",
                "mqtt",
                resolutionSlots,
                this::resolve,
                (_, _, _) -> {
                    if (!afterHandshake) {
                        timer.timeFailure = failure;
                    }
                    return socket;
                },
                timer
        )) {
            // Act
            sut.start(SERVER, TIMEOUT, listener);

            // Assert: even the first time read after connecting owns and releases the socket.
            assertThat(listener.closed.get(5, SECONDS)).isSameAs(failure);
            assertThat(socket.aborted).isDone();
            assertThat(socket.closes).hasValue(0);
            assertThat(listener.endings).hasValue(1);
        } finally {
            socket.abort();
        }
    }

    @Test
    void a_rejected_handshake_deadline_aborts_the_socket() throws Exception {
        // Arrange
        var failure = new IllegalStateException("Timer stopped");
        var timer = new FailingTimer();
        timer.scheduleFailure = failure;
        var socket = new GracefulConnection();
        try (TcpTransport sut = transport(socket, timer)) {
            // Act
            sut.start(SERVER, TIMEOUT, listener);

            // Assert
            assertThat(listener.closed.get(5, SECONDS)).isSameAs(failure);
            assertThat(socket.aborted).isDone();
            assertThat(socket.closes).hasValue(0);
            assertThat(listener.endings).hasValue(1);
        } finally {
            socket.abort();
        }
    }

    @Test
    void failed_timer_cancellation_does_not_replace_a_handshake_failure() throws Exception {
        // Arrange
        var failure = new IOException("Handshake failed");
        var socket = new GracefulConnection();
        socket.handshakeFailure = failure;
        var timer = new FailingTimer();
        timer.cancellationFailure = new IllegalStateException("Cancellation failed");
        try (TcpTransport sut = transport(socket, timer)) {
            // Act
            sut.start(SERVER, TIMEOUT, listener);

            // Assert
            assertThat(listener.closed.get(5, SECONDS)).isSameAs(failure);
            assertThat(socket.aborted).isDone();
            assertThat(listener.endings).hasValue(1);
        } finally {
            socket.abort();
        }
    }

    @Test
    void failed_timer_cancellation_does_not_end_a_successful_connection() throws Exception {
        // Arrange
        var socket = new GracefulConnection();
        var timer = new FailingTimer();
        timer.cancellationFailure = new IllegalStateException("Cancellation failed");
        var receiver = new ConnectedListener();
        try (TcpTransport sut = transport(socket, timer)) {
            // Act
            sut.start(SERVER, TIMEOUT, receiver);
            receiver.connected.get(5, SECONDS);
            timer.clock.advance(TIMEOUT);

            // Assert: a callback left behind by failed cancellation is harmless after handshake.
            assertThat(receiver.closed).isNotDone();
            assertThat(socket.aborted).isNotDone();
        } finally {
            socket.abort();
        }
        assertThat(receiver.closed.get(5, SECONDS)).hasMessage("Transport closed");
        assertThat(receiver.endings).hasValue(1);
    }

    @Test
    void a_rejected_close_grace_deadline_still_aborts_the_socket() throws Exception {
        // Arrange
        var failure = new IllegalStateException("Timer stopped");
        var socket = new GracefulConnection();
        var timer = new FailingTimer();
        var receiver = new ConnectedListener();
        try (TcpTransport sut = transport(socket, timer)) {
            sut.start(SERVER, TIMEOUT, receiver);
            receiver.connected.get(5, SECONDS);
            timer.scheduleFailure = failure;

            // Act
            sut.close();

            // Assert: the original closing cause remains primary and cleanup finishes.
            Throwable ended = receiver.closed.get(5, SECONDS);
            assertThat(ended).hasMessage("Transport closed");
            assertThat(ended.getSuppressed()).containsExactly(failure);
            assertThat(socket.aborted).isDone();
            assertThat(socket.closes).hasValue(0);
            assertThat(receiver.endings).hasValue(1);
        } finally {
            socket.abort();
        }
    }

    @Test
    void failed_timer_cancellation_cannot_skip_abort_after_a_close_failure() throws Exception {
        // Arrange
        var failure = new IOException("TLS close failed");
        var socket = new GracefulConnection();
        socket.closeFailure = failure;
        var timer = new FailingTimer();
        var receiver = new ConnectedListener();
        try (TcpTransport sut = transport(socket, timer)) {
            sut.start(SERVER, TIMEOUT, receiver);
            receiver.connected.get(5, SECONDS);
            timer.cancellationFailure = new IllegalStateException("Cancellation failed");

            // Act
            sut.close();

            // Assert: cancellation only logs; the close failure remains diagnostic information.
            Throwable ended = receiver.closed.get(5, SECONDS);
            assertThat(ended).hasMessage("Transport closed");
            assertThat(ended.getSuppressed()).containsExactly(failure);
            assertThat(socket.aborted).isDone();
            assertThat(socket.closes).hasValue(1);
            assertThat(receiver.endings).hasValue(1);
        } finally {
            socket.abort();
        }
    }

    private TcpTransport transport(TcpTransport.Connection socket, TimerScheduler timer) {
        return new TcpTransport(
                "timer-failure",
                "mqtt",
                resolutionSlots,
                this::resolve,
                (_, _, _) -> socket,
                timer
        );
    }

    private static final class GracefulConnection implements TcpTransport.Connection {

        final CompletableFuture<Void> aborted = new CompletableFuture<>();
        final AtomicInteger closes = new AtomicInteger();
        Runnable handshakeAction = () -> { };
        @Nullable IOException handshakeFailure;
        @Nullable IOException closeFailure;

        @Override
        public void handshake() throws IOException {
            if (handshakeFailure != null) {
                throw handshakeFailure;
            }
            handshakeAction.run();
        }

        @Override
        public InputStream input() {
            return new InputStream() {
                @Override
                public int read() {
                    aborted.join();
                    return -1;
                }
            };
        }

        @Override
        public OutputStream output() {
            return OutputStream.nullOutputStream();
        }

        @Override
        public boolean closesGracefully() {
            return true;
        }

        @Override
        public void close() throws IOException {
            closes.incrementAndGet();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public void abort() {
            aborted.complete(null);
        }
    }

    private static final class FailingTimer implements TimerScheduler {

        final ManualTimerScheduler clock = new ManualTimerScheduler();
        volatile @Nullable RuntimeException timeFailure;
        volatile @Nullable RuntimeException scheduleFailure;
        volatile @Nullable RuntimeException cancellationFailure;

        @Override
        public long nanoTime() {
            RuntimeException failure = timeFailure;
            if (failure != null) {
                throw failure;
            }
            return clock.nanoTime();
        }

        @Override
        public Cancellable schedule(Duration delay, Runnable task) {
            RuntimeException failure = scheduleFailure;
            if (failure != null) {
                throw failure;
            }
            Cancellable scheduled = clock.schedule(delay, task);
            return () -> {
                RuntimeException cancellation = cancellationFailure;
                if (cancellation != null) {
                    throw cancellation;
                }
                scheduled.cancel();
            };
        }
    }

    private InetAddress resolve(String host) {
        lookups.incrementAndGet();
        return ADDRESS;
    }

    private TcpTransport.Connection connect(
            String host,
            InetSocketAddress address,
            Duration timeout
    ) throws IOException {
        connections.incrementAndGet();
        throw connectFailure;
    }

    private static class RecordingListener implements Transport.Listener {

        final CompletableFuture<Throwable> closed = new CompletableFuture<>();
        final AtomicInteger endings = new AtomicInteger();

        @Override
        public void onConnected() {
            throw new AssertionError("Test connector must not establish a connection");
        }

        @Override
        public void onBytes(byte[] bytes) {
            throw new AssertionError("Unexpected bytes");
        }

        @Override
        public void onSent(long ticket) {
            throw new AssertionError("Unexpected send completion");
        }

        @Override
        public void onClosed(Throwable cause) {
            endings.incrementAndGet();
            closed.complete(cause);
        }
    }

    private static final class ConnectedListener extends RecordingListener {

        final CompletableFuture<Void> connected = new CompletableFuture<>();

        @Override
        public void onConnected() {
            connected.complete(null);
        }
    }
}
