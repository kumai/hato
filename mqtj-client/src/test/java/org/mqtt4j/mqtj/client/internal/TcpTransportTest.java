package org.mqtt4j.mqtj.client.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.mqtj.client.internal.common.io.TcpConnection;
import org.mqtt4j.mqtj.spi.Transport;

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

    @ParameterizedTest(name = "literal {0}")
    @ValueSource(strings = {"127.0.0.1", "[::1]", "[::ffff:127.0.0.1]"})
    void ip_literals_connect_even_when_name_resolution_is_full(String host) throws Exception {
        // Arrange
        resolutionSlots.acquire();
        try (var sut = new TcpTransport(
                "literal",
                resolutionSlots,
                this::resolve,
                (address, timeout) -> {
                    assertThat(address.getAddress()).isEqualTo(InetAddress.ofLiteral(host));
                    return connect(address, timeout);
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
                (address, timeout) -> {
                    assertThat(address.getAddress()).isEqualTo(ADDRESS);
                    connecting.complete(timeout);
                    finishConnect.join();
                    return connect(address, timeout);
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

    private InetAddress resolve(String host) {
        lookups.incrementAndGet();
        return ADDRESS;
    }

    private TcpConnection connect(InetSocketAddress address, Duration timeout) throws IOException {
        connections.incrementAndGet();
        throw connectFailure;
    }

    private static final class RecordingListener implements Transport.Listener {

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
}
