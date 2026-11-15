package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.BrokerListener;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;

/// How a broker ends: its listener hears once of a failure that stops it, the broker ends
/// without waiting for that call, and terminated() completes after both.
class BrokerEndTest {

    private static final Duration WAIT = Duration.ofSeconds(5);

    // The failures that the listener below hears of, and the threads that it hears of them on.
    private final List<Throwable> failures = new CopyOnWriteArrayList<>();

    private final List<String> threads = new CopyOnWriteArrayList<>();

    private final BrokerListener listener = new BrokerListener() {
        @Override
        public void onFailed(Throwable failure) {
            failures.add(failure);
            threads.add(Thread.currentThread().getName());
        }
    };

    @Test
    void a_failed_selector_stops_the_broker_and_the_listener_hears_of_it_once() throws Exception {
        var failing = new AtomicBoolean();
        var injected = new AssertionError("injected selector failure");
        try (
                BrokerServer sut = BrokerServer.builder().listener(listener).beforeSelect(() -> {
                    if (failing.compareAndSet(true, false)) {
                        throw injected;
                    }
                }).start(loopback());
                RawSession client = connected(sut.port(), "trigger")
        ) {
            // Act: a publication without a response wakes a selector, which fails before it
            // waits.
            failing.set(true);
            client.send(publication("unheard", "", AT_MOST_ONCE, 0));

            // Assert: the listener heard of it on neither the failed thread nor another one that
            // close() waits for.
            assertThat(sut.terminated()).succeedsWithin(WAIT);
            assertThat(failures).containsExactly(injected);
            assertThat(threads).singleElement().asString()
                    .doesNotContain("-selector-")
                    .doesNotContain("-accept");
        }
    }

    @Test
    void a_failed_accepting_thread_stops_the_broker_and_the_listener_hears_of_it()
            throws Exception {
        var asked = new CountDownLatch(1);
        var failing = new AtomicBoolean();
        var injected = new IllegalStateException("injected accepting failure");
        try (BrokerServer sut = BrokerServer.builder().listener(listener).beforeAccept(() -> {
            if (failing.compareAndSet(true, false)) {
                throw injected;
            }
            asked.countDown();
        }).start(loopback())) {
            // Arrange: the accepting thread has asked the listener once, before any connection
            // came, and waits for one.
            assertThat(asked.await(5, SECONDS)).isTrue();
            // Act: a connection wakes the accepting thread, which fails before it accepts.
            failing.set(true);
            try (var socket = new Socket()) {
                socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), sut.port()));
            } catch (IOException _) {
                // The broker can close its listener before the connection is through.
            }

            // Assert
            assertThat(sut.terminated()).succeedsWithin(WAIT);
            assertThat(failures).containsExactly(injected);
        }
    }

    @Test
    void a_broker_whose_selector_fails_as_it_starts_still_ends() throws Exception {
        // Arrange: the first selector to look for work fails, which can be before the others
        // have started; those then never start.
        var failing = new AtomicBoolean(true);
        var injected = new AssertionError("injected selector failure");
        // Act
        try (BrokerServer sut = BrokerServer.builder().listener(listener).beforeSelect(() -> {
            if (failing.compareAndSet(true, false)) {
                throw injected;
            }
        }).start(loopback())) {
            // Assert
            assertThat(sut.terminated()).succeedsWithin(WAIT);
            assertThat(failures).containsExactly(injected);
        }
    }

    @Test
    @SuppressWarnings("try") // The explicit close is what the test is about.
    void a_broker_that_the_application_closes_tells_its_listener_of_no_failure() throws Exception {
        try (BrokerServer sut = BrokerServer.builder().listener(listener).start(loopback())) {
            // Act
            sut.close();
            // Assert
            assertThat(sut.terminated()).succeedsWithin(WAIT);
            assertThat(failures).isEmpty();
        }
    }

    @Test
    @SuppressWarnings("try") // The explicit close is what the test is about.
    void the_broker_ends_without_waiting_for_its_listener() throws Exception {
        // Arrange: a listener that holds the notice of the failure until the test lets it go.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var holding = new BrokerListener() {
            @Override
            public void onFailed(Throwable failure) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        var failing = new AtomicBoolean();
        InetSocketAddress address = loopback();
        try (
                BrokerServer sut = BrokerServer.builder().listener(holding).beforeSelect(() -> {
                    if (failing.compareAndSet(true, false)) {
                        throw new AssertionError("injected selector failure");
                    }
                }).start(address);
                RawSession client = connected(sut.port(), "trigger")
        ) {
            int port = sut.port();
            failing.set(true);
            client.send(publication("unheard", "", AT_MOST_ONCE, 0));
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act
            sut.close();

            // Assert: the port is free and the connections are closed while the listener holds
            // its call, and the broker ends only once the call returns.
            client.expectClosed();
            var sameAddress = new InetSocketAddress(address.getAddress(), port);
            try (MqttBroker again = MqttBroker.builder().listen(sameAddress).start()) {
                assertThat(again.port()).isEqualTo(port);
            }
            assertThat(sut.terminated()).isNotDone();
            release.countDown();
            assertThat(sut.terminated()).succeedsWithin(WAIT);
        }
    }

    @Test
    void the_listener_hears_of_a_failure_while_another_call_takes_its_time() throws Exception {
        // Arrange: a listener that holds its report of dropped deliveries until the test lets it
        // go, and a session that loses one of two deliveries while its client is away.
        var reporting = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var told = new CountDownLatch(1);
        var holding = new BrokerListener() {
            @Override
            public void onDropped(String clientId, long count) {
                reporting.countDown();
                try {
                    release.await();
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onFailed(Throwable failure) {
                told.countDown();
            }
        };
        var failing = new AtomicBoolean();
        try (
                BrokerServer sut = BrokerServer.builder()
                        .maxQueued(1)
                        .listener(holding)
                        .beforeSelect(() -> {
                            if (failing.compareAndSet(true, false)) {
                                throw new AssertionError("injected selector failure");
                            }
                        })
                        .start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")
        ) {
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.subscribe("queued", AT_LEAST_ONCE);
                away.disconnect();
            }
            publisher.publish("queued", "1", AT_LEAST_ONCE, 1);
            publisher.publish("queued", "2", AT_LEAST_ONCE, 2);
            assertThat(reporting.await(5, SECONDS)).isTrue();

            // Act
            failing.set(true);
            publisher.send(publication("unheard", "", AT_MOST_ONCE, 0));

            // Assert: the notice comes while the report still runs, and the broker ends once
            // both have returned.
            try {
                assertThat(told.await(5, SECONDS)).isTrue();
                assertThat(sut.terminated()).isNotDone();
            } finally {
                release.countDown();
            }
            assertThat(sut.terminated()).succeedsWithin(WAIT);
        }
    }

    @Test
    @SuppressWarnings("try") // The explicit close is what the test is about.
    void an_observation_whose_continuation_waits_for_another_holds_up_none() throws Exception {
        try (MqttBroker sut = MqttBroker.start()) {
            // Arrange: the observation made later waits, in its continuation, for the one made
            // first.
            CompletableFuture<Void> first = sut.terminated();
            CompletableFuture<Void> later = sut.terminated();
            CompletableFuture<Void> waited = later.thenRun(first::join);
            // Act
            sut.close();
            // Assert
            assertThat(first).succeedsWithin(WAIT);
            assertThat(waited).succeedsWithin(WAIT);
        }
    }

    @Test
    @SuppressWarnings("try") // The explicit close is what the test is about.
    void every_call_of_terminated_observes_the_end_apart() throws Exception {
        try (MqttBroker sut = MqttBroker.start()) {
            // Arrange
            CompletableFuture<Void> cancelled = sut.terminated();
            // Act
            cancelled.cancel(true);
            sut.close();
            // Assert: cancelling one future left the end and the other futures alone.
            assertThat(sut.terminated()).succeedsWithin(WAIT);
        }
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
