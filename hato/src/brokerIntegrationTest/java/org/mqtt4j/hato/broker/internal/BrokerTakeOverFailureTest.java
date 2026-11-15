package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.MqttVersion;

import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;

/// A takeover that fails on the way, as on an OutOfMemoryError: the connection that it was for
/// ends alone, and the places that it took and the session that it leaves stay accounted for.
class BrokerTakeOverFailureTest {

    @Test
    void a_failed_takeover_gives_back_the_client_place_that_it_took() throws Exception {
        // Arrange: room for one client, and a takeover that fails for the first one.
        var armed = new AtomicBoolean(true);
        try (BrokerServer sut = BrokerServer.builder()
                .maxClients(1)
                .beforeAttach(failingOnceWhen(armed))
                .start(loopback())) {
            try (RawSession failing = RawSession.open(sut.port())) {
                // Act
                failing.send(RawSession.connect("failing", true, 0));
                // Assert: the connection ends, and its place waits for the next client.
                failing.expectClosed();
            }
            try (RawSession next = connected(sut.port(), "next")) {
                next.ping();
            }
        }
    }

    @Test
    void a_failed_takeover_of_a_connection_gives_back_the_place_that_it_took_over()
            throws Exception {
        // Arrange: room for one client, which a connection has, and a takeover of that
        // connection that fails.
        var armed = new AtomicBoolean();
        try (BrokerServer sut = BrokerServer.builder()
                .maxClients(1)
                .beforeAttach(failingOnceWhen(armed))
                .start(loopback());
                RawSession first = connected(sut.port(), "robot")) {
            armed.set(true);
            try (RawSession second = RawSession.open(sut.port())) {
                // Act
                second.send(RawSession.connect("robot", true, 0));
                // Assert: both connections end, and the place waits for the next client.
                second.expectClosed();
                first.expectClosed();
            }
            try (RawSession next = connected(sut.port(), "next")) {
                next.ping();
            }
        }
    }

    @Test
    void a_session_that_a_failed_takeover_leaves_expires_and_frees_its_place() throws Exception {
        // Arrange: room for one persistent session, and a takeover that fails for a client that
        // asked for a session of 2 seconds, after the broker made that session.
        var timer = new BrokerDeadlineTest.ManualTimer();
        var armed = new AtomicBoolean(true);
        try (BrokerServer sut = BrokerServer.builder()
                .maxSessions(1)
                .scheduler(timer)
                .beforeAttach(failingOnceWhen(armed))
                .start(loopback())) {
            try (RawSession failing = RawSession.open(sut.port(), MqttVersion.V5)) {
                failing.send(RawSession.connectV5("failing", true, 0, 2));
                failing.expectClosed();
            }
            // Act
            timer.clock.advance(Duration.ofSeconds(3));
            // Assert: the session expires on its own, and its place goes to another client's.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                try (RawSession next = connectedV5(sut.port(), "next", true, 60, false)) {
                    next.ping();
                }
            });
        }
    }

    // A hook that fails the next takeover while `armed` holds, and then disarms.
    private static Consumer<String> failingOnceWhen(AtomicBoolean armed) {
        return _ -> {
            if (armed.compareAndSet(true, false)) {
                throw new IllegalStateException("Injected takeover failure");
            }
        };
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
