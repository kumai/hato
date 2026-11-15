package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.RawSession.connect;
import static org.mqtt4j.hato.broker.internal.RawSession.connectV5;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.QUOTA_EXCEEDED;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SERVER_UNAVAILABLE;

/// How many clients the broker serves at once, and how many connections may wait for it to
/// accept their CONNECT.
class BrokerClientLimitTest {

    @Test
    void an_mqtt_3_1_1_client_beyond_the_limit_is_refused_as_unavailable() throws Exception {
        try (BrokerServer sut = servingAtMost(1);
                RawSession served = connected(sut.port(), "served")) {
            // Act & Assert
            assertRefused(sut.port(), connect("further", true, 0), SERVER_UNAVAILABLE);
            served.ping();
        }
    }

    @Test
    void an_mqtt_5_client_beyond_the_limit_is_refused_with_quota_exceeded() throws Exception {
        try (BrokerServer sut = servingAtMost(1);
                RawSession served = connected(sut.port(), "served")) {
            // Act & Assert
            assertRefused(sut.port(), connectV5("further", true, 0, 0), QUOTA_EXCEEDED);
            served.ping();
        }
    }

    @Test
    void a_client_whose_connection_ends_makes_room_for_another() throws Exception {
        try (BrokerServer sut = servingAtMost(1)) {
            // Arrange: a client in the only place, which then goes without a DISCONNECT.
            try (RawSession served = connected(sut.port(), "served")) {
                assertRefused(sut.port(), connect("further", true, 0), SERVER_UNAVAILABLE);
                served.ping();
            }

            // Act & Assert: the place frees as the end of the connection comes to the broker.
            try (RawSession further = connectedEventually(sut.port(), "further", true, false)) {
                further.ping();
            }
        }
    }

    @Test
    void a_client_that_comes_back_for_its_session_needs_no_room() throws Exception {
        try (BrokerServer sut = servingAtMost(1);
                RawSession lost = connected(sut.port(), "returning")) {
            // Act: the client connects again while its first connection still counts.
            try (RawSession back = connected(sut.port(), "returning")) {
                // Assert: the broker takes it and ends the first connection, whose end, which
                // has come to the session by the time it answers the ping, frees no place.
                lost.expectClosed();
                back.ping();
                assertRefused(sut.port(), connect("further", true, 0), SERVER_UNAVAILABLE);
            }
        }
    }

    @Test
    void a_refused_client_finds_its_session_as_it_left_it() throws Exception {
        try (BrokerServer sut = servingAtMost(1)) {
            // Arrange: a session that outlives its connection, and another client in the only
            // place.
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.subscribe("kept", AT_LEAST_ONCE);
            }
            try (RawSession other = connectedEventually(sut.port(), "other", true, false)) {
                // Act: the client asks for a new session, and is refused.
                assertRefused(sut.port(), connect("away", true, 0), SERVER_UNAVAILABLE);
                other.publish("kept", "meanwhile", AT_LEAST_ONCE, 7);
            }

            // Assert: the session and what it waited for are still there.
            try (RawSession back = connectedEventually(sut.port(), "away", false, true)) {
                assertThat(back.expect(Packet.Publish.class).payload())
                        .isEqualTo("meanwhile".getBytes(UTF_8));
            }
        }
    }

    @Test
    void a_client_that_the_session_limit_refuses_takes_no_place() throws Exception {
        BrokerServer.Builder limited = BrokerServer.builder().maxClients(2).maxSessions(1);
        try (BrokerServer sut = limited.start(loopback());
                RawSession lasting = connected(sut.port(), "lasting", false, false)) {
            // Arrange: a second session that would outlive its connection has no room.
            assertRefused(sut.port(), connect("refused", false, 0), SERVER_UNAVAILABLE);

            // Act & Assert: the second place among the clients is still free.
            try (RawSession further = connected(sut.port(), "further")) {
                further.ping();
                lasting.ping();
            }
        }
    }

    @Test
    void a_connection_beyond_those_that_may_wait_for_their_connect_is_closed_at_once()
            throws Exception {
        try (BrokerServer sut = waitingAtMost(2);
                RawSession first = RawSession.open(sut.port());
                RawSession second = RawSession.open(sut.port());
                RawSession third = RawSession.open(sut.port())) {
            // Assert: the broker closes the third, and still takes the CONNECT of the others.
            third.expectClosed();
            for (RawSession waiting : new RawSession[] {first, second}) {
                waiting.send(connect("", true, 0));
                assertThat(waiting.expect(Packet.ConnAck.class).reasonCode())
                        .isEqualTo(ReasonCode.SUCCESS);
            }
        }
    }

    @Test
    void an_accepted_client_no_longer_counts_among_the_waiting_connections() throws Exception {
        try (BrokerServer sut = waitingAtMost(1);
                RawSession accepted = connected(sut.port(), "accepted")) {
            // Arrange: the broker reads the ping only once it has accepted the client for good.
            accepted.ping();

            // Act & Assert
            try (RawSession next = connected(sut.port(), "next")) {
                next.ping();
            }
        }
    }

    @Test
    void a_connection_that_ends_without_a_connect_makes_room_for_another_to_wait()
            throws Exception {
        try (BrokerServer sut = waitingAtMost(1)) {
            // Arrange: a connection that sends nothing, which the broker's limit then closes
            // the next one for.
            try (RawSession _ = RawSession.open(sut.port());
                    RawSession turnedAway = RawSession.open(sut.port())) {
                turnedAway.expectClosed();
            }

            // Act & Assert: the silent one has gone, and the next one waits in its place.
            try (RawSession next = connectedEventually(sut.port(), "next", true, false)) {
                next.ping();
            }
        }
    }

    @Test
    void a_refused_client_leaves_its_place_among_the_waiting_connections() throws Exception {
        BrokerServer.Builder full = BrokerServer.builder().maxClients(1).maxAwaitingConnect(1);
        try (BrokerServer sut = full.start(loopback());
                RawSession served = connected(sut.port(), "served")) {
            // Arrange
            served.ping();

            // Act & Assert: each further client waits in the same place, and is told why it is
            // refused.
            for (int attempt = 0; attempt < 3; attempt++) {
                assertRefused(sut.port(), connect("further", true, 0), SERVER_UNAVAILABLE);
            }
        }
    }

    /// Connects to the broker on `port` and expects it to refuse `request` with `reason` and to
    /// close the connection.
    private static void assertRefused(int port, Packet.Connect request, ReasonCode reason)
            throws Exception {
        try (RawSession refused = RawSession.open(port, request.version())) {
            refused.send(request);
            assertThat(refused.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, reason, EMPTY));
            refused.expectClosed();
        }
    }

    /// An MQTT 3.1.1 session that the broker on `port` accepts once it has room, which it gets
    /// as it learns of the end of an earlier connection. Until then it refuses the client or
    /// closes its connection, and the client tries again.
    private static RawSession connectedEventually(
            int port,
            String clientId,
            boolean cleanSession,
            boolean sessionPresent
    ) {
        var session = new AtomicReference<RawSession>();
        await().ignoreExceptions().untilAsserted(
                () -> session.set(connected(port, clientId, cleanSession, sessionPresent))
        );
        return session.get();
    }

    private static BrokerServer servingAtMost(int clients) throws IOException {
        return BrokerServer.builder().maxClients(clients).start(loopback());
    }

    private static BrokerServer waitingAtMost(int connections) throws IOException {
        return BrokerServer.builder().maxAwaitingConnect(connections).start(loopback());
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
