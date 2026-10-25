package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.internal.RawSession.acceptedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.connectV5;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// How long MQTT 5.0 sessions outlive their connections, on a clock that the test advances.
///
/// Each client leaves with a Will that a watcher receives. The broker publishes it only after it
/// has detached the session from the connection, from which the expiry counts, so the test
/// advances the clock after the Will arrives.
class BrokerSessionExpiryTest {

    private final BrokerDeadlineTest.ManualTimer timer = new BrokerDeadlineTest.ManualTimer();

    @Test
    void a_session_ends_once_its_expiry_has_passed_since_its_connection_ended() throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            leave(open(sut, "expiring", true, 10), watcher, "expiring");

            // Act
            timer.clock.advance(Duration.ofSeconds(10));

            // Assert
            resumed(sut, "expiring", 10, false).close();
        }
    }

    @Test
    void a_connection_within_the_expiry_keeps_the_session_and_its_expiry_counts_again()
            throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            leave(open(sut, "returning", true, 10), watcher, "returning");
            timer.clock.advance(Duration.ofSeconds(10).minusNanos(1));

            // Act
            leave(resumed(sut, "returning", 10, true), watcher, "returning");

            // Assert: the expiry counts from the end of the second connection.
            timer.clock.advance(Duration.ofSeconds(10).minusNanos(1));
            leave(resumed(sut, "returning", 10, true), watcher, "returning");
            timer.clock.advance(Duration.ofSeconds(10));
            resumed(sut, "returning", 10, false).close();
        }
    }

    @Test
    void a_session_without_an_expiry_outlasts_any_wait() throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange: 0xFFFFFFFF in MQTT 5.0, and Clean Session 0 in MQTT 3.1.1.
            leave(open(sut, "lasting", true, 0xFFFF_FFFFL), watcher, "lasting");
            var classic = RawSession.open(sut.port());
            classic.send(new Packet.Connect(
                    "classic",
                    MqttVersion.V3_1_1,
                    false,
                    0,
                    will("classic"),
                    null,
                    null,
                    EMPTY
            ));
            assertThat(classic.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, SUCCESS, EMPTY));
            leave(classic, watcher, "classic");

            // Act: longer than the longest expiry that a session can have.
            timer.clock.advance(Duration.ofSeconds(0xFFFF_FFFFL + 1));

            // Assert
            resumed(sut, "lasting", 0xFFFF_FFFFL, true).close();
            try (var resumed = RawSession.open(sut.port())) {
                resumed.send(RawSession.connect("classic", false, 0));
                assertThat(resumed.expect(Packet.ConnAck.class))
                        .isEqualTo(new Packet.ConnAck(true, SUCCESS, EMPTY));
            }
        }
    }

    @Test
    void a_disconnect_can_shorten_the_expiry() throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            RawSession client = open(sut, "shortened", true, 100);

            // Act
            client.send(disconnectWithWill(5));
            leave(client, watcher, "shortened");

            // Assert
            timer.clock.advance(Duration.ofSeconds(5));
            resumed(sut, "shortened", 100, false).close();
        }
    }

    @Test
    void a_disconnect_with_no_expiry_ends_the_session_with_the_connection() throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            RawSession client = open(sut, "ending", true, 100);

            // Act
            client.send(disconnectWithWill(0));
            leave(client, watcher, "ending");

            // Assert
            resumed(sut, "ending", 100, false).close();
        }
    }

    @Test
    void a_disconnect_cannot_give_an_expiry_to_a_session_that_had_none() throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            RawSession client = open(sut, "unkept", true, 0);
            var expiry = new Property.FourByteValue(PropertyId.SESSION_EXPIRY_INTERVAL, 5);
            var disconnect = new Packet.Disconnect(SUCCESS, new Properties(List.of(expiry)));

            // Act
            client.send(disconnect);

            // Assert: no valid DISCONNECT, so the Will goes out (MQTT 5.0 section 3.14.2.2.2).
            var refusal = new Packet.Disconnect(ReasonCode.PROTOCOL_ERROR, EMPTY);
            assertThat(client.expect(Packet.Disconnect.class)).isEqualTo(refusal);
            leave(client, watcher, "unkept");
            resumed(sut, "unkept", 0, false).close();
        }
    }

    @Test
    void an_expired_session_frees_its_place_among_the_persistent_sessions() throws Exception {
        try (BrokerServer sut = broker(1); RawSession watcher = watching(sut)) {
            // Arrange: the only persistent session that the broker keeps.
            leave(open(sut, "first", true, 10), watcher, "first");
            try (var refused = RawSession.open(sut.port(), MqttVersion.V5)) {
                refused.send(connectV5("second", true, 0, 10));
                assertThat(refused.expect(Packet.ConnAck.class).reasonCode())
                        .isEqualTo(ReasonCode.SERVER_UNAVAILABLE);
            }

            // Act
            timer.clock.advance(Duration.ofSeconds(10));

            // Assert
            open(sut, "second", true, 10).close();
        }
    }

    private BrokerServer broker() throws IOException {
        return broker(16);
    }

    private BrokerServer broker(int maxSessions) throws IOException {
        return BrokerServer.builder()
                .connectTimeout(Duration.ofSeconds(5))
                .scheduler(timer)
                .maxSessions(maxSessions)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    /// A connection that receives the Wills of the clients that leave.
    private static RawSession watching(BrokerServer broker) throws Exception {
        RawSession watcher = RawSession.connected(broker.port(), "watcher");
        watcher.subscribe("left/#", AT_MOST_ONCE);
        return watcher;
    }

    /// An MQTT 5.0 connection of `clientId` with a Will and a Session Expiry Interval of
    /// `expiry` seconds, which the broker accepted without a session.
    private static RawSession open(
            BrokerServer broker,
            String clientId,
            boolean cleanStart,
            long expiry
    ) throws Exception {
        RawSession client = RawSession.open(broker.port(), MqttVersion.V5);
        client.send(connect(clientId, cleanStart, expiry));
        assertThat(client.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(false));
        return client;
    }

    /// Connects `clientId` again without Clean Start and checks whether its session was kept.
    private static RawSession resumed(
            BrokerServer broker,
            String clientId,
            long expiry,
            boolean present
    ) throws Exception {
        RawSession client = RawSession.open(broker.port(), MqttVersion.V5);
        client.send(connect(clientId, false, expiry));
        assertThat(client.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(present));
        return client;
    }

    /// Ends the connection of `client` from the client's side and waits until `watcher` receives
    /// the Will of `clientId`, which the broker publishes after detaching the session.
    private static void leave(RawSession client, RawSession watcher, String clientId)
            throws Exception {
        client.close();
        assertThat(watcher.expect(Packet.Publish.class).topic())
                .isEqualTo(new Topic("left/" + clientId));
    }

    private static Packet.Connect connect(String clientId, boolean cleanStart, long expiry) {
        var interval = new Property.FourByteValue(PropertyId.SESSION_EXPIRY_INTERVAL, expiry);
        return new Packet.Connect(
                clientId,
                MqttVersion.V5,
                cleanStart,
                0,
                will(clientId),
                null,
                null,
                new Properties(List.of(interval))
        );
    }

    private static Packet.Will will(String clientId) {
        return new Packet.Will(
                new Topic("left/" + clientId),
                "gone".getBytes(UTF_8),
                AT_MOST_ONCE,
                false,
                EMPTY
        );
    }

    /// A DISCONNECT that keeps the Will and sets a new Session Expiry Interval.
    private static Packet.Disconnect disconnectWithWill(long expiry) {
        var interval = new Property.FourByteValue(PropertyId.SESSION_EXPIRY_INTERVAL, expiry);
        return new Packet.Disconnect(
                ReasonCode.DISCONNECT_WITH_WILL_MESSAGE,
                new Properties(List.of(interval))
        );
    }
}
