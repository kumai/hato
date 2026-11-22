package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.broker.BrokerListener;
import org.mqtt4j.hato.broker.BrokerListener.DisconnectReason;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.BrokerListener.DisconnectReason.APPLICATION;
import static org.mqtt4j.hato.broker.BrokerListener.DisconnectReason.BROKER_ERROR;
import static org.mqtt4j.hato.broker.BrokerListener.DisconnectReason.CLIENT_DISCONNECT;
import static org.mqtt4j.hato.broker.BrokerListener.DisconnectReason.CONNECTION_LOST;
import static org.mqtt4j.hato.broker.BrokerListener.DisconnectReason.KEEP_ALIVE_TIMEOUT;
import static org.mqtt4j.hato.broker.BrokerListener.DisconnectReason.PROTOCOL_ERROR;
import static org.mqtt4j.hato.broker.BrokerListener.DisconnectReason.TAKEN_OVER;
import static org.mqtt4j.hato.broker.internal.RawSession.connect;
import static org.mqtt4j.hato.broker.internal.RawSession.connectV5;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;

/// What the application's listener hears of the clients that connect and disconnect: each
/// accepted connection, why it ended, in the order of the client's connections, and nothing of
/// the connections that the broker refuses or that its own end closes.
class BrokerConnectionNoticeTest {

    private static final Duration WAIT = Duration.ofSeconds(5);

    // What the listener has heard, in the order of its calls.
    private final BlockingQueue<Heard> heard = new LinkedBlockingQueue<>();

    private final BrokerListener listener = new BrokerListener() {
        @Override
        public void onConnected(Connection connection) {
            heard.add(new Heard(connection, null));
        }

        @Override
        public void onDisconnected(Connection connection, DisconnectReason reason) {
            heard.add(new Heard(connection, reason));
        }
    };

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void the_listener_hears_that_a_client_connected_and_that_it_disconnected(MqttVersion version)
            throws Exception {
        try (BrokerServer sut = broker()) {
            // Act
            try (RawSession client = connectedOf(sut.port(), "robot", version)) {
                Heard connected = next();
                client.disconnect();
                Heard disconnected = next();

                // Assert
                assertThat(connected.reason()).isNull();
                assertThat(connected.connection().clientId()).isEqualTo("robot");
                assertThat(connected.connection().version()).isEqualTo(version);
                assertThat(connected.connection().remoteAddress()).isPresent();
                assertThat(connected.connection().username()).isEmpty();
                assertThat(disconnected.connection()).isSameAs(connected.connection());
                assertThat(disconnected.reason()).isEqualTo(CLIENT_DISCONNECT);
            }
        }
    }

    @Test
    void the_listener_hears_of_a_client_that_went_silent_beyond_its_keep_alive() throws Exception {
        try (BrokerServer sut = broker(); RawSession client = RawSession.open(sut.port())) {
            // Arrange: a keep-alive of one second, which the client then keeps to no more.
            client.send(connect("silent", true, 1));
            client.expect(Packet.ConnAck.class);
            assertThat(next().reason()).isNull();

            // Act
            client.expectClosed();

            // Assert
            assertThat(next().reason()).isEqualTo(KEEP_ALIVE_TIMEOUT);
        }
    }

    @Test
    void the_listener_hears_of_a_connection_that_closed_without_disconnect() throws Exception {
        try (BrokerServer sut = broker()) {
            // Arrange
            RawSession client = connected(sut.port(), "robot");
            assertThat(next().reason()).isNull();

            // Act
            client.close();

            // Assert
            assertThat(next().reason()).isEqualTo(CONNECTION_LOST);
        }
    }

    @Test
    void a_takeover_reports_the_end_of_the_old_connection_before_the_new_one() throws Exception {
        try (BrokerServer sut = broker(); RawSession first = connected(sut.port(), "robot")) {
            // Arrange
            Heard firstConnected = next();

            // Act
            try (RawSession second = connected(sut.port(), "robot")) {
                // Assert
                Heard firstEnded = next();
                Heard secondConnected = next();
                assertThat(firstEnded.connection()).isSameAs(firstConnected.connection());
                assertThat(firstEnded.reason()).isEqualTo(TAKEN_OVER);
                assertThat(secondConnected.reason()).isNull();
                assertThat(secondConnected.connection()).isNotSameAs(firstConnected.connection());
                first.expectClosed();
                second.ping();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void each_notice_carries_the_user_name_of_its_own_connection(MqttVersion version)
            throws Exception {
        try (BrokerServer sut = broker();
                RawSession alice = RawSession.open(sut.port(), version)) {
            // Arrange
            alice.send(connectAs("robot", version, "alice"));
            alice.expect(Packet.ConnAck.class);
            Heard aliceConnected = next();

            // Act: another user connects with the same identifier, and takes the session over.
            try (RawSession bob = RawSession.open(sut.port(), version)) {
                bob.send(connectAs("robot", version, "bob"));
                bob.expect(Packet.ConnAck.class);
                Heard aliceEnded = next();
                Heard bobConnected = next();

                // Assert
                assertThat(aliceConnected.connection().username()).contains("alice");
                assertThat(aliceEnded.connection()).isSameAs(aliceConnected.connection());
                assertThat(aliceEnded.reason()).isEqualTo(TAKEN_OVER);
                assertThat(bobConnected.connection().username()).contains("bob");
                assertThat(bobConnected.connection().toString()).doesNotContain("bob");
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void the_notice_of_a_connection_carries_the_topic_of_its_will(MqttVersion version)
            throws Exception {
        try (BrokerServer sut = broker(); RawSession robot = RawSession.open(sut.port(), version)) {
            // Act: a client with a Will, and then one without.
            robot.send(new Packet.Connect(
                    "robot",
                    version,
                    true,
                    0,
                    new Packet.Will(
                            new Topic("fleet/robot/connection"),
                            "lost".getBytes(UTF_8),
                            Qos.AT_LEAST_ONCE,
                            false,
                            Properties.EMPTY
                    ),
                    null,
                    null,
                    Properties.EMPTY
            ));
            robot.expect(Packet.ConnAck.class);
            Heard withWill = next();
            try (RawSession _ = connectedOf(sut.port(), "other", version)) {
                Heard withoutWill = next();

                // Assert
                assertThat(withWill.connection().willTopic())
                        .contains(new Topic("fleet/robot/connection"));
                assertThat(withoutWill.connection().willTopic()).isEmpty();
            }
        }
    }

    @Test
    void the_end_of_a_session_comes_before_the_next_connection_of_its_client() throws Exception {
        // Arrange: the actor of a session that ends with its connection waits once the session
        // has left the session table, while the client's next connection reaches a new actor on
        // the other actor thread.
        var left = new CountDownLatch(1);
        var resume = new CountDownLatch(1);
        BrokerServer.Builder waitingAfterLeaving = BrokerServer.builder()
                .listener(listener)
                .actors(2)
                .afterLeave(_ -> {
                    if (left.getCount() == 0) {
                        return;
                    }
                    left.countDown();
                    try {
                        // The wait has a limit, so that a test that fails cannot hold the actor
                        // for good; whether it was released or the limit came makes no
                        // difference here.
                        boolean _ = resume.await(WAIT.toSeconds(), SECONDS);
                    } catch (InterruptedException _) {
                        Thread.currentThread().interrupt();
                    }
                });
        try (BrokerServer sut = waitingAfterLeaving.start(loopback());
                RawSession first = connectedV5(sut.port(), "robot")) {
            Heard firstConnected = next();
            // The broker closes the socket only after the actor has let go, so the test waits for
            // the session to leave rather than for the close.
            first.send(new Packet.Disconnect(ReasonCode.SUCCESS, Properties.EMPTY));
            assertThat(left.await(WAIT.toSeconds(), SECONDS)).isTrue();

            // Act
            try (RawSession second = connectedV5(sut.port(), "robot")) {
                resume.countDown();

                // Assert
                Heard firstEnded = next();
                Heard secondConnected = next();
                assertThat(firstEnded.connection()).isSameAs(firstConnected.connection());
                assertThat(firstEnded.reason()).isEqualTo(CLIENT_DISCONNECT);
                assertThat(secondConnected.connection()).isNotSameAs(firstConnected.connection());
                assertThat(secondConnected.reason()).isNull();
                first.expectClosed();
                second.ping();
            }
        } finally {
            resume.countDown();
        }
    }

    @Test
    void the_listener_hears_of_a_client_that_the_application_disconnected() throws Exception {
        try (BrokerServer sut = broker(); RawSession client = connected(sut.port(), "robot")) {
            // Arrange
            assertThat(next().reason()).isNull();

            // Act
            sut.disconnect("robot");

            // Assert
            client.expectClosed();
            assertThat(next().reason()).isEqualTo(APPLICATION);
        }
    }

    @Test
    void the_listener_hears_of_a_client_that_broke_the_protocol() throws Exception {
        try (BrokerServer sut = broker(); RawSession client = connected(sut.port(), "robot")) {
            // Arrange
            assertThat(next().reason()).isNull();

            // Act: a packet of the type that MQTT 3.1.1 reserves.
            client.sendRaw(new byte[] {(byte) 0xF0, 0});

            // Assert
            client.expectClosed();
            assertThat(next().reason()).isEqualTo(PROTOCOL_ERROR);
        }
    }

    @Test
    void the_listener_hears_of_a_connection_that_the_broker_failed_to_handle() throws Exception {
        // Arrange: the broker fails to handle every PINGREQ.
        BrokerServer.Builder failing = BrokerServer.builder()
                .listener(listener)
                .beforeHandle(packet -> {
                    if (packet instanceof Packet.PingReq) {
                        throw new IllegalStateException("injected failure");
                    }
                });
        try (BrokerServer sut = failing.start(loopback());
                RawSession client = connected(sut.port(), "robot")) {
            assertThat(next().reason()).isNull();

            // Act
            client.send(new Packet.PingReq());

            // Assert
            client.expectClosed();
            assertThat(next().reason()).isEqualTo(BROKER_ERROR);
        }
    }

    @Test
    void the_listener_hears_nothing_of_a_client_that_the_broker_refuses() throws Exception {
        // Arrange
        Authenticator onlyRobots = request -> request.clientId().startsWith("robot")
                ? Authenticator.Decision.accept(Authenticator.Scope.all())
                : Authenticator.Decision.refuseNotAuthorized();
        BrokerServer.Builder authenticated = BrokerServer.builder()
                .listener(listener)
                .authenticator(onlyRobots);
        try (BrokerServer sut = authenticated.start(loopback());
                RawSession intruder = RawSession.open(sut.port())) {
            // Act
            intruder.send(connect("intruder", true, 0));
            assertThat(intruder.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(ReasonCode.NOT_AUTHORIZED);

            // Assert: the next notice is that of a client that connects later.
            try (RawSession witness = connected(sut.port(), "robot")) {
                assertThat(next().connection().clientId()).isEqualTo("robot");
                witness.ping();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    @SuppressWarnings("try") // The test closes the client early; closing it again does nothing.
    void the_listener_hears_nothing_of_a_client_that_a_shutdown_refuses_on_its_way_in(
            MqttVersion version
    ) throws Exception {
        // Arrange: the acceptance of the client waits just before its session takes the
        // connection, so that the shutdown comes to its end first and refuses the client.
        var attaching = new CountDownLatch(1);
        var attach = new CountDownLatch(1);
        var left = new CountDownLatch(1);
        BrokerServer.Builder waitingToAttach = BrokerServer.builder()
                .listener(listener)
                .beforeAttach(_ -> {
                    attaching.countDown();
                    try {
                        // The wait has a limit, so that a test that fails cannot hold the actor
                        // for good; whether it was released or the limit came makes no
                        // difference here.
                        boolean _ = attach.await(WAIT.toSeconds(), SECONDS);
                    } catch (InterruptedException _) {
                        Thread.currentThread().interrupt();
                    }
                })
                .afterLeave(_ -> left.countDown());
        try (BrokerServer sut = waitingToAttach.start(loopback());
                RawSession late = RawSession.open(sut.port(), version)) {
            late.send(switch (version) {
                case V3_1_1 -> connect("late", true, 0);
                case V5 -> connectV5("late", true, 0, 0);
            });
            assertThat(attaching.await(WAIT.toSeconds(), SECONDS)).isTrue();
            // The shutdown cannot settle the session while its acceptance waits, so it comes to
            // its end at its deadline.
            var delivered = new CompletableFuture<Boolean>();
            Thread.ofPlatform().start(
                    () -> delivered.complete(sut.shutdown(Duration.ofMillis(100)))
            );
            assertThat(late.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(ReasonCode.SERVER_UNAVAILABLE);

            // Act
            attach.countDown();

            // Assert: the session lets the connection go, and the listener heard nothing of it.
            assertThat(left.await(WAIT.toSeconds(), SECONDS)).isTrue();
            assertThat(heard).isEmpty();
            late.expectClosed();
            late.close();
            assertThat(delivered).succeedsWithin(WAIT).isEqualTo(false);
        } finally {
            attach.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // The test closes the broker itself; closing it again does nothing.
    void the_listener_hears_nothing_of_the_connections_that_closing_the_broker_ends()
            throws Exception {
        try (BrokerServer sut = broker(); RawSession client = connected(sut.port(), "robot")) {
            // Arrange
            assertThat(next().reason()).isNull();

            // Act
            sut.close();

            // Assert: the broker calls its listener no more once it has ended.
            assertThat(sut.terminated()).succeedsWithin(WAIT);
            assertThat(heard).isEmpty();
            client.expectClosed();
        }
    }

    @Test
    @SuppressWarnings("try") // The client reaches the broker by its name alone.
    void the_listener_hears_of_a_client_in_the_same_jvm_without_an_address() throws Exception {
        try (BrokerServer sut = BrokerServer.builder()
                .name("connection-notice-test")
                .listener(listener)
                .start(loopback());
                MqttClient client = MqttClient.builder()
                        .server(URI.create("inproc://connection-notice-test"))
                        .clientId("application")
                        .build()) {
            // Act
            client.connect().get(5, SECONDS);
            Heard connected = next();
            client.disconnect().get(5, SECONDS);
            Heard disconnected = next();

            // Assert
            assertThat(connected.connection().clientId()).isEqualTo("application");
            assertThat(connected.connection().remoteAddress()).isEmpty();
            assertThat(disconnected.reason()).isEqualTo(CLIENT_DISCONNECT);
        }
    }

    private BrokerServer broker() throws Exception {
        return BrokerServer.builder().listener(listener).start(loopback());
    }

    /// The next call of the listener, which must come within a few seconds.
    private Heard next() throws InterruptedException {
        Heard next = heard.poll(WAIT.toSeconds(), SECONDS);
        assertThat(next).as("a call of the listener").isNotNull();
        return next;
    }

    /// A client of `version` connected as `clientId` with a session that ends with its
    /// connection.
    private static RawSession connectedOf(int port, String clientId, MqttVersion version)
            throws Exception {
        return switch (version) {
            case V3_1_1 -> connected(port, clientId);
            case V5 -> connectedV5(port, clientId);
        };
    }

    /// A CONNECT of `version` for `clientId` with the user name `username` and no password, which
    /// starts a session that ends with its connection.
    private static Packet.Connect connectAs(String clientId, MqttVersion version, String username) {
        return new Packet.Connect(
                clientId,
                version,
                true,
                0,
                null,
                username,
                null,
                Properties.EMPTY
        );
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    /// One call of the listener: that `connection` connected, if `reason` is null, or that it
    /// ended for `reason`.
    private record Heard(BrokerListener.Connection connection, @Nullable DisconnectReason reason) {
    }
}
