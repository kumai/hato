package org.mqtt4j.hato.broker.internal;

import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.internal.protocol.Packet;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.ReasonCode.SERVER_SHUTTING_DOWN;
import static org.mqtt4j.hato.ReasonCode.SERVER_UNAVAILABLE;
import static org.mqtt4j.hato.ReasonCode.SUCCESS;
import static org.mqtt4j.hato.broker.internal.RawSession.connect;
import static org.mqtt4j.hato.broker.internal.RawSession.connectV5;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.internal.protocol.Properties.EMPTY;

/// How a broker shuts down: it takes no further connection, delivers to its clients what waits
/// for them, also what they sent just before, then tells them and closes; and what cuts a
/// shutdown short.
@SuppressWarnings("try") // Tests close robots and brokers themselves; closing again does nothing.
class BrokerShutdownTest {

    private static final Duration WAIT = Duration.ofSeconds(5);

    /// What the application publishes last, from its own client over TCP or in the same JVM, at
    /// a QoS, to a robot of an MQTT version.
    record Scenario(boolean inProcess, Qos qos, MqttVersion robot) {}

    static Stream<Scenario> scenarios() {
        return Stream.of(false, true).flatMap(inProcess -> Stream.of(AT_MOST_ONCE, AT_LEAST_ONCE)
                .flatMap(qos -> Stream.of(MqttVersion.V3_1_1, MqttVersion.V5)
                        .map(robot -> new Scenario(inProcess, qos, robot))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("scenarios")
    void a_robot_receives_what_the_application_published_just_before_the_shutdown(
            Scenario scenario
    ) throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession robot = subscribed(sut.port(), scenario.robot())) {
            // Arrange: the application publishes and disconnects its client.
            publishAndLeave(sut, scenario);

            // Act: at once.
            CompletableFuture<Boolean> shutdown = shutdownInBackground(sut, WAIT);

            // Assert: the robot receives the order, and then the end.
            Packet.Publish order = robot.expect(Packet.Publish.class);
            assertThat(order.payload()).isEqualTo("go".getBytes(UTF_8));
            if (order.qos() == AT_LEAST_ONCE) {
                robot.send(new Packet.PubAck(order.packetId(), SUCCESS, EMPTY));
            }
            expectEnd(robot, scenario.robot());
            robot.close();
            assertThat(shutdown).succeedsWithin(WAIT).isEqualTo(true);
        }
    }

    @Test
    void a_shutdown_waits_until_the_robot_acknowledges_what_it_received() throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession robot = subscribed(sut.port(), MqttVersion.V5)) {
            // Arrange: a delivery that the robot has yet to acknowledge.
            Packet.Publish order = orderReceived(sut, robot);

            // Act
            CompletableFuture<Boolean> shutdown = shutdownInBackground(sut, WAIT);

            // Assert: the broker waits for the acknowledgement, serving the robot as before, and
            // then tells the robot of its end.
            await().during(Duration.ofMillis(200)).until(() -> !shutdown.isDone());
            robot.ping();
            robot.send(new Packet.PubAck(order.packetId(), SUCCESS, EMPTY));
            expectEnd(robot, MqttVersion.V5);
            robot.close();
            assertThat(shutdown).succeedsWithin(WAIT).isEqualTo(true);
        }
    }

    @Test
    void a_shutdown_that_runs_out_of_time_closes_and_says_so() throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession robot = subscribed(sut.port(), MqttVersion.V5)) {
            // Arrange: a delivery that the robot never acknowledges.
            orderReceived(sut, robot);

            // Act
            boolean delivered = sut.shutdown(Duration.ofMillis(200));

            // Assert
            assertThat(delivered).isFalse();
            expectEnd(robot, MqttVersion.V5);
        }
    }

    @Test
    void a_shutdown_takes_no_further_connection_and_frees_its_port_and_name_at_once()
            throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession robot = subscribed(sut.port(), MqttVersion.V5)) {
            // Arrange: a shutdown that waits for an acknowledgement.
            Packet.Publish order = orderReceived(sut, robot);
            CompletableFuture<Boolean> shutdown = shutdownInBackground(sut, WAIT);

            // Act & Assert: the port refuses connections, and the name is free, while the broker
            // serves its robot.
            awaitRefused(sut.port());
            try (MqttClient late = MqttClient.builder()
                    .server(sut.uri())
                    .reconnectPolicy(ReconnectPolicy.disabled())
                    .build()) {
                assertThatThrownBy(() -> late.connect().get(5, SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .cause()
                        .isInstanceOf(MqttConnectException.class);
            }
            try (MqttBroker next = MqttBroker.builder().name(sut.name()).start()) {
                assertThat(next.name()).isEqualTo(sut.name());
            }
            robot.send(new Packet.PubAck(order.packetId(), SUCCESS, EMPTY));
            assertThat(shutdown).succeedsWithin(WAIT).isEqualTo(true);
        }
    }

    @Test
    void a_client_that_the_broker_accepts_during_a_shutdown_is_refused() throws Exception {
        // Arrange: the decision on a late client waits until the test lets it go.
        var deciding = new CountDownLatch(1);
        var decide = new CountDownLatch(1);
        Authenticator holdingLateClients = request -> {
            if (request.clientId().equals("late")) {
                deciding.countDown();
                decide.await();
            }
            return Authenticator.Decision.accept(Authenticator.Scope.all());
        };
        try (MqttBroker sut = MqttBroker.builder().authenticator(holdingLateClients).start();
                RawSession robot = subscribed(sut.port(), MqttVersion.V5);
                RawSession late = RawSession.open(sut.port())) {
            Packet.Publish order = orderReceived(sut, robot);
            late.send(connect("late", true, 0));
            assertThat(deciding.await(5, SECONDS)).isTrue();
            CompletableFuture<Boolean> shutdown = shutdownInBackground(sut, WAIT);
            awaitRefused(sut.port());

            // Act
            decide.countDown();

            // Assert
            assertThat(late.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(SERVER_UNAVAILABLE);
            robot.send(new Packet.PubAck(order.packetId(), SUCCESS, EMPTY));
            assertThat(shutdown).succeedsWithin(WAIT).isEqualTo(true);
        } finally {
            decide.countDown();
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_client_whose_authentication_outlasts_the_shutdown_is_refused(MqttVersion version)
            throws Exception {
        // Arrange: the decision on the client waits until the test lets it go, and no other
        // client keeps the shutdown from coming to its end.
        var deciding = new CountDownLatch(1);
        var decide = new CountDownLatch(1);
        Authenticator holding = _ -> {
            deciding.countDown();
            decide.await();
            return Authenticator.Decision.accept(Authenticator.Scope.all());
        };
        try (MqttBroker sut = MqttBroker.builder().authenticator(holding).start();
                RawSession late = RawSession.open(sut.port(), version)) {
            late.send(switch (version) {
                case V3_1_1 -> connect("late", true, 0);
                case V5 -> connectV5("late", true, 0, 0);
            });
            assertThat(deciding.await(5, SECONDS)).isTrue();

            // Act
            CompletableFuture<Boolean> shutdown = shutdownInBackground(sut, WAIT);

            // Assert
            assertThat(late.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(SERVER_UNAVAILABLE);
            late.expectClosed();
            late.close();
            assertThat(shutdown).succeedsWithin(WAIT).isEqualTo(true);
        } finally {
            decide.countDown();
        }
    }

    @Test
    void closing_the_broker_cuts_a_shutdown_short() throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession robot = subscribed(sut.port(), MqttVersion.V5)) {
            // Arrange: a shutdown that would wait for an acknowledgement.
            orderReceived(sut, robot);
            CompletableFuture<Boolean> shutdown = shutdownInBackground(sut, Duration.ofMinutes(1));
            awaitRefused(sut.port());

            // Act
            sut.close();

            // Assert
            assertThat(shutdown).succeedsWithin(WAIT).isEqualTo(false);
            robot.expectClosed();
        }
    }

    @Test
    void an_interrupt_cuts_a_shutdown_short_and_stays_with_the_thread() throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession robot = subscribed(sut.port(), MqttVersion.V5)) {
            // Arrange: a shutdown on a thread of its own that would wait for an acknowledgement.
            orderReceived(sut, robot);
            var delivered = new CompletableFuture<Boolean>();
            var interrupted = new AtomicBoolean();
            Thread shuttingDown = Thread.ofPlatform().start(() -> {
                delivered.complete(sut.shutdown(Duration.ofMinutes(1)));
                interrupted.set(Thread.currentThread().isInterrupted());
            });
            awaitRefused(sut.port());

            // Act
            shuttingDown.interrupt();

            // Assert
            assertThat(delivered).succeedsWithin(WAIT).isEqualTo(false);
            shuttingDown.join();
            assertThat(interrupted).isTrue();
            expectEnd(robot, MqttVersion.V5);
        }
    }

    @Test
    void a_shutdown_publishes_no_will_of_the_connections_that_it_closes() throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession robot = connectedV5(sut.port(), "robot");
                RawSession leaving = RawSession.open(sut.port())) {
            // Arrange: a client with a Will that the robot subscribes to.
            robot.subscribe("wills", AT_LEAST_ONCE);
            leaving.send(new Packet.Connect(
                    "leaving",
                    MqttVersion.V3_1_1,
                    true,
                    0,
                    new Packet.Will(
                            new Topic("wills"),
                            "gone".getBytes(UTF_8),
                            AT_LEAST_ONCE,
                            false,
                            EMPTY
                    ),
                    null,
                    null,
                    EMPTY
            ));
            leaving.expect(Packet.ConnAck.class);

            // Act
            CompletableFuture<Boolean> shutdown = shutdownInBackground(sut, WAIT);

            // Assert: the next packet that the robot receives is the end.
            expectEnd(robot, MqttVersion.V5);
            leaving.expectClosed();
            robot.close();
            leaving.close();
            assertThat(shutdown).succeedsWithin(WAIT).isEqualTo(true);
        }
    }

    @Test
    void a_later_call_returns_the_result_of_the_shutdown_and_one_after_close_returns_false()
            throws Exception {
        try (MqttBroker shutDown = MqttBroker.start(); MqttBroker closed = MqttBroker.start()) {
            // Arrange
            assertThat(shutDown.shutdown(WAIT)).isTrue();
            closed.close();

            // Act & Assert
            assertThat(shutDown.shutdown(WAIT)).isTrue();
            assertThat(closed.shutdown(WAIT)).isFalse();
        }
    }

    @Test
    void a_negative_timeout_is_refused() throws Exception {
        try (MqttBroker sut = MqttBroker.start()) {
            assertThatThrownBy(() -> sut.shutdown(Duration.ofMillis(-1)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /// A robot of `version`, subscribed to orders at QoS 1.
    private static RawSession subscribed(int port, MqttVersion version) throws Exception {
        RawSession robot = switch (version) {
            case V3_1_1 -> connected(port, "robot");
            case V5 -> connectedV5(port, "robot");
        };
        robot.subscribe("orders", AT_LEAST_ONCE);
        return robot;
    }

    /// Has the application's client publish an order at the scenario's QoS and disconnect.
    private static void publishAndLeave(MqttBroker broker, Scenario scenario) throws Exception {
        if (scenario.inProcess()) {
            try (MqttClient application = MqttClient.builder()
                    .server(broker.uri())
                    .clientId("application")
                    .build()) {
                application.connect().get(5, SECONDS);
                application.publish(MqttMessage.of(
                        new Topic("orders"),
                        "go".getBytes(UTF_8),
                        scenario.qos(),
                        false
                )).get(5, SECONDS);
                application.disconnect().get(5, SECONDS);
            }
        } else {
            try (RawSession application = connected(broker.port(), "application")) {
                int packetId = (scenario.qos() == AT_MOST_ONCE) ? 0 : 1;
                application.publish(publication("orders", "go", scenario.qos(), packetId));
                application.send(new Packet.Disconnect(SUCCESS, EMPTY));
            }
        }
    }

    /// Has another client publish an order at QoS 1, which `robot` receives and has yet to
    /// acknowledge.
    private static Packet.Publish orderReceived(MqttBroker broker, RawSession robot)
            throws Exception {
        try (RawSession application = connected(broker.port(), "application")) {
            application.publish(publication("orders", "go", AT_LEAST_ONCE, 1));
            application.disconnect();
        }
        return robot.expect(Packet.Publish.class);
    }

    /// Shuts `broker` down on a thread of its own, within `timeout`.
    private static CompletableFuture<Boolean> shutdownInBackground(
            MqttBroker broker,
            Duration timeout
    ) {
        var delivered = new CompletableFuture<Boolean>();
        Thread.ofPlatform().start(() -> delivered.complete(broker.shutdown(timeout)));
        return delivered;
    }

    /// Checks that `robot` learns of the end: an MQTT 5.0 robot with DISCONNECT first.
    private static void expectEnd(RawSession robot, MqttVersion version) throws Exception {
        if (version == MqttVersion.V5) {
            assertThat(robot.expect(Packet.Disconnect.class).reasonCode())
                    .isEqualTo(SERVER_SHUTTING_DOWN);
        }
        robot.expectClosed();
    }

    /// Waits until the broker on `port` refuses connections, as it does once it shuts down.
    private static void awaitRefused(int port) {
        await().atMost(WAIT).untilAsserted(() -> assertThatThrownBy(() -> {
            try (var socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
                socket.getInputStream();
            }
        }).isInstanceOf(ConnectException.class));
    }
}
