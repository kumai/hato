package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.broker.BrokerListener;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.internal.protocol.Packet;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;

/// How the application closes the connection of a client, by its identifier or as the listener
/// or the list of clients reported it, and what the client keeps of its session.
class BrokerDisconnectTest {

    // The connections that the listener heard of, in the order of its calls.
    private final BlockingQueue<BrokerListener.Connection> connections =
            new LinkedBlockingQueue<>();

    @Test
    void the_application_closes_the_connection_of_a_client_which_learns_why() throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession client = connectedV5(sut.port(), "robot")) {
            // Act
            boolean disconnected = sut.disconnect("robot");

            // Assert
            assertThat(disconnected).isTrue();
            assertThat(client.expect(Packet.Disconnect.class).reasonCode())
                    .isEqualTo(ReasonCode.ADMINISTRATIVE_ACTION);
            client.expectClosed();
        }
    }

    @Test
    void disconnecting_a_client_without_a_connection_does_nothing() throws Exception {
        try (MqttBroker sut = MqttBroker.start()) {
            // Arrange: a session that waits for its client, which has gone.
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.disconnect();
            }

            // Act & Assert
            assertThat(sut.disconnect("away")).isFalse();
            assertThat(sut.disconnect("unknown")).isFalse();
        }
    }

    @Test
    void a_connection_that_the_application_made_up_closes_nothing() throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession client = connected(sut.port(), "robot")) {
            // Arrange: objects of the application's own, as a listener's test makes, one that
            // names the client and one that names none.
            BrokerListener.Connection named = madeUp("robot");
            BrokerListener.Connection unnamed = madeUp(null);

            // Act & Assert
            assertThat(sut.disconnect(named)).isFalse();
            assertThat(sut.disconnect(unnamed)).isFalse();
            client.ping();
        }
    }

    @Test
    void a_disconnected_client_finds_its_session_when_it_comes_back() throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a session that outlives its connection, and a publication that waits for
            // it once the application has closed that connection.
            try (RawSession robot = connected(sut.port(), "robot", false, false)) {
                robot.subscribe("orders", AT_LEAST_ONCE);
                assertThat(sut.disconnect("robot")).isTrue();
                robot.expectClosed();
            }
            publisher.publish("orders", "go", AT_LEAST_ONCE, 1);

            // Act
            try (RawSession robot = connected(sut.port(), "robot", false, true)) {
                // Assert
                assertThat(robot.expect(Packet.Publish.class).topic().name()).isEqualTo("orders");
            }
        }
    }

    @Test
    void the_application_closes_a_connection_that_the_listener_heard_of() throws Exception {
        try (MqttBroker sut = brokerHearingConnections();
                RawSession client = connectedV5(sut.port(), "robot")) {
            // Arrange
            BrokerListener.Connection connection = nextConnection();

            // Act
            boolean disconnected = sut.disconnect(connection);

            // Assert
            assertThat(disconnected).isTrue();
            assertThat(client.expect(Packet.Disconnect.class).reasonCode())
                    .isEqualTo(ReasonCode.ADMINISTRATIVE_ACTION);
            client.expectClosed();
        }
    }

    @Test
    void the_application_closes_a_connection_that_the_list_of_clients_showed() throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession client = connectedV5(sut.port(), "robot")) {
            // Arrange: a broker without a listener, whose list shows the connection.
            await().until(() -> sut.clients().size() == 1);
            BrokerListener.Connection connection = sut.clients().getFirst().connection();

            // Act
            boolean disconnected = sut.disconnect(connection);

            // Assert
            assertThat(disconnected).isTrue();
            assertThat(client.expect(Packet.Disconnect.class).reasonCode())
                    .isEqualTo(ReasonCode.ADMINISTRATIVE_ACTION);
            client.expectClosed();
        }
    }

    @Test
    void closing_a_connection_that_the_listener_heard_of_leaves_a_later_one_open()
            throws Exception {
        try (MqttBroker sut = brokerHearingConnections();
                RawSession first = connected(sut.port(), "robot")) {
            // Arrange: the client connects again, and the second connection takes the session
            // over, before the application closes the first one.
            BrokerListener.Connection firstConnection = nextConnection();
            try (RawSession second = connected(sut.port(), "robot")) {
                first.expectClosed();

                // Act
                boolean disconnected = sut.disconnect(firstConnection);

                // Assert
                assertThat(disconnected).isFalse();
                second.ping();
            }
        }
    }

    private MqttBroker brokerHearingConnections() throws IOException {
        return MqttBroker.builder()
                .listener(new BrokerListener() {
                    @Override
                    public void onConnected(Connection connection) {
                        connections.add(connection);
                    }
                })
                .start();
    }

    /// The next connection that the listener heard of, which must come within a few seconds.
    private BrokerListener.Connection nextConnection() throws InterruptedException {
        BrokerListener.Connection next = connections.poll(5, SECONDS);
        assertThat(next).as("a connection that the listener heard of").isNotNull();
        return next;
    }

    // A connection of the application's own that names `clientId`, which may be null as a
    // careless test makes it.
    private static BrokerListener.Connection madeUp(@Nullable String clientId) {
        return new BrokerListener.Connection() {

            // Breaks the contract on purpose for null, as a careless test may.
            @SuppressWarnings("NullAway")
            @Override
            public String clientId() {
                return clientId;
            }

            @Override
            public Optional<InetSocketAddress> remoteAddress() {
                return Optional.empty();
            }

            @Override
            public MqttVersion version() {
                return MqttVersion.V3_1_1;
            }
        };
    }
}
