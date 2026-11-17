package org.mqtt4j.hato.broker.internal;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.internal.protocol.Packet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;

/// How the application closes the connection of a client by its identifier, and what the
/// client keeps of its session.
class BrokerDisconnectTest {

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
}
