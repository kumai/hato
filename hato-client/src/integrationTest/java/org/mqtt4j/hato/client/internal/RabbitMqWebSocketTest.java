package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.RabbitMq;

/// [WebSocketRoundTripTest] against the WebSocket listener of RabbitMQ's Web MQTT plugin.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class RabbitMqWebSocketTest extends WebSocketRoundTripTest {

    private static final GenericContainer<?> broker = RabbitMq.container();

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @BeforeAll
    static void start() {
        Docker.start(broker);
    }

    @AfterAll
    static void stop() {
        broker.stop();
    }

    @Override
    URI endpoint() {
        return URI.create("ws://" + broker.getHost() + ":"
                + broker.getMappedPort(RabbitMq.WEBSOCKET_PORT) + "/ws");
    }

    // RabbitMQ 4.4.0 has no QoS 2.
    @Override
    Qos maximumQos() {
        return Qos.AT_LEAST_ONCE;
    }
}
