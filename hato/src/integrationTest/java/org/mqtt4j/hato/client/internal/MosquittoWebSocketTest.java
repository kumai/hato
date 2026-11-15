package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Mosquitto;

/// [WebSocketRoundTripTest] against Mosquitto's WebSocket listener, which is Mosquitto's own
/// WebSocket implementation.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class MosquittoWebSocketTest extends WebSocketRoundTripTest {

    private static final GenericContainer<?> broker = Mosquitto.container();

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

    // Mosquitto serves MQTT on every path of its WebSocket listener.
    @Override
    URI endpoint() {
        return URI.create("ws://" + broker.getHost() + ":"
                + broker.getMappedPort(Mosquitto.WEBSOCKET_PORT) + "/mqtt");
    }
}
