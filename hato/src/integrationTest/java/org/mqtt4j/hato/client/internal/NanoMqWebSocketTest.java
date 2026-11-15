package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.NanoMq;

/// [WebSocketRoundTripTest] against the WebSocket listener that NanoMQ opens by default.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class NanoMqWebSocketTest extends WebSocketRoundTripTest {

    private static final GenericContainer<?> broker = NanoMq.container();

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
                + broker.getMappedPort(NanoMq.WEBSOCKET_PORT) + "/mqtt");
    }
}
