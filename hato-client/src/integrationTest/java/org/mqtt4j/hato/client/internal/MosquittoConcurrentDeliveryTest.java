package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Mosquitto;

@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class MosquittoConcurrentDeliveryTest extends ConcurrentDeliveryIntegrationTest {

    private static final GenericContainer<?> broker = Mosquitto.container();

    static boolean dockerIsAvailableOrRequired() {
        return Mosquitto.availableOrRequired();
    }

    @BeforeAll
    static void start() {
        Mosquitto.start(broker);
    }

    @AfterAll
    static void stop() {
        broker.stop();
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(Mosquitto.MQTT_PORT)
        );
    }
}
