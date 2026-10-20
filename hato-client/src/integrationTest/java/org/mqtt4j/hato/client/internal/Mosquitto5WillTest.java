package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Mosquitto;

/// The Will of MQTT 5.0 sessions on Mosquitto.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class Mosquitto5WillTest extends Mqtt5WillTest {

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

    // Recorded for Mosquitto 2.1.2, which keeps a session without expiry until its delayed Will
    // is published.
    @Override
    boolean endsTheDelayWithTheSession() {
        return false;
    }

    // Recorded for Mosquitto 2.1.2, which discards the delayed Will when a connection with
    // Clean Start set replaces the session.
    @Override
    boolean publishesTheWillOfAReplacedSession() {
        return false;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(Mosquitto.MQTT_PORT)
        );
    }
}
