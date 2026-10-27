package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.NanoMq;

/// The Will of MQTT 5.0 sessions on NanoMQ.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class NanoMq5WillTest extends Mqtt5WillTest {

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

    // Recorded for NanoMQ 0.25.7, which ends a session that began with Clean Start together with
    // its connection, and publishes the Will then.
    @Override
    boolean endsTheDelayWithTheSession() {
        return true;
    }

    // Recorded for NanoMQ 0.25.7, which has published the Will as the replaced session ended with
    // its connection.
    @Override
    boolean publishesTheWillOfAReplacedSession() {
        return true;
    }

    // Recorded for NanoMQ 0.25.7.
    @Override
    boolean keepsSessionsThatCleanStartBegan() {
        return false;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(NanoMq.MQTT_PORT)
        );
    }
}
