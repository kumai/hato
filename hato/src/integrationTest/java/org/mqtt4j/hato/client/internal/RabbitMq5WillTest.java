package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.RabbitMq;

/// The Will of MQTT 5.0 sessions on RabbitMQ.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class RabbitMq5WillTest extends Mqtt5WillTest {

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

    // Recorded for RabbitMQ 4.4.0.
    @Override
    boolean endsTheDelayWithTheSession() {
        return true;
    }

    // Recorded for RabbitMQ 4.4.0, which discards the delayed Will when a connection with Clean
    // Start set replaces the session.
    @Override
    boolean publishesTheWillOfAReplacedSession() {
        return false;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(RabbitMq.MQTT_PORT)
        );
    }
}
