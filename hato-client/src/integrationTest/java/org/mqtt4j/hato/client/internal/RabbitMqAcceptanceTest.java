package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.RabbitMq;

/// [AcceptanceTest] against RabbitMQ, with its MQTT plugin.
@EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
class RabbitMqAcceptanceTest extends AcceptanceTest {

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

    // RabbitMQ 4.4.0 takes QoS 2 publications at QoS 1, and grants QoS 1 for QoS 2
    // subscriptions.
    @Override
    boolean takesQos2() {
        return false;
    }

    // RabbitMQ 4.4.0 refuses an unknown user with Bad user name or password.
    @Override
    boolean refusesUnknownCredentials() {
        return true;
    }

    // RabbitMQ 4.4.0 sends no retained message to a new subscription with a wildcard.
    @Override
    boolean sendsRetainedToWildcards() {
        return false;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(RabbitMq.MQTT_PORT)
        );
    }
}
