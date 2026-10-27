package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.RabbitMq;

/// The client against RabbitMQ configured to announce smaller MQTT 5.0 limits.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class RabbitMq5RestrictedTest extends Mqtt5RestrictedTest {

    private static final GenericContainer<?> broker = RabbitMq.restrictedContainer();

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

    // Recorded for RabbitMQ 4.4.0, which has no setting that withdraws retained messages or
    // wildcard subscriptions, and no shared subscriptions.
    @Override
    boolean retainAvailable() {
        return true;
    }

    @Override
    boolean wildcardSubscriptionAvailable() {
        return true;
    }

    @Override
    boolean sharedSubscriptionAvailable() {
        return false;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(RabbitMq.MQTT_PORT)
        );
    }
}
