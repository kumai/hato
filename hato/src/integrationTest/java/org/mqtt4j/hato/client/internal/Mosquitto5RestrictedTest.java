package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Mosquitto;

/// The client against Mosquitto configured to announce smaller MQTT 5.0 limits.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class Mosquitto5RestrictedTest extends Mqtt5RestrictedPublicationsTest {

    private static final GenericContainer<?> broker = Mosquitto.restrictedContainer();

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

    // Recorded for Mosquitto 2.1.2, which has no setting that withdraws either kind.
    @Override
    boolean wildcardSubscriptionAvailable() {
        return true;
    }

    @Override
    boolean sharedSubscriptionAvailable() {
        return true;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(Mosquitto.MQTT_PORT)
        );
    }
}
