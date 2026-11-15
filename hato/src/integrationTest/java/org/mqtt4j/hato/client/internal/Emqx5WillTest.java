package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Emqx;

/// The Will of MQTT 5.0 sessions on EMQX.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class Emqx5WillTest extends Mqtt5WillTest {

    private static final GenericContainer<?> broker = Emqx.container();

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

    // Recorded for EMQX 6.3.2.
    @Override
    boolean endsTheDelayWithTheSession() {
        return true;
    }

    // Recorded for EMQX 6.3.2.
    @Override
    boolean publishesTheWillOfAReplacedSession() {
        return true;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(Emqx.MQTT_PORT)
        );
    }
}
