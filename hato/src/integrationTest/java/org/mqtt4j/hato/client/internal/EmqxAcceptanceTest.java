package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Emqx;

/// [AcceptanceTest] against EMQX.
@EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
class EmqxAcceptanceTest extends AcceptanceTest {

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

    // Under load, EMQX refuses a connection with Server unavailable while it still releases the
    // client identifier of the connection before it, even after that connection has closed, as
    // a CI run showed.
    @Override
    boolean mayRefuseWhileReleasingClientIdentifiers() {
        return true;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(Emqx.MQTT_PORT)
        );
    }
}
