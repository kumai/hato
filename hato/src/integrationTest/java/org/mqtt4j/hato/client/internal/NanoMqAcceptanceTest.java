package org.mqtt4j.hato.client.internal;

import java.net.URI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.NanoMq;

/// [AcceptanceTest] against NanoMQ.
@EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
class NanoMqAcceptanceTest extends AcceptanceTest {

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

    // NanoMQ 0.25.7 resumes the subscriptions of a persistent session, but drops the messages
    // published while the client was away.
    @Override
    boolean keepsMessagesForAbsentClients() {
        return false;
    }

    // NanoMQ 0.25.7 keeps RETAIN when it forwards a retained QoS 0 message to an established
    // subscription.
    @Override
    boolean clearsRetainOfQos0Forwards() {
        return false;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(NanoMq.MQTT_PORT)
        );
    }
}
