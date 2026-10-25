package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.Objects.requireNonNull;

/// The Will of MQTT 5.0 sessions on the embedded broker.
class EmbeddedBroker5WillTest extends Mqtt5WillTest {

    private static @Nullable MqttBroker broker;

    @BeforeAll
    static void start() throws IOException {
        broker = MqttBroker.start();
    }

    @AfterAll
    static void stop() {
        if (broker != null) {
            broker.close();
        }
    }

    // The session ends with the connection, which comes before the delay (MQTT 5.0 section
    // 3.1.3.2.2).
    @Override
    boolean endsTheDelayWithTheSession() {
        return true;
    }

    // The new session ends the old one, whose Will goes out then (MQTT 5.0 sections 3.1.3.2.2
    // and 3.1.4).
    @Override
    boolean publishesTheWillOfAReplacedSession() {
        return true;
    }

    @Override
    URI endpoint() {
        return URI.create("mqtt://localhost:" + requireNonNull(broker).port());
    }
}
