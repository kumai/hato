package org.mqtt4j.mqtj.client.internal;

import java.io.IOException;
import java.net.URI;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.mqtt4j.mqtj.broker.MqttBroker;

import static java.util.Objects.requireNonNull;

class EmbeddedBrokerConnectionTest extends ConnectionTest {

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

    @Override
    URI endpoint() {
        return URI.create("mqtt://localhost:" + requireNonNull(broker).port());
    }
}
