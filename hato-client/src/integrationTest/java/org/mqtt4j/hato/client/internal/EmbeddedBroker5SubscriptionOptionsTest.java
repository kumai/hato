package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.Objects.requireNonNull;

/// The MQTT 5.0 subscription options on the embedded broker.
class EmbeddedBroker5SubscriptionOptionsTest extends Mqtt5SubscriptionOptionsTest {

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
