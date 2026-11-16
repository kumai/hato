package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.Objects.requireNonNull;

/// [ConnectionTest] over an in-process connection to the embedded broker.
class InprocBrokerConnectionTest extends ConnectionTest {

    private static @Nullable MqttBroker broker;

    InprocBrokerConnectionTest(MqttVersion version) {
        super(version);
    }

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
        return requireNonNull(broker).uri();
    }

    // Left out: the test breaks the connection through a TCP transport that it wraps.
    @Override
    void delayed_pingresp_closes_with_a_timeout_and_discards_delayed_input() {
    }
}
