package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.Objects.requireNonNull;

/// [SessionFileTest] against the embedded broker.
/// [SessionFileTest] over an in-process connection to the embedded broker.
class InprocBrokerSessionFileTest extends SessionFileTest {

    private static @Nullable MqttBroker broker;

    InprocBrokerSessionFileTest(MqttVersion version) {
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
    void a_restarted_client_receives_what_the_broker_stored_while_it_was_stopped() {
    }
}
