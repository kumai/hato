package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.Objects.requireNonNull;

/// [AcceptanceTest] against the embedded broker.
/// [AcceptanceTest] over an in-process connection to the embedded broker.
class InprocBrokerAcceptanceTest extends AcceptanceTest {

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
        return requireNonNull(broker).uri();
    }

    // Left out: the test breaks the connection through a TCP transport that it wraps.
    @Override
    void a_restarted_client_continues_its_session_from_a_session_file() {
    }

    // Left out: the test breaks the connection through a TCP transport that it wraps.
    @Override
    void a_network_failure_publishes_the_will() {
    }

    // Left out: the test breaks the connection through a TCP transport that it wraps.
    @Override
    void a_lost_connection_reconnects_and_the_session_goes_on() {
    }

    // Left out: the test breaks the connection through a TCP transport that it wraps.
    @Override
    void replacing_an_unknown_broker_session_publishes_no_will() {
    }
}
