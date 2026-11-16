package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.Objects.requireNonNull;

/// [Qos2ReceptionIntegrationTest] over an in-process connection to the embedded broker.
class InprocBrokerQos2ReceptionTest extends Qos2ReceptionIntegrationTest {

    private static @Nullable MqttBroker broker;

    InprocBrokerQos2ReceptionTest(MqttVersion version) {
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
    void a_failed_handler_is_retried_after_continuation_before_pubrec() {
    }

    // Left out: the test breaks the connection through a TCP transport that it wraps.
    @Override
    void a_manual_condition_survives_disconnection_without_delivering_the_replay_again() {
    }

    // Left out: the test breaks the connection through a TCP transport that it wraps.
    @Override
    void a_continued_exchange_delivers_the_publication_once(Cut cut) {
    }
}
