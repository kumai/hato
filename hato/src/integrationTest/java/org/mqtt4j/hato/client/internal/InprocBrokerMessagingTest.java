package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.Objects.requireNonNull;

/// [MessagingTest] over an in-process connection to the embedded broker.
class InprocBrokerMessagingTest extends MessagingTest {

    private static @Nullable MqttBroker broker;

    InprocBrokerMessagingTest(MqttVersion version) {
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

    @Override
    boolean sharedSubscriptionAvailable() {
        return false;
    }

    // Left out: the test breaks the connection through a TCP transport that it wraps.
    @Override
    void an_established_subscription_resumes_after_explicit_reconnect(
            boolean pull,
            boolean failure
    ) {
    }

    // Left out: the test breaks the connection through a TCP transport that it wraps.
    @Override
    void overload_releases_a_handler_waiting_for_its_own_publication() {
    }

    // Left out: the test breaks the connection through a TCP transport that it wraps.
    @Override
    void a_connection_lost_before_puback_leaves_the_publication_outcome_unknown() {
    }
}
