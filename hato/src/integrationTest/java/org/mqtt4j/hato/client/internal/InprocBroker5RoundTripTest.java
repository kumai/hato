package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.Objects.requireNonNull;

/// [Mqtt5ProductRoundTripTest] over an in-process connection to the embedded broker.
class InprocBroker5RoundTripTest extends Mqtt5ProductRoundTripTest {

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

    // The embedded broker reports success whether or not a subscription matches.
    @Override
    ReasonCode unmatchedPublicationReason(Qos qos) {
        return ReasonCode.SUCCESS;
    }

    @Override
    boolean takesTurns(MqttVersion version) {
        return true;
    }

    @Override
    URI endpoint() {
        return requireNonNull(broker).uri();
    }
}
