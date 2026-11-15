package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.testkit.BrokerCapabilities;
import org.mqtt4j.hato.testkit.BrokerControls;

import static java.util.Objects.requireNonNull;

/// The client against the embedded broker, which the test kit makes announce smaller MQTT 5.0
/// limits before any client connects.
class EmbeddedBroker5RestrictedTest extends Mqtt5RestrictedSubscriptionsTest {

    private static @Nullable MqttBroker broker;

    @BeforeAll
    static void start() throws IOException {
        var started = MqttBroker.start();
        broker = started;
        BrokerCapabilities restricted = BrokerCapabilities.defaults()
                .withMaximumQos(Qos.AT_LEAST_ONCE)
                .withRetainAvailable(false)
                .withWildcardSubscriptionAvailable(false)
                .withReceiveMaximum(2)
                .withMaximumPacketSize(512)
                .withTopicAliasMaximum(0);
        BrokerControls.of(started).announce(restricted);
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
