package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.Objects.requireNonNull;

class EmbeddedBrokerQos2ReceptionTest extends Qos2ReceptionIntegrationTest {

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

    @ParameterizedTest
    @EnumSource(Cut.class)
    void a_missing_session_discards_the_old_reception(Cut cut) throws Exception {
        exchangeAcrossDisconnection(cut, false);
    }

    @Override
    URI endpoint() {
        return URI.create("mqtt://localhost:" + requireNonNull(broker).port());
    }
}
