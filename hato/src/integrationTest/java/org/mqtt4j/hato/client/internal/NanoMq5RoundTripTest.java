package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.NanoMq;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/// The client's MQTT 5.0 connections to NanoMQ, and the limits that NanoMQ announces.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class NanoMq5RoundTripTest extends Mqtt5ProductRoundTripTest {

    private static final GenericContainer<?> broker = NanoMq.container();

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @BeforeAll
    static void start() {
        Docker.start(broker);
    }

    @AfterAll
    static void stop() {
        broker.stop();
    }

    @Test
    void the_connack_reports_the_limits_that_nanomq_announces() throws Exception {
        try (MqttClient sut = client("mqtt5-limits", Duration.ZERO)) {
            // Act
            ConnAck ack = sut.connect().get(5, SECONDS);

            // Assert: recorded from the pinned NanoMQ 0.25.7 image, which announces the maximum
            // packet size of 16 MiB that the client announced for itself, in place of its own.
            assertThat(ack).isEqualTo(ConnAck.of(false, 1).withMaximumPacketSize(16_777_216));
            sut.disconnect().get(5, SECONDS);
        }
    }

    // NanoMQ 0.25.7 reports success whether or not a subscription matches.
    @Override
    ReasonCode unmatchedPublicationReason(Qos qos) {
        return ReasonCode.SUCCESS;
    }

    // NanoMQ 0.25.7 closes a taken over connection without a DISCONNECT.
    @Override
    boolean reportsTakeover() {
        return false;
    }

    // NanoMQ 0.25.7 ends a session that began with Clean Start together with its connection.
    @Override
    boolean keepsSessionsThatCleanStartBegan() {
        return false;
    }

    @Override
    boolean takesTurns(MqttVersion version) {
        return true;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(NanoMq.MQTT_PORT)
        );
    }
}
