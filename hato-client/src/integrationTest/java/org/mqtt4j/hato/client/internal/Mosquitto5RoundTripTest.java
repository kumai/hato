package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/// The client's MQTT 5.0 connections to Mosquitto, and the limits that Mosquitto announces.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class Mosquitto5RoundTripTest extends Mqtt5RoundTripTest {

    private static final GenericContainer<?> broker = Mosquitto.container();

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
    void the_connack_reports_the_limits_that_mosquitto_announces() throws Exception {
        try (MqttClient sut = client("mqtt5-limits", Duration.ZERO)) {
            // Act
            ConnAck ack = sut.connect().get(5, SECONDS);

            // Assert: recorded from the pinned Mosquitto 2.1.2 image, whose only announced limit
            // that ConnAck reports is a maximum packet size of 2,000,000 bytes.
            assertThat(ack).isEqualTo(
                    ConnAck.of(false, 1, Qos.EXACTLY_ONCE, true, true, true, 2_000_000)
            );
            sut.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void publications_beyond_the_receive_maximum_wait_instead_of_being_refused() throws Exception {
        var topic = new Topic("hato/mqtt5/receive-maximum");
        try (MqttClient sut = client("mqtt5-receive-maximum", Duration.ZERO)) {
            // Arrange: Mosquitto takes 20 publications at once and refuses more with Quota
            // exceeded. QoS 2 keeps that room until PUBCOMP, whereas Mosquitto frees it at once
            // for QoS 1, so only a QoS 2 burst runs into the limit.
            sut.connect().get(5, SECONDS);

            // Act: publish all that the client admits at once.
            List<CompletableFuture<PublishResult>> publications = IntStream.range(0, 64)
                    .mapToObj(i -> MqttMessage.of(
                            topic,
                            new byte[] {(byte) i},
                            Qos.EXACTLY_ONCE,
                            false
                    ))
                    .map(sut::publish)
                    .toList();

            // Assert
            for (CompletableFuture<PublishResult> publication : publications) {
                assertThat(publication.get(5, SECONDS)).isEqualTo(PublishResult.of());
            }
        }
    }

    // Mosquitto 2.1.2 reports no matching subscribers in PUBACK, but success in PUBREC.
    @Override
    ReasonCode unmatchedPublicationReason(Qos qos) {
        return qos == Qos.EXACTLY_ONCE ? ReasonCode.SUCCESS : ReasonCode.NO_MATCHING_SUBSCRIBERS;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(Mosquitto.MQTT_PORT)
        );
    }
}
