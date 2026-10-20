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
import org.mqtt4j.hato.testsupport.HiveMq;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/// The client's MQTT 5.0 connections to HiveMQ CE, and the limits that HiveMQ CE announces.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class HiveMq5RoundTripTest extends Mqtt5RoundTripTest {

    private static final GenericContainer<?> broker = HiveMq.container();

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
    void the_connack_reports_the_limits_that_hivemq_announces() throws Exception {
        try (MqttClient sut = client("mqtt5-limits", Duration.ZERO)) {
            // Act
            ConnAck ack = sut.connect().get(5, SECONDS);

            // Assert: recorded from the pinned HiveMQ CE 2026.5 image, which announces only a
            // Receive Maximum and a Topic Alias Maximum. ConnAck reports neither, so it reports
            // what MQTT 5.0 assumes when a broker announces nothing.
            assertThat(ack).isEqualTo(ConnAck.of(false, 1));
            sut.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void publications_beyond_the_receive_maximum_wait_instead_of_being_refused() throws Exception {
        var topic = new Topic("hato/mqtt5/receive-maximum");
        try (MqttClient sut = client("mqtt5-receive-maximum", Duration.ZERO)) {
            // Arrange: HiveMQ CE takes 10 QoS 2 publications before their PUBCOMP and ends the
            // connection with Receive Maximum exceeded when an 11th arrives.
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

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(HiveMq.MQTT_PORT)
        );
    }
}
