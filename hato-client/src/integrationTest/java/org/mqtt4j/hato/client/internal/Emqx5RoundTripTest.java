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
import org.mqtt4j.hato.testsupport.Emqx;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/// The client's MQTT 5.0 connections to EMQX, and the limits that EMQX announces.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class Emqx5RoundTripTest extends Mqtt5ProductRoundTripTest {

    private static final GenericContainer<?> broker = Emqx.container();

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
    void the_connack_reports_the_limits_that_emqx_announces() throws Exception {
        try (MqttClient sut = client("mqtt5-limits", Duration.ZERO)) {
            // Act
            ConnAck ack = sut.connect().get(5, SECONDS);

            // Assert: recorded from the pinned EMQX 6.3.2 image, whose only announced limit that
            // ConnAck reports is a maximum packet size of 1,048,576 bytes.
            assertThat(ack).isEqualTo(
                    ConnAck.of(false, 1, Qos.EXACTLY_ONCE, true, true, true, 1_048_576)
            );
            sut.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void publications_beyond_the_receive_maximum_wait_instead_of_being_refused() throws Exception {
        var topic = new Topic("hato/mqtt5/receive-maximum");
        try (MqttClient sut = client("mqtt5-receive-maximum", Duration.ZERO)) {
            // Arrange: EMQX announces a Receive Maximum of 32, so the client sends 32 QoS 2
            // publications at most before their PUBCOMP. EMQX itself ends a connection only
            // when more than 100 await release, more than the client admits at once, so this
            // burst runs the waiting publications against EMQX without risking a refusal.
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

            // Assert: no subscription matches the topic.
            PublishResult unmatched = PublishResult.of(
                    unmatchedPublicationReason(Qos.EXACTLY_ONCE)
            );
            for (CompletableFuture<PublishResult> publication : publications) {
                assertThat(publication.get(5, SECONDS)).isEqualTo(unmatched);
            }
        }
    }

    // EMQX 6.3.2 reports no matching subscribers in both PUBACK and PUBREC.
    @Override
    ReasonCode unmatchedPublicationReason(Qos qos) {
        return ReasonCode.NO_MATCHING_SUBSCRIBERS;
    }

    @Override
    boolean takesTurns(MqttVersion version) {
        return true;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(Emqx.MQTT_PORT)
        );
    }
}
