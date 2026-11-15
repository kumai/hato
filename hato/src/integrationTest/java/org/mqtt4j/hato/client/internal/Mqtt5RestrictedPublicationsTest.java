package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The limits of [Mqtt5RestrictedTest], and no retained messages and a Receive Maximum of 2,
/// shared by Mosquitto, EMQX, HiveMQ CE, and the embedded broker. RabbitMQ 4.4.0 has a setting
/// for neither.
abstract class Mqtt5RestrictedPublicationsTest extends Mqtt5RestrictedTest {

    @Test
    void a_retained_publication_is_refused_and_the_connection_goes_on() throws Exception {
        assertRefusedWhileTheConnectionGoesOn(
                MqttMessage.of(new Topic(uniqueTopic()), new byte[] {1}, Qos.AT_LEAST_ONCE, true),
                ReasonCode.RETAIN_NOT_SUPPORTED
        );
    }

    @Test
    void publications_beyond_the_receive_maximum_wait_for_room() throws Exception {
        var topic = new Topic(uniqueTopic());
        try (MqttClient sut = client()) {
            // Arrange: the broker takes two QoS 1 publications at once.
            sut.connect().get(5, SECONDS);

            // Act
            List<CompletableFuture<PublishResult>> publications = IntStream.range(0, 20)
                    .mapToObj(i -> MqttMessage.of(
                            topic,
                            new byte[] {(byte) i},
                            Qos.AT_LEAST_ONCE,
                            false
                    ))
                    .map(sut::publish)
                    .toList();

            // Assert: a broker that saw more would end the connection.
            for (CompletableFuture<PublishResult> publication : publications) {
                assertThat(publication).succeedsWithin(Duration.ofSeconds(5));
            }
        }
    }

    @Override
    boolean retainAvailable() {
        return false;
    }
}
