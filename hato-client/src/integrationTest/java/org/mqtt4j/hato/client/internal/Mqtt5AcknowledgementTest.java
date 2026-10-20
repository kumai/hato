package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.endpoint;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.listen;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A raw MQTT 5.0 peer chooses the reason codes of its acknowledgements, which the client
/// receives through the MQTT 5.0 codec.
@Timeout(30)
class Mqtt5AcknowledgementTest {

    @Test
    void a_publication_without_matching_subscribers_succeeds() throws Exception {
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut, MqttVersion.V5)) {
            // Arrange
            CompletableFuture<PublishResult> published = sut.publish(message(Qos.AT_LEAST_ONCE));
            var publication = (Packet.Publish) peer.receive();

            // Act
            peer.send(new Packet.PubAck(
                    publication.packetId(),
                    ReasonCode.NO_MATCHING_SUBSCRIBERS,
                    EMPTY
            ));

            // Assert
            assertThat(published.get(5, SECONDS)).isEqualTo(PublishResult.of());
        }
    }

    @Test
    void a_refused_publication_fails_with_its_reason_code() throws Exception {
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut, MqttVersion.V5)) {
            // Arrange
            CompletableFuture<PublishResult> published = sut.publish(message(Qos.AT_LEAST_ONCE));
            var publication = (Packet.Publish) peer.receive();

            // Act
            peer.send(new Packet.PubAck(
                    publication.packetId(),
                    ReasonCode.QUOTA_EXCEEDED,
                    EMPTY
            ));

            // Assert
            assertThat(refusal(published).reasonCode()).isEqualTo(ReasonCode.QUOTA_EXCEEDED);
            peer.expectPublicationBarrier(sut);
        }
    }

    @Test
    void a_refused_qos_2_publication_ends_without_pubrel() throws Exception {
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut, MqttVersion.V5)) {
            // Arrange
            CompletableFuture<PublishResult> published = sut.publish(message(Qos.EXACTLY_ONCE));
            var publication = (Packet.Publish) peer.receive();

            // Act
            peer.send(new Packet.PubRec(
                    publication.packetId(),
                    ReasonCode.NOT_AUTHORIZED,
                    EMPTY
            ));

            // Assert: the next packet is the barrier, so no PUBREL answered the refusal.
            assertThat(refusal(published).reasonCode()).isEqualTo(ReasonCode.NOT_AUTHORIZED);
            peer.expectPublicationBarrier(sut);
        }
    }

    @Test
    void a_refused_subscription_fails_and_the_connection_goes_on() throws Exception {
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut, MqttVersion.V5)) {
            // Arrange
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("private/#"), Qos.AT_LEAST_ONCE),
                    _ -> { }
            );
            var request = (Packet.Subscribe) peer.receive();

            // Act
            peer.send(new Packet.SubAck(
                    request.packetId(),
                    List.of(ReasonCode.NOT_AUTHORIZED),
                    EMPTY
            ));

            // Assert
            assertThat(subscription.established()).failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause()
                    .isInstanceOfSatisfying(MqttSubscriptionRefusedException.class, refusal ->
                            assertThat(refusal.reasonCode()).isEqualTo(ReasonCode.NOT_AUTHORIZED));
            peer.roundTripBarrier(sut);
        }
    }

    @Test
    void an_unsuback_reason_code_confirms_the_removal() throws Exception {
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut, MqttVersion.V5)) {
            // Arrange
            Subscription subscription = peer.subscribe(
                    sut,
                    "data",
                    AckMode.AFTER_HANDLER,
                    _ -> { }
            );
            CompletableFuture<Void> removed = subscription.unsubscribe();
            var request = (Packet.Unsubscribe) peer.receive();

            // Act
            peer.send(new Packet.UnsubAck(
                    request.packetId(),
                    List.of(ReasonCode.NO_SUBSCRIPTION_EXISTED),
                    EMPTY
            ));

            // Assert
            removed.get(5, SECONDS);
            peer.roundTripBarrier(sut);
        }
    }

    private static MqttClient client(ServerSocket server) {
        return Mqtt5Clients.client(endpoint(server), "mqtt5-acknowledgements", Duration.ZERO);
    }

    private static MqttMessage message(Qos qos) {
        return MqttMessage.of(new Topic("data"), new byte[] {1}, qos, false);
    }

    private static MqttPublicationRefusedException refusal(
            CompletableFuture<PublishResult> published
    ) {
        Throwable failure = assertThat(published).failsWithin(Duration.ofSeconds(5))
                .withThrowableOfType(ExecutionException.class)
                .actual()
                .getCause();
        assertThat(failure).isInstanceOf(MqttPublicationRefusedException.class);
        return (MqttPublicationRefusedException) failure;
    }
}
