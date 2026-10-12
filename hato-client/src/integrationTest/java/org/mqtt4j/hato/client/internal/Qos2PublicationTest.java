package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.endpoint;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.listen;
import static org.mqtt4j.hato.client.internal.SessionPersistenceIntegrationTest.failureOf;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

@Timeout(30)
class Qos2PublicationTest {

    @Test
    void pubcomp_alone_completes_the_exchange_despite_duplicate_and_unknown_responses()
            throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            CompletableFuture<PublishResult> publication = sut.publish(message((byte) 1));
            Packet.Publish publish = assertInstanceOf(Packet.Publish.class, peer.receive());
            assertThat(publish.qos()).isEqualTo(Qos.EXACTLY_ONCE);
            assertThat(publish.dup()).isFalse();
            assertThat(publication).isNotDone();

            // Act & Assert: an unknown PUBREC still gets PUBREL, without touching this exchange.
            int unknownId = publish.packetId() == 65_535 ? 65_534 : 65_535;
            peer.send(new Packet.PubRec(unknownId, ReasonCode.SUCCESS, EMPTY));
            expectRelease(peer, unknownId);
            peer.send(new Packet.PubRec(unknownId, ReasonCode.SUCCESS, EMPTY));
            expectRelease(peer, unknownId);
            peer.send(new Packet.PubComp(unknownId, ReasonCode.SUCCESS, EMPTY));
            peer.roundTripBarrier(sut);
            assertThat(publication).isNotDone();

            peer.send(new Packet.PubRec(publish.packetId(), ReasonCode.SUCCESS, EMPTY));
            expectRelease(peer, publish.packetId());
            peer.send(new Packet.PubRec(publish.packetId(), ReasonCode.SUCCESS, EMPTY));
            expectRelease(peer, publish.packetId());
            peer.roundTripBarrier(sut);
            assertThat(publication).isNotDone();

            peer.send(new Packet.PubComp(publish.packetId(), ReasonCode.SUCCESS, EMPTY));
            assertThat(publication.get(5, SECONDS)).isEqualTo(PublishResult.of());
            peer.send(new Packet.PubComp(publish.packetId(), ReasonCode.SUCCESS, EMPTY));
            peer.roundTripBarrier(sut);
        }
    }

    @Test
    void continuation_preserves_publish_order_and_pubrec_order_without_republishing_releases()
            throws Exception {
        // Arrange: two remaining PUBLISH packets and two PUBREL packets share the retained table.
        String clientId = "hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19);
        var ended = new LinkedBlockingQueue<Disconnection>();
        var futures = new ArrayList<CompletableFuture<PublishResult>>();
        var failures = new ArrayList<Throwable>();
        var originals = new ArrayList<Packet.Publish>();
        try (ServerSocket server = listen(); MqttClient sut = MqttClient.builder()
                .server(endpoint(server))
                .version(MqttVersion.V3_1_1)
                .clientId(clientId)
                .cleanStart(false)
                .sessionExpiry(Duration.ofSeconds(30))
                .connectionListener(ended::add)
                .build()) {
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                for (byte value = 1; value <= 4; value++) {
                    MqttMessage message = message(value);
                    if (value == 2) {
                        message = message.withQos(Qos.AT_LEAST_ONCE);
                    }
                    futures.add(sut.publish(message));
                    Packet.Publish publish = assertInstanceOf(Packet.Publish.class, peer.receive());
                    assertThat(publish.payload()).containsExactly(value);
                    assertThat(publish.dup()).isFalse();
                    originals.add(publish);
                }
                for (int index : List.of(3, 0)) {
                    int packetId = originals.get(index).packetId();
                    peer.send(new Packet.PubRec(packetId, ReasonCode.SUCCESS, EMPTY));
                    expectRelease(peer, packetId);
                }
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();
            for (CompletableFuture<PublishResult> future : futures) {
                Throwable failure = failureOf(future);
                assertThat(failure).isInstanceOf(MqttConnectionLostException.class);
                failures.add(failure);
            }

            // Act: PUBLISH keeps its original order, PUBREL follows the PUBREC reception order.
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut, true, request -> {
                assertThat(request.clientId()).isEqualTo(clientId);
                assertThat(request.cleanStart()).isFalse();
            })) {
                for (int index : List.of(1, 2)) {
                    Packet.Publish old = originals.get(index);
                    assertThat(peer.receive()).isEqualTo(new Packet.Publish(
                            old.topic(),
                            old.payload(),
                            old.qos(),
                            old.retain(),
                            true,
                            old.packetId(),
                            old.properties()
                    ));
                }
                for (int index : List.of(3, 0)) {
                    expectRelease(peer, originals.get(index).packetId());
                }
                peer.send(
                        new Packet.PubAck(originals.get(1).packetId(), ReasonCode.SUCCESS, EMPTY)
                );
                peer.send(
                        new Packet.PubRec(originals.get(2).packetId(), ReasonCode.SUCCESS, EMPTY)
                );
                expectRelease(peer, originals.get(2).packetId());
                for (int index : List.of(3, 0, 2)) {
                    peer.send(
                            new Packet.PubComp(
                                    originals.get(index).packetId(),
                                    ReasonCode.SUCCESS,
                                    EMPTY
                            )
                    );
                }

                // Assert: an extra PUBLISH or PUBREL would precede this round-trip marker.
                peer.roundTripBarrier(sut);
                for (int index = 0; index < futures.size(); index++) {
                    assertThat(failureOf(futures.get(index))).isSameAs(failures.get(index));
                }
            }
        }
    }

    private static void expectRelease(ScriptedBroker peer, int packetId) throws Exception {
        assertThat(peer.receive()).isEqualTo(
                new Packet.PubRel(packetId, ReasonCode.SUCCESS, EMPTY)
        );
    }

    private static MqttMessage message(byte value) {
        return MqttMessage.of(new Topic("data"), new byte[] {value}, Qos.EXACTLY_ONCE, false);
    }
}
