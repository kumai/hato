package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A raw broker checks that an acceptor that waits holds back hand-off but not keep-alive.
class HandOffKeepAliveTest {

    @Test
    @Timeout(30)
    void a_waiting_acceptor_does_not_hold_back_keep_alive() throws Exception {
        // Arrange: a one-second keep-alive, and an acceptor that waits on its first delivery.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (
                ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = MqttClient.builder()
                        .server(ScriptedBroker.endpoint(server))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .keepAlive(Duration.ofSeconds(1))
                        .connectionListener(ended::add)
                        .build();
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)
        ) {
            Subscription subscription = sut.handOff(
                    Subscribe.of(new TopicFilter("work"), Qos.AT_LEAST_ONCE),
                    _ -> {
                        entered.countDown();
                        MessagingTest.waitFor(release);
                        return true;
                    }
            );
            var request = (Packet.Subscribe) peer.receive();
            peer.send(new Packet.SubAck(
                    request.packetId(),
                    List.of(ReasonCode.GRANTED_QOS_1),
                    EMPTY
            ));
            subscription.established().get(5, SECONDS);

            // Act
            peer.publish("work", 1);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Assert: keep-alive goes on for two intervals while the acceptor waits, and the
            // PUBACK follows the acceptance.
            for (int interval = 0; interval < 2; interval++) {
                assertThat(peer.receive()).isInstanceOf(Packet.PingReq.class);
                peer.send(new Packet.PingResp());
            }
            release.countDown();
            assertThat(peer.receive()).isEqualTo(new Packet.PubAck(1, ReasonCode.SUCCESS, EMPTY));
            assertThat(ended).isEmpty();
        } finally {
            release.countDown();
        }
    }
}
