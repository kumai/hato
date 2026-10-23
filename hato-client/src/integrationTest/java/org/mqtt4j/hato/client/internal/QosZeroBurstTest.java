package org.mqtt4j.hato.client.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A raw broker sends QoS 0 publications as fast as the socket takes them, as RabbitMQ does for a
/// QoS 0 subscription of a clean session, which it sends without flow control.
class QosZeroBurstTest {

    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    // Several times the 65,536 packets that the client queues for routing: the client reads
    // faster than it routes, and falls behind by more than that within the burst.
    private static final int PUBLICATIONS = 400_000;

    @Test
    @Timeout(60)
    void a_qos_0_burst_faster_than_the_client_routes_leaves_the_connection_up() throws Exception {
        // Arrange: a QoS 1 publication after the burst marks its end; unlike QoS 0, the client
        // may not drop it.
        var ended = new LinkedBlockingQueue<Disconnection>();
        var marked = new CountDownLatch(1);
        try (
                ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = MqttClient.builder()
                        .server(ScriptedBroker.endpoint(server))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .connectionListener(ended::add)
                        .build();
                ScriptedBroker broker = ScriptedBroker.connect(server, sut)
        ) {
            subscribe(sut, broker, "burst", Qos.AT_MOST_ONCE, _ -> { });
            subscribe(sut, broker, "marker", Qos.AT_LEAST_ONCE, _ -> marked.countDown());
            var burst = new ByteArrayOutputStream();
            byte[] publication = codec.encode(new Packet.Publish(new Topic("burst"),
                    new byte[1], Qos.AT_MOST_ONCE, false, false, 0, EMPTY));
            for (int i = 0; i < PUBLICATIONS; i++) {
                burst.writeBytes(publication);
            }
            burst.writeBytes(codec.encode(new Packet.Publish(new Topic("marker"),
                    new byte[1], Qos.AT_LEAST_ONCE, false, false, 1, EMPTY)));

            // Act: the broker writes for as long as the client reads, as a real one would.
            Thread.ofPlatform().daemon().start(() -> {
                try {
                    broker.send(burst.toByteArray());
                } catch (IOException _) {
                    // A client that ended the connection fails the assertions below.
                }
            });

            // Assert: like a full subscription buffer, the burst leaves the connection up, and
            // the message after it arrives.
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> marked.getCount() == 0 || !ended.isEmpty());
            assertThat(ended).isEmpty();
            assertThat(marked.getCount()).isZero();
        }
    }

    private static void subscribe(
            MqttClient sut,
            ScriptedBroker broker,
            String filter,
            Qos qos,
            Consumer<MqttDelivery> handler
    ) throws Exception {
        Subscription subscription = sut.subscribe(
                Subscribe.of(new TopicFilter(filter), qos),
                handler
        );
        var request = (Packet.Subscribe) broker.receive();
        broker.send(new Packet.SubAck(
                request.packetId(),
                List.of(ReasonCode.of(qos.level())),
                EMPTY
        ));
        subscription.established().get(5, SECONDS);
    }
}
