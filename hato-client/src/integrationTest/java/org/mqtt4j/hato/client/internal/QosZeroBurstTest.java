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

    private final LinkedBlockingQueue<Disconnection> ended = new LinkedBlockingQueue<>();

    private final CountDownLatch marked = new CountDownLatch(1);

    @Test
    @Timeout(60)
    void a_qos_0_burst_faster_than_the_client_routes_leaves_the_connection_up() throws Exception {
        try (
                ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = client(server);
                ScriptedBroker broker = ScriptedBroker.connect(server, sut)
        ) {
            // Arrange
            subscribe(sut, broker);

            // Act
            sendBurst(broker);

            // Assert: like a full subscription buffer, the burst leaves the connection up, and
            // the message after it arrives.
            awaitMarker();
            assertThat(ended).isEmpty();
            assertThat(marked.getCount()).isZero();
        }
    }

    @Test
    @Timeout(60)
    void a_qos_0_burst_over_tcp_is_held_back_rather_than_dropped() throws Exception {
        try (
                ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = client(server);
                ScriptedBroker broker = ScriptedBroker.connect(server, sut)
        ) {
            // Arrange
            subscribe(sut, broker);

            // Act
            sendBurst(broker);

            // Assert: no publication was dropped before routing. A subscription's buffer may
            // still drop some while its handler is not running.
            awaitMarker();
            assertThat(ended).isEmpty();
            assertThat(sut.stats().get(5, SECONDS).receiving().unrouted()).isZero();
        }
    }

    private MqttClient client(ServerSocket server) {
        return MqttClient.builder()
                .server(ScriptedBroker.endpoint(server))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .connectionListener(ended::add)
                .build();
    }

    /// Subscribes to "burst" at QoS 0, and to "marker" at QoS 1 for the publication after the
    /// burst that marks its end, which, unlike QoS 0, the client may not drop.
    private void subscribe(MqttClient sut, ScriptedBroker broker) throws Exception {
        subscribe(sut, broker, "burst", Qos.AT_MOST_ONCE, _ -> { });
        subscribe(sut, broker, "marker", Qos.AT_LEAST_ONCE, _ -> marked.countDown());
    }

    /// Writes the burst and the marker for as long as the client reads, as a real broker would.
    private static void sendBurst(ScriptedBroker broker) {
        var burst = new ByteArrayOutputStream();
        byte[] publication = codec.encode(new Packet.Publish(new Topic("burst"),
                new byte[1], Qos.AT_MOST_ONCE, false, false, 0, EMPTY));
        for (int i = 0; i < PUBLICATIONS; i++) {
            burst.writeBytes(publication);
        }
        burst.writeBytes(codec.encode(new Packet.Publish(new Topic("marker"),
                new byte[1], Qos.AT_LEAST_ONCE, false, false, 1, EMPTY)));
        Thread.ofPlatform().daemon().start(() -> {
            try {
                broker.send(burst.toByteArray());
            } catch (IOException _) {
                // A client that ended the connection fails the assertions.
            }
        });
    }

    private void awaitMarker() {
        await().atMost(Duration.ofSeconds(30))
                .until(() -> marked.getCount() == 0 || !ended.isEmpty());
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
