package org.mqtt4j.mqtj.client.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.mqtj.*;
import org.mqtt4j.mqtj.client.internal.common.io.FrameReader;
import org.mqtt4j.mqtj.client.internal.common.io.FrameWriter;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketCodec;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.mqtj.client.internal.common.protocol.Properties.EMPTY;

/// A raw broker measures the client's silence while a receive backlog drains. A real broker
/// closes a connection that sends nothing for one and a half keep-alive intervals.
class KeepAliveBacklogTest {

    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    private static final int SUBSCRIPTIONS = 256;

    private static final int PUBLICATIONS = 16_384;

    @Test
    @Timeout(60)
    void a_backlog_for_256_overlapping_subscriptions_does_not_hold_back_keep_alive()
            throws Exception {
        // Arrange: every publication matches all 256 filters, so the backlog outlasts the
        // broker's allowance of 1.5 seconds for a one-second keep-alive.
        var ended = new LinkedBlockingQueue<Disconnection>();
        var handled = new AtomicLong();
        try (
                ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                MqttClient sut = MqttClient.builder()
                        .server(URI.create("mqtt://127.0.0.1:" + server.getLocalPort()))
                        .version(MqttVersion.V3_1_1)
                        .keepAlive(Duration.ofSeconds(1))
                        .connectionListener(ended::add)
                        .build()
        ) {
            CompletableFuture<ConnAck> connecting = sut.connect();
            try (Socket socket = server.accept()) {
                var reader = new FrameReader(socket.getInputStream(), 1024 * 1024);
                var writer = new FrameWriter(socket.getOutputStream());
                assertThat(receive(reader)).isInstanceOf(Packet.Connect.class);
                writer.write(codec.encode(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY)));
                connecting.get(5, SECONDS);
                for (int i = 0; i < SUBSCRIPTIONS; i++) {
                    CompletableFuture<Subscription> subscribing = sut.subscribe(
                            Subscribe.of(new TopicFilter(filter(i)), Qos.AT_MOST_ONCE),
                            _ -> handled.incrementAndGet()
                    );
                    var subscribe = (Packet.Subscribe) receive(reader);
                    writer.write(codec.encode(new Packet.SubAck(
                            subscribe.packetId(), List.of(ReasonCode.SUCCESS), EMPTY
                    )));
                    subscribing.get(5, SECONDS);
                }
                var silence = new Silence();
                // A platform thread keeps the broker's clock independent of virtual threads.
                Thread.ofPlatform().daemon().start(() -> answerPings(reader, writer, silence));
                var burst = new ByteArrayOutputStream();
                byte[] publication = codec.encode(new Packet.Publish(new Topic("a/b/c/d/e/f/g/h"),
                        new byte[1], Qos.AT_MOST_ONCE, false, false, 0, EMPTY));
                for (int i = 0; i < PUBLICATIONS; i++) {
                    burst.writeBytes(publication);
                }

                // Act
                synchronized (writer) {
                    socket.getOutputStream().write(burst.toByteArray());
                }
                await().atMost(Duration.ofSeconds(30)).until(() ->
                        handled.get() == (long) PUBLICATIONS * SUBSCRIPTIONS || !ended.isEmpty());
                // One more interval shows that keep-alive continues after the backlog.
                Thread.sleep(1_200);

                // Assert
                assertThat(ended).isEmpty();
                assertThat(handled).hasValue((long) PUBLICATIONS * SUBSCRIPTIONS);
                assertThat(silence.longest()).isLessThanOrEqualTo(Duration.ofMillis(1_500));
            }
        }
    }

    /// Each of the 256 filters keeps or replaces each of eight levels with +.
    private static String filter(int index) {
        var filter = new StringBuilder();
        for (int level = 0; level < 8; level++) {
            if (level > 0) {
                filter.append('/');
            }
            filter.append((index & 1 << level) != 0 ? "+" : String.valueOf((char) ('a' + level)));
        }
        return filter.toString();
    }

    private static void answerPings(FrameReader reader, FrameWriter writer, Silence silence) {
        try {
            while (true) {
                Packet packet = codec.decode(reader.read().orElseThrow());
                silence.received();
                if (packet instanceof Packet.PingReq) {
                    synchronized (writer) {
                        writer.write(codec.encode(new Packet.PingResp()));
                    }
                }
            }
        } catch (IOException | RuntimeException _) {
            // The test closes the socket when it is done.
        }
    }

    private static Packet receive(FrameReader reader) throws IOException {
        return codec.decode(reader.read().orElseThrow());
    }

    /// The longest time without a client packet, measured from the end of setup.
    private static final class Silence {

        private long last = System.nanoTime();

        private long longest;

        synchronized void received() {
            long now = System.nanoTime();
            longest = Math.max(longest, now - last);
            last = now;
        }

        synchronized Duration longest() {
            return Duration.ofNanos(Math.max(longest, System.nanoTime() - last));
        }
    }
}
