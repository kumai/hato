package org.mqtt4j.hato.client.internal;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.io.FrameReader;
import org.mqtt4j.hato.client.internal.common.io.FrameWriter;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.spi.MetricsListener;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A raw TCP or TLS peer whose replies are chosen by each test. A TLS peer completes its
/// handshake on the first read. It speaks MQTT 3.1.1 unless a test chooses another version.
final class ScriptedBroker implements AutoCloseable {

    static MqttClient client(ServerSocket server) {
        return MqttClient.builder()
                .server(endpoint(server))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }

    /// A client like [#client(ServerSocket)] that reports its timings to `metrics`.
    static MqttClient client(ServerSocket server, MetricsListener metrics) {
        return MqttClient.builder()
                .server(endpoint(server))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .metricsListener(metrics)
                .build();
    }

    static ServerSocket listen() throws IOException {
        var server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        server.setSoTimeout(5000);
        return server;
    }

    static URI endpoint(ServerSocket server) {
        return URI.create("mqtt://127.0.0.1:" + server.getLocalPort());
    }

    private final Socket socket;
    private final BufferedInputStream input;
    private final FrameReader reader;
    private final FrameWriter writer;
    private final PacketCodec codec;

    private ScriptedBroker(Socket socket, MqttVersion version) throws IOException {
        this.socket = socket;
        socket.setSoTimeout(5000);
        // Small packets, such as PUBREL, go out at once, as the client's own do; reads are
        // buffered, so that the client's small acknowledgements cost no system call for each
        // byte of their headers.
        socket.setTcpNoDelay(true);
        input = new BufferedInputStream(socket.getInputStream());
        reader = new FrameReader(input, 1024 * 1024);
        writer = new FrameWriter(socket.getOutputStream());
        codec = PacketCodec.of(version);
    }

    static ScriptedBroker connect(ServerSocket server, MqttClient sut) throws Exception {
        return connect(server, sut, false, _ -> { });
    }

    static ScriptedBroker connect(
            ServerSocket server,
            MqttClient sut,
            MqttVersion version
    ) throws Exception {
        return connect(server, sut, version, false, _ -> { });
    }

    static ScriptedBroker accept(ServerSocket server) throws IOException {
        return accept(server, MqttVersion.V3_1_1);
    }

    static ScriptedBroker accept(ServerSocket server, MqttVersion version) throws IOException {
        return new ScriptedBroker(server.accept(), version);
    }

    static ScriptedBroker connect(
            ServerSocket server,
            MqttClient sut,
            boolean sessionPresent,
            Consumer<Packet.Connect> verifyConnect
    ) throws Exception {
        return connect(server, sut, MqttVersion.V3_1_1, sessionPresent, verifyConnect);
    }

    private static ScriptedBroker connect(
            ServerSocket server,
            MqttClient sut,
            MqttVersion version,
            boolean sessionPresent,
            Consumer<Packet.Connect> verifyConnect
    ) throws Exception {
        CompletableFuture<ConnAck> connected = sut.connect();
        ScriptedBroker peer = accept(server, version);
        try {
            assertThat(peer.receive()).isInstanceOfSatisfying(Packet.Connect.class, verifyConnect);
            peer.send(new Packet.ConnAck(sessionPresent, ReasonCode.SUCCESS, EMPTY));
            assertThat(connected.get(5, SECONDS).sessionPresent()).isEqualTo(sessionPresent);
            return peer;
        } catch (Exception | AssertionError failure) {
            peer.close();
            throw failure;
        }
    }

    Subscription subscribe(
            MqttClient sut,
            String topic,
            AckMode ackMode,
            Consumer<MqttDelivery> handler
    ) throws Exception {
        return subscribe(sut, topic, ackMode, 256, handler);
    }

    Subscription subscribePull(MqttClient sut, String topic, AckMode mode, int capacity)
            throws Exception {
        return subscribePull(sut, topic, Qos.AT_LEAST_ONCE, mode, capacity);
    }

    Subscription subscribePull(
            MqttClient sut,
            String topic,
            Qos qos,
            AckMode mode,
            int capacity
    ) throws Exception {
        Subscription subscription = sut.subscribe(
                Subscribe.of(new TopicFilter(topic), qos)
                        .withAckMode(mode)
                        .withBufferCapacity(capacity)
        );
        var request = (Packet.Subscribe) receive();
        send(new Packet.SubAck(request.packetId(), List.of(ReasonCode.of(qos.level())), EMPTY));
        subscription.established().get(5, SECONDS);
        return subscription;
    }

    Subscription subscribe(
            MqttClient sut,
            String topic,
            AckMode ackMode,
            int capacity,
            Consumer<MqttDelivery> handler
    ) throws Exception {
        Subscription subscription = sut.subscribe(
                Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE)
                        .withAckMode(ackMode)
                        .withBufferCapacity(capacity),
                handler
        );
        var request = (Packet.Subscribe) receive();
        send(new Packet.SubAck(request.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
        subscription.established().get(5, SECONDS);
        return subscription;
    }

    void publish(String topic, int packetId) throws IOException {
        send(new Packet.Publish(
                new Topic(topic), new byte[] {1}, Qos.AT_LEAST_ONCE,
                false, false, packetId, EMPTY
        ));
    }

    void publishBurst(String topic, int first, int count, boolean dup) throws IOException {
        var bytes = new ByteArrayOutputStream();
        for (int id = first; id < first + count; id++) {
            bytes.writeBytes(codec.encode(new Packet.Publish(
                    new Topic(topic), new byte[] {(byte) id}, Qos.AT_LEAST_ONCE,
                    false, dup, id, EMPTY
            )));
        }
        socket.getOutputStream().write(bytes.toByteArray());
    }

    void expectAcknowledgements(int first, int count) throws IOException {
        for (int id = first; id < first + count; id++) {
            assertThat(receive()).isEqualTo(new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY));
        }
    }

    void roundTripBarrier(MqttClient sut) throws Exception {
        CompletableFuture<PublishResult> published = sut.publish(MqttMessage.of(
                new Topic("barrier"), new byte[] {0}, Qos.AT_LEAST_ONCE, false
        ));
        var publication = (Packet.Publish) receive();
        assertThat(publication.topic()).isEqualTo(new Topic("barrier"));
        send(new Packet.PubAck(publication.packetId(), ReasonCode.SUCCESS, EMPTY));
        // This PUBACK follows all preceding input in the same byte stream and mailbox.
        published.get(5, SECONDS);
    }

    void expectPublicationBarrier(MqttClient sut) throws Exception {
        sut.publish(MessagingTest.message("barrier", (byte) 0)).get(5, SECONDS);
        assertThat(receive()).isInstanceOfSatisfying(
                Packet.Publish.class,
                publication -> assertThat(publication.topic()).isEqualTo(new Topic("barrier"))
        );
    }

    Packet receive() throws IOException {
        return codec.decode(reader.read().orElseThrow(() -> new AssertionError(
                "The client closed the connection before sending the expected packet"
        )));
    }

    /// Whether bytes from the client have already arrived, so that [#receive()] would not wait
    /// for the start of the next packet.
    boolean hasPending() throws IOException {
        return input.available() > 0;
    }

    void send(Packet packet) throws IOException {
        writer.write(codec.encode(packet));
    }

    void send(byte[] bytes) throws IOException {
        writer.write(bytes);
    }

    void expectClosed() throws IOException {
        assertThat(reader.read()).isEmpty();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
