package org.mqtt4j.mqtj.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.mqtt4j.mqtj.broker.internal.common.io.FrameReader;
import org.mqtt4j.mqtj.broker.internal.common.io.FrameWriter;
import org.mqtt4j.mqtj.broker.internal.common.io.TcpConnection;
import org.mqtt4j.mqtj.broker.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.mqtj.broker.internal.common.value.MqttVersion;
import org.mqtt4j.mqtj.broker.internal.common.value.Qos;
import org.mqtt4j.mqtj.broker.internal.common.value.ReasonCode;
import org.mqtt4j.mqtj.broker.internal.common.value.Topic;
import org.mqtt4j.mqtj.broker.internal.common.value.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.mqtj.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.mqtj.broker.internal.common.value.ReasonCode.SUCCESS;

/// One raw connection using the broker's embedded codec, as in the common Mosquitto tests.
/// Every blocking read has a deadline and closes the connection when it expires.
final class RawSession implements AutoCloseable {

    static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);

    private final TcpConnection connection;
    private final FrameReader reader;
    private final FrameWriter writer;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private RawSession(TcpConnection connection) {
        this.connection = connection;
        reader = new FrameReader(connection.input(), 1024 * 1024);
        writer = new FrameWriter(connection.output());
    }

    static RawSession open(int port) throws IOException {
        return new RawSession(TcpConnection.connect(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), port),
                Duration.ofSeconds(5)
        ));
    }

    static RawSession connected(int port, String clientId) throws Exception {
        RawSession session = open(port);
        try {
            session.send(connect(clientId, true, 0));
            assertThat(session.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, SUCCESS, EMPTY));
            return session;
        } catch (Exception | AssertionError failure) {
            session.close();
            throw failure;
        }
    }

    static Packet.Connect connect(String clientId, boolean cleanSession, int keepAlive) {
        return new Packet.Connect(
                clientId, MqttVersion.V3_1_1, cleanSession, keepAlive, null, null, null, EMPTY
        );
    }

    static Packet.Subscription subscription(String filter, Qos qos) {
        return new Packet.Subscription(new TopicFilter(filter), qos, false, false, 0);
    }

    static Packet.Publish publication(String topic, String payload, Qos qos, int packetId) {
        return new Packet.Publish(
                new Topic(topic), payload.getBytes(UTF_8), qos, false, false, packetId, EMPTY
        );
    }

    void subscribe(String filter, Qos qos) throws Exception {
        send(new Packet.Subscribe(1, List.of(subscription(filter, qos)), EMPTY));
        ReasonCode granted = qos == Qos.AT_MOST_ONCE ? SUCCESS : ReasonCode.GRANTED_QOS_1;
        assertThat(expect(Packet.SubAck.class))
                .isEqualTo(new Packet.SubAck(1, List.of(granted), EMPTY));
    }

    void publish(String topic, String payload, Qos qos, int packetId) throws Exception {
        send(publication(topic, payload, qos, packetId));
        if (qos == Qos.AT_LEAST_ONCE) {
            assertThat(expect(Packet.PubAck.class))
                    .isEqualTo(new Packet.PubAck(packetId, SUCCESS, EMPTY));
        }
    }

    void ping() throws Exception {
        send(new Packet.PingReq());
        expect(Packet.PingResp.class);
    }

    void send(Packet packet) throws IOException {
        sendRaw(codec.encode(packet));
    }

    void sendRaw(byte[] bytes) throws IOException {
        writer.write(bytes);
    }

    <T extends Packet> T expect(Class<T> type) throws Exception {
        byte[] frame = nextFrame(READ_TIMEOUT).orElseThrow(() -> new AssertionError(
                "The broker closed the connection instead of sending " + type.getSimpleName()
        ));
        Packet packet = codec.decode(frame);
        assertThat(packet).isInstanceOf(type);
        return type.cast(packet);
    }

    void expectClosed() throws Exception {
        expectClosed(READ_TIMEOUT);
    }

    void expectClosed(Duration timeout) throws Exception {
        try {
            assertThat(nextFrame(timeout)).as("packet instead of connection closure").isEmpty();
        } catch (ExecutionException failure) {
            // Closing while peer bytes are in flight can send a TCP reset instead of FIN.
            assertThat(failure.getCause()).isInstanceOf(SocketException.class);
        }
    }

    private Optional<byte[]> nextFrame(Duration timeout) throws Exception {
        Future<Optional<byte[]>> frame = executor.submit(reader::read);
        try {
            return frame.get(timeout.toMillis(), MILLISECONDS);
        } catch (TimeoutException failure) {
            connection.close();
            throw new AssertionError("No packet or closure within " + timeout, failure);
        }
    }

    @Override
    public void close() throws IOException {
        connection.close();
        executor.close();
    }
}
