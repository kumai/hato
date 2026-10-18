package org.mqtt4j.hato.common.io;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.common.protocol.Packet;
import org.mqtt4j.hato.common.protocol.PacketCodec;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/// One TCP connection to a broker container that sends and receives whole packets with the
/// codec of one MQTT version. Each wait for an incoming packet is bounded, so a missing
/// response fails the test instead of hanging it.
final class RawSession implements AutoCloseable {

    private static final int MAX_PACKET_SIZE = 1024 * 1024;
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(10);

    private final PacketCodec codec;
    private final TcpConnection connection;
    private final FrameReader reader;
    private final FrameWriter writer;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private RawSession(PacketCodec codec, TcpConnection connection) {
        this.codec = codec;
        this.connection = connection;
        this.reader = new FrameReader(connection.input(), MAX_PACKET_SIZE);
        this.writer = new FrameWriter(connection.output());
    }

    /// Connects to the broker's MQTT port; `codec` decides the wire version of every packet.
    static RawSession open(PacketCodec codec, GenericContainer<?> broker) throws IOException {
        return new RawSession(codec, TcpConnection.connect(
                broker.getHost(),
                broker.getMappedPort(Mosquitto.MQTT_PORT),
                CONNECT_TIMEOUT
        ));
    }

    void send(Packet packet) throws IOException {
        writer.write(codec.encode(packet));
    }

    /// Sends the bytes as given, for packets the model cannot represent.
    void sendRaw(byte[] packet) throws IOException {
        writer.write(packet);
    }

    /// Returns the next packet's bytes, for comparing the broker's exact wire form.
    byte[] nextFrame() throws Exception {
        return read().orElseThrow(() -> new AssertionError(
                "The broker closed the connection instead of sending a packet"
        ));
    }

    <T extends Packet> T expect(Class<T> type) throws Exception {
        byte[] frame = read().orElseThrow(() -> new AssertionError(
                "The broker closed the connection instead of sending " + type.getSimpleName()
        ));
        Packet packet = codec.decode(frame);
        assertThat(packet).isInstanceOf(type);
        return type.cast(packet);
    }

    void expectEndOfStream() throws Exception {
        assertThat(read()).as("packet received instead of the end of stream").isEmpty();
    }

    private Optional<byte[]> read() throws Exception {
        Future<Optional<byte[]>> frame = executor.submit(reader::read);
        try {
            return frame.get(READ_TIMEOUT.toMillis(), MILLISECONDS);
        } catch (TimeoutException e) {
            // Closing the socket releases the blocked read, so close() can end the executor.
            connection.close();
            throw new AssertionError("No packet from the broker within " + READ_TIMEOUT, e);
        }
    }

    @Override
    public void close() throws IOException {
        connection.close();
        executor.close();
    }
}
