package org.mqtt4j.hato.common.io;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.jspecify.annotations.Nullable;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.common.protocol.Packet;
import org.mqtt4j.hato.common.protocol.PacketCodec;
import org.mqtt4j.hato.common.value.Topic;

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

    /// A read that outlasted the wait of [#poll(Duration)]. The next read takes its frame instead
    /// of reading again, so the frame reader never has two readers and no frame is skipped.
    private @Nullable Future<Optional<byte[]>> pendingRead;

    private RawSession(PacketCodec codec, TcpConnection connection) {
        this.codec = codec;
        this.connection = connection;
        this.reader = new FrameReader(connection.input(), MAX_PACKET_SIZE);
        this.writer = new FrameWriter(connection.output());
    }

    /// Connects to the MQTT listener that the broker container exposes at `port`; `codec`
    /// decides the wire version of every packet.
    static RawSession open(PacketCodec codec, GenericContainer<?> broker, int port)
            throws IOException {
        return new RawSession(codec, TcpConnection.connect(
                broker.getHost(),
                broker.getMappedPort(port),
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

    /// Returns the next packet, or empty when the broker closes the connection instead.
    Optional<Packet> next() throws Exception {
        Optional<byte[]> frame = read();
        if (frame.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(codec.decode(frame.get()));
    }

    /// Reads until a publication to `end` arrives, and returns the packets before it, in order.
    ///
    /// MQTT keeps the order of publications within a topic only. A broker that does not keep
    /// it across topics can send publications after the one to `end` that were published before
    /// it, so with a positive `stragglers` this also takes the packets that follow it, until none
    /// arrives for that long.
    List<Packet> packetsBefore(Topic end, Duration stragglers) throws Exception {
        var packets = new ArrayList<Packet>();
        while (true) {
            Packet packet = expect(Packet.class);
            if (packet instanceof Packet.Publish publish && publish.topic().equals(end)) {
                break;
            }
            packets.add(packet);
        }
        if (stragglers.isPositive()) {
            Optional<Packet> late = poll(stragglers);
            while (late.isPresent()) {
                packets.add(late.get());
                late = poll(stragglers);
            }
        }
        return packets;
    }

    /// Returns the next packet if it arrives within `wait`, and otherwise empty, leaving the
    /// connection open for later reads.
    Optional<Packet> poll(Duration wait) throws Exception {
        Future<Optional<byte[]>> frame = nextRead();
        try {
            byte[] bytes = frame.get(wait.toMillis(), MILLISECONDS).orElseThrow(
                    () -> new AssertionError("The broker closed the connection")
            );
            return Optional.of(codec.decode(bytes));
        } catch (TimeoutException _) {
            pendingRead = frame;
            return Optional.empty();
        }
    }

    void expectEndOfStream() throws Exception {
        assertThat(read()).as("packet received instead of the end of stream").isEmpty();
    }

    private Optional<byte[]> read() throws Exception {
        Future<Optional<byte[]>> frame = nextRead();
        try {
            return frame.get(READ_TIMEOUT.toMillis(), MILLISECONDS);
        } catch (TimeoutException e) {
            // Closing the socket releases the blocked read, so close() can end the executor.
            connection.close();
            throw new AssertionError("No packet from the broker within " + READ_TIMEOUT, e);
        }
    }

    private Future<Optional<byte[]>> nextRead() {
        Future<Optional<byte[]>> frame = pendingRead;
        pendingRead = null;
        return frame != null ? frame : executor.submit(reader::read);
    }

    @Override
    public void close() throws IOException {
        connection.close();
        executor.close();
    }
}
