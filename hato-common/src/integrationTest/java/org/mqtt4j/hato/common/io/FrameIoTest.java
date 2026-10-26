package org.mqtt4j.hato.common.io;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.common.protocol.Packet;
import org.mqtt4j.hato.common.protocol.PacketCodec;
import org.mqtt4j.hato.common.value.MqttVersion;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FrameIoTest {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void fragmented_socket_writes_form_one_packet(int chunkSize) throws Exception {
        // Arrange: close sockets before the executor, including after a failed assertion.
        try (
                var executor = Executors.newVirtualThreadPerTaskExecutor();
                var server = listen();
                var connection = connectTo(server);
                var peer = server.accept()
        ) {
            byte[] packet = HexFormat.of().parseHex("3289000003612f62000a6869");
            var sut = new FrameReader(connection.input(), packet.length);
            Future<Optional<byte[]>> received = executor.submit(sut::read);

            // Act: the read must wait for the rest of the header or body after each fragment.
            for (int offset = 0; offset < packet.length; offset += chunkSize) {
                int length = Math.min(chunkSize, packet.length - offset);
                peer.getOutputStream().write(packet, offset, length);
                peer.getOutputStream().flush();
                if (offset + length < packet.length) {
                    assertThatExceptionOfType(TimeoutException.class)
                            .isThrownBy(() -> received.get(20, MILLISECONDS));
                }
            }

            // Assert: completion must not require the peer to close its output.
            assertThat(received.get(5, SECONDS).orElseThrow()).isEqualTo(packet);
        }
    }

    @Test
    void codec_and_frames_exchange_packets_in_both_directions() throws Exception {
        try (
                var executor = Executors.newVirtualThreadPerTaskExecutor();
                var server = listen();
                var connection = connectTo(server);
                var peer = server.accept()
        ) {
            peer.setSoTimeout(5000);
            PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);
            var reader = new FrameReader(connection.input(), 1024);
            var writer = new FrameWriter(connection.output());
            var peerReader = new FrameReader(peer.getInputStream(), 1024);
            Future<List<Optional<byte[]>>> received = executor.submit(
                    () -> List.of(reader.read(), reader.read(), reader.read())
            );

            writer.write(codec.encode(new Packet.PingReq()));
            assertThat(codec.decode(peerReader.read().orElseThrow()))
                    .isEqualTo(new Packet.PingReq());
            // Two packets arrive in one socket write, followed by clean EOF.
            peer.getOutputStream().write(HexFormat.of().parseHex("d000c000"));
            peer.getOutputStream().flush();
            peer.shutdownOutput();

            List<Optional<byte[]>> packets = received.get(5, SECONDS);
            assertThat(codec.decode(packets.get(0).orElseThrow())).isEqualTo(new Packet.PingResp());
            assertThat(codec.decode(packets.get(1).orElseThrow())).isEqualTo(new Packet.PingReq());
            assertThat(packets.get(2)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"30", "3080", "400200"})
    void peer_shutdown_during_a_packet_is_a_truncated_frame(String prefixHex) throws Exception {
        try (
                var executor = Executors.newVirtualThreadPerTaskExecutor();
                var server = listen();
                var connection = connectTo(server);
                var peer = server.accept()
        ) {
            var sut = new FrameReader(connection.input(), 1024);
            Future<Optional<byte[]>> received = executor.submit(sut::read);

            peer.getOutputStream().write(HexFormat.of().parseHex(prefixHex));
            peer.shutdownOutput();

            assertThatExceptionOfType(ExecutionException.class)
                    .isThrownBy(() -> received.get(5, SECONDS))
                    .withCauseInstanceOf(EOFException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "30", "3080", "400200"})
    void local_close_releases_pending_frame_reads_and_prevents_writes(String prefixHex)
            throws Exception {
        try (
                var executor = Executors.newVirtualThreadPerTaskExecutor();
                var server = listen()
        ) {
            var connection = connectTo(server);
            try (var peer = server.accept()) {
                var reader = new FrameReader(connection.input(), 1024);
                var writer = new FrameWriter(connection.output());
                var started = new CountDownLatch(1);
                peer.getOutputStream().write(HexFormat.of().parseHex(prefixHex));
                peer.getOutputStream().flush();
                Future<Optional<byte[]>> received = executor.submit(() -> {
                    started.countDown();
                    return reader.read();
                });
                assertThat(started.await(5, SECONDS)).isTrue();
                assertThatExceptionOfType(TimeoutException.class)
                        .isThrownBy(() -> received.get(100, MILLISECONDS));

                connection.close();

                assertThatExceptionOfType(ExecutionException.class)
                        .isThrownBy(() -> received.get(5, SECONDS))
                        .withCauseInstanceOf(IOException.class);
                assertThatThrownBy(() -> writer.write(HexFormat.of().parseHex("c000")))
                        .isInstanceOf(IOException.class);
            } finally {
                connection.close();
            }
        }
    }

    private static ServerSocket listen() throws IOException {
        var server = new ServerSocket(0, 1, LOOPBACK);
        server.setSoTimeout(5000);
        return server;
    }

    private static TcpConnection connectTo(ServerSocket server) throws IOException {
        return TcpConnection.connect(
                LOOPBACK.getHostAddress(), server.getLocalPort(), Duration.ofSeconds(5)
        );
    }
}
