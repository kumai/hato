package org.mqtt4j.hato.comparison;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.comparison.Mqtt311.Packet;
import org.mqtt4j.hato.comparison.Mqtt311.PublishFrame;
import org.mqtt4j.hato.comparison.Mqtt311.Subscribe;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/// A broker stand-in for one subscriber, so that the time measured is the client's alone.
///
/// It accepts one connection, grants the subscriptions of its first SUBSCRIBE, and then sends
/// `count` messages to `topic`, numbered from 0. QoS 1 messages go out with at most `window`
/// unacknowledged, the way RabbitMQ sends them under its MQTT prefetch, which is 10 by default.
/// QoS 0 messages go out as fast as the socket takes them, so a client that stops reading slows
/// them down, and one that reads on regardless may drop some. The stand-in answers PINGREQ and
/// does no other broker work.
final class StubBroker implements AutoCloseable {

    // How long a subscriber may leave every message of the window unacknowledged.
    private static final Duration ACKNOWLEDGEMENT_TIMEOUT = Duration.ofMinutes(1);

    private final ServerSocket server;

    private final String topic;

    private final int qos;

    private final int payloadSize;

    private final int window;

    private final long count;

    // Sending thread and reading thread both write: messages and PINGRESP.
    private final ReentrantLock writing = new ReentrantLock();

    private volatile @Nullable Socket connection;

    private volatile @Nullable IOException readFailure;

    StubBroker(String topic, int qos, int payloadSize, int window, long count) throws IOException {
        this.server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        this.topic = topic;
        this.qos = qos;
        this.payloadSize = payloadSize;
        this.window = window;
        this.count = count;
    }

    int port() {
        return server.getLocalPort();
    }

    /// Serves one subscriber: waits for it to connect and subscribe, then sends every message.
    /// Returns once every QoS 0 message has been written, or once the subscriber has acknowledged
    /// every QoS 1 message. The connection stays open until [#close()].
    void serve() throws IOException, InterruptedException {
        Socket socket = server.accept();
        connection = socket;
        socket.setTcpNoDelay(true);
        var in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        var out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
        expect(in, Mqtt311.CONNECT);
        Mqtt311.writeConnAck(out);
        out.flush();
        Subscribe subscribe = Mqtt311.subscribe(expect(in, Mqtt311.SUBSCRIBE).body());
        Mqtt311.writeSubAck(out, subscribe);
        out.flush();

        // Each permit is room for one unacknowledged QoS 1 message. The subscriber's packets
        // arrive on a thread of their own, which returns a permit for each PUBACK.
        var unacknowledged = new Semaphore(window);
        Thread.ofPlatform()
                .name("stub-broker-reader")
                .daemon()
                .start(() -> read(in, out, unacknowledged));
        send(out, unacknowledged);
        if (qos > 0) {
            // Every permit back means every message acknowledged.
            await(unacknowledged, window);
        }
        IOException failure = readFailure;
        if (failure != null) {
            throw failure;
        }
    }

    private void send(OutputStream out, Semaphore unacknowledged)
            throws IOException, InterruptedException {
        var frame = new PublishFrame(topic, qos, payloadSize);
        int packetId = 0;
        for (long sequence = 0; sequence < count; sequence++) {
            if (qos > 0) {
                if (!unacknowledged.tryAcquire()) {
                    // The subscriber cannot acknowledge what is still in the buffer.
                    flush(out);
                    await(unacknowledged, 1);
                }
                packetId = packetId % 65_535 + 1;
            }
            writing.lock();
            try {
                frame.write(out, packetId, sequence, Payloads.nowMicros());
            } finally {
                writing.unlock();
            }
        }
        flush(out);
    }

    private void await(Semaphore unacknowledged, int permits)
            throws IOException, InterruptedException {
        if (!unacknowledged.tryAcquire(permits, ACKNOWLEDGEMENT_TIMEOUT.toMillis(), MILLISECONDS)) {
            IOException failure = readFailure;
            if (failure != null) {
                throw failure;
            }
            throw new IOException("No acknowledgement for " + ACKNOWLEDGEMENT_TIMEOUT);
        }
    }

    private void flush(OutputStream out) throws IOException {
        writing.lock();
        try {
            out.flush();
        } finally {
            writing.unlock();
        }
    }

    /// Reads the subscriber's packets until it disconnects or the connection closes.
    private void read(DataInputStream in, OutputStream out, Semaphore unacknowledged) {
        try {
            while (true) {
                Packet packet = Mqtt311.read(in);
                switch (packet.type()) {
                    case Mqtt311.PUBACK -> unacknowledged.release();
                    case Mqtt311.PINGREQ -> {
                        writing.lock();
                        try {
                            Mqtt311.writePingResp(out);
                            out.flush();
                        } finally {
                            writing.unlock();
                        }
                    }
                    case Mqtt311.DISCONNECT -> {
                        return;
                    }
                    default -> throw new IOException("Unexpected packet type " + packet.type());
                }
            }
        } catch (EOFException | SocketException _) {
            // The subscriber, or close(), ended the connection.
        } catch (IOException e) {
            readFailure = e;
        }
    }

    private static Packet expect(DataInputStream in, int type) throws IOException {
        Packet packet = Mqtt311.read(in);
        if (packet.type() != type) {
            throw new IOException("Expected packet type " + type + ", got " + packet.type());
        }
        return packet;
    }

    @Override
    public void close() throws IOException {
        try {
            Socket socket = connection;
            if (socket != null) {
                socket.close();
            }
        } finally {
            server.close();
        }
    }
}
