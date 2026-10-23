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
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.comparison.Mqtt311.Packet;
import org.mqtt4j.hato.comparison.Mqtt311.Subscribe;

/// A broker stand-in for one subscriber, so that the time measured is the client's alone.
///
/// It accepts one connection, grants the subscriptions of its first SUBSCRIBE, and then sends
/// the messages of its [Traffic] through a [PacedSender]. QoS 1 messages go out with at most the
/// traffic's window unacknowledged, the way RabbitMQ sends them under its MQTT prefetch, which is
/// 10 by default. QoS 0 messages go out as the socket takes them, so a client that stops reading
/// holds them back, and one that reads on regardless may drop some. The stand-in answers PINGREQ
/// and does no other broker work.
final class StubBroker implements AutoCloseable {

    private final ServerSocket server;

    private final Traffic traffic;

    // Sending thread and reading thread both write: messages and PINGRESP.
    private final ReentrantLock writing = new ReentrantLock();

    private volatile @Nullable Socket connection;

    private volatile @Nullable PacedSender sender;

    private volatile @Nullable IOException readFailure;

    StubBroker(Traffic traffic) throws IOException {
        this.server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        this.traffic = traffic;
    }

    int port() {
        return server.getLocalPort();
    }

    /// See [PacedSender#maxLatenessMicros()]; 0 before sending starts.
    long maxLatenessMicros() {
        PacedSender sending = sender;
        return sending == null ? 0 : sending.maxLatenessMicros();
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

        var sending = new PacedSender(traffic, out, writing);
        sender = sending;
        // The subscriber's packets arrive on a thread of their own while this one sends.
        Thread.ofPlatform()
                .name("stub-broker-reader")
                .daemon()
                .start(() -> read(in, out, sending));
        try {
            sending.send(true);
        } catch (IOException e) {
            // A failed read explains a failed send better than the send does.
            IOException failure = readFailure;
            throw failure != null ? failure : e;
        }
        IOException failure = readFailure;
        if (failure != null) {
            throw failure;
        }
    }

    /// Reads the subscriber's packets until it disconnects or the connection closes.
    private void read(DataInputStream in, OutputStream out, PacedSender sending) {
        try {
            while (true) {
                Packet packet = Mqtt311.read(in);
                switch (packet.type()) {
                    case Mqtt311.PUBACK -> sending.acknowledged();
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
