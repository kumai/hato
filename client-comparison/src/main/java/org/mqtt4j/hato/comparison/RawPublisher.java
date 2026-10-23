package org.mqtt4j.hato.comparison;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.comparison.Mqtt311.Packet;

/// A publisher for runs against a real broker. It speaks the comparison's own codec, so that the
/// traffic that the subscribers receive does not depend on any client under comparison, and it
/// sends through a [PacedSender], so that each message carries the time that it was due.
final class RawPublisher implements AutoCloseable {

    private final Socket socket;

    private final DataInputStream in;

    private final BufferedOutputStream out;

    // The sending thread writes messages, and close() writes DISCONNECT.
    private final ReentrantLock writing = new ReentrantLock();

    private volatile @Nullable IOException readFailure;

    private volatile @Nullable PacedSender sender;

    /// Connects to the broker at `host` and `port` with a clean session.
    RawPublisher(String host, int port, String clientId) throws IOException {
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
        Mqtt311.writeConnect(out, clientId);
        out.flush();
        Mqtt311.checkConnAck(Mqtt311.read(in));
    }

    /// Publishes every message of `traffic`, and returns once the broker has acknowledged every
    /// QoS 1 message, or with `awaitAcknowledgements` false once all are written, with the most
    /// that a message went out after it was due, in microseconds. The broker holds the publisher
    /// back only through the traffic's window.
    long publish(Traffic traffic, boolean awaitAcknowledgements)
            throws IOException, InterruptedException {
        var sending = new PacedSender(traffic, out, writing);
        sender = sending;
        Thread.ofPlatform()
                .name("raw-publisher-reader")
                .daemon()
                .start(() -> read(sending));
        try {
            sending.send(awaitAcknowledgements);
        } catch (IOException e) {
            IOException failure = readFailure;
            throw failure != null ? failure : e;
        }
        return sending.maxLatenessMicros();
    }

    /// How far publishing has got, as [PacedSender#progress()] tells it, with any failure to read
    /// the broker's answers.
    String progress() {
        PacedSender sending = sender;
        IOException failure = readFailure;
        return (sending == null ? "not started" : sending.progress())
                + (failure == null ? "" : ", reading failed: " + failure);
    }

    /// Reads the broker's packets until the connection closes.
    private void read(PacedSender sender) {
        try {
            while (true) {
                Packet packet = Mqtt311.read(in);
                switch (packet.type()) {
                    case Mqtt311.PUBACK -> sender.acknowledged();
                    case Mqtt311.PINGRESP -> {
                        // Keep-alive is off; a broker that pings anyway needs no answer.
                    }
                    default -> throw new IOException("Unexpected packet type " + packet.type());
                }
            }
        } catch (EOFException | SocketException _) {
            // close(), or the broker, ended the connection.
        } catch (IOException e) {
            readFailure = e;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            writing.lock();
            try {
                Mqtt311.writeDisconnect(out);
                out.flush();
            } finally {
                writing.unlock();
            }
        } finally {
            socket.close();
        }
    }
}
