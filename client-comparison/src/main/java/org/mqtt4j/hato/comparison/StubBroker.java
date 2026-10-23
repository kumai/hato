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
/// the messages of its [Traffic], numbered from 0. QoS 1 messages go out with at most `window`
/// unacknowledged, the way RabbitMQ sends them under its MQTT prefetch, which is 10 by default.
/// QoS 0 messages go out as the socket takes them, so a client that stops reading holds them back,
/// and one that reads on regardless may drop some. The stand-in answers PINGREQ and does no other
/// broker work.
///
/// Without a rate, the messages go out as fast as the subscriber takes them, each carrying the
/// time that it was written. With a rate, message `n` is due `n / rate` seconds after the first
/// and carries the time that it was due. A message that a full window or a client that stopped
/// reading held back thus counts its wait as latency, as it would wait in a broker's queue,
/// instead of hiding it.
final class StubBroker implements AutoCloseable {

    // How long a subscriber may leave every message of the window unacknowledged.
    private static final Duration ACKNOWLEDGEMENT_TIMEOUT = Duration.ofMinutes(1);

    private final ServerSocket server;

    private final Traffic traffic;

    // Sending thread and reading thread both write: messages and PINGRESP.
    private final ReentrantLock writing = new ReentrantLock();

    private volatile @Nullable Socket connection;

    private volatile @Nullable IOException readFailure;

    // The most that a paced message went out after it was due, in microseconds.
    private volatile long maxLatenessMicros;

    StubBroker(Traffic traffic) throws IOException {
        this.server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        this.traffic = traffic;
    }

    /// What the stand-in sends: `count` messages of `payloadSize` bytes to `topic` at `qos`, with
    /// at most `window` of them unacknowledged at QoS 1, at `rate` messages per second, or as fast
    /// as the subscriber takes them when `rate` is 0.
    record Traffic(String topic, int qos, int payloadSize, int window, long count, long rate) {

        boolean paced() {
            return rate > 0;
        }
    }

    int port() {
        return server.getLocalPort();
    }

    /// The most that a paced message went out after it was due so far, in microseconds: 0 for
    /// traffic without a rate.
    ///
    /// A message goes out late when the subscriber holds the stand-in back, through a full window
    /// or by not reading, and also when the stand-in itself does not get to run. A QoS 0 run
    /// against a client that keeps reading shows the stand-in alone, and so how much of the
    /// latencies measured the stand-in can account for.
    long maxLatenessMicros() {
        return maxLatenessMicros;
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
        var unacknowledged = new Semaphore(traffic.window());
        Thread.ofPlatform()
                .name("stub-broker-reader")
                .daemon()
                .start(() -> read(in, out, unacknowledged));
        send(out, unacknowledged);
        if (traffic.qos() > 0) {
            // Every permit back means every message acknowledged.
            await(unacknowledged, traffic.window());
        }
        IOException failure = readFailure;
        if (failure != null) {
            throw failure;
        }
    }

    /// Sends every message. Each pass writes the messages that are due, all of them when the
    /// traffic has no rate, and then flushes them, so that a paced message goes out when it is
    /// due.
    ///
    /// Message `n` is due `n / rate` seconds after the start on the wall clock, the clock that
    /// the subscriber reads latency from: a schedule kept on another clock and converted would
    /// carry any delay between the two first readings into every latency. Between messages the
    /// sender spins rather than parks, so that no message goes out late by however long the
    /// scheduler oversleeps; with a rate, the stand-in keeps one processor busy.
    private void send(OutputStream out, Semaphore unacknowledged)
            throws IOException, InterruptedException {
        var frame = new PublishFrame(traffic.topic(), traffic.qos(), traffic.payloadSize());
        long startMicros = Payloads.nowMicros();
        int packetId = 0;
        long sequence = 0;
        while (sequence < traffic.count()) {
            long due = traffic.count();
            if (traffic.paced()) {
                long elapsed = Payloads.nowMicros() - startMicros;
                due = Math.min(due, elapsed * traffic.rate() / 1_000_000 + 1);
                if (sequence == due) {
                    Thread.onSpinWait();
                    continue;
                }
                // The first message due in this pass is the one that has waited longest.
                long lateness = elapsed - sequence * 1_000_000 / traffic.rate();
                if (lateness > maxLatenessMicros) {
                    maxLatenessMicros = lateness;
                }
            }
            for (; sequence < due; sequence++) {
                if (traffic.qos() > 0) {
                    if (!unacknowledged.tryAcquire()) {
                        // The subscriber cannot acknowledge what is still in the buffer.
                        flush(out);
                        await(unacknowledged, 1);
                    }
                    packetId = packetId % 65_535 + 1;
                }
                long sentMicros = traffic.paced()
                        ? startMicros + sequence * 1_000_000 / traffic.rate()
                        : Payloads.nowMicros();
                writing.lock();
                try {
                    frame.write(out, packetId, sequence, sentMicros);
                } finally {
                    writing.unlock();
                }
            }
            flush(out);
        }
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
