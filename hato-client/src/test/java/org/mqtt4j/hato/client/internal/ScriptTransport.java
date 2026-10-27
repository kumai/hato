package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Transport;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.RemainingLength;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A scripted transport for client tests: it records the packets of each send, completes sends
/// unless told not to, and lets a test feed packets and connection events to the client. A send
/// carries several packets when the client writes them together. Like a real transport, a
/// started one reports its close once; received bytes and that report are serialized. It hands
/// what it reads to the client in one of the two ways that the real transports do, which decides
/// the thread that runs the client's events of a read; see [Reads].
final class ScriptTransport implements Transport {

    /// How a transport hands what it reads to the client.
    enum Reads {
        /// As copied bytes, as the WebSocket transport does: the actor thread runs the events.
        COPIED,
        /// As reads of a buffer that it lends, as the TCP transport does: the thread that reads
        /// runs the events itself, unless a read fills the buffer.
        LENT
    }

    private final PacketCodec codec;
    private final Reads reads;

    final LinkedBlockingQueue<Packet> sent = new LinkedBlockingQueue<>();
    volatile @Nullable Listener listener;
    volatile boolean closed;
    volatile Runnable beforeSend = () -> { };
    volatile Consumer<Packet> afterSend = _ -> { };
    Runnable onStart = () -> { };
    boolean connectOnStart = true;
    volatile SendResult sendResult = SendResult.ACCEPTED;
    volatile boolean completeSends = true;
    volatile long pendingTicket;
    @Nullable Throwable startFailure;
    Duration startTimeout = Duration.ZERO;
    // When set, close() leaves its report to finishClose(), as a transport still closing would.
    volatile boolean deferCloseReport;

    /// A transport that speaks MQTT 3.1.1 and hands over copied bytes.
    ScriptTransport() {
        this(Reads.COPIED);
    }

    /// A transport that speaks MQTT 3.1.1 and hands over what it reads as `reads` says.
    ScriptTransport(Reads reads) {
        this(MqttVersion.V3_1_1, reads);
    }

    /// A transport that decodes sends and encodes received packets in `version`, and hands over
    /// copied bytes.
    ScriptTransport(MqttVersion version) {
        this(version, Reads.COPIED);
    }

    /// A transport that decodes sends and encodes received packets in `version`, and hands over
    /// what it reads as `reads` says.
    ScriptTransport(MqttVersion version, Reads reads) {
        codec = PacketCodec.of(version);
        this.reads = reads;
    }

    /// The client's listener. With lent reads, the bytes that a test hands to it arrive as lent
    /// reads too, so that a connection never mixes the two ways, which no real transport does.
    Listener receiver() {
        Listener current = requireNonNull(listener);
        return switch (reads) {
            case COPIED -> current;
            case LENT -> new Listener() {
                @Override
                public void onConnected() {
                    current.onConnected();
                }

                @Override
                public void onBytes(byte[] bytes) {
                    read(bytes, false);
                }

                @Override
                public void onSent(long ticket) {
                    current.onSent(ticket);
                }

                @Override
                public void onClosed(Throwable cause) {
                    current.onClosed(cause);
                }
            };
        };
    }

    /// Hands `packet` to the client as one read.
    void receive(Packet packet) {
        read(codec.encode(packet), false);
    }

    /// Hands `packet` to the client as one read whose events the actor thread runs: copied bytes,
    /// or a lent read that fills the buffer, which the thread that reads leaves to the actor
    /// thread. A test that blocks in those events holds the actor thread this way, rather than
    /// the thread that hands the packet over.
    void receiveForActor(Packet packet) {
        read(codec.encode(packet), true);
    }

    /// Lends `packet` to the client as one read that leaves room in its buffer, whichever way
    /// this transport reads otherwise.
    void lend(Packet packet) {
        lend(codec.encode(packet), false);
    }

    void accept() {
        receive(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));
    }

    private void read(byte[] bytes, boolean fillsBuffer) {
        switch (reads) {
            case COPIED -> {
                synchronized (this) {
                    requireNonNull(listener).onBytes(bytes);
                }
            }
            case LENT -> lend(bytes, fillsBuffer);
        }
    }

    /// Lends `bytes` to the client as one read, and then tells it, without this transport's
    /// monitor, that the read is over, as the TCP transport does.
    private void lend(byte[] bytes, boolean fillsBuffer) {
        BorrowingListener borrower;
        synchronized (this) {
            borrower = (BorrowingListener) requireNonNull(listener);
            byte[] buffer = fillsBuffer ? bytes : Arrays.copyOf(bytes, bytes.length + 1);
            borrower.onBorrowedBytes(buffer, 0, bytes.length);
        }
        borrower.afterBorrowedBytes();
    }

    synchronized void finishClose() {
        receiver().onClosed(new IOException("Transport closed"));
    }

    void completeSend() {
        long ticket = pendingTicket;
        pendingTicket = 0;
        receiver().onSent(ticket);
    }

    @Override
    public void start(URI endpoint, Duration timeout, Listener receiver) {
        startTimeout = timeout;
        listener = receiver;
        if (connectOnStart) {
            receiver.onConnected();
        }
        onStart.run();
        if (startFailure != null) {
            closed = true;
            receiver.onClosed(startFailure);
        }
    }

    @Override
    public SendResult send(long ticket, byte[] bytes) {
        beforeSend.run();
        if (closed) {
            return SendResult.CLOSED;
        }
        SendResult result = sendResult;
        if (result != SendResult.ACCEPTED) {
            return result;
        }
        if (pendingTicket != 0) {
            throw new IllegalStateException("A send is already outstanding");
        }
        pendingTicket = ticket;
        List<Packet> packets = decodeAll(codec, bytes);
        // Decide before a test can observe this send and change the flag for later sends.
        boolean complete = completeSends;
        sent.addAll(packets);
        if (complete) {
            completeSend();
        }
        packets.forEach(afterSend);
        return SendResult.ACCEPTED;
    }

    /// The packets that the bytes of one send carry, in order.
    static List<Packet> decodeAll(PacketCodec codec, byte[] bytes) {
        var packets = new ArrayList<Packet>();
        var input = ByteBuffer.wrap(bytes);
        while (input.hasRemaining()) {
            int start = input.position();
            input.get();
            int end = RemainingLength.read(input) + input.position();
            packets.add(codec.decode(Arrays.copyOfRange(bytes, start, end)));
            input.position(end);
        }
        return packets;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        Listener current = listener;
        if (current != null && !deferCloseReport) {
            current.onClosed(new IOException("Transport closed"));
        }
    }
}
