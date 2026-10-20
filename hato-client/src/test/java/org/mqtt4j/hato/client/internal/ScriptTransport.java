package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.spi.Transport;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A scripted transport for client tests: it records decoded sends, completes them unless told
/// not to, and lets a test feed packets and connection events to the client. Like a real
/// transport, a started one reports its close once; received bytes and that report are
/// serialized.
final class ScriptTransport implements Transport {

    private final PacketCodec codec;

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

    /// A transport that speaks MQTT 3.1.1.
    ScriptTransport() {
        this(MqttVersion.V3_1_1);
    }

    /// A transport that decodes sends and encodes received packets in `version`.
    ScriptTransport(MqttVersion version) {
        codec = PacketCodec.of(version);
    }

    Listener receiver() {
        return requireNonNull(listener);
    }

    synchronized void receive(Packet packet) {
        receiver().onBytes(codec.encode(packet));
    }

    synchronized void accept() {
        receiver().onBytes(codec.encode(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY)));
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
        Packet packet = codec.decode(bytes);
        // Decide before a test can observe this send and change the flag for later sends.
        boolean complete = completeSends;
        sent.add(packet);
        if (complete) {
            completeSend();
        }
        afterSend.accept(packet);
        return SendResult.ACCEPTED;
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
