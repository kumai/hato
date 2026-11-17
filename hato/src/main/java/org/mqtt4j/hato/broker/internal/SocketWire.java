package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.cert.X509Certificate;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.internal.io.FrameAssembler;
import org.mqtt4j.hato.internal.io.SendQueue;
import org.mqtt4j.hato.internal.io.WriteParts;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.PacketCodec;

import static java.util.Objects.requireNonNull;

// A network connection's socket, which the selector that it is assigned to reads and writes.
// Packets arrive as bytes that it puts together and decodes, and leave encoded: a packet as it is
// queued, and a delivery as it is written, its headers before the payload that it shares with the
// publication's deliveries to other sessions. A connection to the TLS listener has those bytes
// pass through TLS on the way (see TlsChannel).
final class SocketWire implements Wire {

    private final Socket socket;

    // Where the socket connects from, kept so that logs need not ask the socket each time.
    private final @Nullable SocketAddress remote;

    private final Reactor reactor;

    // The TLS between the socket and the bytes of the packets, or null for a plain connection.
    private final @Nullable TlsChannel tls;

    // What has arrived and not been taken, ready to be read from. Through TLS, it has room for
    // the bytes of a whole record.
    private final ByteBuffer input;

    // Puts the packets together from the input. Its size limit is the one for a CONNECT until
    // the broker accepts the client, and from then on the one for the client's packets.
    private final FrameAssembler frames;

    private ByteBuffer[] writingParts = new ByteBuffer[0];

    // The connection that this wire carries, once it has registered.
    private @Nullable Connection connection;

    // Only the selector sets this, after OP_READ, and clears it after a zero-byte read.
    boolean readable;

    @Nullable SelectionKey key;

    // A wire for `socket`, through TLS when it comes from `listener`, or plain for null.
    SocketWire(Socket socket, Routing routing, @Nullable TlsListener listener) throws IOException {
        this.socket = socket;
        remote = socket.getRemoteSocketAddress();
        socket.setTcpNoDelay(true);
        frames = new FrameAssembler(
                Math.min(routing.settings.maxPacketSize(), routing.settings.maxConnectSize())
        );
        reactor = routing.reactors[Math.floorMod(
                routing.nextReactor.getAndIncrement(),
                routing.reactors.length
        )];
        socket.getChannel().configureBlocking(false);
        if (listener == null) {
            tls = null;
            input = ByteBuffer.allocate(8192).flip();
        } else {
            tls = new TlsChannel(
                    listener.engine(remote),
                    socket.getChannel(),
                    reactor,
                    listener,
                    peer()
            );
            input = ByteBuffer.allocate(tls.inputCapacity()).flip();
        }
    }

    SocketChannel channel() {
        return socket.getChannel();
    }

    // The connection that this wire carries, for its selector.
    Connection connection() {
        return requireNonNull(connection, "registered");
    }

    @Override
    public @Nullable SocketAddress remoteAddress() {
        return remote;
    }

    @Override
    public Object peer() {
        return remote == null ? "an unconnected socket" : remote;
    }

    @Override
    public boolean inProcess() {
        return false;
    }

    @Override
    public boolean overTls() {
        return tls != null;
    }

    @Override
    public List<X509Certificate> clientCertificates() {
        return (tls == null) ? List.of() : tls.clientCertificates();
    }

    @Override
    public Loop loop() {
        return reactor;
    }

    @Override
    public boolean register(Connection connection) {
        this.connection = connection;
        if (tls != null) {
            tls.attach(connection);
        }
        return reactor.register(connection, this);
    }

    @Override
    public boolean inputWaiting() {
        return readable || input.hasRemaining() || (tls != null && tls.wantsTurn());
    }

    @Override
    public @Nullable Arrival next(boolean connected, MqttVersion version) {
        while (input.hasRemaining()) {
            byte[] bytes = frames.next(input);
            if (bytes != null) {
                Packet packet = connected
                        ? PacketCodec.of(version).decode(bytes)
                        : PacketCodec.decodeConnect(bytes, Connection.SUPPORTED_VERSIONS);
                return new Arrival(packet, bytes.length);
            }
        }
        return null;
    }

    @Override
    public int read() throws IOException {
        input.compact();
        int count;
        try {
            count = (tls == null) ? socket.getChannel().read(input) : tls.read(input);
        } finally {
            input.flip();
        }
        if (count == 0) {
            readable = false;
        }
        return count;
    }

    @Override
    public void expectInput() {
        readable = true;
    }

    @Override
    public void endOutput() throws IOException {
        socket.getChannel().shutdownOutput();
    }

    @Override
    public int discard() throws IOException {
        input.clear();
        int count;
        try {
            count = socket.getChannel().read(input);
        } finally {
            input.clear().flip();
        }
        return count;
    }

    @Override
    public void limit(int maximumPacketSize) {
        frames.limit(maximumPacketSize);
    }

    @Override
    public Outbound queued(Packet packet, MqttVersion version) {
        return new Outbound.Encoded(PacketCodec.of(version).encode(packet));
    }

    // The parts of the one write that carries `sends`: the bytes of each packet that was queued
    // encoded, and the headers of each delivery, prepared now, before its payload. The
    // deliveries that `skip` leaves unsent have none.
    @Override
    public void prepare(List<SendQueue.Send<Outbound>> sends, boolean skip, long now) {
        var parts = new WriteParts();
        for (SendQueue.Send<Outbound> send : sends) {
            switch (send.value()) {
                case Outbound.Encoded(var bytes) -> parts.add(bytes);
                case Outbound.Publish(var delivery, var sentBefore) -> {
                    if (!skip) {
                        parts.add(delivery.header(sentBefore, now));
                        parts.add(delivery.payload());
                    }
                }
                case Outbound.Plain _, Outbound.End _ -> throw new IllegalStateException(
                        "A socket queues every packet encoded"
                );
            }
        }
        writingParts = parts.toArray();
    }

    @Override
    public void prepareLast(Outbound last) {
        writingParts = switch (last) {
            case Outbound.Encoded(var bytes) -> new ByteBuffer[] {ByteBuffer.wrap(bytes)};
            case Outbound.End _ -> new ByteBuffer[0];
            case Outbound.Plain _, Outbound.Publish _ -> throw new IllegalStateException(
                    "A last packet is queued encoded: " + last
            );
        };
    }

    @Override
    public boolean writing() {
        return writingParts.length > 0;
    }

    @Override
    public boolean write() throws IOException {
        if (writingParts.length > 0) {
            if (tls != null) {
                if (!tls.write(writingParts)) {
                    return false;
                }
            } else {
                writeParts();
                for (ByteBuffer part : writingParts) {
                    if (part.hasRemaining()) {
                        return false;
                    }
                }
            }
        }
        writingParts = new ByteBuffer[0];
        return true;
    }

    // A gathering write copies heap buffers into temporary direct buffers. Cap the part of
    // a large payload passed to each call, so a partial write does not copy its whole tail.
    private void writeParts() throws IOException {
        for (int i = 0; i < writingParts.length; i++) {
            ByteBuffer part = writingParts[i];
            if (part.remaining() > Connection.BATCH_BYTES) {
                int limit = part.limit();
                part.limit(part.position() + Connection.BATCH_BYTES);
                try {
                    if (socket.getChannel().write(writingParts, 0, i + 1) > 0) {
                        connection().wake();
                    }
                } finally {
                    part.limit(limit);
                }
                return;
            }
        }
        socket.getChannel().write(writingParts);
    }

    @Override
    public void interest(boolean read, boolean write) {
        SelectionKey current = key;
        if (current != null && current.isValid()) {
            int interests;
            if (tls != null) {
                interests = tls.interest(read, write);
            } else {
                interests = read ? SelectionKey.OP_READ : 0;
                if (write) {
                    interests |= SelectionKey.OP_WRITE;
                }
            }
            current.interestOps(interests);
        }
    }

    @Override
    public void close() throws IOException {
        SelectionKey current = key;
        if (current != null) {
            current.cancel();
        }
        if (tls != null) {
            tls.close();
        }
        socket.close();
    }
}
