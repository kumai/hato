package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.internal.io.SendQueue;
import org.mqtt4j.hato.internal.protocol.Packet;

// What carries the packets of one connection. The connection keeps the protocol and its state;
// the wire takes the packets in and puts them out, in the form that it gives them on the way.
// The connection's loop calls every method but queued(), which a session's actor calls too,
// under the connection's monitor.
sealed interface Wire permits SocketWire, MemoryWire {

    // Where the client connects from over a network, or null.
    @Nullable SocketAddress remoteAddress();

    // The peer, as logs name it.
    Object peer();

    // Whether the client runs in the same JVM.
    boolean inProcess();

    // The loop that serves the connection.
    Loop loop();

    // Has the loop serve `connection`, which this wire carries, from now on. Returns false if
    // the loop has closed.
    boolean register(Connection connection);

    // Whether input waits: what has arrived and not been taken, or the loop's word that more
    // can be read.
    boolean inputWaiting();

    // The next packet that has arrived whole, and the bytes that it took, or null until more
    // arrives. Until the broker has accepted a CONNECT on the connection, as `connected` says,
    // the packet must be a CONNECT, and one of a version that the broker does not support throws
    // UnsupportedProtocolVersionException; afterwards it is of `version`. A packet that is not
    // what the protocol allows there throws PacketDecodeException, after which the input has no
    // packet boundary any more.
    @Nullable Arrival next(boolean connected, MqttVersion version);

    // Takes in what has arrived since the last call. Returns how much, 0 if nothing, or -1 at
    // the end of the input.
    int read() throws IOException;

    // Has the loop's next look at the connection read, whether or not the loop has seen input
    // arrive since the last read.
    void expectInput();

    // Ends the output, after the writes so far, while the input goes on.
    void endOutput() throws IOException;

    // Takes in what has arrived and drops it. Returns how much, 0 if nothing, or -1 at the end
    // of the input.
    int discard() throws IOException;

    // Refuses the packets from now on that are larger than `maximumPacketSize`.
    void limit(int maximumPacketSize);

    // The form in which `packet`, of `version`, waits in the send queue.
    Outbound queued(Packet packet, MqttVersion version);

    // Prepares the write of `sends`, the deliveries among them at `now`, and leaves the
    // deliveries out when `skip`.
    void prepare(List<SendQueue.Send<Outbound>> sends, boolean skip, long now);

    // Prepares the write of the last packet, after which the connection closes; nothing for the
    // end of the output without a packet.
    void prepareLast(Outbound last);

    // Whether some of the prepared write has yet to go out.
    boolean writing();

    // Writes what it can of the prepared write. Returns true once all of it has gone out.
    boolean write() throws IOException;

    // Tells the loop what the connection waits for: input when `read`, and room for the rest of
    // a write when `write`.
    void interest(boolean read, boolean write);

    // Lets go of what carries the packets, once the connection has ended.
    void close() throws IOException;

    // A packet that has arrived, and the bytes that it took.
    record Arrival(Packet packet, int size) {}
}
