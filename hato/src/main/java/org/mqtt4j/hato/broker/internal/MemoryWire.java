package org.mqtt4j.hato.broker.internal;

import java.net.SocketAddress;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.internal.inproc.InprocLink;
import org.mqtt4j.hato.internal.io.SendQueue;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.PacketCodec;
import org.mqtt4j.hato.internal.protocol.PacketDecodeException;

// A connection with a client of the same JVM, which passes the packets themselves: they arrive as
// the client made them, with the bytes that they take on the wire, and leave as the broker makes
// them, deliveries included, so neither side encodes or decodes them. The limits count those
// bytes as they count the bytes of a socket's packets. The in-process loop serves the connection,
// and the link's queues stand for the buffers of a socket.
final class MemoryWire implements Wire {

    private final InprocLink link;

    private final InprocLoop loop;

    // The largest packet that the broker takes: the one for a CONNECT until the broker accepts
    // the client, and from then on the one for the client's packets.
    private int limit;

    // The packets of the write in progress, and how many of them the client has been given.
    private List<InprocLink.Item> writingItems = List.of();

    private int given;

    MemoryWire(InprocLink link, InprocLoop loop, Routing routing) {
        this.link = link;
        this.loop = loop;
        limit = Math.min(routing.settings.maxPacketSize(), routing.settings.maxConnectSize());
    }

    @Override
    public @Nullable SocketAddress remoteAddress() {
        return null;
    }

    @Override
    public Object peer() {
        return "an in-process client";
    }

    @Override
    public boolean inProcess() {
        return true;
    }

    @Override
    public boolean overTls() {
        return false;
    }

    @Override
    public List<X509Certificate> clientCertificates() {
        return List.of();
    }

    @Override
    public Loop loop() {
        return loop;
    }

    @Override
    public boolean register(Connection connection) {
        return loop.register(connection);
    }

    @Override
    public boolean inputWaiting() {
        return link.input() || link.clientEnded();
    }

    @Override
    public @Nullable Arrival next(boolean connected, MqttVersion version) {
        InprocLink.Item item = link.take();
        if (item == null) {
            return null;
        }
        if (item.size() > limit) {
            throw new PacketDecodeException(
                    ReasonCode.PACKET_TOO_LARGE,
                    "Packet of " + item.size() + " bytes exceeds the limit of " + limit
            );
        }
        if (!connected && !(item.packet() instanceof Packet.Connect)) {
            throw new PacketDecodeException(ReasonCode.PROTOCOL_ERROR, "Expected CONNECT");
        }
        return new Arrival(item.packet(), item.size());
    }

    @Override
    public int read() {
        if (link.input()) {
            return 1;
        }
        return link.clientEnded() ? -1 : 0;
    }

    // The link's queues wake the loop as input arrives, so the loop looks at it anyway.
    @Override
    public void expectInput() {
    }

    @Override
    public void endOutput() {
        link.endBroker();
    }

    @Override
    public int discard() {
        int count = 0;
        while (link.take() != null) {
            count++;
        }
        return (count == 0 && link.clientEnded()) ? -1 : count;
    }

    @Override
    public void limit(int maximumPacketSize) {
        limit = maximumPacketSize;
    }

    @Override
    public Outbound queued(Packet packet, MqttVersion version) {
        return new Outbound.Plain(packet, PacketCodec.of(version).size(packet));
    }

    @Override
    public void prepare(List<SendQueue.Send<Outbound>> sends, boolean skip, long now) {
        var items = new ArrayList<InprocLink.Item>(sends.size());
        for (SendQueue.Send<Outbound> send : sends) {
            switch (send.value()) {
                case Outbound.Plain(var packet, var size) -> {
                    items.add(new InprocLink.Item(packet, size));
                }
                case Outbound.Publish(var delivery, var sentBefore) -> {
                    if (!skip) {
                        items.add(new InprocLink.Item(
                                delivery.packet(sentBefore, now),
                                delivery.byteCount
                        ));
                    }
                }
                case Outbound.Encoded _, Outbound.End _ -> throw new IllegalStateException(
                        "An in-process connection queues no encoded packet"
                );
            }
        }
        writingItems = items;
        given = 0;
    }

    @Override
    public void prepareLast(Outbound last) {
        writingItems = switch (last) {
            case Outbound.Plain(var packet, var size) -> List.of(new InprocLink.Item(packet, size));
            case Outbound.End _ -> List.of();
            case Outbound.Encoded _, Outbound.Publish _ -> throw new IllegalStateException(
                    "A last packet is queued as it is: " + last
            );
        };
        given = 0;
    }

    @Override
    public boolean writing() {
        return !writingItems.isEmpty();
    }

    // Gives the client what its queue has room for. The client's reader wakes the loop as it
    // makes room for the rest.
    @Override
    public boolean write() {
        while (given < writingItems.size()) {
            if (!link.deliver(writingItems.get(given))) {
                return false;
            }
            given++;
        }
        writingItems = List.of();
        given = 0;
        return true;
    }

    // The link's queues wake the loop: the client's sends and the room that its reader makes.
    @Override
    public void interest(boolean read, boolean write) {
    }

    @Override
    public void close() {
        link.endBroker();
    }
}
