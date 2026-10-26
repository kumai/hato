package org.mqtt4j.hato.client.internal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.client.internal.common.protocol.RemainingLength;
import org.mqtt4j.hato.client.internal.common.protocol.TopicCache;

/// Adapts arbitrary transport chunks to the existing codec without buffering the whole stream.
/// A packet that lies whole within one chunk is decoded where it lies. A packet that spans
/// chunks keeps the parts of them that it has received, which the transport handed over and
/// never reuses, and is copied into one array once its last byte arrives, so its storage follows
/// the bytes received rather than the declared length. A serialized transport callback owns this
/// parser and at most one unfinished packet.
final class IncomingFrames {

    private final int maxPacketSize;

    private final PacketCodec codec;

    private final BiConsumer<Packet, Integer> receiver;

    // The connection's topic names, which most streams repeat, so that each is decoded and
    // checked once rather than for every PUBLISH.
    private final TopicCache topics = new TopicCache();

    // The fixed header of the packet being read, which may arrive over several chunks: its first
    // byte and at most four bytes of Remaining Length.
    private final byte[] header = new byte[5];

    private int headerSize;

    // The size of the packet whose fixed header is complete but whose body is not, or 0.
    private int packetSize;

    // The bytes of that packet received so far, counting its fixed header.
    private int position;

    // The parts of the chunks that hold the body received so far, in order of arrival.
    private final List<Part> parts = new ArrayList<>();

    IncomingFrames(int maxPacketSize, PacketCodec codec, BiConsumer<Packet, Integer> receiver) {
        this.maxPacketSize = maxPacketSize;
        this.codec = codec;
        this.receiver = receiver;
    }

    void accept(byte[] bytes) {
        // Where the fixed header of the packet being read starts in this chunk, or -1 when it
        // started in an earlier one.
        int start = -1;
        int offset = 0;
        while (offset < bytes.length) {
            if (packetSize == 0) {
                if (headerSize == 0) {
                    start = offset;
                }
                header[headerSize++] = bytes[offset++];
                if (headerSize == 1) {
                    continue;
                }
                if ((header[headerSize - 1] & 0x80) != 0 && headerSize < 5) {
                    continue;
                }
                int length = RemainingLength.read(ByteBuffer.wrap(header, 1, headerSize - 1));
                int size = headerSize + length;
                if (size > maxPacketSize) {
                    throw new PacketDecodeException(
                            ReasonCode.PACKET_TOO_LARGE,
                            "Incoming packet exceeds " + maxPacketSize + " bytes"
                    );
                }
                if (start >= 0 && length <= bytes.length - offset) {
                    // The whole packet lies in this chunk, so it is decoded there.
                    offset += length;
                    headerSize = 0;
                    receiver.accept(codec.decode(bytes, start, size, topics), size);
                    continue;
                }
                packetSize = size;
                position = headerSize;
            }
            int count = Math.min(bytes.length - offset, packetSize - position);
            if (count > 0) {
                parts.add(new Part(bytes, offset, count));
                offset += count;
                position += count;
            }
            if (position == packetSize) {
                byte[] packet = assembled();
                clear();
                receiver.accept(codec.decode(packet, 0, packet.length, topics), packet.length);
            }
        }
    }

    void clear() {
        parts.clear();
        headerSize = 0;
        packetSize = 0;
        position = 0;
    }

    /// The packet being read, copied once from its fixed header and the parts of its body.
    private byte[] assembled() {
        var packet = new byte[packetSize];
        System.arraycopy(header, 0, packet, 0, headerSize);
        int at = headerSize;
        for (Part part : parts) {
            System.arraycopy(part.chunk(), part.offset(), packet, at, part.length());
            at += part.length();
        }
        return packet;
    }

    /// The `length` bytes of `chunk` from `offset` that belong to the packet being read.
    private record Part(byte[] chunk, int offset, int length) {}
}
