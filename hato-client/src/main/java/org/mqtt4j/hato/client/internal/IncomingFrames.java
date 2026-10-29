package org.mqtt4j.hato.client.internal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.client.internal.common.protocol.RemainingLength;
import org.mqtt4j.hato.client.internal.common.protocol.TopicCache;

/// Adapts arbitrary transport chunks to the existing codec without buffering the whole stream.
/// A packet that lies whole within one chunk is decoded where it lies. A packet that spans
/// chunks is copied, once, into storage of its own as its bytes arrive:
///
/// - A PUBLISH whose headers come with its fixed header keeps them in an array of their own,
///   and its payload in another, which the decoded packet takes over.
/// - Any other packet goes into one array of its size.
///
/// A packet of at most [#UPFRONT_LIMIT] bytes gets that storage as soon as its fixed header
/// arrives. A larger packet keeps the parts received so far and joins them once its last byte
/// arrives, so that its storage follows the bytes received rather than the length that the
/// peer declares, which could claim memory without sending it.
///
/// A chunk is either handed over, and then kept as it is when a large packet needs parts of it,
/// or lent, and then copied from before the call returns. A serialized transport callback owns
/// this parser and at most one unfinished packet.
final class IncomingFrames {

    /// The largest packet that gets storage of its full size as soon as its fixed header arrives.
    /// A peer that declares a packet and sends no more of it makes a connection hold at most
    /// this much.
    static final int UPFRONT_LIMIT = 128 * 1024;

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

    // The headers of a PUBLISH that is read apart from its payload, which `frame` or the parts
    // then hold, or null.
    private byte @Nullable [] publishHeaders;

    // The storage of the packet being read, at its size, or of its payload when its headers are
    // kept apart. Null for a packet beyond UPFRONT_LIMIT, which keeps parts instead.
    private byte @Nullable [] frame;

    // The parts of the chunks that hold what has arrived so far of a packet beyond UPFRONT_LIMIT
    // after its fixed header, or after its headers when they are kept apart, in order of
    // arrival.
    private final List<Part> parts = new ArrayList<>();

    IncomingFrames(int maxPacketSize, PacketCodec codec, BiConsumer<Packet, Integer> receiver) {
        this.maxPacketSize = maxPacketSize;
        this.codec = codec;
        this.receiver = receiver;
    }

    /// Takes a chunk that the transport hands over and never reuses.
    void accept(byte[] bytes) {
        accept(bytes, 0, bytes.length, false);
    }

    /// Takes the `length` bytes of `buffer` from `offset`, which the transport only lends: it
    /// reuses the buffer once this returns, so nothing here keeps a reference to it.
    void acceptLent(byte[] buffer, int offset, int length) {
        accept(buffer, offset, offset + length, true);
    }

    // Reads the bytes of `bytes` from `from` to `to`, which `lent` says the transport reuses.
    private void accept(byte[] bytes, int from, int to, boolean lent) {
        // Where the fixed header of the packet being read starts in this chunk, or -1 when it
        // started in an earlier one.
        int start = -1;
        int offset = from;
        while (offset < to) {
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
                if (start >= 0 && length <= to - offset) {
                    // The whole packet lies in this chunk, so it is decoded there.
                    offset += length;
                    headerSize = 0;
                    receiver.accept(codec.decode(bytes, start, size, topics), size);
                    continue;
                }
                packetSize = size;
                position = headerSize;
                offset = begin(bytes, start, offset, to);
            }
            int count = Math.min(to - offset, packetSize - position);
            if (count > 0) {
                keep(bytes, offset, count, lent);
                offset += count;
                position += count;
            }
            if (position == packetSize) {
                finish();
            }
        }
    }

    /// Gives the packet whose fixed header has just completed storage of its own, as the class
    /// describes, and returns where its bytes continue in the chunk: `offset`, where the fixed
    /// header ends, unless the PUBLISH headers that follow it have been kept already. `start` is
    /// where the fixed header starts in the chunk, or -1 when it started in an earlier one, and
    /// the chunk ends at `to`.
    private int begin(byte[] bytes, int start, int offset, int to) {
        boolean upfront = packetSize <= UPFRONT_LIMIT;
        if (start >= 0 && (header[0] & 0xF0) == 0x30) {
            int headersSize = codec.publishHeadersSize(bytes, start, to - start);
            if (headersSize >= headerSize && headersSize <= packetSize) {
                publishHeaders = Arrays.copyOfRange(bytes, start, start + headersSize);
                if (upfront) {
                    frame = new byte[packetSize - headersSize];
                }
                position = headersSize;
                return start + headersSize;
            }
        }
        if (upfront) {
            var whole = new byte[packetSize];
            System.arraycopy(header, 0, whole, 0, headerSize);
            frame = whole;
        }
        return offset;
    }

    // Keeps the `count` bytes of the packet being read that lie in the chunk from `offset`.
    private void keep(byte[] bytes, int offset, int count, boolean lent) {
        byte[] storage = frame;
        if (storage != null) {
            byte[] headers = publishHeaders;
            int at = headers == null ? position : position - headers.length;
            System.arraycopy(bytes, offset, storage, at, count);
        } else if (lent) {
            parts.add(new Part(Arrays.copyOfRange(bytes, offset, offset + count), 0, count));
        } else {
            parts.add(new Part(bytes, offset, count));
        }
    }

    // Decodes the packet whose last byte has arrived, from the storage that kept it.
    private void finish() {
        int size = packetSize;
        byte[] headers = publishHeaders;
        byte[] storage = frame;
        byte[] kept = storage != null ? storage : assembled();
        clear();
        Packet packet = headers != null
                ? codec.decodePublish(headers, 0, headers.length, kept, topics)
                : codec.decode(kept, 0, kept.length, topics);
        receiver.accept(packet, size);
    }

    void clear() {
        parts.clear();
        headerSize = 0;
        packetSize = 0;
        position = 0;
        publishHeaders = null;
        frame = null;
    }

    /// The parts of the packet being read, copied once into one array: its payload when its
    /// headers are kept apart, and otherwise the whole packet from its fixed header.
    private byte[] assembled() {
        byte[] headers = publishHeaders;
        var kept = new byte[headers == null ? packetSize : packetSize - headers.length];
        int at = 0;
        if (headers == null) {
            System.arraycopy(header, 0, kept, 0, headerSize);
            at = headerSize;
        }
        for (Part part : parts) {
            System.arraycopy(part.chunk(), part.offset(), kept, at, part.length());
            at += part.length();
        }
        return kept;
    }

    /// The `length` bytes of `chunk` from `offset` that belong to the packet being read.
    private record Part(byte[] chunk, int offset, int length) {}
}
