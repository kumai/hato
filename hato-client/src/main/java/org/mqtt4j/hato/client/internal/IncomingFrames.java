package org.mqtt4j.hato.client.internal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.BiConsumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.client.internal.common.protocol.RemainingLength;

/// Adapts arbitrary transport chunks to the existing codec without buffering the whole stream.
/// A serialized transport callback owns this parser and at most one unfinished packet.
final class IncomingFrames {

    private final int maxPacketSize;

    private final PacketCodec codec;

    private final BiConsumer<Packet, Integer> receiver;

    private final byte[] header = new byte[5];

    private int headerSize;

    private byte @Nullable [] body;

    private int packetSize;

    private int position;

    IncomingFrames(int maxPacketSize, PacketCodec codec, BiConsumer<Packet, Integer> receiver) {
        this.maxPacketSize = maxPacketSize;
        this.codec = codec;
        this.receiver = receiver;
    }

    void accept(byte[] bytes) {
        int offset = 0;
        while (offset < bytes.length) {
            byte[] packet = body;
            if (packet == null) {
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
                packetSize = size;
                packet = Arrays.copyOf(header,
                        headerSize + Math.min(length, bytes.length - offset));
                body = packet;
                position = headerSize;
            }
            int count = Math.min(bytes.length - offset, packetSize - position);
            if (count > packet.length - position) {
                // Grow for bytes already received; doubling avoids copying for every fragment.
                packet = Arrays.copyOf(packet,
                        Math.min(packetSize, Math.max(position + count, packet.length * 2)));
                body = packet;
            }
            System.arraycopy(bytes, offset, packet, position, count);
            offset += count;
            position += count;
            if (position == packetSize) {
                clear();
                receiver.accept(codec.decode(packet), packet.length);
            }
        }
    }

    void clear() {
        body = null;
        packetSize = 0;
        headerSize = 0;
        position = 0;
    }
}
