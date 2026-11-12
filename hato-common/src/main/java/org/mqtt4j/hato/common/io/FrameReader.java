package org.mqtt4j.hato.common.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// Reads complete MQTT Control Packets, including their fixed headers, from a byte stream.
///
/// The rules of framing are those of [FrameAssembler], which puts the packets together; this
/// reader waits for their bytes. Only framing is checked;
/// [org.mqtt4j.hato.common.protocol.PacketCodec] validates packet contents and version-specific
/// rules. Received bytes are preserved, including non-minimal Remaining Length encodings.
///
/// The input is borrowed and is never closed here. The owner serializes calls to [#read] and
/// closes the connection after a read failure; the stream may then be partway through a packet.
/// With [TcpConnection], closing the connection from another thread cancels a pending read.
/// No read-ahead, threads, or logging are added.
///
/// See [MQTT 3.1.1 Plus Errata 01, section 2.2: Fixed header](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html).
/// See also [MQTT 5.0, section 3.1.2.11.4: Maximum Packet Size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
public final class FrameReader {

    private final InputStream input;

    private final FrameAssembler frames;

    // The bytes on their way from the stream to the assembler: one byte of a fixed header, or a
    // part of a body.
    private final byte[] chunk = new byte[8 * 1024];

    /// Creates a reader with an inclusive packet size limit in bytes, including the fixed header.
    ///
    /// @throws IllegalArgumentException if `maxPacketSize` is not positive
    public FrameReader(InputStream input, int maxPacketSize) {
        this.input = requireNonNull(input, "input must not be null");
        frames = new FrameAssembler(maxPacketSize);
    }

    /// Replaces the size limit for the packets that later calls to [#read] return, as when a
    /// server announces a Maximum Packet Size in CONNACK. The owner calls it between reads.
    ///
    /// @throws IllegalArgumentException if `maxPacketSize` is not positive
    public void limit(int maxPacketSize) {
        frames.limit(maxPacketSize);
    }

    /// Blocks until one complete packet is available; returns empty only for EOF before it starts.
    ///
    /// The size limit is checked after reading the fixed header, before consuming any body bytes.
    /// Storage grows with received data, not the declared length. The next packet is not consumed.
    ///
    /// @throws EOFException if the stream ends partway through the header or body
    /// @throws IOException if the input fails
    /// @throws org.mqtt4j.hato.common.protocol.PacketDecodeException with reason
    ///         `PACKET_TOO_LARGE` if the size limit is exceeded, or `MALFORMED_PACKET` if
    ///         Remaining Length continues beyond four bytes
    public Optional<byte[]> read() throws IOException {
        int first = input.read();
        if (first == -1) {
            return Optional.empty();
        }

        // The fixed header goes to the assembler byte by byte, so that the stream is read no
        // further than the header needs. A packet without a body is complete with it.
        byte[] packet = headerByte(first);
        while (packet == null && frames.missing() == 0) {
            int next = input.read();
            if (next == -1) {
                throw new EOFException("Stream ended during the MQTT fixed header");
            }
            packet = headerByte(next);
        }

        // The body follows in parts, each no larger than what the packet still lacks.
        while (packet == null) {
            int wanted = Math.min(chunk.length, frames.missing());
            int count = input.readNBytes(chunk, 0, wanted);
            if (count < wanted) {
                throw new EOFException("Stream ended during the MQTT packet body");
            }
            packet = frames.next(ByteBuffer.wrap(chunk, 0, count));
        }
        return Optional.of(packet);
    }

    private byte @Nullable [] headerByte(int value) {
        chunk[0] = (byte) value;
        return frames.next(ByteBuffer.wrap(chunk, 0, 1));
    }
}
