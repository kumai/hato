package org.mqtt4j.hato.common.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// Writes complete, already encoded MQTT Control Packets to a byte stream.
///
/// The output is borrowed and is never closed here. The owner serializes calls to [#write] and
/// closes the connection after a write failure, which may have sent only part of a packet.
/// No buffering, threads, or logging are added.
public final class FrameWriter {

    private final OutputStream output;

    // The channel that `output` writes to, for gathering writes, or null.
    private final @Nullable GatheringByteChannel channel;

    /// Creates a writer using the borrowed output stream.
    public FrameWriter(OutputStream output) {
        this(output, null);
    }

    /// Creates a writer using the borrowed output stream and, for [#writeParts(ByteBuffer[])], the
    /// borrowed channel that the stream writes to, if any.
    public FrameWriter(OutputStream output, @Nullable GatheringByteChannel channel) {
        this.output = requireNonNull(output, "output must not be null");
        this.channel = channel;
    }

    /// Writes one complete packet with one [OutputStream#write(byte[])] call, then flushes.
    ///
    /// The caller supplies a complete packet from
    /// [org.mqtt4j.hato.common.protocol.PacketCodec#encode] or
    /// [org.mqtt4j.hato.common.protocol.PacketCodec#encodePublish].
    /// Its bytes are not validated here; do not modify the array during this call. A successful
    /// flush does not acknowledge peer receipt, and a failed write is not retried or flushed.
    ///
    /// @throws IOException if writing or flushing fails
    public void write(byte[] packet) throws IOException {
        requireNonNull(packet, "packet must not be null");
        output.write(packet);
        output.flush();
    }

    /// Writes one complete packet given in two parts, such as a PUBLISH header from
    /// [org.mqtt4j.hato.common.protocol.PacketCodec#encodePublishHeader] and its payload, with
    /// one [OutputStream#write(byte[])] call for each, then flushes. This spares joining a large
    /// payload to its header in a new array; the parts may leave in separate TCP segments.
    ///
    /// @throws IOException if writing or flushing fails
    public void write(byte[] head, byte[] rest) throws IOException {
        requireNonNull(head, "head must not be null");
        requireNonNull(rest, "rest must not be null");
        output.write(head);
        output.write(rest);
        output.flush();
    }

    /// Writes the bytes that remain in `parts`, which hold complete packets one after another, and
    /// flushes: with gathering writes of the channel, which spare joining the parts in a new
    /// array, or without a channel with one [OutputStream#write(byte[], int, int)] call for each
    /// part. Do not modify the parts during this call; their positions advance.
    ///
    /// @throws IOException if writing or flushing fails
    public void writeParts(ByteBuffer[] parts) throws IOException {
        requireNonNull(parts, "parts must not be null");
        if (channel == null) {
            for (ByteBuffer part : parts) {
                output.write(part.array(), part.arrayOffset() + part.position(), part.remaining());
                part.position(part.limit());
            }
            output.flush();
            return;
        }
        long remaining = 0;
        for (ByteBuffer part : parts) {
            remaining += part.remaining();
        }
        // A blocking channel writes everything it is given, unless the operating system takes
        // fewer buffers at once than the parts, so the loop goes on until nothing remains.
        while (remaining > 0) {
            remaining -= channel.write(parts);
        }
    }
}
