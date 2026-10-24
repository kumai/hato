package org.mqtt4j.hato.common.io;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

/// Writes complete, already encoded MQTT Control Packets to a byte stream.
///
/// The output is borrowed and is never closed here. The owner serializes calls to [#write] and
/// closes the connection after a write failure, which may have sent only part of a packet.
/// No buffering, threads, or logging are added.
public final class FrameWriter {

    private final OutputStream output;

    /// Creates a writer using the borrowed output stream.
    public FrameWriter(OutputStream output) {
        this.output = requireNonNull(output, "output must not be null");
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
}
