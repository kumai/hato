package org.mqtt4j.hato.common.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

import org.mqtt4j.hato.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.common.protocol.RemainingLength;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.common.value.ReasonCode.PACKET_TOO_LARGE;

/// Reads complete MQTT Control Packets, including their fixed headers, from a byte stream.
///
/// Only framing is checked here; [org.mqtt4j.hato.common.protocol.PacketCodec] validates packet
/// contents and version-specific rules. Received bytes are preserved, including non-minimal
/// Remaining Length encodings.
///
/// The input is borrowed and is never closed here. The owner serializes calls to [#read] and
/// closes the connection after a read failure; the stream may then be partway through a packet.
/// With [TcpConnection], closing the connection from another thread cancels a pending read.
/// No read-ahead, threads, or logging are added.
///
/// See [MQTT 3.1.1 Plus Errata 01, section 2.2: Fixed header](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html).
/// See also [MQTT 5.0, section 3.1.2.11.4: Maximum Packet Size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
public final class FrameReader {

    private static final int INITIAL_CAPACITY = 16 * 1024;

    private final InputStream input;

    private final int maxPacketSize;

    /// Creates a reader with an inclusive packet size limit in bytes, including the fixed header.
    ///
    /// @throws IllegalArgumentException if `maxPacketSize` is not positive
    public FrameReader(InputStream input, int maxPacketSize) {
        this.input = requireNonNull(input, "input must not be null");
        if (maxPacketSize <= 0) {
            throw new IllegalArgumentException("Maximum packet size must be positive");
        }
        this.maxPacketSize = maxPacketSize;
    }

    /// Blocks until one complete packet is available; returns empty only for EOF before it starts.
    ///
    /// The size limit is checked after reading the fixed header, before consuming any body bytes.
    /// Storage grows with received data, not the declared length. The next packet is not consumed.
    ///
    /// @throws EOFException if the stream ends partway through the header or body
    /// @throws IOException if the input fails
    /// @throws PacketDecodeException with reason `PACKET_TOO_LARGE` if the size limit is exceeded,
    ///         or `MALFORMED_PACKET` if Remaining Length continues beyond four bytes
    public Optional<byte[]> read() throws IOException {
        int first = input.read();
        if (first == -1) {
            return Optional.empty();
        }

        // Scan at most four length bytes; RemainingLength owns their decoding and validation.
        var header = new byte[5];
        header[0] = (byte) first;
        int headerSize = 1;
        while (headerSize < header.length) {
            int digit = input.read();
            if (digit == -1) {
                throw new EOFException("Stream ended during the MQTT fixed header");
            }
            header[headerSize++] = (byte) digit;
            if ((digit & 0x80) == 0) {
                break;
            }
        }
        int remaining = RemainingLength.read(ByteBuffer.wrap(header, 1, headerSize - 1));
        int packetSize = headerSize + remaining;
        if (packetSize > maxPacketSize) {
            throw new PacketDecodeException(
                    PACKET_TOO_LARGE,
                    "MQTT packet size " + packetSize + " exceeds maximum " + maxPacketSize
            );
        }

        byte[] packet = Arrays.copyOf(header, Math.min(packetSize, INITIAL_CAPACITY));
        int received = headerSize;
        while (received < packetSize) {
            if (received == packet.length) {
                // Grow only after filling the buffer; doubling keeps total copying linear.
                packet = Arrays.copyOf(packet, Math.min(packetSize, packet.length * 2));
            }
            received += input.readNBytes(packet, received, packet.length - received);
            if (received < packet.length) {
                throw new EOFException("Stream ended during the MQTT packet body");
            }
        }
        return Optional.of(packet);
    }
}
