package org.mqtt4j.hato.common.io;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.common.protocol.RemainingLength;

import static org.mqtt4j.hato.common.value.ReasonCode.PACKET_TOO_LARGE;

/// Assembles complete MQTT Control Packets, including their fixed headers, from bytes that
/// arrive in pieces of any size.
///
/// Only framing is checked here; [org.mqtt4j.hato.common.protocol.PacketCodec] validates packet
/// contents and version-specific rules. Received bytes are preserved, including non-minimal
/// Remaining Length encodings.
///
/// The size limit is checked as the fixed header completes, before any byte of the body is
/// taken. Storage grows with the bytes received, not with the length that the peer declares,
/// which could claim memory without sending it.
///
/// One owner serializes the calls. After a failure the bytes that follow are no packet
/// boundary any more, so the owner ends the connection.
///
/// See [MQTT 3.1.1 Plus Errata 01, section 2.2: Fixed header](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html).
/// See also [MQTT 5.0, section 3.1.2.11.4: Maximum Packet Size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
public final class FrameAssembler {

    // What a packet gets as its fixed header completes, unless it is smaller or more of it
    // has arrived already.
    private static final int INITIAL_CAPACITY = 16 * 1024;

    private int maxPacketSize;

    // The fixed header of the packet being assembled, as far as it has arrived: its first byte
    // and at most four bytes of Remaining Length.
    private final byte[] header = new byte[5];

    private int headerSize;

    // The packet whose fixed header is complete, in storage that grows up to its size, or null
    // while no fixed header is complete.
    private byte @Nullable [] packet;

    private int packetSize;

    // The bytes of that packet that have arrived, counting its fixed header.
    private int received;

    /// Creates an assembler with an inclusive packet size limit in bytes, which counts the
    /// fixed header.
    ///
    /// @throws IllegalArgumentException if `maxPacketSize` is not positive
    public FrameAssembler(int maxPacketSize) {
        this.maxPacketSize = checkedLimit(maxPacketSize);
    }

    /// Replaces the size limit for the packets whose fixed headers complete from now on, as
    /// when a Maximum Packet Size has been announced.
    ///
    /// @throws IllegalArgumentException if `maxPacketSize` is not positive
    public void limit(int maxPacketSize) {
        this.maxPacketSize = checkedLimit(maxPacketSize);
    }

    private static int checkedLimit(int maxPacketSize) {
        if (maxPacketSize <= 0) {
            throw new IllegalArgumentException("Maximum packet size must be positive");
        }
        return maxPacketSize;
    }

    /// How many bytes the packet being assembled still lacks, or 0 while no fixed header is
    /// complete: at a packet boundary, and partway through a fixed header.
    public int missing() {
        return (packet == null) ? 0 : packetSize - received;
    }

    /// Takes bytes from `input` up to the end of the packet being assembled, and never beyond.
    /// Returns that packet once its last byte has arrived, in an array of exactly its size
    /// that the caller owns, and null when the input ran out first; the next call goes on.
    ///
    /// @throws PacketDecodeException with reason `PACKET_TOO_LARGE` if the packet exceeds the
    ///         size limit, or `MALFORMED_PACKET` if Remaining Length continues beyond four bytes
    public byte @Nullable [] next(ByteBuffer input) {
        // Null stands for no packet yet, without an Optional: a broker's selector calls this
        // for every packet that it receives.
        byte[] storage = packet;
        if (storage == null) {
            // A fixed header ends with the first length byte that has no continuation flag.
            // The fourth length byte ends it in any case: RemainingLength rejects a flag there.
            do {
                if (!input.hasRemaining()) {
                    return null;
                }
                header[headerSize++] = input.get();
            } while (headerSize == 1
                    || ((header[headerSize - 1] & 0x80) != 0 && headerSize < header.length));
            int headerLength = headerSize;
            // Whatever comes of this header, the next one starts afresh.
            headerSize = 0;
            int size = headerLength
                    + RemainingLength.read(ByteBuffer.wrap(header, 1, headerLength - 1));
            if (size > maxPacketSize) {
                throw new PacketDecodeException(
                        PACKET_TOO_LARGE,
                        "MQTT packet size " + size + " exceeds maximum " + maxPacketSize
                );
            }
            // The copy can carry bytes of an earlier, longer header behind this one; the body
            // overwrites them before the packet is complete.
            storage = Arrays.copyOf(header, capacity(size, headerLength, input.remaining()));
            packet = storage;
            packetSize = size;
            received = headerLength;
        }
        int count = Math.min(input.remaining(), packetSize - received);
        if (received + count > storage.length) {
            storage = Arrays.copyOf(storage, capacity(packetSize, received, count));
            packet = storage;
        }
        input.get(storage, received, count);
        received += count;
        if (received < packetSize) {
            return null;
        }
        packet = null;
        return storage;
    }

    // How large the storage of a packet of `size` bytes becomes when `arriving` bytes follow
    // the `kept` ones that it holds: room for those, and at least the initial capacity or
    // twice the bytes kept, so that growing copies each byte a bounded number of times, but
    // never more than the packet takes.
    private static int capacity(int size, int kept, int arriving) {
        long wanted = Math.max((long) kept + arriving, Math.max(INITIAL_CAPACITY, 2L * kept));
        return (int) Math.min(size, wanted);
    }
}
