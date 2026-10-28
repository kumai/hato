package org.mqtt4j.hato.common.protocol;

import java.util.BitSet;
import java.util.OptionalInt;

/// Allocates Packet Identifiers for one sender's MQTT session.
///
/// Identifiers range from 1 to 65535 and remain in use until explicitly released. Allocation
/// starts at 1, skips occupied identifiers, and wraps after 65535. PUBLISH, SUBSCRIBE, and
/// UNSUBSCRIBE share this pool; identifiers received from the peer belong to a separate space.
///
/// This class is not thread-safe. The session owner serializes access and decides when an
/// exchange is complete. Retransmissions keep their existing identifier rather than allocating
/// another one; a resumed session must retain the allocations for its unfinished exchanges.
///
/// See [MQTT 3.1.1 Plus Errata 01, section 2.3.1: Packet Identifier](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html).
public final class PacketIds {

    private static final int MAX_PACKET_ID = 65535;

    // Grows to the highest identifier allocated, so a session that allocates few keeps it small.
    private final BitSet inUse = new BitSet();
    private int nextId = 1;

    /// Reserves the next unused identifier in circular ascending order.
    ///
    /// Returns empty when all 65535 identifiers are in use, without changing the state.
    public OptionalInt allocate() {
        // Scan from the cursor, then wrap. Each scan is bounded by the 65535-ID space;
        // add a free-ID index if profiling shows near-full scans becoming costly.
        int id = inUse.nextClearBit(nextId);
        if (id > MAX_PACKET_ID) {
            id = inUse.nextClearBit(1);
        }
        if (id > MAX_PACKET_ID) {
            return OptionalInt.empty();
        }

        // [MQTT-2.3.1-2]: never reuse an identifier while its exchange is unfinished.
        inUse.set(id);
        nextId = id == MAX_PACKET_ID ? 1 : id + 1;
        return OptionalInt.of(id);
    }

    /// Makes an allocated identifier available again without moving the allocation cursor.
    ///
    /// The owner calls this after completing the exchange, not for an intermediate QoS 2 ack.
    ///
    /// @throws IllegalArgumentException if `packetId` is outside 1 to 65535
    /// @throws IllegalStateException if `packetId` is not currently in use
    public void release(int packetId) {
        checkPacketId(packetId);
        if (!inUse.get(packetId)) {
            throw new IllegalStateException("Packet identifier is not in use: " + packetId);
        }
        inUse.clear(packetId);
    }

    /// Whether the identifier is currently reserved by this sender.
    ///
    /// @throws IllegalArgumentException if `packetId` is outside 1 to 65535
    public boolean isInUse(int packetId) {
        checkPacketId(packetId);
        return inUse.get(packetId);
    }

    private static void checkPacketId(int packetId) {
        // [MQTT-2.3.1-1]: a Packet Identifier is a nonzero unsigned two-byte integer.
        if (packetId < 1 || packetId > MAX_PACKET_ID) {
            throw new IllegalArgumentException("Packet identifier must be between 1 and 65535");
        }
    }
}
