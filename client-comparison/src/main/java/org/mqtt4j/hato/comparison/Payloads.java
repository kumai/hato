package org.mqtt4j.hato.comparison;

import java.nio.ByteBuffer;
import java.time.Instant;

/// The header that leads every message payload in the comparison: the message's sequence number,
/// counted from 0, and the time it was sent in microseconds since the epoch, each an 8-byte
/// big-endian value. Zeros fill the rest of the payload.
///
/// Sender and subscriber run on the same machine, so the send time and the subscriber's clock are
/// readings of the same wall clock.
final class Payloads {

    /// The bytes that the header takes; no payload is shorter.
    static final int HEADER_SIZE = 16;

    private static final int SENT_OFFSET = 8;

    private Payloads() {
    }

    static long sequence(byte[] payload) {
        return ByteBuffer.wrap(payload).getLong(0);
    }

    static long sentMicros(byte[] payload) {
        return ByteBuffer.wrap(payload).getLong(SENT_OFFSET);
    }

    /// Reads the sequence number from `payload` at its position, without moving it.
    static long sequence(ByteBuffer payload) {
        return payload.getLong(payload.position());
    }

    /// Reads the send time from `payload` relative to its position, without moving it.
    static long sentMicros(ByteBuffer payload) {
        return payload.getLong(payload.position() + SENT_OFFSET);
    }

    /// Writes the header at `offset` of `frame`, which holds a whole packet.
    static void writeHeader(ByteBuffer frame, int offset, long sequence, long sentMicros) {
        frame.putLong(offset, sequence);
        frame.putLong(offset + SENT_OFFSET, sentMicros);
    }

    /// The current wall-clock time in microseconds since the epoch.
    static long nowMicros() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000 + now.getNano() / 1_000;
    }
}
