package org.mqtt4j.hato.common.protocol;

/// Locates the payload of a PUBLISH from its first bytes, so that a reader can give the payload
/// an array of its own before the rest of the packet arrives. Only the lengths that lead to the
/// payload are read; decoding checks the packet.
final class PublishHeaders {

    private PublishHeaders() {}

    /// The size of the fixed and variable headers of the PUBLISH whose first `available` bytes
    /// lie in `bytes` from `offset`, or -1 when those bytes do not locate its payload: they end
    /// before the headers do, or a variable byte integer in them runs past four bytes.
    /// `properties` says whether the headers end with properties, as in MQTT 5.0.
    static int size(byte[] bytes, int offset, int available, boolean properties) {
        int end = offset + available;
        if (available < 1) {
            return -1;
        }
        int qos = ((bytes[offset] & 0xFF) >>> 1) & 3;
        // The fixed header: the first byte, then the Remaining Length.
        int at = skipVariableByteInteger(bytes, offset + 1, end);
        // The topic name and its two-byte length.
        if (at < 0 || end - at < 2) {
            return -1;
        }
        at += 2 + (((bytes[at] & 0xFF) << 8) | (bytes[at + 1] & 0xFF));
        if (qos != 0) {
            at += 2;
        }
        if (properties) {
            // The properties' length, a variable byte integer, then the properties.
            if (at >= end) {
                return -1;
            }
            int length = 0;
            int digits = 0;
            int digit;
            do {
                if (at == end || digits == 4) {
                    return -1;
                }
                digit = bytes[at++] & 0xFF;
                length |= (digit & 0x7F) << (7 * digits++);
            } while ((digit & 0x80) != 0);
            at += length;
        }
        return at <= end ? at - offset : -1;
    }

    // Where the variable byte integer that starts at `at` ends, or -1 when it does not end
    // before `end` or within four bytes.
    private static int skipVariableByteInteger(byte[] bytes, int at, int end) {
        for (int digits = 0; digits < 4; digits++) {
            if (at == end) {
                return -1;
            }
            if ((bytes[at++] & 0x80) == 0) {
                return at;
            }
        }
        return -1;
    }
}
