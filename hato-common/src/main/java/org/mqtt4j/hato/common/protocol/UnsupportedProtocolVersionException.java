package org.mqtt4j.hato.common.protocol;

import java.io.Serial;

/// Indicates that CONNECT named MQTT but requested an unimplemented protocol level.
///
/// This is distinct from malformed input: the server may need to send a version refusal.
/// The remainder of the CONNECT has not been interpreted as a supported version.
///
/// See [MQTT 3.1.1 Plus Errata 01, section 3.1.2.2: Protocol Level](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html).
public final class UnsupportedProtocolVersionException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final int protocolLevel;

    /// Creates a refusal for the unsigned one-byte level received on the wire.
    ///
    /// @throws IllegalArgumentException if `protocolLevel` is outside 0 to 255
    public UnsupportedProtocolVersionException(int protocolLevel) {
        super("Unsupported MQTT protocol level: " + protocolLevel);
        if (protocolLevel < 0 || protocolLevel > 255) {
            throw new IllegalArgumentException("Protocol level must be between 0 and 255");
        }
        this.protocolLevel = protocolLevel;
    }

    /// Returns the requested level, including levels unknown to the MQTT version enum.
    public int protocolLevel() {
        return protocolLevel;
    }
}
