package org.mqtt4j.hato.testkit;

import static java.util.Objects.requireNonNull;

/// A response that the broker holds back for a test: its kind, the identifier of the client
/// that it is for, and the packet identifier of the request that it answers. Experimental, like
/// [BrokerControls].
public record HeldResponse(ResponseKind kind, String clientId, int packetId) {

    /// @throws NullPointerException if `kind` or `clientId` is null
    public HeldResponse {
        requireNonNull(kind, "kind must not be null");
        requireNonNull(clientId, "clientId must not be null");
    }
}
