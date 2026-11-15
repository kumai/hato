package org.mqtt4j.hato;

import org.mqtt4j.hato.client.internal.ClientSnapshot;

/// Shows a client's snapshot as statistics, as the builder lets the client do, for tests that
/// construct the client directly.
public final class ClientStatsAccess {

    private ClientStatsAccess() {
    }

    public static ClientStats of(ClientSnapshot snapshot) {
        return ClientStats.of(snapshot);
    }
}
