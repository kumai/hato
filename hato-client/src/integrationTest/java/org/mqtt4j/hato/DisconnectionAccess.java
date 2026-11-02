package org.mqtt4j.hato;

import org.jspecify.annotations.Nullable;

/// Makes connection endings as the builder lets the client do, for tests that compare the
/// endings that a listener received.
public final class DisconnectionAccess {

    private DisconnectionAccess() {
    }

    public static Disconnection of(
            @Nullable Throwable cause,
            boolean userRequested,
            long connectionAttempt
    ) {
        return Disconnection.of(cause, userRequested, connectionAttempt, null);
    }
}
