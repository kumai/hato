package org.mqtt4j.hato.client.internal;

import org.jspecify.annotations.Nullable;

/// The inline handlers of one client: marks the threads that run them, so that the client's own
/// waits can refuse to run inside them. Such a handler may hold up the reading of its connection
/// until it returns, so a wait for an answer that the connection would bring could only end
/// with the connection. The client refuses every wait for itself inside such a handler, wherever
/// the handler runs, so that the outcome does not depend on where it ran or on how soon the
/// answer came.
final class InlineHandlers {

    // The client whose inline handler runs on each thread, or null.
    private static final ThreadLocal<@Nullable InlineHandlers> running = new ThreadLocal<>();

    /// Marks the calling thread as running an inline handler of this client, and returns what it
    /// ran before, which [#exit(InlineHandlers)] restores.
    @Nullable InlineHandlers enter() {
        InlineHandlers previous = running.get();
        running.set(this);
        return previous;
    }

    /// Ends the mark that [#enter()] set, restoring `previous`.
    static void exit(@Nullable InlineHandlers previous) {
        running.set(previous);
    }

    /// Throws when the calling thread runs an inline handler of this client.
    ///
    /// @throws IllegalStateException if an inline handler of this client runs on the calling
    ///         thread
    void refuseWait() {
        if (running.get() == this) {
            throw new IllegalStateException(
                    "An inline handler cannot wait for its own client, since its connection may "
                            + "read nothing until the handler returns"
            );
        }
    }
}
