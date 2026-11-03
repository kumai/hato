package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.util.Optional;

/// Where a client keeps the local state of a persistent session between client objects: one
/// versioned record, which one client object at a time holds. A client object holds the store
/// from its claim of the record until it has saved its state there, or given the session up, and
/// none of its connections can send anything more, so that no other client object claims the
/// session, and connects with it, while the broker's session may still change with this one's
/// connections. The holder alone replaces the record.
///
/// Each replacement also names the version that it replaces. A writer that ignores the hold, as
/// a network file system may let one, then finds out that another one replaced the record and
/// changes nothing, instead of bringing back a stale state. The writer picks each new version,
/// so that versions need not count up: versions that never repeat, even after the record is
/// deleted or found corrupt, keep a client object that holds an old version from taking a new
/// record for the one it wrote.
interface SessionStore {

    /// Holds the store for this client object alone until [#release()], unless another one
    /// holds it. A client object that holds the store already keeps holding it.
    ///
    /// @return whether this client object holds the store; false while another one does
    /// @throws IOException if the store cannot be held for another reason; this client object
    ///     then does not hold it
    boolean hold() throws IOException;

    /// Gives the store up, so that another client object can hold it, if this one holds it.
    void release();

    /// The stored record, or empty when there is none.
    ///
    /// @return the record and its version
    /// @throws IOException if the store cannot be read
    Optional<Stored> read() throws IOException;

    /// Replaces the record with `content` under version `next` if its version is still
    /// `expected`, which is 0 while there is none. A replacement survives a crash of the process
    /// or of the machine once this returns.
    ///
    /// @param next the version of the new record, never 0
    /// @return whether the record was replaced; false, with nothing changed, when its version
    ///     differs
    /// @throws IOException if the store cannot be read or written; the record is then either
    ///     replaced or unchanged
    /// @throws IllegalStateException if this client object does not hold the store
    boolean replace(long expected, long next, byte[] content) throws IOException;

    /// A stored record.
    ///
    /// @param version the record's version, never 0
    /// @param content the record's content
    record Stored(long version, byte[] content) {}
}
