package org.mqtt4j.hato.client.internal;

import org.mqtt4j.hato.Transport;

/// A transport that lets the client make each write itself, on the thread that sends, instead of
/// handing it to a thread of the transport's own. That saves a thread hand-off on every write,
/// which an acknowledgement waits for before the broker sends more under its receive window.
/// [#write] waits for the connection to take the bytes, so only a thread that may wait calls it,
/// such as the client's sender. Otherwise a write is a [Transport#send]: one may be outstanding,
/// and the transport reports it through the same callbacks.
interface BlockingWriteTransport {

    /// Writes `bytes` on the calling thread and reports the write through `onSent` before it
    /// returns. A write that fails ends the connection, whose `onClosed` releases it, which can
    /// also come before this returns. Returns and throws as [Transport#send] does.
    Transport.SendResult write(long ticket, byte[] bytes);
}
