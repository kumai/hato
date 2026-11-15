package org.mqtt4j.hato.client.internal;

/// A transport whose reading the client can pause, so that a peer that sends faster than the
/// client routes what it receives is held back by flow control, such as TCP's, rather than by the
/// client's limits on what it holds. Both methods return at once, may be called from any thread,
/// including inside the transport's callbacks, and do nothing once the transport has closed.
/// Bytes that the transport is already delivering still arrive after a pause.
interface PausableTransport {

    void pauseReading();

    void resumeReading();
}
