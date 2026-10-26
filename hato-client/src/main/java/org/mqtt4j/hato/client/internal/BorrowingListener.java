package org.mqtt4j.hato.client.internal;

import org.mqtt4j.hato.Transport;

/// A transport listener that also takes received bytes that the transport only lends: they lie
/// in a read buffer of the transport's own, which it reuses once the call returns, so that the
/// transport does not copy each read into a new array. Otherwise the calls follow the rules of
/// [Transport.Listener#onBytes]. A transport that reads into a buffer of its own, as the
/// built-in TCP transport does, lends to a listener that offers this.
interface BorrowingListener extends Transport.Listener {

    void onBorrowedBytes(byte[] buffer, int offset, int length);
}
