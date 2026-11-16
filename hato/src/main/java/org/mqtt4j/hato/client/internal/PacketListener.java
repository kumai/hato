package org.mqtt4j.hato.client.internal;

import java.util.List;

import org.mqtt4j.hato.internal.inproc.InprocLink;

/// A transport listener that also takes packets as they are, from a transport that passes them in
/// process rather than bytes. Such a transport calls [#onPackets] for each read, where a byte
/// transport calls [#onBorrowedBytes], and then [#afterBorrowedBytes()] as after any read.
/// Otherwise the calls follow the rules of [BorrowingListener].
interface PacketListener extends BorrowingListener {

    /// Takes the packets of one read, in the order in which they came, each with the bytes that it
    /// takes on the wire. `filled` says that the read took as many as one read may, so that more
    /// may wait behind them, as a read that fills a buffer suggests.
    void onPackets(List<InprocLink.Item> packets, boolean filled);
}
