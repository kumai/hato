package org.mqtt4j.mqtj.spi;

import java.net.URI;
import java.time.Duration;

import org.mqtt4j.mqtj.client.internal.TcpTransport;

/// An experimental, single-use, ordered byte connection. This contract may evolve as other
/// transport implementations are exercised. It has no knowledge of MQTT packets.
///
/// All methods and listener callbacks must return promptly without waiting for I/O or capacity.
/// Callbacks may occur inline. Received arrays transfer ownership to the listener; they must
/// never be reused. Successful send admission transfers ownership of the array to the transport
/// until `onSent` or `onClosed`; a rejected array remains the caller's. Neither side may mutate
/// an admitted array. At most one send may be outstanding. Queuing, capacity limits, and
/// admission policy belong to the caller, not the transport.
///
/// Callbacks are serialized. `onConnected` precedes bytes and send completions. Bytes arrive in
/// wire order with arbitrary chunk boundaries. Each admitted send completes once after writing;
/// admission alone is not completion. The next send is allowed once `onSent` is invoked,
/// including from that callback. A single final `onClosed` releases an unfinished send instead
/// of `onSent`. No callbacks follow it. Closing releases pending I/O and delayed work, including
/// a connection that finishes after close was requested.
/// Runtime exceptions from the factory, start, or send fail the current connection attempt or
/// connection without closing the client permanently. The client closes any created transport.
/// Close failures are logged and cleanup continues. Errors from the factory, start, or send
/// remain fatal to the client.
public interface Transport extends AutoCloseable {

    /// The immediate admission result; only [#ACCEPTED] transfers buffer ownership.
    enum SendResult {

        ACCEPTED,
        CLOSED
    }

    /// Starts one attempt. The timeout bounds connection establishment, not subsequent reads.
    void start(URI endpoint, Duration timeout, Listener listener);

    /// Starts one write without waiting. Call after [Listener#onConnected] has been invoked
    /// and after any previous send has completed. Completion may be notified before this returns.
    /// [SendResult#CLOSED] includes shutdown in progress. Once started, the transport reports
    /// its cause through exactly one [Listener#onClosed] callback, which may already have arrived.
    ///
    /// @throws IllegalStateException if not yet connected or a send is outstanding,
    ///         unless closing or closed
    SendResult send(long ticket, byte[] bytes);

    /// Requests shutdown. Safe before start, concurrent with other methods, and idempotent.
    @Override
    void close();

    /// Creates a TCP transport whose worker names include the client identifier.
    static Transport tcp(String clientId) {
        return new TcpTransport(clientId);
    }

    @FunctionalInterface
    interface Factory {

        /// Returns a fresh transport promptly, without connecting it.
        Transport create(String clientId);
    }

    interface Listener {

        void onConnected();

        void onBytes(byte[] bytes);

        void onSent(long ticket);

        void onClosed(Throwable cause);
    }
}
