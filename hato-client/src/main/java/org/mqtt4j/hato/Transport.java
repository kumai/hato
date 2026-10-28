package org.mqtt4j.hato;

import java.net.URI;
import java.time.Duration;

import org.mqtt4j.hato.client.internal.DefaultTimerScheduler;
import org.mqtt4j.hato.client.internal.TcpTransport;

/// A single-use, ordered byte connection. It has no knowledge of MQTT packets.
///
/// All methods and listener callbacks must return promptly without waiting for I/O or capacity.
/// Callbacks may occur inline. Received arrays transfer ownership to the listener; they must
/// never be reused. Successful send admission transfers ownership of the array to the transport
/// until [onSent][Listener#onSent(long)] or [onClosed][Listener#onClosed(Throwable)]; a rejected
/// array remains the caller's. Neither side may mutate an admitted array. At most one send may
/// be outstanding. Queuing, capacity limits, and admission policy belong to the caller, not the
/// transport.
///
/// Callbacks are serialized. [onConnected][Listener#onConnected()] precedes bytes and send
/// completions. Bytes arrive in wire order with arbitrary chunk boundaries. Each successfully
/// written send must be reported exactly once through `onSent`, before `onClosed`, even if EOF,
/// failure, or a close request races with write completion. Shutdown must not discard a
/// successful write's pending notification.
/// Completion means successful local writing, not admission or confirmation of peer receipt.
/// The next send is allowed once `onSent` is invoked, including from that callback, unless closing.
/// A single final `onClosed` releases any send whose write did not complete successfully instead
/// of `onSent`. No callbacks follow it. Closing releases pending I/O and delayed work, including
/// a connection that finishes after close was requested.
/// Runtime exceptions from the factory, start, or send fail the current connection attempt or
/// connection without closing the client permanently. The client closes any created transport.
/// Close failures are logged and cleanup continues. Errors from the factory, start, or send
/// remain fatal to the client.
public interface Transport extends AutoCloseable {

    /// The immediate admission result; only [#ACCEPTED] transfers buffer ownership.
    enum SendResult {

        /// The transport admitted the send and owns its array until it reports the write through
        /// [Listener#onSent(long)] or releases the array through [Listener#onClosed(Throwable)].
        ACCEPTED,

        /// The transport is closing or closed and did not admit the send; the array remains the
        /// caller's.
        CLOSED
    }

    /// Starts this transport's only connection. The timeout bounds its establishment, including
    /// any TLS handshake, not subsequent reads. Implementations must reject unsupported URI
    /// schemes here with [IllegalArgumentException], rather than silently use a different
    /// transport.
    ///
    /// @param endpoint
    ///     the URI to connect to
    /// @param timeout
    ///     the bound on establishing the connection
    /// @param listener
    ///     the listener for this connection's callbacks
    void start(URI endpoint, Duration timeout, Listener listener);

    /// Starts one write without waiting. Call after [Listener#onConnected] has been invoked
    /// and after any previous send has completed. Completion may be notified before this returns.
    /// [SendResult#CLOSED] includes shutdown in progress. Once started, the transport reports
    /// its cause through exactly one [Listener#onClosed] callback, which may already have arrived.
    ///
    /// @param ticket
    ///     the caller's identifier for this send, which [Listener#onSent(long)] reports
    /// @param bytes
    ///     the bytes to write
    /// @return
    ///     the immediate admission result
    /// @throws IllegalStateException
    ///     if not yet connected or a send is outstanding, unless closing or closed
    SendResult send(long ticket, byte[] bytes);

    /// Requests shutdown. Safe before start, concurrent with other methods, and idempotent.
    @Override
    void close();

    /// Creates a plain TCP transport accepting only `mqtt` URIs (default port 1883).
    /// The client identifier is used for diagnostics. Its deadlines use real time, not the
    /// scheduler passed to [MqttClientBuilder#timerScheduler(TimerScheduler)].
    ///
    /// @param clientId
    ///     the client identifier
    /// @return
    ///     a new transport, not yet started
    static Transport tcp(String clientId) {
        return new TcpTransport(clientId, DefaultTimerScheduler.instance);
    }

    /// Creates a client's transports, one for each connection; see
    /// [MqttClientBuilder#transportFactory(Transport.Factory)].
    @FunctionalInterface
    interface Factory {

        /// Returns a fresh transport promptly, without connecting it.
        ///
        /// @param clientId
        ///     the identifier of the client that requests the transport
        /// @return
        ///     a fresh transport, not yet connected
        Transport create(String clientId);
    }

    /// Receives the serialized callbacks of one transport, as [Transport] describes.
    interface Listener {

        /// Called when the connection is established, before any bytes or send completions.
        void onConnected();

        /// Called with received bytes, in wire order and with arbitrary chunk boundaries. The
        /// listener owns the array, which the transport must never reuse.
        ///
        /// @param bytes
        ///     the received bytes
        void onBytes(byte[] bytes);

        /// Called exactly once for each successfully written send, before
        /// [#onClosed(Throwable)]. The next send is allowed from this call on, including from
        /// within it, unless closing.
        ///
        /// @param ticket
        ///     the ticket that [Transport#send(long, byte[])] received for the written send
        void onSent(long ticket);

        /// Called once, last, when the connection ends. It releases any send whose write did not
        /// complete successfully, and no callbacks follow it.
        ///
        /// @param cause
        ///     why the connection ended
        void onClosed(Throwable cause);
    }
}
