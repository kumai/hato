package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.UnaryOperator;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.TimerScheduler;
import org.mqtt4j.hato.Transport;
import org.mqtt4j.hato.client.internal.common.log.Log;

import static java.util.Objects.requireNonNull;

/// One WebSocket connection through the JDK's WebSocket client, which carries the MQTT byte
/// stream in binary messages. The JDK calls its listener and completes its sends on threads that
/// it chooses, sometimes at the same time, so one monitor serializes the callbacks to the client,
/// as in TcpTransport. The client's [WebSocketTransports] owns the HttpClient that opens it.
final class WebSocketTransport implements Transport {

    private static final Log log = Log.of(WebSocketTransport.class);

    // How long a close waits for the peer's Close after sending its own, as TLS waits for the
    // peer's close_notify.
    private static final Duration CLOSE_GRACE = Duration.ofSeconds(1);

    private final String clientId;

    private final WebSocketTransports owner;

    private final TimerScheduler timer;

    // Wraps each WebSocket that opens; tests use it to hold or fail writes.
    private final UnaryOperator<WebSocket> opened;

    private final Object callbacks = new Object();

    private @Nullable URI endpoint;

    // Runs the completion of each write. The JDK can complete a write inside sendBinary, which
    // runs inside a callback when the client writes from onConnected or onSent; completing on
    // another thread keeps the callbacks from nesting. The client's HttpClient owns it.
    private @Nullable Executor completions;

    private @Nullable Listener listener;

    // The JDK's WebSocket once it has opened, and the wrapper that this transport writes to.
    private @Nullable WebSocket attached;

    private @Nullable WebSocket socket;

    private @Nullable Throwable terminalCause;

    private TimerScheduler.@Nullable Cancellable deadline;

    private TimerScheduler.@Nullable Cancellable closeGrace;

    private boolean started;

    private boolean connected;

    private boolean closed;

    // A write that the JDK has not completed yet. Its array stays the transport's until then.
    private boolean sending;

    // A graceful close that waits for the peer's Close.
    private boolean closing;

    WebSocketTransport(
            String clientId,
            WebSocketTransports owner,
            TimerScheduler timer,
            UnaryOperator<WebSocket> opened
    ) {
        this.clientId = requireNonNull(clientId, "clientId");
        this.owner = owner;
        this.timer = timer;
        this.opened = opened;
    }

    @Override
    public void start(URI endpoint, Duration timeout, Listener receiver) {
        requireNonNull(endpoint, "endpoint");
        requireNonNull(timeout, "timeout");
        requireNonNull(receiver, "listener");
        if (timeout.isZero() || timeout.isNegative()
                || timeout.compareTo(Duration.ofMillis(Integer.MAX_VALUE)) > 0) {
            throw new IllegalArgumentException(
                    "Connect timeout must be positive and at most 2147483647 milliseconds"
            );
        }
        String scheme = owner.scheme();
        if (!scheme.equals(endpoint.getScheme())) {
            throw new IllegalArgumentException("Transport requires " + scheme + " scheme");
        }
        synchronized (callbacks) {
            if (started) {
                throw new IllegalStateException("Transport already started");
            }
            started = true;
            this.endpoint = endpoint;
            listener = receiver;
            if (closed) {
                notifyClosed();
                return;
            }
        }
        try {
            HttpClient http = owner.httpClient();
            // The JDK's own deadline starts again when it resends the request on a new
            // connection, so this one bounds the whole attempt on the client's clock.
            TimerScheduler.Cancellable expiry = timer.schedule(timeout, this::expire);
            synchronized (callbacks) {
                if (closed) {
                    cancel(expiry);
                    return;
                }
                deadline = expiry;
                completions = http.executor().orElseThrow();
            }
            // The future is never cancelled: a cancel that races the upgrade response opens a
            // WebSocket that the HttpClient keeps counting, and then never terminates. A
            // WebSocket that opens after the transport has ended is aborted instead.
            http.newWebSocketBuilder()
                    .subprotocols("mqtt")
                    .connectTimeout(timeout)
                    .buildAsync(endpoint, new Receiver())
                    .whenComplete(this::established);
        } catch (RuntimeException failure) {
            terminate(failure, false);
        }
    }

    @Override
    public SendResult send(long ticket, byte[] bytes) {
        requireNonNull(bytes, "bytes");
        WebSocket open;
        synchronized (callbacks) {
            if (closed) {
                return SendResult.CLOSED;
            }
            if (!connected) {
                throw new IllegalStateException("Transport is not connected");
            }
            if (sending) {
                throw new IllegalStateException("A send is already outstanding");
            }
            sending = true;
            open = requireNonNull(socket);
        }
        // The JDK reads the array until the write completes, which returns its ownership.
        CompletableFuture<WebSocket> written;
        try {
            written = open.sendBinary(ByteBuffer.wrap(bytes), true);
        } catch (RuntimeException failure) {
            written = CompletableFuture.failedFuture(failure);
        }
        written.whenCompleteAsync(
                (_, failure) -> sent(ticket, failure),
                requireNonNull(completions)
        );
        return SendResult.ACCEPTED;
    }

    @Override
    public void close() {
        terminate(new IOException("Transport closed"), true);
    }

    // The upgrade's result. A failure ends the transport; a WebSocket that opens is attached
    // here or by onOpen, whichever comes first.
    private void established(@Nullable WebSocket raw, @Nullable Throwable failure) {
        if (failure != null) {
            Throwable cause = failure.getCause();
            terminate(
                    failure instanceof CompletionException && cause != null ? cause : failure,
                    false
            );
        } else if (raw != null) {
            attach(raw);
        }
    }

    // Takes over a WebSocket that has opened, or aborts it if the transport has already ended,
    // such as after its deadline. Returns whether the transport now receives on it.
    private boolean attach(WebSocket raw) {
        boolean abort;
        boolean warn = false;
        synchronized (callbacks) {
            // onOpen and the upgrade's future both bring the same WebSocket.
            if (attached != null) {
                return !closed;
            }
            attached = raw;
            abort = closed;
            if (!abort) {
                socket = opened.apply(raw);
                connected = true;
                cancel(deadline);
                // A broker that chooses no subprotocol breaks [MQTT-6.0.0-4], but the stream is
                // still MQTT, so the client warns once and continues.
                warn = raw.getSubprotocol().isEmpty() && owner.firstWithoutSubprotocol();
                requireNonNull(listener).onConnected();
            }
        }
        if (abort) {
            raw.abort();
            return false;
        }
        if (warn) {
            log.warn("Broker {} chose no WebSocket subprotocol for {}; continuing with mqtt",
                    endpoint, clientId);
        }
        return true;
    }

    private void sent(long ticket, @Nullable Throwable failure) {
        if (failure != null) {
            synchronized (callbacks) {
                sending = false;
            }
            // A failed write returns its array through onClosed instead of onSent.
            terminate(failure, false);
            synchronized (callbacks) {
                notifyClosed();
            }
            return;
        }
        synchronized (callbacks) {
            sending = false;
            // Report a finished write even while closing, as when the peer closes after reading
            // DISCONNECT. onClosed waits for it, so it still comes last.
            Listener receiver = listener;
            if (receiver != null) {
                receiver.onSent(ticket);
            }
            notifyClosed();
        }
    }

    // Ends the transport with `cause` once. A graceful end of an open WebSocket sends Close and
    // waits for the peer's Close for a grace period; every other end aborts at once.
    private void terminate(Throwable cause, boolean graceful) {
        end(cause, graceful, false);
    }

    // The deadline ends only an attempt that has not connected: a cancellation can come too late
    // to stop its timer.
    private void expire() {
        end(new SocketTimeoutException("WebSocket connection deadline expired"), false, true);
    }

    private void end(Throwable cause, boolean graceful, boolean onlyConnecting) {
        WebSocket open;
        boolean waits;
        synchronized (callbacks) {
            if (closed || (onlyConnecting && connected)) {
                return;
            }
            closed = true;
            terminalCause = cause;
            cancel(deadline);
            open = socket;
            waits = graceful && open != null && !sending && startGrace(cause);
            closing = waits;
        }
        if (open != null) {
            if (waits) {
                sendClose(open);
            } else {
                open.abort();
            }
        }
        synchronized (callbacks) {
            notifyClosed();
        }
    }

    // Caller holds callbacks. Whether the grace period for the peer's Close has started; a clock
    // that refuses it leaves only an abort.
    private boolean startGrace(Throwable cause) {
        try {
            closeGrace = timer.schedule(CLOSE_GRACE, this::closingEnded);
            return true;
        } catch (RuntimeException failure) {
            cause.addSuppressed(failure);
            return false;
        }
    }

    // A graceful close starts with this side's Close. When the JDK has already answered the
    // peer's Close, the output is closed and this send fails; the close then only waits.
    private static void sendClose(WebSocket open) {
        try {
            open.sendClose(WebSocket.NORMAL_CLOSURE, "");
        } catch (RuntimeException failure) {
            log.debug("WebSocket Close was not sent", failure);
        }
    }

    // The peer's Close has arrived or the grace period has passed: a graceful close ends now.
    private void closingEnded() {
        WebSocket open;
        synchronized (callbacks) {
            if (!closing) {
                return;
            }
            closing = false;
            cancel(closeGrace);
            open = socket;
        }
        if (open != null) {
            open.abort();
        }
        synchronized (callbacks) {
            notifyClosed();
        }
    }

    // The peer or the JDK ended the WebSocket. That finishes a graceful close, which was waiting
    // for it, and otherwise ends the transport with `cause`.
    private void ended(Throwable cause) {
        boolean waiting;
        synchronized (callbacks) {
            waiting = closing;
        }
        if (waiting) {
            closingEnded();
        } else {
            terminate(cause, false);
        }
    }

    private static void cancel(TimerScheduler.@Nullable Cancellable timer) {
        if (timer == null) {
            return;
        }
        try {
            timer.cancel();
        } catch (Throwable failure) {
            log.warn("Transport timer cancellation failed", failure);
        }
    }

    // Caller holds callbacks. Reports the end once nothing that may still call back is running.
    private void notifyClosed() {
        Listener receiver = listener;
        Throwable cause = terminalCause;
        if (closed && !sending && !closing && receiver != null && cause != null) {
            listener = null;
            receiver.onClosed(cause);
        }
    }

    private final class Receiver implements WebSocket.Listener {

        @Override
        public void onOpen(WebSocket webSocket) {
            if (attach(webSocket)) {
                webSocket.request(1);
            }
        }

        // Each part becomes one chunk of the byte stream at once; the next one is requested
        // only after it, so nothing else accumulates here.
        @Override
        public @Nullable CompletionStage<?> onBinary(
                WebSocket webSocket,
                ByteBuffer data,
                boolean last
        ) {
            var bytes = new byte[data.remaining()];
            data.get(bytes);
            synchronized (callbacks) {
                Listener receiver = listener;
                if (closed || receiver == null) {
                    return null;
                }
                if (bytes.length > 0) {
                    receiver.onBytes(bytes);
                }
            }
            webSocket.request(1);
            return null;
        }

        // MQTT travels only in binary messages, and the client closes on other data
        // ([MQTT-6.0.0-1]).
        @Override
        public @Nullable CompletionStage<?> onText(
                WebSocket webSocket,
                CharSequence data,
                boolean last
        ) {
            terminate(new ProtocolException("The peer sent a text message"), false);
            return null;
        }

        @Override
        public @Nullable CompletionStage<?> onClose(
                WebSocket webSocket,
                int statusCode,
                String reason
        ) {
            ended(new IOException("The peer closed the WebSocket with " + statusCode
                    + (reason.isEmpty() ? "" : ": " + reason)));
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            ended(error);
        }
    }
}
