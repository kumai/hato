package org.mqtt4j.hato.client.internal;

import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.spi.TimerScheduler;
import org.mqtt4j.hato.spi.Transport;

import static java.util.Objects.requireNonNull;

/// The WebSocket transports of one client, and the HttpClient that they share. The client's first
/// WebSocket connection creates the HttpClient, later connections reuse it, and the client
/// releases it once every transport has closed. Its executor runs the JDK's work on virtual
/// threads that the client owns, so the client's termination can wait for them.
public final class WebSocketTransports implements Transport.Factory {

    private final String threadName;

    private final TimerScheduler timer;

    // Wraps each WebSocket that opens; tests use it to hold or fail writes.
    private final UnaryOperator<WebSocket> opened;

    private final AtomicBoolean warnedWithoutSubprotocol = new AtomicBoolean();

    private final Object lock = new Object();

    private @Nullable HttpClient http;

    private @Nullable ExecutorService executor;

    private boolean released;

    public WebSocketTransports(String clientId, TimerScheduler timer) {
        this(clientId, timer, UnaryOperator.identity());
    }

    WebSocketTransports(String clientId, TimerScheduler timer, UnaryOperator<WebSocket> opened) {
        threadName = "hato-" + requireNonNull(clientId, "clientId");
        this.timer = timer;
        this.opened = opened;
    }

    @Override
    public Transport create(String clientId) {
        return new WebSocketTransport(clientId, this, timer, opened);
    }

    /// The client's HttpClient, created by its first WebSocket connection. It connects directly,
    /// as TCP does, whatever proxy the JVM configures for HTTP.
    ///
    /// @throws IllegalStateException once the client has released it
    HttpClient httpClient() {
        synchronized (lock) {
            if (released) {
                throw new IllegalStateException("The client has released its HttpClient");
            }
            HttpClient client = http;
            if (client == null) {
                ExecutorService work = Executors.newThreadPerTaskExecutor(
                        Thread.ofVirtual().name(threadName + "-http-", 0).factory()
                );
                client = HttpClient.newBuilder()
                        .executor(work)
                        .proxy(HttpClient.Builder.NO_PROXY)
                        .build();
                executor = work;
                http = client;
            }
            return client;
        }
    }

    /// True only for the first connection of this client whose broker chose no subprotocol, so
    /// that the client warns about it once.
    boolean firstWithoutSubprotocol() {
        return warnedWithoutSubprotocol.compareAndSet(false, true);
    }

    /// Releases the HttpClient once no transport uses it, without waiting: a thread of its own
    /// waits until the HttpClient and its executor have ended, and then runs `ended`, at once
    /// when there is no HttpClient. HttpClient's close() and shutdown() would wait for every
    /// WebSocket that is still open, so it is shut down at once instead.
    void release(Runnable ended) {
        HttpClient client;
        ExecutorService work;
        synchronized (lock) {
            released = true;
            client = http;
            work = executor;
            http = null;
            executor = null;
        }
        if (client == null || work == null) {
            ended.run();
            return;
        }
        client.shutdownNow();
        Thread.ofVirtual().name(threadName + "-http-release").start(() -> {
            try {
                // shutdownNow() has aborted every operation, so this ends with the JDK's own
                // threads; until then, the client's termination keeps waiting.
                while (!client.awaitTermination(Duration.ofSeconds(10))) {
                    // Each wait ends after ten seconds at the latest; keep waiting.
                }
                work.close();
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            } finally {
                ended.run();
            }
        });
    }
}
