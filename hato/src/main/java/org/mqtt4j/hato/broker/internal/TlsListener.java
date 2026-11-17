package org.mqtt4j.hato.broker.internal;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import org.jspecify.annotations.Nullable;

// What the connections of the broker's TLS listener share: the application's context, the
// parameters that every engine takes, and the threads that run the engines' delegated tasks.
//
// A delegated task does the costly work of a handshake, and it calls the key and trust managers of
// the application's context, which may take their time, as for a certificate revocation check. So
// the tasks run on threads of their own, at most as many at once as the settings allow, and none
// on a selector or a thread of the actor pool. A task comes only while a handshake runs, and the
// broker takes part in no handshake on an accepted connection, so each connection that waits for
// its CONNECT has at most one batch of tasks waiting, and the queue holds as many batches as
// there may be such connections. Only the batches of connections that ended meanwhile, as at their
// deadline, can fill it, while tasks that never return hold every thread; a handshake that finds
// no room then fails at once. The threads start as tasks come, and end once idle.
final class TlsListener {

    // How long a thread for delegated tasks waits for another one before it ends.
    private static final long IDLE_SECONDS = 30;

    private final SSLContext context;

    // Checked and detached from the application's object when the broker started.
    private final SSLParameters parameters;

    // The port of the listener, for logs.
    final int port;

    final ThreadPoolExecutor tasks;

    // `threads` bounds the delegated tasks that run at once, and `waiting` the batches that wait
    // for a thread. The pool ends its part of `termination` once the broker has shut it down and
    // its tasks have returned.
    TlsListener(
            SSLContext context,
            SSLParameters parameters,
            int port,
            int threads,
            int waiting,
            Termination termination
    ) {
        this.context = context;
        this.parameters = parameters;
        this.port = port;
        var next = new AtomicInteger();
        tasks = new ThreadPoolExecutor(
                threads,
                threads,
                IDLE_SECONDS,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(waiting),
                task -> Thread.ofPlatform().daemon()
                        .name("hato-broker-" + port + "-tls-" + next.getAndIncrement())
                        .unstarted(task)
        ) {

            // The pool ends once the broker has shut it down and its tasks have returned.
            @Override
            protected void terminated() {
                termination.partEnded();
            }
        };
        tasks.allowCoreThreadTimeOut(true);
    }

    // A server engine for a connection from `remote`, if known, ready for the client's first
    // handshake. Creating it calls none of the application's code.
    SSLEngine engine(@Nullable SocketAddress remote) throws SSLException {
        // The peer's address lets the application's trust manager see whom it checks.
        SSLEngine engine = (remote instanceof InetSocketAddress address)
                ? context.createSSLEngine(address.getHostString(), address.getPort())
                : context.createSSLEngine();
        engine.setUseClientMode(false);
        engine.setSSLParameters(parameters);
        engine.beginHandshake();
        return engine;
    }

    // Has the threads for delegated tasks end. The tasks that run are the application's code, so
    // an interrupt is all that asks them to return; the broker's end waits until they have.
    void close() {
        tasks.shutdownNow();
    }
}
