package org.mqtt4j.mqtj.testsupport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

/// JDK-only TLS termination in front of a plain TCP broker, independent of every MQTJ module.
public final class TlsRelay implements AutoCloseable {

    private final SSLContext context;
    private final ServerSocket listener;
    private final InetSocketAddress target;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    public TlsRelay(URI plainBroker) throws IOException, GeneralSecurityException {
        context = Tls.serverContext();
        target = new InetSocketAddress(
                plainBroker.getHost(),
                plainBroker.getPort() == -1 ? 1883 : plainBroker.getPort()
        );
        listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        workers.execute(this::accept);
    }

    public URI uri() {
        return URI.create("mqtts://localhost:" + listener.getLocalPort());
    }

    private void accept() {
        while (!closed.get()) {
            try {
                Socket socket = listener.accept();
                register(socket);
                workers.execute(() -> relay(socket));
            } catch (IOException | RejectedExecutionException failure) {
                if (!closed.get()) {
                    throw new IllegalStateException("TLS relay accept failed", failure);
                }
            }
        }
    }

    private void relay(Socket raw) {
        var broker = new Socket();
        try {
            register(broker);
            // Keep the raw socket so teardown can interrupt a blocked TLS write without its lock.
            var tls = (SSLSocket) context.getSocketFactory().createSocket(
                    raw,
                    "localhost",
                    raw.getPort(),
                    false
            );
            tls.setUseClientMode(false);
            tls.startHandshake();
            broker.connect(target, 5000);
            workers.execute(() -> forward(tls, broker, raw, broker));
            forward(broker, tls, raw, broker);
        } catch (IOException | RejectedExecutionException _) {
            // Failed handshakes and client disconnects end this relay connection only.
        } finally {
            closeSocket(raw);
            closeSocket(broker);
            sockets.remove(raw);
            sockets.remove(broker);
        }
    }

    private static void forward(Socket source, Socket destination, Socket raw, Socket broker) {
        try {
            source.getInputStream().transferTo(destination.getOutputStream());
        } catch (IOException _) {
            // Either direction ending closes the pair and releases the other direction.
        } finally {
            closeSocket(raw);
            closeSocket(broker);
        }
    }

    private void register(Socket socket) {
        sockets.add(socket);
        if (closed.get()) {
            closeSocket(socket);
            sockets.remove(socket);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                listener.close();
            } finally {
                sockets.forEach(TlsRelay::closeSocket);
                workers.shutdownNow();
            }
        }
    }

    private static void closeSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException _) {
            // Teardown is best effort after both forwarding directions have been stopped.
        }
    }
}
