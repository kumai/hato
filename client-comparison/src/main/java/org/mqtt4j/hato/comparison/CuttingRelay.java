package org.mqtt4j.hato.comparison;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/// A TCP relay between a subscriber and the broker that can cut every connection through it at
/// once, on both sides, as a network that fails would. It goes on accepting connections, so that
/// a client can reconnect through it.
final class CuttingRelay implements AutoCloseable {

    // How long a connection to the broker may take before the relay gives up on it.
    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;

    private final ServerSocket listener;

    private final InetSocketAddress broker;

    // Both sockets of every connection through the relay, to cut them.
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();

    private volatile boolean closed;

    /// Listens on loopback for connections to relay to the broker at `host` and `port`.
    CuttingRelay(String host, int port) throws IOException {
        this.listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        this.broker = new InetSocketAddress(host, port);
        Thread.ofPlatform().name("relay-accept").daemon().unstarted(this::accept).start();
    }

    int port() {
        return listener.getLocalPort();
    }

    /// Closes every connection through the relay, on both sides.
    void cut() {
        for (Socket socket : sockets) {
            close(socket);
        }
    }

    private void accept() {
        while (!closed) {
            try {
                Socket client = listener.accept();
                Thread.ofPlatform().name("relay").daemon().start(() -> relay(client));
            } catch (IOException e) {
                if (!closed) {
                    System.err.println("relay: accepting failed: " + e);
                }
                return;
            }
        }
    }

    private void relay(Socket client) {
        var upstream = new Socket();
        sockets.add(client);
        sockets.add(upstream);
        try {
            client.setTcpNoDelay(true);
            upstream.setTcpNoDelay(true);
            upstream.connect(broker, CONNECT_TIMEOUT_MILLIS);
            Thread.ofPlatform()
                    .name("relay-down")
                    .daemon()
                    .start(() -> forward(upstream, client));
            forward(client, upstream);
        } catch (IOException _) {
            // A connection that could not reach the broker ends here; the client tries again.
        } finally {
            close(client);
            close(upstream);
        }
    }

    /// Copies one direction until it ends, and then ends the connection in both directions.
    private void forward(Socket from, Socket to) {
        try {
            from.getInputStream().transferTo(to.getOutputStream());
        } catch (IOException _) {
            // A cut, or either side closing, ends the copy.
        } finally {
            close(from);
            close(to);
        }
    }

    private void close(Socket socket) {
        sockets.remove(socket);
        try {
            socket.close();
        } catch (IOException _) {
            // Closing a socket that failed leaves nothing more to release.
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        try {
            cut();
        } finally {
            listener.close();
        }
    }
}
