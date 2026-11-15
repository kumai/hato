package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/// A local TCP relay to another address. It can cut every connection through it at once, as a
/// network failure would, and keeps accepting new connections afterwards.
final class CuttingRelay implements AutoCloseable {

    private final InetSocketAddress target;

    private final ServerSocket server;

    // Both ends of every connection that the relay carries.
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();

    CuttingRelay(InetSocketAddress target) throws IOException {
        this.target = target;
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofVirtual().name("relay-accept").start(this::accept);
    }

    int port() {
        return server.getLocalPort();
    }

    /// Closes every connection through the relay.
    void cut() {
        sockets.forEach(CuttingRelay::closeQuietly);
        sockets.clear();
    }

    @Override
    public void close() throws IOException {
        server.close();
        cut();
    }

    private void accept() {
        while (!server.isClosed()) {
            Socket client;
            try {
                client = server.accept();
            } catch (IOException _) {
                return;
            }
            try {
                var upstream = new Socket(target.getAddress(), target.getPort());
                upstream.setTcpNoDelay(true);
                client.setTcpNoDelay(true);
                sockets.add(client);
                sockets.add(upstream);
                Thread.ofVirtual().name("relay-up").start(() -> pump(client, upstream));
                Thread.ofVirtual().name("relay-down").start(() -> pump(upstream, client));
            } catch (IOException _) {
                closeQuietly(client);
            }
        }
    }

    /// Copies `from` to `to` until either side ends, then ends both.
    private void pump(Socket from, Socket to) {
        try {
            InputStream input = from.getInputStream();
            OutputStream output = to.getOutputStream();
            input.transferTo(output);
        } catch (IOException _) {
            // A cut or the peer's close ends the copy.
        } finally {
            sockets.remove(from);
            sockets.remove(to);
            closeQuietly(from);
            closeQuietly(to);
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException _) {
            // Closing is all that is wanted.
        }
    }
}
