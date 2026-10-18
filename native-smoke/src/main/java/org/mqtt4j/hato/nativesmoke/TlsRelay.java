package org.mqtt4j.hato.nativesmoke;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

/// Terminates TLS in front of the plain TCP broker with JDK classes only. It repeats the relay
/// of test-support because depending on test-support would put Testcontainers on the image's
/// classpath. Each accepted connection is relayed by its own virtual threads.
final class TlsRelay implements AutoCloseable {

    private final SSLContext context;
    private final InetSocketAddress broker;
    private final ServerSocket listener;

    TlsRelay(int brokerPort, SSLContext context) throws IOException {
        this.context = context;
        broker = new InetSocketAddress(InetAddress.getLoopbackAddress(), brokerPort);
        listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofVirtual().name("hato-smoke-relay").start(this::accept);
    }

    URI uri() {
        return URI.create("mqtts://localhost:" + listener.getLocalPort());
    }

    private void accept() {
        try {
            while (true) {
                Socket raw = listener.accept();
                Thread.ofVirtual().start(() -> relay(raw));
            }
        } catch (IOException failure) {
            // Closing the listener stops accepting. Relayed connections end with their client
            // or the broker.
            if (!listener.isClosed()) {
                System.err.println("TLS relay stopped accepting: " + failure);
            }
        }
    }

    private void relay(Socket raw) {
        var plain = new Socket();
        try {
            // Layer TLS over the raw socket and close only that one: SSLSocket.close can wait
            // behind a stalled write (JDK-8241239).
            var tls = (SSLSocket) context.getSocketFactory().createSocket(
                    raw,
                    "localhost",
                    raw.getPort(),
                    false
            );
            tls.setUseClientMode(false);
            tls.startHandshake();
            plain.connect(broker, 5000);
            Thread.ofVirtual().start(() -> forward(tls, plain, raw, plain));
            forward(plain, tls, raw, plain);
        } catch (IOException failure) {
            // The client fails as well; this names the relay's side of a failed handshake.
            System.err.println("TLS relay failed: " + failure);
        } finally {
            closeSocket(raw);
            closeSocket(plain);
        }
    }

    // Copies one direction until it ends, then closes both sockets to end the other direction.
    private static void forward(Socket from, Socket to, Socket raw, Socket plain) {
        try {
            from.getInputStream().transferTo(to.getOutputStream());
        } catch (IOException _) {
            // A closed socket ends the copy like an end of stream.
        } finally {
            closeSocket(raw);
            closeSocket(plain);
        }
    }

    private static void closeSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException _) {
            // Best effort: the relayed connection ends whether or not this close succeeds.
        }
    }

    @Override
    public void close() throws IOException {
        listener.close();
    }
}
