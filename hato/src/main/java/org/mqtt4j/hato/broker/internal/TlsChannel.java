package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.internal.log.Log;

import static java.util.Objects.requireNonNull;

// The TLS of one connection to the broker's TLS listener: the engine between the connection's
// socket and the bytes of its packets. The selector that the connection is assigned to does all of
// it but the engine's delegated tasks: it decrypts what arrives, encrypts what leaves, and sends
// the records of the handshake. The delegated tasks, which do the costly work of a handshake and
// call the key and trust managers of the application's context, run on the listener's threads for
// them (see TlsListener). Meanwhile the selector leaves the engine alone, because a task holds the
// engine's lock for as long as the application's managers take, and goes on with its other
// connections; the end of the tasks brings it back.
//
// Ciphertext passes through the selector's buffers, and a connection keeps only what is left over:
// a record that has partly arrived, and records that the socket has not taken yet, which go out
// before any later ones. The input of a connection is empty whenever it reads (see Connection), and
// it has room for the bytes of a whole record, so what one read takes from the socket fits it.
//
// The broker takes part in no handshake on an accepted connection: a client that asks for a new
// one, as TLS 1.2 renegotiation does, loses its connection. A key update of TLS 1.3 is no
// handshake, and the engine answers it as it reads. All but the delegated tasks runs on the
// selector.
final class TlsChannel {

    private static final Log log = Log.of(TlsChannel.class);

    private static final ByteBuffer NOTHING = ByteBuffer.allocate(0);

    // How many records one write encrypts at most: those of a batch of sends (see Connection), so
    // that a connection that writes much holds the selector no longer than a plain one does.
    private static final int RECORDS_PER_WRITE = 4;

    // How many times one read takes from the socket while what arrives makes no bytes of packets
    // yet, such as the records of a handshake, before the selector turns to other connections.
    private static final int ROUNDS_PER_READ = 4;

    private final SSLEngine engine;

    private final SocketChannel channel;

    private final Reactor reactor;

    private final TlsListener listener;

    // The client, as logs name it.
    private final Object peer;

    // The largest record that the engine makes or takes, in bytes.
    private final int recordSize;

    // The connection that the channel carries, once it has registered.
    private @Nullable Connection connection;

    // Ciphertext that has arrived and makes no whole record yet, or that the input had no room
    // for, or null.
    private @Nullable ByteBuffer pendingInput;

    // Records that the socket has not taken yet, which go out before any later ones, or null.
    private @Nullable ByteBuffer pendingOutput;

    // Whether the first handshake has finished.
    private boolean established;

    // Whether the engine's delegated tasks run, during which nothing on the selector touches the
    // engine.
    private boolean delegated;

    // Whether there is TLS work to go on with without new input: the delegated tasks have ended,
    // or records wait that the input had no room for.
    private boolean resumed;

    // Whether the input has ended: the socket's, or with the client's close_notify.
    private boolean inputEnded;

    // Whether the last decryption stopped for want of more ciphertext.
    private boolean wanting;

    // Whether a write to the socket has failed. Nothing goes out any more; what has arrived is
    // still read, as a plain connection reads it after a failed write.
    private boolean outputFailed;

    TlsChannel(
            SSLEngine engine,
            SocketChannel channel,
            Reactor reactor,
            TlsListener listener,
            Object peer
    ) {
        this.engine = engine;
        this.channel = channel;
        this.reactor = reactor;
        this.listener = listener;
        this.peer = peer;
        recordSize = engine.getSession().getPacketBufferSize();
    }

    // How many bytes of packets the input of the connection must have room for: those of one
    // record.
    int inputCapacity() {
        return engine.getSession().getApplicationBufferSize();
    }

    void attach(Connection connection) {
        this.connection = connection;
    }

    // Whether the connection has TLS work to go on with without new input from the socket.
    boolean wantsTurn() {
        return !delegated && (resumed || (pendingOutput != null && !outputFailed));
    }

    // Takes in what has arrived, and puts the bytes of packets that it decrypts into `input`,
    // which is empty and has room for a record's. Returns how many, or -1 once the input has
    // ended and every packet byte before the end is in. On the way, it sends what the handshake
    // makes, and hands the delegated tasks that it needs to the listener's threads, after which it
    // returns until they end.
    int read(ByteBuffer input) throws IOException {
        resumed = false;
        if (delegated) {
            return 0;
        }
        try {
            boolean _ = flushBeforeReading();
            int produced = 0;
            for (int round = 0; round < ROUNDS_PER_READ; round++) {
                ByteBuffer left = pendingInput;
                pendingInput = null;
                ByteBuffer net = reactor.tlsInput(
                        recordSize + ((left == null) ? 0 : left.remaining())
                );
                if (left != null) {
                    net.put(left);
                }
                int count = inputEnded ? -1 : channel.read(net);
                if (count < 0) {
                    inputEnded = true;
                }
                net.flip();
                produced += unwrap(net, input);
                // A record that has partly arrived waits for the rest, unless the input has
                // ended, which leaves it incomplete for good.
                if (net.hasRemaining() && !(inputEnded && wanting)) {
                    pendingInput = ByteBuffer.allocate(net.remaining()).put(net).flip();
                }
                if (produced > 0 || count <= 0 || delegated || resumed) {
                    break;
                }
            }
            return (produced == 0 && inputEnded && pendingInput == null) ? -1 : produced;
        } catch (SSLException failure) {
            failed(failure);
            throw failure;
        }
    }

    // Decrypts the records of `net` into `input` as far as both allow, and carries the handshake
    // on: it sends the records that the engine makes, and hands the delegated tasks over. Returns
    // how many bytes of packets it put into `input`.
    private int unwrap(ByteBuffer net, ByteBuffer input) throws IOException {
        int produced = 0;
        wanting = false;
        while (true) {
            SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();
            if (status == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                delegate();
                return produced;
            }
            if (status == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                if (!wrapOwn()) {
                    return produced;
                }
                continue;
            }
            if (!net.hasRemaining()) {
                wanting = true;
                return produced;
            }
            SSLEngineResult result = engine.unwrap(net, input);
            produced += result.bytesProduced();
            if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
                established = true;
            }
            switch (result.getStatus()) {
                case OK -> {
                    // A record goes in whole, so no progress means that none waits whole.
                    if (result.bytesConsumed() == 0 && result.bytesProduced() == 0) {
                        wanting = true;
                        return produced;
                    }
                }
                case BUFFER_UNDERFLOW -> {
                    wanting = true;
                    return produced;
                }
                case BUFFER_OVERFLOW -> {
                    // The input has no room for the next record's bytes, which wait until the
                    // connection has taken those before. No readiness of the socket will bring
                    // the connection back for them, so it comes back of its own.
                    resumed = true;
                    requireNonNull(connection, "registered").wake();
                    return produced;
                }
                case CLOSED -> {
                    // The client's close_notify ends the input, and nothing after it is read.
                    // TLS 1.2 answers it with the broker's own, which the engine makes next.
                    inputEnded = true;
                    net.position(net.limit());
                }
            }
        }
    }

    // Makes the records that the engine has to send apart from the packets: those of the
    // handshake, of a key update, an alert, or close_notify. Sends them after what waits, and
    // returns whether there were any. A failed send leaves the read to go on, as a failed flush
    // does.
    private boolean wrapOwn() throws SSLException {
        ByteBuffer out = reactor.tlsOutput(recordSize);
        SSLEngineResult result = engine.wrap(NOTHING, out);
        if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
            established = true;
        }
        out.flip();
        try {
            send(out);
        } catch (IOException _) {
            // Nothing goes out any more; the next write meets the failure.
        }
        return result.bytesProduced() > 0;
    }

    // Hands the engine's delegated tasks to the listener's threads, and leaves the engine alone
    // until they end, when the connection's selector comes back to it.
    private void delegate() throws SSLException {
        if (established) {
            log.info("Broker on port {} refused a new TLS handshake that {} asked for",
                    listener.port, peer);
            throw new SSLException("The client asked for a new handshake, which is refused");
        }
        var batch = new ArrayList<Runnable>();
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            batch.add(task);
        }
        if (batch.isEmpty()) {
            // An engine whose task ended with an Error waits for that task for good. Without
            // this, the connection would go back and forth between the selector and the threads
            // until its deadline.
            throw new SSLException("The TLS engine waits for a task that it does not give");
        }
        Connection owner = requireNonNull(connection, "registered");
        delegated = true;
        try {
            listener.tasks.execute(() -> runDelegated(owner, batch));
        } catch (RejectedExecutionException | OutOfMemoryError failure) {
            delegated = false;
            throw new SSLException("No thread for the TLS handshake", failure);
        }
    }

    // Runs on a thread of the listener. An exception of a task stays with the engine, which
    // reports it at its next use on the selector. An Error, which escapes the engine, ends the
    // tasks of this connection alone, as other failures in the work of one connection do; the
    // engine then fails the handshake.
    private void runDelegated(Connection owner, List<Runnable> batch) {
        try {
            // The tasks of a connection that has ended meanwhile, as at its deadline, would call
            // the application's managers for nothing.
            if (!owner.isEnded()) {
                for (Runnable task : batch) {
                    task.run();
                }
            }
        } catch (Throwable failure) {
            log.warn("Broker on port {} failed a task of the TLS handshake with {}",
                    listener.port, peer, failure);
        } finally {
            // The selector takes the engine back. Its queue hands over what the tasks did to the
            // engine. A broker that has closed drops this, and its end disposes the connection.
            reactor.execute(owner, () -> {
                delegated = false;
                resumed = true;
                owner.wakeNow();
            });
        }
    }

    // Encrypts the bytes of `parts`, after what waits to go out, and sends them as far as the
    // socket takes them, a few records at most. Returns true once all of them have gone out.
    boolean write(ByteBuffer[] parts) throws IOException {
        if (outputFailed) {
            throw new IOException("A write to the socket failed before");
        }
        if (delegated || !flush()) {
            return false;
        }
        ByteBuffer out = reactor.tlsOutput(recordSize * RECORDS_PER_WRITE);
        int records = 0;
        while (records < RECORDS_PER_WRITE && remains(parts)) {
            SSLEngineResult result = engine.wrap(parts, out);
            switch (result.getStatus()) {
                case OK -> {
                    if (result.bytesProduced() == 0) {
                        throw new SSLException("The TLS engine made no record of what to send");
                    }
                }
                case BUFFER_OVERFLOW, BUFFER_UNDERFLOW -> records = RECORDS_PER_WRITE;
                case CLOSED -> throw new SSLException("The TLS output has closed");
            }
            records++;
        }
        out.flip();
        send(out);
        return pendingOutput == null && !remains(parts);
    }

    private static boolean remains(ByteBuffer[] parts) {
        for (ByteBuffer part : parts) {
            if (part.hasRemaining()) {
                return true;
            }
        }
        return false;
    }

    // Sends `out` after what waits to go out, and keeps what the socket does not take.
    private void send(ByteBuffer out) throws IOException {
        if (!out.hasRemaining() || outputFailed) {
            return;
        }
        ByteBuffer waiting = pendingOutput;
        if (waiting == null) {
            write(out);
            if (out.hasRemaining()) {
                pendingOutput = ByteBuffer.allocate(out.remaining()).put(out).flip();
            }
        } else {
            pendingOutput = ByteBuffer.allocate(waiting.remaining() + out.remaining())
                    .put(waiting)
                    .put(out)
                    .flip();
        }
    }

    // Sends what waits to go out. Returns whether nothing waits any more.
    private boolean flush() throws IOException {
        ByteBuffer waiting = pendingOutput;
        if (waiting == null) {
            return true;
        }
        write(waiting);
        if (waiting.hasRemaining()) {
            return false;
        }
        pendingOutput = null;
        return true;
    }

    // Sends what waits to go out before a read, as far as the socket takes it. A failure leaves
    // the read to go on: what has arrived is still read, and the next write meets the failure.
    private boolean flushBeforeReading() {
        if (outputFailed) {
            return false;
        }
        try {
            return flush();
        } catch (IOException _) {
            return false;
        }
    }

    private void write(ByteBuffer out) throws IOException {
        try {
            channel.write(out);
        } catch (IOException failure) {
            outputFailed = true;
            pendingOutput = null;
            throw failure;
        }
    }

    // What the connection waits for on its socket: input when `read`, and room for the rest of a
    // write when `write`, besides what TLS waits for itself. While the delegated tasks run, the
    // connection waits for nothing, and their end brings it back.
    int interest(boolean read, boolean write) {
        if (delegated) {
            return 0;
        }
        int interests = read ? SelectionKey.OP_READ : 0;
        // Records of the engine's own wait for room only while the connection reads, which
        // flushes them; otherwise its next write takes them along.
        if (write || (read && pendingOutput != null && !outputFailed)) {
            interests |= SelectionKey.OP_WRITE;
        }
        return interests;
    }

    // Before the first handshake has finished, a failure is the client's, or that of its settings
    // or the broker's, which an operator wants to see: the broker logs it with the client's
    // address and sends the alert that the engine has made, if the socket takes it at once. Later
    // failures end the connection as its other I/O failures do.
    private void failed(SSLException failure) {
        if (established) {
            return;
        }
        log.info("Broker on port {} failed the TLS handshake with {}: {}",
                listener.port, peer, failure.getMessage());
        sendLast();
    }

    // Tells the client that the broker closes the connection, with close_notify, if the socket
    // takes it at once; the connection closes either way. Called once the connection has ended.
    void close() {
        // The engine's methods take the lock that running delegated tasks hold.
        if (!delegated && !engine.isOutboundDone()) {
            engine.closeOutbound();
            sendLast();
        }
    }

    // Sends the last record that the engine makes, an alert or close_notify, after what waits,
    // if the socket takes them at once. Nothing waits for the client, and nothing fails here.
    private void sendLast() {
        if (delegated || outputFailed) {
            return;
        }
        try {
            ByteBuffer out = reactor.tlsOutput(recordSize);
            SSLEngineResult _ = engine.wrap(NOTHING, out);
            out.flip();
            if (flush()) {
                write(out);
            }
        } catch (IOException | RuntimeException _) {
            // The connection closes anyway, and a client that misses the record sees it close.
        }
    }

    // The certificates that the client presented in its handshake and the context's trust manager
    // accepted, its own first, or none.
    List<X509Certificate> clientCertificates() {
        Certificate[] chain;
        try {
            chain = engine.getSession().getPeerCertificates();
        } catch (SSLPeerUnverifiedException _) {
            return List.of();
        }
        var certificates = new ArrayList<X509Certificate>(chain.length);
        for (Certificate certificate : chain) {
            if (certificate instanceof X509Certificate x509) {
                certificates.add(x509);
            }
        }
        return List.copyOf(certificates);
    }
}
