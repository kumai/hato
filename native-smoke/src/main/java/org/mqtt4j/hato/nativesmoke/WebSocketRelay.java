package org.mqtt4j.hato.nativesmoke;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

/// Serves MQTT over WebSocket in front of the plain TCP broker with JDK classes only. It answers
/// the opening handshake with the mqtt subprotocol, then hands the payloads of the client's
/// binary frames to the broker, and sends each read from the broker to the client as one binary
/// frame. It handles only what the JDK's WebSocket client sends: masked binary and continuation
/// frames, Ping, and Close. Each accepted connection is relayed by its own virtual threads.
final class WebSocketRelay implements AutoCloseable {

    // Appended to the client's key before hashing it into the handshake's answer (RFC 6455, 1.3).
    private static final String ACCEPT_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private static final int CONTINUATION = 0x0;

    private static final int BINARY = 0x2;

    private static final int CLOSE = 0x8;

    private static final int PING = 0x9;

    private static final int PONG = 0xA;

    // The status code 1000 (Normal Closure), as the payload of a Close frame.
    private static final byte[] NORMAL_CLOSURE = {0x03, (byte) 0xE8};

    private final InetSocketAddress broker;
    private final ServerSocket listener;

    WebSocketRelay(int brokerPort) throws IOException {
        broker = new InetSocketAddress(InetAddress.getLoopbackAddress(), brokerPort);
        listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofVirtual().name("hato-smoke-ws-relay").start(this::accept);
    }

    int port() {
        return listener.getLocalPort();
    }

    private void accept() {
        try {
            while (true) {
                Socket client = listener.accept();
                Thread.ofVirtual().start(() -> relay(client));
            }
        } catch (IOException failure) {
            // Closing the listener stops accepting. Relayed connections end with their client
            // or the broker.
            if (!listener.isClosed()) {
                System.err.println("WebSocket relay stopped accepting: " + failure);
            }
        }
    }

    private void relay(Socket client) {
        var plain = new Socket();
        try {
            var input = new DataInputStream(new BufferedInputStream(client.getInputStream()));
            OutputStream output = client.getOutputStream();
            answerHandshake(input, output);
            plain.connect(broker, 5000);
            Thread.ofVirtual().start(() -> toClient(plain, client, output));
            toBroker(input, output, plain.getOutputStream());
        } catch (IOException failure) {
            // The client fails as well; this names the relay's side of a failed handshake.
            System.err.println("WebSocket relay failed: " + failure);
        } finally {
            closeSocket(client);
            closeSocket(plain);
        }
    }

    // Reads the opening handshake and answers it, choosing mqtt when the client offers it.
    private static void answerHandshake(InputStream input, OutputStream output)
            throws IOException {
        // The request line comes first, and only the header fields after it matter here.
        String _ = readLine(input);
        var fields = new HashMap<String, String>();
        for (String line = readLine(input); !line.isEmpty(); line = readLine(input)) {
            int colon = line.indexOf(':');
            if (colon < 0) {
                throw new ProtocolException("Malformed header field: " + line);
            }
            fields.merge(
                    line.substring(0, colon).trim().toLowerCase(Locale.ROOT),
                    line.substring(colon + 1).trim(),
                    (first, second) -> first + "," + second
            );
        }
        String key = fields.get("sec-websocket-key");
        if (key == null) {
            throw new ProtocolException("The opening handshake has no Sec-WebSocket-Key");
        }
        boolean mqtt = Arrays.stream(fields.getOrDefault("sec-websocket-protocol", "").split(","))
                .map(String::trim)
                .anyMatch("mqtt"::equals);
        String answer = "HTTP/1.1 101 Switching Protocols\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Accept: " + accept(key) + "\r\n"
                + (mqtt ? "Sec-WebSocket-Protocol: mqtt\r\n" : "")
                + "\r\n";
        output.write(answer.getBytes(ISO_8859_1));
    }

    // Hands the payloads of the client's frames to the broker until the client's Close, which
    // is answered with a Close, or the end of either connection.
    private static void toBroker(DataInputStream frames, OutputStream client, OutputStream broker) {
        try {
            while (true) {
                int first = frames.readUnsignedByte();
                // The JDK's client masks every frame, as RFC 6455 requires of clients.
                long length = frames.readUnsignedByte() & 0x7F;
                if (length == 126) {
                    length = frames.readUnsignedShort();
                } else if (length == 127) {
                    length = frames.readLong();
                }
                var mask = new byte[4];
                frames.readFully(mask);
                var payload = new byte[Math.toIntExact(length)];
                frames.readFully(payload);
                for (int i = 0; i < payload.length; i++) {
                    payload[i] ^= mask[i & 3];
                }
                switch (first & 0x0F) {
                    case BINARY, CONTINUATION -> broker.write(payload);
                    case PING -> send(client, PONG, payload);
                    case CLOSE -> {
                        send(client, CLOSE, NORMAL_CLOSURE);
                        return;
                    }
                    // MQTT travels only in binary frames, so anything else ends the relay.
                    default -> {
                        return;
                    }
                }
            }
        } catch (IOException _) {
            // A closed connection ends the relay like a Close.
        }
    }

    // Sends what the broker writes, one binary frame for each read, and a Close when the broker
    // ends the connection. Closing both sockets then ends the other direction.
    private static void toClient(Socket broker, Socket client, OutputStream output) {
        try {
            InputStream input = broker.getInputStream();
            var buffer = new byte[16 * 1024];
            for (int count = input.read(buffer); count >= 0; count = input.read(buffer)) {
                send(output, BINARY, Arrays.copyOf(buffer, count));
            }
            send(output, CLOSE, NORMAL_CLOSURE);
        } catch (IOException _) {
            // A closed socket ends the copy like an end of stream.
        } finally {
            closeSocket(client);
            closeSocket(broker);
        }
    }

    // Writes one unmasked frame. Both directions of a connection write to the client through
    // the same stream, so each frame is written whole under its monitor.
    private static void send(OutputStream output, int opcode, byte[] payload) throws IOException {
        var frame = new ByteArrayOutputStream(payload.length + 10);
        frame.write(0x80 | opcode);
        if (payload.length < 126) {
            frame.write(payload.length);
        } else if (payload.length <= 0xFFFF) {
            frame.write(126);
            frame.write(payload.length >>> 8);
            frame.write(payload.length);
        } else {
            frame.write(127);
            for (int shift = 56; shift >= 0; shift -= 8) {
                frame.write((int) ((long) payload.length >>> shift));
            }
        }
        frame.writeBytes(payload);
        synchronized (output) {
            output.write(frame.toByteArray());
        }
    }

    private static String readLine(InputStream input) throws IOException {
        var line = new ByteArrayOutputStream();
        for (int value = input.read(); value != '\n'; value = input.read()) {
            if (value < 0) {
                throw new EOFException("The client closed the connection inside the handshake");
            }
            line.write(value);
        }
        byte[] bytes = line.toByteArray();
        int length = bytes.length > 0 && bytes[bytes.length - 1] == '\r'
                ? bytes.length - 1
                : bytes.length;
        return new String(bytes, 0, length, ISO_8859_1);
    }

    // The Sec-WebSocket-Accept value that answers `key`.
    private static String accept(String key) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1")
                    .digest((key + ACCEPT_GUID).getBytes(ISO_8859_1));
            return Base64.getEncoder().encodeToString(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError("Every Java platform supports SHA-1", impossible);
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
