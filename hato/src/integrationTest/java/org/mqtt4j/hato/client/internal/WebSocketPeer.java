package org.mqtt4j.hato.client.internal;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ProtocolException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.jspecify.annotations.Nullable;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;

/// A WebSocket server for tests, with only what they need of RFC 6455: the opening handshake, the
/// client's masked frames, and the frames that a test sends. A test drives each connection on its
/// own thread and chooses the handshake's answer, how data is split into frames, and how the
/// connection ends. It listens on the loopback address, with or without TLS, and without
/// extensions.
final class WebSocketPeer implements AutoCloseable {

    static final int CONTINUATION = 0x0;

    static final int TEXT = 0x1;

    static final int BINARY = 0x2;

    static final int CLOSE = 0x8;

    static final int PING = 0x9;

    static final int PONG = 0xA;

    // Appended to the client's key before hashing it into the handshake's answer (RFC 6455, 1.3).
    private static final String ACCEPT_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private final ServerSocket server;

    // The start of this server's URIs, up to the port.
    private final String origin;

    private WebSocketPeer(ServerSocket server, String origin) {
        this.server = server;
        this.origin = origin;
    }

    /// Listens on an ephemeral loopback port. Accepting waits at most five seconds.
    static WebSocketPeer listen() throws IOException {
        var server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        server.setSoTimeout(5000);
        return new WebSocketPeer(server, "ws://127.0.0.1:");
    }

    /// Serves `wss` on `server`, a loopback TLS socket such as the one from Tls.serverSocket(),
    /// whose certificate names `localhost`; the server's URIs use that name.
    static WebSocketPeer listen(SSLServerSocket server) {
        return new WebSocketPeer(server, "wss://localhost:");
    }

    /// Limits the receive buffer of the connections accepted afterward, so that a server that
    /// does not read stalls the client's writes.
    void receiveBufferSize(int bytes) throws IOException {
        server.setReceiveBufferSize(bytes);
    }

    /// The URI of this server with `target`, the path and query of the request.
    URI uri(String target) {
        return URI.create(origin + port() + target);
    }

    int port() {
        return server.getLocalPort();
    }

    /// Accepts the next connection and reads its opening handshake, which the test then answers.
    /// With TLS, reading the opening handshake first completes the TLS handshake. Reads on the
    /// connection wait at most five seconds.
    Connection accept() throws IOException {
        Socket socket = server.accept();
        try {
            socket.setSoTimeout(5000);
            return new Connection(socket);
        } catch (IOException | RuntimeException failure) {
            socket.close();
            throw failure;
        }
    }

    @Override
    public void close() throws IOException {
        server.close();
    }

    /// One client connection, from its opening handshake on.
    static final class Connection implements AutoCloseable {

        private final Socket socket;

        private final InputStream input;

        private final OutputStream output;

        private final String target;

        // The request's header fields in their order, with names in lower case.
        private final List<Header> headers;

        // The client's binary messages as one byte stream, and this side's writes as binary
        // messages; both after the handshake.
        private final InputStream binaryInput = new BinaryInput();

        private final OutputStream binaryOutput = new BinaryOutput();

        private Connection(Socket socket) throws IOException {
            this.socket = socket;
            input = new BufferedInputStream(socket.getInputStream());
            output = socket.getOutputStream();
            String requestLine = readLine();
            String[] parts = requestLine.split(" ");
            if (parts.length != 3 || !parts[0].equals("GET")) {
                throw new ProtocolException("Not an opening handshake: " + requestLine);
            }
            target = parts[1];
            var fields = new ArrayList<Header>();
            for (String line = readLine(); !line.isEmpty(); line = readLine()) {
                int colon = line.indexOf(':');
                if (colon < 0) {
                    throw new ProtocolException("Malformed header field: " + line);
                }
                fields.add(new Header(
                        line.substring(0, colon).trim().toLowerCase(Locale.ROOT),
                        line.substring(colon + 1).trim()
                ));
            }
            headers = List.copyOf(fields);
        }

        /// The request target of the handshake: the path and query of the client's URI.
        String target() {
            return target;
        }

        /// The value of the request's first header field named `name`, in any case.
        Optional<String> header(String name) {
            String lowerCase = name.toLowerCase(Locale.ROOT);
            return headers.stream()
                    .filter(header -> header.name().equals(lowerCase))
                    .map(Header::value)
                    .findFirst();
        }

        /// The subprotocols that the client offers, in its order.
        List<String> subprotocols() {
            return headers.stream()
                    .filter(header -> header.name().equals("sec-websocket-protocol"))
                    .flatMap(header -> Arrays.stream(header.value().split(",")))
                    .map(String::trim)
                    .filter(value -> !value.isEmpty())
                    .toList();
        }

        /// Answers the handshake with 101 Switching Protocols and one `Sec-WebSocket-Protocol`
        /// field for each of `subprotocols`, so none when it is empty.
        void open(String... subprotocols) throws IOException {
            String key = header("Sec-WebSocket-Key")
                    .orElseThrow(() -> new ProtocolException("No Sec-WebSocket-Key"));
            var response = new StringBuilder();
            response.append("HTTP/1.1 101 Switching Protocols\r\n")
                    .append("Upgrade: websocket\r\n")
                    .append("Connection: Upgrade\r\n")
                    .append("Sec-WebSocket-Accept: ")
                    .append(accept(key))
                    .append("\r\n");
            for (String subprotocol : subprotocols) {
                response.append("Sec-WebSocket-Protocol: ").append(subprotocol).append("\r\n");
            }
            response.append("\r\n");
            output.write(response.toString().getBytes(ISO_8859_1));
            output.flush();
        }

        /// Starts the answer to the handshake and stops inside its header fields, so the client
        /// waits for the rest.
        void openPartially() throws IOException {
            output.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                    .getBytes(ISO_8859_1));
            output.flush();
        }

        /// Sends one frame, unmasked as server frames are. A message of several frames starts
        /// with its data opcode and continues with `CONTINUATION`; only the last one has `fin`.
        void send(int opcode, boolean fin, byte[] payload) throws IOException {
            var frame = new ByteArrayOutputStream();
            frame.write((fin ? 0x80 : 0) | opcode);
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
            frame.write(payload);
            output.write(frame.toByteArray());
            output.flush();
        }

        void sendBinary(byte[] payload) throws IOException {
            send(BINARY, true, payload);
        }

        void sendText(String text) throws IOException {
            send(TEXT, true, text.getBytes(UTF_8));
        }

        void sendPing(byte[] payload) throws IOException {
            send(PING, true, payload);
        }

        void sendClose(int code, String reason) throws IOException {
            byte[] text = reason.getBytes(UTF_8);
            var payload = new byte[2 + text.length];
            payload[0] = (byte) (code >>> 8);
            payload[1] = (byte) code;
            System.arraycopy(text, 0, payload, 2, text.length);
            send(CLOSE, true, payload);
        }

        /// Reads the client's next frame and unmasks it. Fails when the frame is not masked, as
        /// every client frame must be, or sets the bits that only extensions use.
        Frame receive() throws IOException {
            FrameHeader header = readHeader();
            return new Frame(header.opcode(), header.fin(), payload(header));
        }

        /// The bytes of the client's binary messages in order, however the frames split them.
        /// Reading takes only the bytes that it returns from the socket, so a reader that stops
        /// also stops the client's writes. It answers a Ping with its Pong, and it ends at the
        /// client's Close, which this side does not answer, or when the client drops or resets
        /// the connection between frames.
        InputStream binaryInput() {
            return binaryInput;
        }

        /// Sends each write as one binary message in a single frame.
        OutputStream binaryOutput() {
            return binaryOutput;
        }

        /// The TLS session of a connection to a `wss` server.
        SSLSession tlsSession() {
            return ((SSLSocket) socket).getSession();
        }

        boolean isClosed() {
            return socket.isClosed();
        }

        /// Drops the connection without a Close frame.
        @Override
        public void close() throws IOException {
            socket.close();
        }

        private FrameHeader readHeader() throws IOException {
            int first = readByte();
            int second = readByte();
            if ((first & 0x70) != 0) {
                throw new ProtocolException("Reserved bits without an extension");
            }
            if ((second & 0x80) == 0) {
                throw new ProtocolException("The client sent an unmasked frame");
            }
            long length = second & 0x7F;
            if (length == 126) {
                length = (readByte() << 8) | readByte();
            } else if (length == 127) {
                length = 0;
                for (int i = 0; i < 8; i++) {
                    length = (length << 8) | readByte();
                }
            }
            if (length > Integer.MAX_VALUE - 8) {
                throw new ProtocolException("Frame too large for a test: " + length);
            }
            return new FrameHeader(first & 0x0F, (first & 0x80) != 0, (int) length, readFully(4));
        }

        private byte[] payload(FrameHeader header) throws IOException {
            byte[] payload = readFully(header.length());
            unmask(payload, 0, payload.length, header.mask(), 0);
            return payload;
        }

        // Unmasks `length` bytes that start `position` bytes into the frame's payload.
        private static void unmask(
                byte[] bytes,
                int offset,
                int length,
                byte[] mask,
                long position
        ) {
            for (int i = 0; i < length; i++) {
                bytes[offset + i] ^= mask[(int) ((position + i) & 3)];
            }
        }

        private record FrameHeader(int opcode, boolean fin, int length, byte[] mask) {}

        private final class BinaryInput extends InputStream {

            // The data frame whose payload is being read, and how far.
            private @Nullable FrameHeader frame;

            private int position;

            private boolean ended;

            @Override
            public int read() throws IOException {
                var one = new byte[1];
                return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
            }

            @Override
            public int read(byte[] bytes, int offset, int length) throws IOException {
                if (length == 0) {
                    return 0;
                }
                FrameHeader data = frame;
                while (data == null || position == data.length()) {
                    if (ended) {
                        return -1;
                    }
                    data = nextData();
                }
                int count = input.read(bytes, offset, Math.min(length, data.length() - position));
                if (count < 0) {
                    throw new EOFException("The client closed the connection inside a frame");
                }
                unmask(bytes, offset, count, data.mask(), position);
                position += count;
                return count;
            }

            // The header of the next data frame, after answering or skipping control frames;
            // null at the end of the stream.
            private @Nullable FrameHeader nextData() throws IOException {
                FrameHeader header;
                try {
                    header = readHeader();
                } catch (EOFException | SocketException _) {
                    // The client dropped the connection, with or without a reset.
                    ended = true;
                    return null;
                }
                switch (header.opcode()) {
                    case BINARY, CONTINUATION -> {
                        frame = header;
                        position = 0;
                        return header;
                    }
                    case PING -> send(PONG, true, payload(header));
                    case PONG -> input.skipNBytes(header.length());
                    case CLOSE -> {
                        input.skipNBytes(header.length());
                        ended = true;
                    }
                    default -> throw new ProtocolException(
                            "Unexpected frame with opcode " + header.opcode()
                    );
                }
                return null;
            }
        }

        private final class BinaryOutput extends OutputStream {

            @Override
            public void write(int value) throws IOException {
                sendBinary(new byte[] {(byte) value});
            }

            @Override
            public void write(byte[] bytes, int offset, int length) throws IOException {
                sendBinary(Arrays.copyOfRange(bytes, offset, offset + length));
            }
        }

        private String readLine() throws IOException {
            var line = new ByteArrayOutputStream();
            for (int value = readByte(); value != '\n'; value = readByte()) {
                line.write(value);
            }
            byte[] bytes = line.toByteArray();
            int length = bytes.length > 0 && bytes[bytes.length - 1] == '\r'
                    ? bytes.length - 1
                    : bytes.length;
            return new String(bytes, 0, length, ISO_8859_1);
        }

        private int readByte() throws IOException {
            int value = input.read();
            if (value < 0) {
                throw new EOFException("The client closed the connection");
            }
            return value;
        }

        private byte[] readFully(int length) throws IOException {
            byte[] bytes = input.readNBytes(length);
            if (bytes.length != length) {
                throw new EOFException("The client closed the connection inside a frame");
            }
            return bytes;
        }
    }

    /// A frame from the client, unmasked.
    record Frame(int opcode, boolean fin, byte[] payload) {

        /// The status code of a Close frame, or 1005 (No Status Received) when it has none.
        int closeCode() {
            return payload.length < 2 ? 1005 : ((payload[0] & 0xFF) << 8) | (payload[1] & 0xFF);
        }

        /// The reason of a Close frame, empty when it has none.
        String closeReason() {
            return payload.length <= 2 ? "" : new String(payload, 2, payload.length - 2, UTF_8);
        }
    }

    private record Header(String name, String value) {}

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
}
