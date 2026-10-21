package org.mqtt4j.hato.client.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.client.internal.WebSocketPeer.Frame;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.WebSocketPeer.BINARY;
import static org.mqtt4j.hato.client.internal.WebSocketPeer.CLOSE;
import static org.mqtt4j.hato.client.internal.WebSocketPeer.CONTINUATION;
import static org.mqtt4j.hato.client.internal.WebSocketPeer.PONG;

/// The test WebSocket server against the JDK's client, an implementation of RFC 6455 that shares
/// no code with it: the handshake and every kind of frame that the server offers make the round
/// trip.
@Timeout(30)
class WebSocketPeerTest {

    private final HttpClient http = HttpClient.newHttpClient();

    private final Recorder recorder = new Recorder();

    @AfterEach
    void stopClient() {
        // close() would wait for the WebSockets that a test leaves open.
        http.shutdownNow();
    }

    @Test
    void the_handshake_shows_the_request_and_answers_with_the_chosen_subprotocol()
            throws Exception {
        try (WebSocketPeer sut = WebSocketPeer.listen()) {
            // Arrange
            CompletableFuture<WebSocket> opening = http.newWebSocketBuilder()
                    .subprotocols("mqtt", "mqttv3.1")
                    .buildAsync(sut.uri("/mqtt?client=probe"), recorder);

            // Act
            try (WebSocketPeer.Connection connection = sut.accept()) {
                connection.open("mqtt");

                // Assert: the client opens only when the answer hashes its key correctly.
                WebSocket client = opening.get(5, SECONDS);
                assertThat(client.getSubprotocol()).isEqualTo("mqtt");
                assertThat(connection.target()).isEqualTo("/mqtt?client=probe");
                assertThat(connection.header("sec-websocket-version")).contains("13");
                assertThat(connection.subprotocols()).containsExactly("mqtt", "mqttv3.1");
            }
        }
    }

    @Test
    void binary_messages_cross_in_both_directions() throws Exception {
        try (WebSocketPeer sut = WebSocketPeer.listen(); Link link = link(sut)) {
            // Act
            link.client().sendBinary(ByteBuffer.wrap(new byte[] {1, 2, 3}), true).get(5, SECONDS);
            Frame received = link.server().receive();
            link.server().sendBinary(new byte[] {4, 5});

            // Assert: the client's frame arrives unmasked.
            assertThat(received.opcode()).isEqualTo(BINARY);
            assertThat(received.fin()).isTrue();
            assertThat(received.payload()).containsExactly(1, 2, 3);
            assertThat(recorder.events.poll(5, SECONDS))
                    .usingRecursiveComparison()
                    .isEqualTo(new Binary(new byte[] {4, 5}, true));
        }
    }

    @Test
    void split_messages_cross_as_continuation_frames_in_both_directions() throws Exception {
        try (WebSocketPeer sut = WebSocketPeer.listen(); Link link = link(sut)) {
            // Act
            link.server().send(BINARY, false, new byte[] {1});
            link.server().send(CONTINUATION, true, new byte[] {2, 3});
            link.client().sendBinary(ByteBuffer.wrap(new byte[] {7}), false).get(5, SECONDS);
            link.client().sendBinary(ByteBuffer.wrap(new byte[] {8}), true).get(5, SECONDS);

            // Assert
            assertThat(recorder.events.poll(5, SECONDS))
                    .usingRecursiveComparison()
                    .isEqualTo(new Binary(new byte[] {1}, false));
            assertThat(recorder.events.poll(5, SECONDS))
                    .usingRecursiveComparison()
                    .isEqualTo(new Binary(new byte[] {2, 3}, true));
            Frame first = link.server().receive();
            Frame last = link.server().receive();
            assertThat(first.opcode()).isEqualTo(BINARY);
            assertThat(first.fin()).isFalse();
            assertThat(first.payload()).containsExactly(7);
            assertThat(last.opcode()).isEqualTo(CONTINUATION);
            assertThat(last.fin()).isTrue();
            assertThat(last.payload()).containsExactly(8);
        }
    }

    @ParameterizedTest(name = "{0} bytes")
    @ValueSource(ints = {200, 70_000})
    void long_messages_use_the_extended_payload_lengths_in_both_directions(int length)
            throws Exception {
        try (WebSocketPeer sut = WebSocketPeer.listen(); Link link = link(sut)) {
            // Arrange: 200 bytes need the 16-bit payload length, and 70,000 the 64-bit one.
            var payload = new byte[length];
            for (int i = 0; i < length; i++) {
                payload[i] = (byte) i;
            }

            // Act
            link.client().sendBinary(ByteBuffer.wrap(payload), true).get(5, SECONDS);
            byte[] received = serverMessage(link.server());
            link.server().sendBinary(payload);

            // Assert
            assertThat(received).isEqualTo(payload);
            assertThat(clientMessage()).isEqualTo(payload);
        }
    }

    @Test
    void a_text_frame_reaches_the_client_as_text() throws Exception {
        try (WebSocketPeer sut = WebSocketPeer.listen(); Link link = link(sut)) {
            // Act
            link.server().sendText("not mqtt");

            // Assert
            assertThat(recorder.events.poll(5, SECONDS)).isEqualTo(new Text("not mqtt", true));
        }
    }

    @Test
    void the_client_answers_a_ping_with_its_payload() throws Exception {
        try (WebSocketPeer sut = WebSocketPeer.listen(); Link link = link(sut)) {
            // Act
            link.server().sendPing(new byte[] {9, 8});

            // Assert
            Frame pong = link.server().receive();
            assertThat(pong.opcode()).isEqualTo(PONG);
            assertThat(pong.payload()).containsExactly(9, 8);
        }
    }

    @Test
    void a_close_from_the_client_carries_its_code_and_reason() throws Exception {
        try (WebSocketPeer sut = WebSocketPeer.listen(); Link link = link(sut)) {
            // Act
            link.client().sendClose(1001, "going away").get(5, SECONDS);
            Frame close = link.server().receive();
            link.server().sendClose(1001, "going away");

            // Assert
            assertThat(close.opcode()).isEqualTo(CLOSE);
            assertThat(close.closeCode()).isEqualTo(1001);
            assertThat(close.closeReason()).isEqualTo("going away");
            assertThat(recorder.events.poll(5, SECONDS)).isEqualTo(new Close(1001, "going away"));
        }
    }

    @Test
    void a_close_from_the_server_reaches_the_client_which_answers_it() throws Exception {
        try (WebSocketPeer sut = WebSocketPeer.listen(); Link link = link(sut)) {
            // Act
            link.server().sendClose(1000, "done");

            // Assert
            assertThat(recorder.events.poll(5, SECONDS)).isEqualTo(new Close(1000, "done"));
            assertThat(link.server().receive().opcode()).isEqualTo(CLOSE);
        }
    }

    @Test
    void dropping_the_connection_ends_the_client_with_an_abnormal_closure() throws Exception {
        try (WebSocketPeer sut = WebSocketPeer.listen(); Link link = link(sut)) {
            // Act
            link.server().close();

            // Assert: the JDK reports the end without a Close frame as status 1006, which no
            // frame may carry, and not as an error.
            assertThat(recorder.events.poll(5, SECONDS)).isEqualTo(new Close(1006, ""));
        }
    }

    // Opens the JDK client, offering mqtt, and answers its handshake with mqtt.
    private Link link(WebSocketPeer server) throws Exception {
        CompletableFuture<WebSocket> opening = http.newWebSocketBuilder()
                .subprotocols("mqtt")
                .buildAsync(server.uri("/mqtt"), recorder);
        WebSocketPeer.Connection connection = server.accept();
        try {
            connection.open("mqtt");
            return new Link(opening.get(5, SECONDS), connection);
        } catch (Exception | AssertionError failure) {
            connection.close();
            throw failure;
        }
    }

    // The next message from the client, from its frames up to the one with fin.
    private static byte[] serverMessage(WebSocketPeer.Connection server) throws IOException {
        var message = new ByteArrayOutputStream();
        Frame frame;
        do {
            frame = server.receive();
            message.writeBytes(frame.payload());
        } while (!frame.fin());
        return message.toByteArray();
    }

    // The next binary message that the JDK client hands over, which it may split into parts.
    private byte[] clientMessage() throws InterruptedException {
        var message = new ByteArrayOutputStream();
        Binary part;
        do {
            part = (Binary) requireNonNull(recorder.events.poll(5, SECONDS));
            message.writeBytes(part.data());
        } while (!part.last());
        return message.toByteArray();
    }

    /// The JDK client and the server's side of its connection.
    private record Link(
            WebSocket client,
            WebSocketPeer.Connection server
    ) implements AutoCloseable {

        @Override
        public void close() throws IOException {
            server.close();
        }
    }

    // Compared with usingRecursiveComparison(), which compares the data by content.
    private record Binary(byte[] data, boolean last) {}

    private record Text(String data, boolean last) {}

    private record Close(int code, String reason) {}

    /// Records what the JDK client receives, and asks for each next message.
    private static final class Recorder implements WebSocket.Listener {

        final LinkedBlockingQueue<Object> events = new LinkedBlockingQueue<>();

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(1);
        }

        @Override
        public @Nullable CompletionStage<?> onBinary(
                WebSocket webSocket,
                ByteBuffer data,
                boolean last
        ) {
            var bytes = new byte[data.remaining()];
            data.get(bytes);
            events.add(new Binary(bytes, last));
            webSocket.request(1);
            return null;
        }

        @Override
        public @Nullable CompletionStage<?> onText(
                WebSocket webSocket,
                CharSequence data,
                boolean last
        ) {
            events.add(new Text(data.toString(), last));
            webSocket.request(1);
            return null;
        }

        @Override
        public @Nullable CompletionStage<?> onClose(
                WebSocket webSocket,
                int statusCode,
                String reason
        ) {
            events.add(new Close(statusCode, reason));
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            events.add(error);
        }
    }
}
