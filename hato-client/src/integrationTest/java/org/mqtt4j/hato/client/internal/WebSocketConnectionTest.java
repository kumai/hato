package org.mqtt4j.hato.client.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.WebSocketPeer.Frame;
import org.mqtt4j.hato.client.internal.common.io.FrameReader;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.testsupport.Tls;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.WebSocketPeer.BINARY;
import static org.mqtt4j.hato.client.internal.WebSocketPeer.CONTINUATION;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// MQTT clients that connect with a `ws` or `wss` URI, against the test WebSocket server acting as
/// the broker: the handshake follows the URI, packets survive any split into frames, the client's
/// HttpClient ends with the client, and `wss` uses the TLS parameters that the builder checked.
@Timeout(30)
class WebSocketConnectionTest {

    private final PacketCodec codec = PacketCodec.of(MqttVersion.V5);

    private final LinkedBlockingQueue<Disconnection> ends = new LinkedBlockingQueue<>();

    @Test
    void a_client_connects_with_the_path_and_query_of_its_ws_uri() throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen();
                MqttClient sut = client(server.uri("/mqtt?tenant=a"), ReconnectPolicy.disabled())) {
            // Act
            CompletableFuture<ConnAck> connected = sut.connect();
            try (WebSocketPeer.Connection connection = server.accept()) {
                connection.open("mqtt");
                Frame connect = connection.receive();
                connection.sendBinary(codec.encode(
                        new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY)
                ));

                // Assert
                assertThat(connection.target()).isEqualTo("/mqtt?tenant=a");
                assertThat(connect.opcode()).isEqualTo(BINARY);
                assertThat(codec.decode(connect.payload())).isInstanceOf(Packet.Connect.class);
                assertThat(connected.get(5, SECONDS).sessionPresent()).isFalse();
            }
        }
    }

    @Test
    void packets_split_into_frames_or_joined_in_one_arrive_whole() throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen();
                MqttClient sut = client(server.uri("/mqtt"), ReconnectPolicy.disabled())) {
            // Arrange
            CompletableFuture<ConnAck> connected = sut.connect();
            try (Broker broker = accept(server)) {
                assertThat(broker.receive()).isInstanceOf(Packet.Connect.class);
                byte[] connack = codec.encode(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));

                // Act: the CONNACK in two frames, the second a continuation.
                broker.connection().send(BINARY, false, Arrays.copyOfRange(connack, 0, 2));
                broker.connection().send(
                        CONTINUATION,
                        true,
                        Arrays.copyOfRange(connack, 2, connack.length)
                );
                connected.get(5, SECONDS);
                var received = new LinkedBlockingQueue<MqttMessage>();
                Subscription subscription = sut.subscribe(
                        Subscribe.of(new TopicFilter("ws/joined"), Qos.AT_MOST_ONCE),
                        delivery -> received.add(delivery.message())
                );
                var request = (Packet.Subscribe) broker.receive();
                // Act: the SUBACK and a PUBLISH in one frame.
                var joined = new ByteArrayOutputStream();
                joined.writeBytes(codec.encode(new Packet.SubAck(
                        request.packetId(),
                        List.of(ReasonCode.SUCCESS),
                        EMPTY
                )));
                joined.writeBytes(codec.encode(new Packet.Publish(
                        new Topic("ws/joined"),
                        new byte[] {7},
                        Qos.AT_MOST_ONCE,
                        false,
                        false,
                        0,
                        EMPTY
                )));
                broker.connection().sendBinary(joined.toByteArray());

                // Assert
                subscription.established().get(5, SECONDS);
                assertThat(requireNonNull(received.poll(5, SECONDS)).payload()).containsExactly(7);
            }
        }
    }

    @Test
    void a_text_message_ends_the_connection_and_the_client_connects_again() throws Exception {
        ReconnectPolicy soon = ReconnectPolicy.exponentialBackoff(
                Duration.ofMillis(10),
                Duration.ofMillis(10)
        );
        try (WebSocketPeer server = WebSocketPeer.listen();
                MqttClient sut = client(server.uri("/mqtt"), soon)) {
            // Arrange
            try (Broker first = connect(server, sut)) {
                // Act
                first.connection().sendText("not mqtt");

                // Assert: the end follows the reconnect policy as other transport failures do.
                Disconnection end = requireNonNull(ends.poll(5, SECONDS));
                assertThat(end.reconnectDelay()).isPresent();
            }
            try (Broker second = accept(server)) {
                assertThat(second.receive()).isInstanceOf(Packet.Connect.class);
            }
        }
    }

    @Test
    void a_packet_beyond_the_maximum_size_ends_the_connection() throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen();
                MqttClient sut = MqttClient.builder()
                        .server(server.uri("/mqtt"))
                        .maxPacketSize(1024)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .connectionListener(ends::add)
                        .build();
                Broker broker = connect(server, sut)) {
            // Act
            broker.connection().sendBinary(codec.encode(new Packet.Publish(
                    new Topic("ws/large"),
                    new byte[2000],
                    Qos.AT_MOST_ONCE,
                    false,
                    false,
                    0,
                    EMPTY
            )));

            // Assert: as over TCP, the client says why it closes.
            assertThat(broker.receive())
                    .isEqualTo(new Packet.Disconnect(ReasonCode.PACKET_TOO_LARGE, EMPTY));
            assertThat(requireNonNull(ends.poll(5, SECONDS)).cause()).get()
                    .isInstanceOfSatisfying(MqttProtocolException.class, failure ->
                            assertThat(failure.reasonCode())
                                    .isEqualTo(ReasonCode.PACKET_TOO_LARGE));
        }
    }

    @Test
    void the_client_keeps_one_http_client_across_reconnections() throws Exception {
        // Arrange: the client's WebSocket transports come from a factory that the test holds.
        // The factory is the only place where the client creates an HttpClient.
        var webSockets = new WebSocketTransports("ws-reconnecting", DefaultTimerScheduler.instance);
        ReconnectPolicy soon = ReconnectPolicy.exponentialBackoff(
                Duration.ofMillis(10),
                Duration.ofMillis(10)
        );
        try (WebSocketPeer server = WebSocketPeer.listen();
                MqttClient sut = MqttClient.builder()
                        .server(server.uri("/mqtt"))
                        .transportFactory(webSockets)
                        .reconnectPolicy(soon)
                        .build()) {
            HttpClient first;
            try (Broker _ = connect(server, sut)) {
                first = webSockets.httpClient();
            }

            // Act: the broker dropped the first connection, and the client reconnects.
            try (Broker broker = accept(server)) {
                assertThat(broker.receive()).isInstanceOf(Packet.Connect.class);
                broker.send(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));
                sut.whenConnected().get(5, SECONDS);

                // Assert
                assertThat(webSockets.httpClient()).isSameAs(first);
            }
        }
    }

    @Test
    void closing_a_client_with_an_open_websocket_ends_its_http_client_before_termination()
            throws Exception {
        // Arrange: the client's WebSocket transports come from a factory that the test holds.
        var webSockets = new WebSocketTransports("ws-closing", DefaultTimerScheduler.instance);
        try (WebSocketPeer server = WebSocketPeer.listen()) {
            MqttClient sut = MqttClient.builder()
                    .server(server.uri("/mqtt"))
                    .transportFactory(webSockets)
                    .reconnectPolicy(ReconnectPolicy.disabled())
                    .build();
            try (Broker _ = connect(server, sut)) {
                HttpClient http = webSockets.httpClient();
                var endedFirst = new CompletableFuture<Boolean>();

                // Act: no DISCONNECT, and the broker never answers the closing handshake.
                sut.close();
                sut.terminated().thenRun(() -> endedFirst.complete(http.isTerminated()));

                // Assert: termination waits for the HttpClient and its threads.
                assertThat(endedFirst.get(10, SECONDS)).isTrue();
            }
        }
    }

    @Test
    void the_client_connects_directly_whatever_proxy_the_jvm_sets_for_http() throws Exception {
        // Arrange: a proxy on a port where nothing listens. Without other hosts to exclude,
        // the JVM's proxy selector would send loopback connections there too.
        int closedPort;
        try (var unused = new ServerSocket(0)) {
            closedPort = unused.getLocalPort();
        }
        List<String> names = List.of("http.proxyHost", "http.proxyPort", "http.nonProxyHosts");
        List<String> saved = names.stream().map(System::getProperty).toList();
        System.setProperty("http.proxyHost", "127.0.0.1");
        System.setProperty("http.proxyPort", Integer.toString(closedPort));
        System.setProperty("http.nonProxyHosts", "");
        try (WebSocketPeer server = WebSocketPeer.listen();
                MqttClient sut = client(server.uri("/mqtt"), ReconnectPolicy.disabled())) {
            // Act & Assert
            try (Broker _ = connect(server, sut)) {
                assertThat(sut.whenConnected()).isDone();
            }
        } finally {
            for (int i = 0; i < names.size(); i++) {
                String value = saved.get(i);
                if (value == null) {
                    System.clearProperty(names.get(i));
                } else {
                    System.setProperty(names.get(i), value);
                }
            }
        }
    }

    @Test
    void a_wss_client_of_an_ip_address_uses_the_server_names_of_its_parameters()
            throws Exception {
        // Arrange: the certificate names test.hato.invalid and no IP address, so the client
        // verifies it against the server name.
        try (WebSocketPeer server = WebSocketPeer.listen(Tls.serverSocket());
                MqttClient sut = MqttClient.builder()
                        .server(URI.create("wss://127.0.0.1:" + server.port() + "/mqtt"))
                        .sslContext(Tls.clientContext())
                        .sslParameters(parameters -> parameters.setServerNames(
                                List.of(new SNIHostName("test.hato.invalid"))
                        ))
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .build()) {
            // Act
            try (Broker broker = connect(server, sut)) {
                // Assert
                var session = (ExtendedSSLSession) broker.connection().tlsSession();
                assertThat(session.getRequestedServerNames())
                        .containsExactly(new SNIHostName("test.hato.invalid"));
            }
        }
    }

    @Test
    void wss_parameters_are_copied_at_build_and_reused_for_later_connections() throws Exception {
        // Arrange: application code retains and then mutates the customizable JDK value before
        // the first connection creates the client's HttpClient.
        var customized = new AtomicReference<SSLParameters>();
        var calls = new AtomicInteger();
        ReconnectPolicy soon = ReconnectPolicy.exponentialBackoff(
                Duration.ofMillis(10),
                Duration.ofMillis(10)
        );
        try (WebSocketPeer server = WebSocketPeer.listen(Tls.serverSocket());
                MqttClient sut = MqttClient.builder()
                        .server(server.uri("/mqtt"))
                        .sslContext(Tls.clientContext())
                        .sslParameters(parameters -> {
                            customized.set(parameters);
                            calls.incrementAndGet();
                        })
                        .reconnectPolicy(soon)
                        .build()) {
            requireNonNull(customized.get()).setProtocols(new String[] {"invalid-protocol"});

            // Act: the broker drops the first connection, and the client connects again.
            try (Broker _ = connect(server, sut)) {
                assertThat(calls).hasValue(1);
            }
            try (Broker broker = accept(server)) {
                assertThat(broker.receive()).isInstanceOf(Packet.Connect.class);
            }

            // Assert: both connections used the checked copy, without another callback.
            assertThat(calls).hasValue(1);
        }
    }

    private MqttClient client(URI endpoint, ReconnectPolicy policy) {
        return MqttClient.builder()
                .server(endpoint)
                .reconnectPolicy(policy)
                .connectionListener(ends::add)
                .build();
    }

    private Broker accept(WebSocketPeer server) throws IOException {
        WebSocketPeer.Connection connection = server.accept();
        try {
            connection.open("mqtt");
            return new Broker(connection, codec);
        } catch (IOException | RuntimeException failure) {
            connection.close();
            throw failure;
        }
    }

    // Accepts the client's connection and answers its CONNECT.
    private Broker connect(WebSocketPeer server, MqttClient sut) throws Exception {
        CompletableFuture<ConnAck> connected = sut.connect();
        Broker broker = accept(server);
        try {
            assertThat(broker.receive()).isInstanceOf(Packet.Connect.class);
            broker.send(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));
            connected.get(5, SECONDS);
            return broker;
        } catch (Exception | AssertionError failure) {
            broker.close();
            throw failure;
        }
    }

    /// The broker's side of one WebSocket connection, reading and writing MQTT packets in the
    /// client's binary messages.
    private static final class Broker implements AutoCloseable {

        private final WebSocketPeer.Connection connection;

        private final PacketCodec codec;

        private final FrameReader reader;

        Broker(WebSocketPeer.Connection connection, PacketCodec codec) {
            this.connection = connection;
            this.codec = codec;
            reader = new FrameReader(connection.binaryInput(), 1024 * 1024);
        }

        WebSocketPeer.Connection connection() {
            return connection;
        }

        Packet receive() throws IOException {
            return codec.decode(reader.read().orElseThrow(() -> new AssertionError(
                    "The client closed the connection before sending the expected packet"
            )));
        }

        void send(Packet packet) throws IOException {
            connection.sendBinary(codec.encode(packet));
        }

        @Override
        public void close() throws IOException {
            connection.close();
        }
    }
}
