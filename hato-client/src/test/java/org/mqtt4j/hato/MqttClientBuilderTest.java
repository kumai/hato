package org.mqtt4j.hato;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.spi.Transport;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttClientBuilderTest {

    @ParameterizedTest(name = "accepts {0}")
    @ValueSource(strings = {
            "mqtt://localhost",
            "mqtt://mqtt-broker:1883",
            "mqtt://127.0.0.1:65535",
            "mqtt://[::1]:1883",
            "mqtts://localhost",
            "mqtts://mqtt-broker:8883",
            "mqtts://[::1]:8883"
    })
    void supported_hosts_and_ip_literals_are_accepted(String uri) {
        // Arrange
        MqttClientBuilder sut = MqttClient.builder();
        URI endpoint = URI.create(uri);

        // Act & Assert
        assertThatCode(() -> sut.server(endpoint)).doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "rejects {0}: {1}")
    @CsvSource({
            "mqtt://mqtt_broker, Illegal character in hostname",
            "mqtt://mqtt_broker:1883, Illegal character in hostname",
            "mqtt://broker:port, Illegal character in port number"
    })
    void invalid_server_authorities_report_the_input_and_reason(String uri, String reason) {
        // Arrange
        MqttClientBuilder sut = MqttClient.builder();
        URI endpoint = URI.create(uri);

        // Act & Assert: the parser's exception repeats the whole input, so it is not attached.
        assertThatThrownBy(() -> sut.server(endpoint))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(uri)
                .hasMessageContaining(reason)
                .hasNoCause();
    }

    @ParameterizedTest(name = "hides user information in {0}")
    @CsvSource({
            "mqtt://alice:s3cret@localhost, s3cret, mqtt://***@localhost",
            "mqtt://alice:p@ss@localhost, p@ss, mqtt://***@localhost",
            "mqtt://alice:s3cret@mqtt_broker:1883, s3cret, mqtt://***@mqtt_broker:1883",
            "mqtt:alice:s3cret@localhost, s3cret, mqtt:***@localhost"
    })
    void rejection_messages_never_reveal_user_information(
            String uri,
            String password,
            String shown
    ) {
        // Arrange
        MqttClientBuilder sut = MqttClient.builder();
        URI endpoint = URI.create(uri);

        // Act & Assert
        assertThatThrownBy(() -> sut.server(endpoint))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(shown)
                .hasMessageNotContaining("alice")
                .hasMessageNotContaining(password)
                .hasNoCause();
    }

    @Test
    void a_server_is_required() {
        assertThatThrownBy(() -> MqttClient.builder().build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void a_client_without_a_version_speaks_mqtt_5() throws Exception {
        // Arrange: a transport that keeps the CONNECT without answering it.
        var transport = new ConnectCapture();
        MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .transportFactory(_ -> transport)
                .build();

        // Act
        CompletableFuture<ConnAck> _ = sut.connect();
        byte[] connect = transport.connect.get(5, SECONDS);
        sut.close();

        // Assert
        assertThat(protocolLevel(connect)).isEqualTo(MqttVersion.V5.protocolLevel());
    }

    @ParameterizedTest(name = "accepts {0}")
    @EnumSource(MqttVersion.class)
    void every_protocol_version_is_accepted(MqttVersion version) {
        MqttClientBuilder sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(version);

        assertThatCode(() -> sut.build().close()).doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "rejects {0}")
    @ValueSource(strings = {
            "ssl://localhost",
            "tls://localhost",
            "tcp://localhost",
            "http://localhost",
            "mqtt://localhost:0",
            "mqtt://localhost:65536",
            "mqtt://localhost/topic",
            "mqtt://localhost?x=1",
            "mqtt://localhost#fragment",
            "mqtt:/"
    })
    void unsupported_or_ambiguous_uris_are_rejected(String uri) {
        // Arrange
        MqttClientBuilder sut = MqttClient.builder();
        URI endpoint = URI.create(uri);

        // Act & Assert
        assertThatThrownBy(() -> sut.server(endpoint))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(uri);
    }

    @Test
    void invalid_durations_packet_sizes_and_identifiers_fail_before_starting_io() {
        // Arrange
        MqttClientBuilder sut = MqttClient.builder();

        // Act & Assert
        assertThatThrownBy(() -> sut.keepAlive(Duration.ofMillis(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.keepAlive(Duration.ofSeconds(65536)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.connectionTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.maxPacketSize(1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.clientId("bad\0id"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.clientId(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.clientId("unpaired\uD800"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.clientId("あ".repeat(21_846)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void response_timeouts_are_positive_and_at_most_the_largest_supported_delay() {
        // Arrange
        MqttClientBuilder sut = MqttClient.builder();
        var largest = Duration.ofMillis(Integer.MAX_VALUE);

        // Act & Assert
        assertThatThrownBy(() -> sut.responseTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.responseTimeout(Duration.ofNanos(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.responseTimeout(largest.plusNanos(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> sut.responseTimeout(largest).responseTimeout(Duration.ofNanos(1)))
                .doesNotThrowAnyException();
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the setters' non-null contracts.
    void reconnection_settings_cannot_be_null() {
        assertThatThrownBy(() -> configuredBuilder().reconnectPolicy(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> configuredBuilder().responseTimeout(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void default_clean_sessions_do_not_require_an_explicit_identifier() {
        assertThatCode(() -> configuredBuilder().build().close()).doesNotThrowAnyException();
    }

    @Test
    void maximum_connect_fields_are_accepted_before_starting_io() {
        String text = "あ".repeat(21_845);
        var credentials = MqttCredentials.of(text, new byte[65_535]);
        var will = MqttMessage.of(new Topic(text), new byte[65_535], Qos.EXACTLY_ONCE, true);
        MqttClientBuilder sut = configuredBuilder()
                .clientId(text)
                .keepAlive(Duration.ofSeconds(65_535))
                .credentials(() -> credentials)
                .will(will);

        assertThatCode(() -> sut.build().close()).doesNotThrowAnyException();
    }

    @Test
    void building_does_not_evaluate_credentials() {
        var calls = new AtomicInteger();
        MqttClientBuilder sut = configuredBuilder().credentials(() -> {
            calls.incrementAndGet();
            return MqttCredentials.of("user");
        });

        sut.build().close();

        assertThat(calls).hasValue(0);
    }

    @Test
    void oversized_will_payloads_are_rejected_at_build() {
        var will = MqttMessage.of(
                new Topic("status"),
                new byte[65_536],
                Qos.AT_MOST_ONCE,
                false
        );
        MqttClientBuilder sut = configuredBuilder().will(will);

        assertThatThrownBy(sut::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Will payload");
    }

    @Test
    void an_mqtt_5_will_carries_its_message_properties_and_delay_in_connect() throws Exception {
        // Arrange
        MqttMessage will = willMessage()
                .withContentType("text/plain")
                .withMessageExpiry(Duration.ofMinutes(1))
                .withUserProperties(List.of(new UserProperty("state", "offline")));
        MqttClientBuilder sut = mqtt5Builder().will(will).willDelay(Duration.ofMillis(1_200));

        // Act
        Packet.Connect connect = sentConnect(sut);

        // Assert: the delay rounds up to whole seconds.
        Packet.Will sent = requireNonNull(connect.will());
        assertThat(sent.properties().entries()).containsExactlyInAnyOrder(
                new Property.StringValue(PropertyId.CONTENT_TYPE, "text/plain"),
                new Property.FourByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, 60),
                new Property.UserProperty("state", "offline"),
                new Property.FourByteValue(PropertyId.WILL_DELAY_INTERVAL, 2)
        );
    }

    @Test
    void a_zero_will_delay_is_not_sent() throws Exception {
        // Arrange
        MqttClientBuilder sut = mqtt5Builder().will(willMessage()).willDelay(Duration.ZERO);

        // Act
        Packet.Connect connect = sentConnect(sut);

        // Assert
        assertThat(requireNonNull(connect.will()).properties().entries()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 4_294_967_295L})
    void will_delays_within_four_bytes_of_seconds_are_accepted(long seconds) {
        // Arrange
        MqttClientBuilder sut = mqtt5Builder()
                .will(willMessage())
                .willDelay(Duration.ofSeconds(seconds));

        // Act & Assert
        assertThatCode(() -> sut.build().close()).doesNotThrowAnyException();
    }

    static Stream<Named<Duration>> willDelaysOutOfRange() {
        return Stream.of(
                Named.of("a negative delay", Duration.ofNanos(-1)),
                Named.of(
                        "a nanosecond beyond 4294967295 seconds",
                        Duration.ofSeconds(4_294_967_295L).plusNanos(1)
                )
        );
    }

    @ParameterizedTest
    @MethodSource("willDelaysOutOfRange")
    void will_delays_out_of_range_are_rejected_at_build(Duration delay) {
        // Arrange
        MqttClientBuilder sut = mqtt5Builder().will(willMessage()).willDelay(delay);

        // Act & Assert
        assertThatThrownBy(sut::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Will delay must be");
    }

    @Test
    void a_will_delay_without_a_will_is_rejected_at_build() {
        // Arrange
        MqttClientBuilder sut = mqtt5Builder().willDelay(Duration.ofSeconds(5));

        // Act & Assert
        assertThatThrownBy(sut::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs a Will");
    }

    @Test
    void will_properties_may_take_up_to_65535_bytes() {
        // Arrange: a User Property takes five bytes besides its name and value.
        var largest = willMessage().withUserProperties(List.of(
                new UserProperty("n", "v".repeat(65_529))
        ));
        var oneMore = willMessage().withUserProperties(List.of(
                new UserProperty("n", "v".repeat(65_530))
        ));

        // Act & Assert
        assertThatCode(() -> mqtt5Builder().will(largest).build().close())
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> mqtt5Builder().will(oneMore).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Will properties");
    }

    @Test
    void a_will_delay_counts_toward_the_will_properties() {
        // Arrange: properties of 65535 bytes leave no room for the delay's five bytes.
        var full = willMessage().withUserProperties(List.of(
                new UserProperty("n", "v".repeat(65_529))
        ));
        MqttClientBuilder sut = mqtt5Builder().will(full).willDelay(Duration.ofSeconds(1));

        // Act & Assert
        assertThatThrownBy(sut::build).isInstanceOf(IllegalArgumentException.class);
    }

    static Stream<Named<UnaryOperator<MqttClientBuilder>>> mqtt5OnlyWillSettings() {
        return Stream.of(
                Named.of(
                        "a Will with properties",
                        builder -> builder.will(willMessage().withPayloadUtf8(true))
                ),
                Named.of(
                        "a Will delay",
                        builder -> builder.will(willMessage()).willDelay(Duration.ofSeconds(1))
                )
        );
    }

    @ParameterizedTest
    @MethodSource("mqtt5OnlyWillSettings")
    void an_mqtt_3_1_1_client_rejects_will_settings_that_only_mqtt_5_carries(
            UnaryOperator<MqttClientBuilder> setting
    ) {
        // Arrange
        MqttClientBuilder sut = setting.apply(configuredBuilder());

        // Act & Assert
        assertThatThrownBy(sut::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("MQTT 3.1.1");
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the setters' non-null contracts.
    void credentials_and_will_cannot_be_null() {
        assertThatThrownBy(() -> configuredBuilder().credentials(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> configuredBuilder().will(null))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest(name = "accepts expiry {0}")
    @ValueSource(strings = {"PT0S", "PT1S", "PT1.5S", "PT4294967295S"})
    void supported_session_expiry_is_accepted(String expiry) {
        MqttClientBuilder sut = configuredBuilder()
                .clientId("session-client")
                .sessionExpiry(Duration.parse(expiry));

        assertThatCode(() -> sut.build().close()).doesNotThrowAnyException();
    }

    @Test
    void persistent_sessions_require_an_explicit_identifier() {
        MqttClientBuilder sut = configuredBuilder()
                .sessionExpiry(Duration.ofSeconds(1));

        assertThatThrownBy(sut::build).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "rejects expiry {0} at build")
    @ValueSource(strings = {
            "PT-0.000000001S",
            "PT0.000000001S",
            "PT0.999999999S",
            "PT4294967295.000000001S",
            "PT4294967296S",
            "PT9223372036854775807S"
    })
    void invalid_session_expiry_is_rejected_at_build(String expiry) {
        MqttClientBuilder sut = configuredBuilder()
                .clientId("session-client")
                .sessionExpiry(Duration.parse(expiry));

        assertThatThrownBy(sut::build).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @SuppressWarnings("NullAway")
    void session_expiry_cannot_be_null() {
        assertThatThrownBy(() -> configuredBuilder().sessionExpiry(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void tls_uses_the_selected_context_defaults_with_hostname_verification() throws Exception {
        // Arrange
        SSLContext context = SSLContext.getInstance("TLSv1.2");
        context.init(null, null, null);
        SSLParameters defaults = context.getDefaultSSLParameters();
        var calls = new AtomicInteger();
        var customized = new ArrayList<SSLParameters>();
        MqttClientBuilder sut = configuredTlsBuilder()
                .sslContext(context)
                .sslParameters(parameters -> {
                    assertThat(parameters.getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
                    assertThat(parameters.getProtocols()).containsExactly(defaults.getProtocols());
                    assertThat(parameters.getCipherSuites())
                            .containsExactly(defaults.getCipherSuites());
                    customized.add(parameters);
                    calls.incrementAndGet();
                });
        assertThat(calls).hasValue(0);

        // Act
        sut.build().close();
        sut.build().close();

        // Assert: one invocation per build, with a fresh set of defaults each time.
        assertThat(calls).hasValue(2);
        assertThat(customized.get(0)).isNotSameAs(customized.get(1));
    }

    @Test
    void tls_defaults_do_not_require_an_explicit_context() {
        assertThatCode(() -> configuredTlsBuilder().build().close()).doesNotThrowAnyException();
    }

    @Test
    void hostname_verification_can_be_explicitly_disabled() {
        assertThatCode(() -> configuredTlsBuilder()
                .sslParameters(parameters -> parameters.setEndpointIdentificationAlgorithm(null))
                .build().close()).doesNotThrowAnyException();
    }

    @Test
    void plaintext_connections_reject_an_explicit_tls_context_at_build() throws Exception {
        SSLContext context = SSLContext.getDefault();
        MqttClientBuilder sut = configuredBuilder().sslContext(context);

        assertThatThrownBy(sut::build).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MqttClient.builder()
                .sslContext(context)
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .build()).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void plaintext_connections_reject_a_tls_customizer_without_invoking_it() {
        var calls = new AtomicInteger();
        MqttClientBuilder sut = configuredBuilder().sslParameters(_ -> calls.incrementAndGet());

        assertThatThrownBy(sut::build).isInstanceOf(IllegalArgumentException.class);
        assertThat(calls).hasValue(0);
    }

    @Test
    void custom_transports_reject_an_explicit_tls_context_at_build() throws Exception {
        SSLContext context = SSLContext.getDefault();
        MqttClientBuilder sut = configuredTlsBuilder()
                .transportFactory(Transport::tcp)
                .sslContext(context);

        assertThatThrownBy(sut::build).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> configuredTlsBuilder()
                .sslContext(context)
                .transportFactory(Transport::tcp)
                .build()).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void custom_transports_reject_a_tls_customizer_without_invoking_it() {
        var calls = new AtomicInteger();
        MqttClientBuilder sut = configuredTlsBuilder()
                .sslParameters(_ -> calls.incrementAndGet())
                .transportFactory(Transport::tcp);

        assertThatThrownBy(sut::build).isInstanceOf(IllegalArgumentException.class);
        assertThat(calls).hasValue(0);
    }

    @Test
    void a_custom_transport_may_handle_tls_without_builder_tls_settings() {
        assertThatCode(() -> configuredTlsBuilder()
                .transportFactory(Transport::tcp)
                .build().close()).doesNotThrowAnyException();
    }

    @Test
    void unsupported_tls_protocols_are_rejected_at_build() {
        MqttClientBuilder sut = configuredTlsBuilder()
                .sslParameters(parameters -> parameters.setProtocols(new String[]{"invalid"}));

        assertThatThrownBy(sut::build).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unsupported_tls_cipher_suites_are_rejected_at_build() {
        MqttClientBuilder sut = configuredTlsBuilder()
                .sslParameters(parameters -> parameters.setCipherSuites(new String[]{"invalid"}));

        assertThatThrownBy(sut::build).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void an_uninitialized_tls_context_is_rejected_at_build() throws Exception {
        MqttClientBuilder sut = configuredTlsBuilder().sslContext(SSLContext.getInstance("TLS"));

        assertThatThrownBy(sut::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void a_customizer_failure_prevents_building_the_client() {
        var failure = new IllegalStateException("customization failed");
        MqttClientBuilder sut = configuredTlsBuilder().sslParameters(_ -> {
            throw failure;
        });

        assertThatThrownBy(sut::build).isSameAs(failure);
    }

    @Test
    @SuppressWarnings("NullAway")
    void tls_settings_cannot_be_null() {
        assertThatThrownBy(() -> configuredTlsBuilder().sslContext(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> configuredTlsBuilder().sslParameters(null))
                .isInstanceOf(NullPointerException.class);
    }

    private static MqttClientBuilder configuredTlsBuilder() {
        return configuredBuilder().server(URI.create("mqtts://localhost"));
    }

    private static MqttClientBuilder configuredBuilder() {
        return MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1);
    }

    private static MqttClientBuilder mqtt5Builder() {
        return MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V5);
    }

    private static MqttMessage willMessage() {
        return MqttMessage.of(new Topic("status"), new byte[0], Qos.AT_LEAST_ONCE, false);
    }

    /// The CONNECT that a client from `builder` sends first, read without answering it.
    private static Packet.Connect sentConnect(MqttClientBuilder builder) throws Exception {
        var transport = new ConnectCapture();
        MqttClient client = builder.transportFactory(_ -> transport).build();
        try {
            CompletableFuture<ConnAck> _ = client.connect();
            byte[] connect = transport.connect.get(5, SECONDS);
            return PacketCodec.decodeConnect(connect, Set.of(MqttVersion.V5));
        } finally {
            client.close();
        }
    }

    /// The protocol level of `connect`: the byte that follows the protocol name.
    private static int protocolLevel(byte[] connect) {
        byte[] name = {0, 4, 'M', 'Q', 'T', 'T'};
        for (int start = 0; start + name.length < connect.length; start++) {
            if (Arrays.equals(connect, start, start + name.length, name, 0, name.length)) {
                return connect[start + name.length];
            }
        }
        throw new AssertionError("CONNECT without a protocol name");
    }

    /// Connects at once and keeps the first packet that the client sends, its CONNECT, without
    /// answering it.
    private static final class ConnectCapture implements Transport {

        final CompletableFuture<byte[]> connect = new CompletableFuture<>();

        private @Nullable Listener listener;

        @Override
        public synchronized void start(URI endpoint, Duration timeout, Listener receiver) {
            listener = receiver;
            receiver.onConnected();
        }

        @Override
        public SendResult send(long ticket, byte[] bytes) {
            boolean _ = connect.complete(bytes);
            return SendResult.ACCEPTED;
        }

        @Override
        public synchronized void close() {
            Listener receiver = listener;
            listener = null;
            if (receiver != null) {
                receiver.onClosed(new IOException("Closed"));
            }
        }
    }
}
