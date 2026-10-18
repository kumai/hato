package org.mqtt4j.hato;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.spi.Transport;

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
    void server_and_supported_version_are_required() {
        assertThatThrownBy(() -> MqttClient.builder().build())
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .build())
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> MqttClient.builder().version(MqttVersion.V5))
                .isInstanceOf(IllegalArgumentException.class);
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
    @SuppressWarnings("NullAway") // Deliberately violates the setters' non-null contracts.
    void credentials_and_will_cannot_be_null() {
        assertThatThrownBy(() -> configuredBuilder().credentials(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> configuredBuilder().will(null))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest(name = "accepts cleanStart={0}, expiry={1}")
    @CsvSource({
            "true, PT0S",
            "false, PT1S",
            "false, PT1.5S",
            "false, PT4294967295S"
    })
    void supported_session_settings_can_be_supplied_in_either_order(
            boolean cleanStart,
            String expiry
    ) {
        Duration duration = Duration.parse(expiry);

        assertThatCode(() -> configuredBuilder()
                .clientId("session-client")
                .cleanStart(cleanStart)
                .sessionExpiry(duration)
                .build().close()).doesNotThrowAnyException();
        assertThatCode(() -> configuredBuilder()
                .clientId("session-client")
                .sessionExpiry(duration)
                .cleanStart(cleanStart)
                .build().close()).doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "rejects cleanStart={0}, expiry={1} at build")
    @CsvSource({"true, PT1S", "false, PT0S"})
    void unsupported_session_combinations_are_rejected_at_build(
            boolean cleanStart,
            String expiry
    ) {
        MqttClientBuilder sut = configuredBuilder()
                .clientId("session-client")
                .cleanStart(cleanStart)
                .sessionExpiry(Duration.parse(expiry));

        assertThatThrownBy(sut::build).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void persistent_sessions_require_an_explicit_identifier() {
        MqttClientBuilder sut = configuredBuilder()
                .cleanStart(false)
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
                .cleanStart(false)
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
}
