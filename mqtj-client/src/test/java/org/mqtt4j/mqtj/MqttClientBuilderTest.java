package org.mqtt4j.mqtj;

import java.net.URI;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttClientBuilderTest {

    @ParameterizedTest(name = "accepts {0}")
    @ValueSource(strings = {
            "mqtt://localhost",
            "mqtt://mqtt-broker:1883",
            "mqtt://127.0.0.1:65535",
            "mqtt://[::1]:1883"
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
            "mqtts://localhost",
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
    }

    @Test
    void default_clean_sessions_do_not_require_an_explicit_identifier() {
        assertThatCode(() -> configuredBuilder().build().close()).doesNotThrowAnyException();
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

    private static MqttClientBuilder configuredBuilder() {
        return MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1);
    }
}
