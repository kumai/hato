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
}
