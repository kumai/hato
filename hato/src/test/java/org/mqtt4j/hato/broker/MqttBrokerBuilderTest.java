package org.mqtt4j.hato.broker;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttBrokerBuilderTest {

    static Stream<Arguments> limitsOutOfRange() {
        return Stream.of(
                Arguments.of("a packet size of 1", setting(sut -> sut.maxPacketSize(1))),
                Arguments.of(
                        "a packet size of 268435461",
                        setting(sut -> sut.maxPacketSize(268_435_461))
                ),
                Arguments.of("no connections", setting(sut -> sut.maxConnections(0))),
                Arguments.of(
                        "no persistent sessions",
                        setting(sut -> sut.maxPersistentSessions(0))
                ),
                Arguments.of(
                        "a session expiry under one second",
                        setting(sut -> sut.maxSessionExpiry(Duration.ofMillis(999)))
                ),
                Arguments.of(
                        "a negative session expiry",
                        setting(sut -> sut.maxSessionExpiry(Duration.ofSeconds(-1)))
                ),
                Arguments.of("no retained messages", setting(sut -> sut.maxRetainedMessages(0))),
                Arguments.of("no retained memory", setting(sut -> sut.maxRetainedBytes(0))),
                Arguments.of("no queued deliveries", setting(sut -> sut.maxQueuedDeliveries(-1))),
                Arguments.of(
                        "no queued delivery memory",
                        setting(sut -> sut.maxQueuedDeliveryBytes(0))
                ),
                Arguments.of(
                        "no time for an authentication",
                        setting(sut -> sut.authenticationTimeout(Duration.ZERO))
                ),
                Arguments.of(
                        "an authentication timeout beyond 2147483647 ms",
                        setting(sut -> sut.authenticationTimeout(
                                Duration.ofMillis(Integer.MAX_VALUE).plusMillis(1)
                        ))
                )
        );
    }

    @ParameterizedTest(name = "rejects {0}")
    @MethodSource("limitsOutOfRange")
    void rejects_a_limit_out_of_its_range_at_once(
            String description,
            Consumer<MqttBrokerBuilder> setting
    ) {
        // Arrange
        MqttBrokerBuilder sut = MqttBroker.builder();
        // Act & Assert
        assertThatThrownBy(() -> setting.accept(sut))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void accepts_the_bounds_of_each_range() {
        // Arrange
        MqttBrokerBuilder sut = MqttBroker.builder();
        // Act & Assert
        assertThatCode(() -> sut.maxPacketSize(2)
                .maxPacketSize(268_435_460)
                .maxConnections(1)
                .maxPersistentSessions(1)
                .maxSessionExpiry(Duration.ofSeconds(1))
                .maxSessionExpiry(Duration.ofSeconds(Long.MAX_VALUE))
                .maxRetainedMessages(1)
                .maxRetainedBytes(1)
                .maxQueuedDeliveries(1)
                .maxQueuedDeliveryBytes(1)
                .authenticationTimeout(Duration.ofMillis(1))
                .authenticationTimeout(Duration.ofMillis(Integer.MAX_VALUE))
        ).doesNotThrowAnyException();
    }

    @Test
    void rejects_an_unresolved_address_at_once() {
        // Arrange
        MqttBrokerBuilder sut = MqttBroker.builder();
        // Act & Assert
        assertThatThrownBy(() -> sut.listen(InetSocketAddress.createUnresolved("localhost", 0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejects_an_unresolved_tls_address_at_once() throws Exception {
        // Arrange
        MqttBrokerBuilder sut = MqttBroker.builder();
        SSLContext context = initializedContext();
        // Act & Assert
        assertThatThrownBy(() -> sut.listenTls(
                InetSocketAddress.createUnresolved("localhost", 0),
                context
        )).isInstanceOf(IllegalArgumentException.class);
    }

    static Stream<Arguments> unusableTlsSettings() throws Exception {
        SSLContext uninitialized = SSLContext.getInstance("TLS");
        SSLContext initialized = initializedContext();
        return Stream.of(
                Arguments.of(
                        "TLS parameters without a TLS listener",
                        setting(sut -> sut.sslParameters(_ -> { }))
                ),
                Arguments.of(
                        "a context that is not initialized",
                        setting(sut -> sut.listenTls(loopback(), uninitialized))
                ),
                Arguments.of(
                        "parameters that the context cannot use",
                        setting(sut -> sut.listenTls(loopback(), initialized)
                                .sslParameters(parameters -> parameters.setProtocols(
                                        new String[] {"TLSv0.9"}
                                )))
                )
        );
    }

    @ParameterizedTest(name = "rejects {0}")
    @MethodSource("unusableTlsSettings")
    void start_rejects_tls_settings_that_it_cannot_use(
            String description,
            Consumer<MqttBrokerBuilder> setting
    ) {
        // Arrange
        MqttBrokerBuilder sut = MqttBroker.builder();
        setting.accept(sut);
        // Act & Assert
        assertThatThrownBy(sut::start).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void start_refuses_a_tls_listener_beyond_the_loopback_address_without_an_authenticator()
            throws Exception {
        // Arrange
        MqttBrokerBuilder sut = MqttBroker.builder()
                .listenTls(new InetSocketAddress(0), initializedContext());
        // Act & Assert
        assertThatThrownBy(sut::start).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "Fleet", "fleet_a", "-fleet", "site..fleet", "fleet.1"})
    void rejects_a_name_that_is_no_host_name_in_lower_case_at_once(String name) {
        // Arrange
        MqttBrokerBuilder sut = MqttBroker.builder();
        // Act & Assert
        assertThatThrownBy(() -> sut.name(name)).isInstanceOf(IllegalArgumentException.class);
    }

    private static Consumer<MqttBrokerBuilder> setting(Consumer<MqttBrokerBuilder> setting) {
        return setting;
    }

    // A context with neither keys nor trust of its own, enough for settings that start no
    // handshake.
    private static SSLContext initializedContext() throws GeneralSecurityException {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, null, null);
        return context;
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
