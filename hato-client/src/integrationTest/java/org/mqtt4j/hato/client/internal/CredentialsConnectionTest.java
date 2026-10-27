package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.SessionPersistenceIntegrationTest.failureOf;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class CredentialsConnectionTest {

    @Test
    void every_explicit_connection_sends_fresh_credentials_and_the_same_will() throws Exception {
        // Arrange
        var evaluations = new AtomicInteger();
        var will = MqttMessage.of(
                new Topic("status/credentials"),
                new byte[] {0, (byte) 0xFF},
                Qos.EXACTLY_ONCE,
                true
        );
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = MqttClient.builder()
                        .server(ScriptedBroker.endpoint(server))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .credentials(() -> MqttCredentials.of(
                                "user-" + evaluations.incrementAndGet(),
                                new byte[] {0, (byte) 0xFF, (byte) evaluations.get()}
                        ))
                        .will(will)
                        .build()) {
            // Act & Assert: reconnecting reevaluates the supplier, including binary passwords,
            // and repeats the Will with its QoS and retention.
            for (int attempt = 1; attempt <= 2; attempt++) {
                int expectedAttempt = attempt;
                try (ScriptedBroker peer = ScriptedBroker.connect(server, sut, false, request -> {
                    assertThat(request.username()).isEqualTo("user-" + expectedAttempt);
                    assertThat(request.password())
                            .containsExactly((byte) 0, (byte) 0xFF, (byte) expectedAttempt);
                    assertThat(request.will()).isEqualTo(connectWill(will));
                })) {
                    assertThat(evaluations).hasValue(expectedAttempt);
                    CompletableFuture<Void> disconnected = sut.disconnect();
                    assertThat(peer.receive()).isInstanceOf(Packet.Disconnect.class);
                    disconnected.get(5, SECONDS);
                    peer.expectClosed();
                }
            }
        }
    }

    @Test
    void the_largest_accepted_connect_inputs_reach_the_broker_intact() throws Exception {
        // Arrange: every CONNECT string and binary field at the 65,535-byte limit.
        String text = "あ".repeat(21_845);
        var password = new byte[65_535];
        var will = MqttMessage.of(new Topic(text), new byte[65_535], Qos.EXACTLY_ONCE, true);
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = MqttClient.builder()
                        .server(ScriptedBroker.endpoint(server))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .clientId(text)
                        .keepAlive(Duration.ofSeconds(65_535))
                        .credentials(() -> MqttCredentials.of(text, password))
                        .will(will)
                        .build()) {
            // Act & Assert
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut, false, request -> {
                assertThat(request.clientId()).isEqualTo(text);
                assertThat(request.keepAliveSeconds()).isEqualTo(65_535);
                assertThat(request.username()).isEqualTo(text);
                assertThat(request.password()).containsExactly(password);
                assertThat(request.will()).isEqualTo(connectWill(will));
            })) {
                peer.roundTripBarrier(sut);
            }
        }
    }

    @Test
    void username_only_credentials_leave_the_password_absent() throws Exception {
        // Arrange
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = MqttClient.builder()
                        .server(ScriptedBroker.endpoint(server))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .credentials(() -> MqttCredentials.of("user"))
                        .build()) {
            // Act & Assert
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut, false, request -> {
                assertThat(request.username()).isEqualTo("user");
                assertThat(request.password()).isNull();
            })) {
                peer.roundTripBarrier(sut);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"4, BAD_USER_NAME_OR_PASSWORD", "5, NOT_AUTHORIZED"})
    void authentication_refusals_preserve_the_reason_and_allow_a_later_attempt(
            int returnCode,
            ReasonCode expected
    ) throws Exception {
        // Arrange
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = MqttClient.builder()
                        .server(ScriptedBroker.endpoint(server))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .credentials(() -> MqttCredentials.of("user", new byte[] {1}))
                        .build()) {
            // Act
            CompletableFuture<ConnAck> connected = sut.connect();
            try (ScriptedBroker peer = ScriptedBroker.accept(server)) {
                assertThat(peer.receive()).isInstanceOf(Packet.Connect.class);
                peer.send(new byte[] {0x20, 2, 0, (byte) returnCode});

                // Assert: raw MQTT 3.1.1 codes map to the public reason without closing the client.
                MqttConnectException failure = assertInstanceOf(
                        MqttConnectException.class,
                        failureOf(connected)
                );
                assertThat(failure.reasonCode()).contains(expected);
                peer.expectClosed();
            }
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                peer.roundTripBarrier(sut);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void credentials_reach_no_log_at_any_level(MqttVersion version) throws Exception {
        // Arrange: every log of the client and the broker records at its finest level.
        String username = "credential-user-7d3a";
        byte[] password = "credential-password-1c9e".getBytes(StandardCharsets.UTF_8);
        var records = new CopyOnWriteArrayList<LogRecord>();
        var handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                records.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        Logger hato = Logger.getLogger("org.mqtt4j.hato");
        Level previousLevel = hato.getLevel();
        boolean previousParents = hato.getUseParentHandlers();
        hato.addHandler(handler);
        hato.setUseParentHandlers(false);
        hato.setLevel(Level.ALL);
        try (MqttBroker broker = MqttBroker.start();
                MqttClient sut = MqttClient.builder()
                        .server(URI.create("mqtt://localhost:" + broker.port()))
                        .version(version)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .credentials(() -> MqttCredentials.of(username, password))
                        .build()) {
            // Act: the credentials travel in the CONNECT of an accepted connection.
            sut.connect().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);
        } finally {
            hato.removeHandler(handler);
            hato.setLevel(previousLevel);
            hato.setUseParentHandlers(previousParents);
        }

        // Assert: the logs saw the exchange down to wire dumps, and neither credential in any
        // form, as a handler that prints messages, parameters, and exceptions would write them.
        assertThat(records).extracting(LogRecord::getLevel).contains(Level.FINER);
        var formatter = new SimpleFormatter();
        HexFormat hex = HexFormat.of();
        assertThat(records).extracting(formatter::format).allSatisfy(text -> assertThat(text)
                .doesNotContain(
                        username,
                        hex.formatHex(username.getBytes(StandardCharsets.UTF_8)),
                        new String(password, StandardCharsets.UTF_8),
                        hex.formatHex(password),
                        Arrays.toString(password)
                ));
    }

    private static Packet.Will connectWill(MqttMessage will) {
        return new Packet.Will(will.topic(), will.payload(), will.qos(), will.retain(), EMPTY);
    }
}
