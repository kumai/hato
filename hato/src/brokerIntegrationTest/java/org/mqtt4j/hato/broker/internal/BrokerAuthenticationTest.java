package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.internal.log.Log;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Property;
import org.mqtt4j.hato.internal.protocol.PropertyId;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.internal.protocol.Properties.EMPTY;

/// The application's authenticator: what it learns, how its decisions answer the client, and
/// that a slow, failing, or late decision holds up no other client.
class BrokerAuthenticationTest {

    private static final Duration WAIT = Duration.ofSeconds(5);

    private static final byte[] PASSWORD = "secret-2f61".getBytes(StandardCharsets.UTF_8);

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void the_authenticator_learns_what_the_connect_carries(MqttVersion version) throws Exception {
        // Arrange
        var asked = new AtomicReference<Authenticator.Request>();
        var decidingThread = new AtomicReference<String>();
        Authenticator sut = request -> {
            asked.set(request);
            decidingThread.set(Thread.currentThread().getName());
            return Authenticator.Decision.accept(Authenticator.Scope.all());
        };
        try (BrokerServer broker = BrokerServer.builder().authenticator(sut).start(loopback());
                RawSession client = RawSession.open(broker.port(), version)) {
            // Act
            client.send(connect("robot-1", version, "user-9a1c", PASSWORD));
            // Assert
            assertThat(client.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(ReasonCode.SUCCESS);
            Authenticator.Request request = asked.get();
            assertThat(request.clientId()).isEqualTo("robot-1");
            assertThat(request.username()).contains("user-9a1c");
            assertThat(request.password()).hasValueSatisfying(
                    password -> assertThat(password).containsExactly(PASSWORD)
            );
            assertThat(request.remoteAddress()).hasValueSatisfying(
                    address -> assertThat(address.getAddress().isLoopbackAddress()).isTrue()
            );
            assertThat(request.version()).isEqualTo(version);
            assertThat(request.toString()).doesNotContain("user-9a1c", "secret");
            // The decision ran on a thread for decisions, not on one of the broker's I/O.
            assertThat(decidingThread.get()).contains("-auth-");
        }
    }

    @Test
    void the_authenticator_learns_the_identifier_that_the_broker_assigns() throws Exception {
        // Arrange
        var asked = new AtomicReference<Authenticator.Request>();
        Authenticator sut = request -> {
            asked.set(request);
            return Authenticator.Decision.accept(Authenticator.Scope.all());
        };
        try (BrokerServer broker = BrokerServer.builder().authenticator(sut).start(loopback());
                RawSession client = RawSession.open(broker.port(), MqttVersion.V5)) {
            // Act: an MQTT 5.0 client without an identifier.
            client.send(connect("", MqttVersion.V5, null, null));
            Packet.ConnAck connAck = client.expect(Packet.ConnAck.class);
            // Assert
            assertThat(connAck.properties().find(PropertyId.ASSIGNED_CLIENT_IDENTIFIER))
                    .contains(new Property.StringValue(
                            PropertyId.ASSIGNED_CLIENT_IDENTIFIER,
                            asked.get().clientId()
                    ));
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_refusal_for_credentials_answers_with_bad_user_name_or_password(MqttVersion version)
            throws Exception {
        assertThatRefused(
                version,
                _ -> Authenticator.Decision.refuseBadCredentials(),
                ReasonCode.BAD_USER_NAME_OR_PASSWORD
        );
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_refusal_as_not_authorized_answers_with_not_authorized(MqttVersion version)
            throws Exception {
        assertThatRefused(
                version,
                _ -> Authenticator.Decision.refuseNotAuthorized(),
                ReasonCode.NOT_AUTHORIZED
        );
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_decision_that_throws_refuses_the_client_as_server_unavailable(MqttVersion version)
            throws Exception {
        assertThatRefused(
                version,
                _ -> {
                    throw new IllegalStateException("the accounts are out of reach");
                },
                ReasonCode.SERVER_UNAVAILABLE
        );
    }

    @Test
    void a_refused_client_leaves_the_connection_of_its_identifier_alone() throws Exception {
        // Arrange: an authenticator that accepts the first client of an identifier alone.
        var first = new AtomicBoolean(true);
        Authenticator sut = _ -> first.getAndSet(false)
                ? Authenticator.Decision.accept(Authenticator.Scope.all())
                : Authenticator.Decision.refuseNotAuthorized();
        try (BrokerServer broker = BrokerServer.builder().authenticator(sut).start(loopback());
                RawSession earlier = RawSession.open(broker.port(), MqttVersion.V5);
                RawSession later = RawSession.open(broker.port(), MqttVersion.V5)) {
            earlier.send(connect("same", MqttVersion.V5, null, null));
            assertThat(earlier.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(ReasonCode.SUCCESS);
            // Act
            later.send(connect("same", MqttVersion.V5, null, null));
            // Assert
            assertThat(later.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(ReasonCode.NOT_AUTHORIZED);
            earlier.ping();
        }
    }

    @Test
    void a_decision_that_takes_its_time_holds_up_no_other_client() throws Exception {
        // Arrange: an authenticator that holds its decision on one client.
        var deciding = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Authenticator sut = request -> {
            if (request.clientId().equals("slow")) {
                deciding.countDown();
                release.await();
            }
            return Authenticator.Decision.accept(Authenticator.Scope.all());
        };
        try (BrokerServer broker = BrokerServer.builder().authenticator(sut).start(loopback());
                RawSession connected = RawSession.open(broker.port(), MqttVersion.V5);
                RawSession slow = RawSession.open(broker.port(), MqttVersion.V5);
                RawSession quick = RawSession.open(broker.port(), MqttVersion.V5)) {
            connected.send(connect("connected", MqttVersion.V5, null, null));
            assertThat(connected.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(ReasonCode.SUCCESS);
            slow.send(connect("slow", MqttVersion.V5, null, null));
            assertThat(deciding.await(5, SECONDS)).isTrue();
            // Act & Assert: another client connects, and a connected one is answered.
            try {
                quick.send(connect("quick", MqttVersion.V5, null, null));
                assertThat(quick.expect(Packet.ConnAck.class).reasonCode())
                        .isEqualTo(ReasonCode.SUCCESS);
                connected.ping();
            } finally {
                release.countDown();
            }
            assertThat(slow.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(ReasonCode.SUCCESS);
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_decision_that_takes_too_long_refuses_the_client_as_server_unavailable(
            MqttVersion version
    ) throws Exception {
        // Arrange: a decision that waits until the test lets it go, well after its deadline.
        var release = new CountDownLatch(1);
        Authenticator sut = _ -> {
            release.await();
            return Authenticator.Decision.accept(Authenticator.Scope.all());
        };
        try (BrokerServer broker = BrokerServer.builder()
                .authenticator(sut)
                .authenticationTimeout(Duration.ofMillis(200))
                .start(loopback());
                RawSession client = RawSession.open(broker.port(), version)) {
            try {
                // Act
                client.send(connect("late", version, null, null));
                // Assert
                assertThat(client.expect(Packet.ConnAck.class).reasonCode())
                        .isEqualTo(ReasonCode.SERVER_UNAVAILABLE);
                client.expectClosed();
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void a_decision_that_comes_after_its_deadline_has_no_effect() throws Exception {
        // Arrange: the first decision accepts a persistent session, but only after its
        // deadline; later decisions accept at once.
        var release = new CountDownLatch(1);
        var lateDecisionReturned = new CountDownLatch(1);
        var first = new AtomicBoolean(true);
        Authenticator sut = _ -> {
            if (first.getAndSet(false)) {
                release.await();
                lateDecisionReturned.countDown();
            }
            return Authenticator.Decision.accept(Authenticator.Scope.all());
        };
        try (BrokerServer broker = BrokerServer.builder()
                .authenticator(sut)
                .authenticationTimeout(Duration.ofMillis(200))
                .start(loopback())) {
            try (RawSession late = RawSession.open(broker.port(), MqttVersion.V5)) {
                late.send(RawSession.connectV5("kept", false, 0, 60));
                assertThat(late.expect(Packet.ConnAck.class).reasonCode())
                        .isEqualTo(ReasonCode.SERVER_UNAVAILABLE);
            }
            // Act: the decision accepts the client after the broker has refused it.
            release.countDown();
            assertThat(lateDecisionReturned.await(5, SECONDS)).isTrue();
            // Assert: the late decision started no session that a later client resumes.
            try (RawSession later = RawSession.open(broker.port(), MqttVersion.V5)) {
                later.send(RawSession.connectV5("kept", false, 0, 60));
                Packet.ConnAck connAck = later.expect(Packet.ConnAck.class);
                assertThat(connAck.reasonCode()).isEqualTo(ReasonCode.SUCCESS);
                assertThat(connAck.sessionPresent()).isFalse();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_connect_beyond_the_decisions_that_may_run_is_refused_at_once(MqttVersion version)
            throws Exception {
        // Arrange: room for one decision, which waits until the test lets it go.
        var deciding = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Authenticator sut = request -> {
            if (request.clientId().equals("first")) {
                deciding.countDown();
                release.await();
            }
            return Authenticator.Decision.accept(Authenticator.Scope.all());
        };
        try (BrokerServer broker = BrokerServer.builder()
                .authenticator(sut)
                .maxAuthentications(1)
                .start(loopback());
                RawSession first = RawSession.open(broker.port(), MqttVersion.V5);
                RawSession second = RawSession.open(broker.port(), version)) {
            first.send(connect("first", MqttVersion.V5, null, null));
            assertThat(deciding.await(5, SECONDS)).isTrue();
            try {
                // Act
                second.send(connect("second", version, null, null));
                // Assert: MQTT 3.1.1 has no return code for a busy server.
                assertThat(second.expect(Packet.ConnAck.class).reasonCode()).isEqualTo(
                        (version == MqttVersion.V5)
                                ? ReasonCode.SERVER_BUSY
                                : ReasonCode.SERVER_UNAVAILABLE
                );
            } finally {
                release.countDown();
            }
            assertThat(first.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(ReasonCode.SUCCESS);
        }
    }

    @Test
    @SuppressWarnings("try") // The explicit close is what the test is about.
    void closing_the_broker_interrupts_a_decision_and_its_end_waits_for_it() throws Exception {
        // Arrange: a decision that does not stop for the interrupt until the test lets it go.
        var deciding = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Authenticator sut = _ -> {
            deciding.countDown();
            while (true) {
                try {
                    release.await();
                    return Authenticator.Decision.accept(Authenticator.Scope.all());
                } catch (InterruptedException _) {
                    interrupted.countDown();
                }
            }
        };
        try (BrokerServer broker = BrokerServer.builder().authenticator(sut).start(loopback());
                RawSession client = RawSession.open(broker.port(), MqttVersion.V5)) {
            client.send(connect("waiting", MqttVersion.V5, null, null));
            assertThat(deciding.await(5, SECONDS)).isTrue();
            // Act
            broker.close();
            // Assert
            assertThat(interrupted.await(5, SECONDS)).isTrue();
            assertThat(broker.terminated()).isNotDone();
            release.countDown();
            assertThat(broker.terminated()).succeedsWithin(WAIT);
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void credentials_reach_no_log_when_a_client_is_refused(MqttVersion version) throws Exception {
        // Arrange: every log of the broker records at its finest level, and the authenticator
        // refuses one client for its credentials and fails for another.
        String username = "credential-user-5b0e";
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
        assertThat(Log.awaitLevels(WAIT)).isTrue();
        Authenticator sut = request -> {
            if (request.clientId().equals("failing")) {
                throw new IllegalStateException("the accounts are out of reach");
            }
            return Authenticator.Decision.refuseBadCredentials();
        };
        try (BrokerServer broker = BrokerServer.builder().authenticator(sut).start(loopback())) {
            for (String clientId : List.of("refused", "failing")) {
                try (RawSession client = RawSession.open(broker.port(), version)) {
                    // Act
                    client.send(connect(clientId, version, username, PASSWORD));
                    client.expect(Packet.ConnAck.class);
                    client.expectClosed();
                }
            }
        } finally {
            // The handler stays until it has what the broker logged.
            assertThat(Log.awaitWritten(WAIT)).isTrue();
            hato.removeHandler(handler);
            hato.setLevel(previousLevel);
            hato.setUseParentHandlers(previousParents);
        }

        // Assert: the logs saw the refusals, and neither credential in any form.
        var formatter = new SimpleFormatter();
        HexFormat hex = HexFormat.of();
        assertThat(records).extracting(formatter::format)
                .anySatisfy(text -> assertThat(text).contains("refused"))
                .allSatisfy(text -> assertThat(text).doesNotContain(
                        username,
                        hex.formatHex(username.getBytes(StandardCharsets.UTF_8)),
                        new String(PASSWORD, StandardCharsets.UTF_8),
                        hex.formatHex(PASSWORD),
                        Arrays.toString(PASSWORD)
                ));
    }

    // Starts a broker with `sut`, has a client of `version` connect with credentials, and checks
    // that the broker refuses it with `expected` and closes its connection.
    private static void assertThatRefused(
            MqttVersion version,
            Authenticator sut,
            ReasonCode expected
    ) throws Exception {
        try (BrokerServer broker = BrokerServer.builder().authenticator(sut).start(loopback());
                RawSession client = RawSession.open(broker.port(), version)) {
            // Act
            client.send(connect("client", version, "robot", PASSWORD));
            // Assert
            assertThat(client.expect(Packet.ConnAck.class).reasonCode()).isEqualTo(expected);
            client.expectClosed();
        }
    }

    // A CONNECT that starts a clean session without a keep-alive, with the credentials given.
    private static Packet.Connect connect(
            String clientId,
            MqttVersion version,
            @Nullable String username,
            byte @Nullable [] password
    ) {
        return new Packet.Connect(clientId, version, true, 0, null, username, password, EMPTY);
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
