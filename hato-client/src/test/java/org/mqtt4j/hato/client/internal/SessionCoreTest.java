package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.sun.management.ThreadMXBean;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttCredentials;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttNotConnectedException;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttPublicationRefusedException;
import org.mqtt4j.hato.MqttSubscriptionRefusedException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.RetainHandling;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.UserProperty;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.Qos2SendTest.permutations;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CLOSED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CONNECTING;
import static org.mqtt4j.hato.client.internal.SessionCore.State.DISCONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.Timer.CONNECT;
import static org.mqtt4j.hato.client.internal.SessionCore.Timer.KEEP_ALIVE;
import static org.mqtt4j.hato.client.internal.SessionCore.Timer.PING;
import static org.mqtt4j.hato.client.internal.SessionCore.Timer.RESPONSE;
import static org.mqtt4j.hato.client.internal.SessionPersistenceTest.messageWithWireBytes;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class SessionCoreTest {

    private final RecordingPort port = new RecordingPort();
    private final SessionCore sut = new SessionCore(
            port,
            "client",
            Duration.ofSeconds(30),
            Duration.ofSeconds(10),
            MqttVersion.V3_1_1,
            Duration.ZERO,
            16 * 1024 * 1024,
            null,
            false,
            ReconnectBackoff.disabled(),
            false,
            Duration.ofSeconds(60)
    );

    // The time that interleaved chains pass to the core. Only the keep-alive step advances it,
    // so the core never sees time run backwards.
    private long now;

    @Test
    void credentials_precede_transport_creation_and_connect_waits_for_the_transport() {
        // Arrange
        SessionCore sut = credentialedCore();
        ConnectionAttempt attempt = attempt(1);
        var credentials = MqttCredentials.of("user", new byte[] {1, 2});
        sut.connect(attempt, 0);

        // Act & Assert
        assertThat(port.evaluations).containsExactly(attempt);
        assertThat(port.transportOpenings).isEmpty();
        assertThat(port.sent).isEmpty();
        sut.credentials(attempt, credentials, null, 0);
        sut.credentials(attempt, MqttCredentials.of("replacement"), null, 0);
        assertThat(port.transportOpenings)
                .containsExactly(new TransportOpening(1, Duration.ofSeconds(30)));
        assertThat(port.sent).isEmpty();
        sut.opened(1);
        sut.opened(1);

        assertThat(port.sent).singleElement().isInstanceOfSatisfying(
                Packet.Connect.class,
                packet -> {
                    assertThat(packet.username()).isEqualTo("user");
                    assertThat(packet.password()).containsExactly(1, 2);
                }
        );
    }

    @Test
    void a_connection_without_a_credentials_supplier_opens_transport_immediately() {
        // Act
        sut.connect(attempt(1), 0);

        // Assert
        assertThat(port.evaluations).isEmpty();
        assertThat(port.transportOpenings)
                .containsExactly(new TransportOpening(1, Duration.ofSeconds(30)));
        assertThat(port.sent).isEmpty();
        sut.opened(1);
        assertThat(port.sent).singleElement().isInstanceOf(Packet.Connect.class);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, Long.MAX_VALUE - 1})
    void credentials_use_the_original_deadline_even_when_the_clock_wraps(long started) {
        // Arrange
        SessionCore sut = credentialedCore();
        ConnectionAttempt attempt = attempt(1);
        sut.connect(attempt, started);

        // Act
        sut.credentials(attempt, MqttCredentials.of("user"), null, started + seconds(20));

        // Assert: transport gets only the remaining budget and the total timer is unchanged.
        assertThat(port.transportOpenings)
                .containsExactly(new TransportOpening(1, Duration.ofSeconds(10)));
        assertThat(port.timers).containsEntry(CONNECT, Duration.ofSeconds(30));
        assertThat(port.sent).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(longs = {30_000_000_000L, 30_000_000_001L, 45_000_000_000L})
    void expired_credentials_cannot_open_transport_before_the_timeout_callback(long elapsed) {
        // Arrange: the scheduled deadline has passed, but its callback has not run yet.
        SessionCore sut = credentialedCore();
        ConnectionAttempt attempt = attempt(1);
        sut.connect(attempt, 0);

        // Act
        sut.credentials(attempt, MqttCredentials.of("late"), null, elapsed);

        // Assert
        assertThat(port.transportOpenings).isEmpty();
        assertThat(port.sent).isEmpty();
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasCauseInstanceOf(TimeoutException.class);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.timers).isEmpty();
    }

    @Test
    void a_deadline_during_evaluation_names_the_credentials_phase_and_cancels_it() {
        // Arrange
        SessionCore sut = credentialedCore();
        ConnectionAttempt attempt = attempt(1);
        sut.connect(attempt, 0);

        // Act
        sut.timer(1, CONNECT, seconds(30));

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasRootCauseMessage("Credentials deadline expired");
        assertThat(port.cancellations).containsExactly(attempt);
        assertThat(port.transportOpenings).isEmpty();
    }

    @Test
    void a_supplier_failure_ends_only_its_attempt_and_preserves_the_cause() {
        // Arrange
        SessionCore sut = credentialedCore();
        ConnectionAttempt attempt = attempt(1);
        var failure = new IllegalArgumentException("credentials unavailable");
        sut.connect(attempt, 0);

        // Act
        sut.credentials(attempt, null, failure, seconds(20));
        sut.connect(attempt(2), seconds(20));

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class).hasCause(failure);
        assertThat(port.transportOpenings).isEmpty();
        assertThat(port.sent).isEmpty();
        assertThat(sut.state()).isEqualTo(CONNECTING);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void stale_credentials_cannot_cross_attempt_switch_deadline_or_close(int seed) {
        // Arrange
        SessionCore sut = credentialedCore();
        ConnectionAttempt old = attempt(1);
        ConnectionAttempt current = attempt(2);
        var credentials = MqttCredentials.of("old");
        sut.connect(old, 0);
        var events = new ArrayList<Runnable>(List.of(
                () -> {
                    if (port.transportOpenings.stream().anyMatch(open -> open.generation() == 1)) {
                        sut.opened(1);
                    }
                },
                () -> sut.credentials(old, credentials, null, 0),
                () -> sut.credentials(old, null, new IOException("old failure"), 0),
                () -> sut.timer(1, CONNECT, seconds(30)),
                () -> {
                    sut.failed(1, new IOException("replace attempt"));
                    sut.connect(current, 0);
                },
                () -> sut.close(null)
        ));
        Collections.shuffle(events, new Random(seed));

        // Act
        events.forEach(Runnable::run);
        sut.credentials(old, credentials, null, 0);
        sut.credentials(current, MqttCredentials.of("new"), null, 0);

        // Assert: only the first connection can send before it ends; the second never has
        // credentials until after close. Every admitted operation has exactly one outcome.
        assertThat(port.sent).hasSizeLessThanOrEqualTo(1);
        assertThat(port.transportOpenings).hasSizeLessThanOrEqualTo(1);
        assertThat(port.transportOpenings).allSatisfy(open -> {
            assertThat(open.generation()).isEqualTo(1);
        });
        assertThat(port.sent).allSatisfy(packet -> assertThat(packet)
                .isInstanceOfSatisfying(
                        Packet.Connect.class,
                        connect -> assertThat(connect.username()).isEqualTo("old")
                ));
        assertThat(port.results).containsOnlyKeys(1L, 2L);
        assertThat(sut.state()).isEqualTo(CLOSED);
    }

    @Test
    void old_credentials_and_failures_cannot_change_a_later_attempt() {
        // Arrange
        SessionCore sut = credentialedCore();
        ConnectionAttempt old = attempt(1);
        ConnectionAttempt current = attempt(2);
        sut.connect(old, 0);
        sut.timer(1, CONNECT, seconds(30));
        sut.connect(current, seconds(30));

        // Act
        sut.credentials(old, null, new IOException("late failure"), seconds(31));
        sut.credentials(old, MqttCredentials.of("old"), null, seconds(31));

        // Assert
        assertThat(port.cancellations).containsExactly(old);
        assertThat(sut.state()).isEqualTo(CONNECTING);
        assertThat(port.transportOpenings).isEmpty();
        assertThat(port.sent).isEmpty();
        assertThat(port.results).containsOnlyKeys(1L);
        sut.credentials(current, MqttCredentials.of("current"), null, seconds(32));
        assertThat(port.transportOpenings)
                .containsExactly(new TransportOpening(2, Duration.ofSeconds(28)));
        sut.opened(2);
        assertThat(port.sent).singleElement().isInstanceOfSatisfying(
                Packet.Connect.class,
                packet -> assertThat(packet.username()).isEqualTo("current")
        );
    }

    @Test
    void an_unsolicited_connack_cannot_bypass_credentials_evaluation() {
        // Arrange
        SessionCore sut = credentialedCore();
        ConnectionAttempt attempt = attempt(1);
        sut.connect(attempt, 0);

        // Act
        sut.received(1, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0, 0);
        sut.credentials(attempt, MqttCredentials.of("late"), null, 0);

        // Assert
        assertThat(port.sent).isEmpty();
        assertThat(port.transportOpenings).isEmpty();
        assertThat(port.connections).isEmpty();
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasCauseInstanceOf(MqttProtocolException.class);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void deferred_removal_waits_for_send_capacity_and_completes_only_after_unsuback() {
        // Arrange
        connected();
        establishSubscriptions(1);
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act
        sut.unsubscribe(1000, 2);
        sut.sendCapacityAvailable(1);
        sut.sendCapacityAvailable(1);

        // Assert
        assertThat(port.results).doesNotContainKey(1000L);
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.sendCapacityAvailable(1);
        var packet = (Packet.Unsubscribe) port.sent.getLast();
        assertThat(port.results).doesNotContainKey(1000L);
        receive(1, new Packet.UnsubAck(packet.packetId(), List.of(), EMPTY), 0);
        assertThat(requireNonNull(port.results.get(1000L)).failure()).isNull();
        assertThat(port.removed).containsExactly(2L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void deferred_removals_bound_inflight_work_and_resume_as_each_unsuback_arrives() {
        // Arrange
        connected();
        establishSubscriptions(100);
        for (int i = 0; i < 100; i++) {
            sut.unsubscribe(1000 + i, 2 + i);
        }

        // Act
        sut.sendCapacityAvailable(1);

        // Assert: only a bounded window is sent even though regular send capacity remains.
        assertThat(port.sent).filteredOn(Packet.Unsubscribe.class::isInstance).hasSize(16);
        for (int i = 0; i < 100; i++) {
            List<Packet.Unsubscribe> removals = port.sent.stream()
                    .filter(Packet.Unsubscribe.class::isInstance)
                    .map(Packet.Unsubscribe.class::cast).toList();
            assertThat(removals).hasSize(Math.min(100, i + 16));
            Packet.Unsubscribe removal = removals.get(i);
            assertThat(removal.filters()).containsExactly(new TopicFilter("slot/" + i));
            receive(1, new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY), 0);
            assertThat(requireNonNull(port.results.get(1000L + i)).failure()).isNull();
        }
        assertThat(port.removed).hasSize(100).doesNotHaveDuplicates();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest
    @EnumSource(SendQueue.OfferResult.class)
    void clean_session_termination_finishes_sent_and_queued_removals(
            SendQueue.OfferResult result
    ) {
        // Arrange
        connected();
        establishSubscriptions(20);
        port.sendResult = result;
        for (int i = 0; i < 20; i++) {
            sut.unsubscribe(1000 + i, 2 + i);
        }
        sut.sendCapacityAvailable(1);
        var failure = new IOException("connection lost during removal");

        // Act
        sut.failed(1, failure);

        // Assert: ending a clean session proves that all broker registrations are gone.
        for (int i = 0; i < 20; i++) {
            assertThat(requireNonNull(port.results.get(1000L + i)).failure()).isNull();
        }
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        port.sent.clear();
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.connect(attempt(2000), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        sut.sendCapacityAvailable(1);
        sut.sendCapacityAvailable(2);
        assertThat(port.sent).isEmpty();
        for (int i = 0; i < 20; i++) {
            assertThat(requireNonNull(port.results.get(1000L + i)).failure()).isNull();
        }
        assertThat(port.removed).hasSize(20).doesNotHaveDuplicates();
        // Exchanges abandoned with the old connection no longer occupy the closure window.
        sut.subscribe(3000, subscription("slot/0"));
        var subscribe = (Packet.Subscribe) port.sent.getLast();
        receive(2, new Packet.SubAck(subscribe.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        sut.unsubscribe(3001, 3000);
        sut.sendCapacityAvailable(2);
        assertThat(port.sent.getLast()).isInstanceOf(Packet.Unsubscribe.class);
    }

    @Test
    void a_pending_disconnect_prevents_deferred_removals_from_being_sent() {
        // Arrange
        connected();
        establishSubscriptions(1);
        sut.publish(3, qosOneMessage());
        int id = port.publicationIds.getLast();
        port.sendResult = SendQueue.OfferResult.FULL;
        sut.unsubscribe(1000, 2);

        // Act: returned send capacity and a PUBACK can both arrive before DISCONNECT is written.
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.disconnect(2000);
        sut.sendCapacityAvailable(1);
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert
        assertThat(port.sent.getLast()).isInstanceOf(Packet.Disconnect.class);
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
        sut.sent(1, port.ticket, 0);
        assertThat(requireNonNull(port.results.get(1000L)).failure()).isNull();
        sut.connect(attempt(2001), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        assertThat(requireNonNull(port.results.get(1000L)).failure()).isNull();
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
    }

    @Test
    void confirmed_and_pending_subscriptions_keep_their_handles_after_reconnect() {
        // Arrange: one confirmed subscription and one whose first SUBACK never arrives.
        connected();
        sut.subscribe(2, Subscribe.of(new TopicFilter("a/#"), Qos.AT_LEAST_ONCE));
        var initial = (Packet.Subscribe) port.sent.getLast();
        receive(1, new Packet.SubAck(initial.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        var handle = (Subscription) requireNonNull(port.outcomes.get(2L)).value();
        sut.subscribe(3, subscription("other/#"));

        // Act
        sut.failed(1, new IOException("lost before initial SUBACK"));
        port.sent.clear();
        sut.connect(attempt(4), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert: both registrations survive, without replacing their delivery lanes.
        assertThat(requireNonNull(port.results.get(4L)).value()).isEqualTo(ConnAck.of(false, 2));
        assertThat(port.outcomes).doesNotContainKey(3L);
        assertThat(port.removed).isEmpty();
        assertThat(port.registered).containsExactly(2L, 3L);
        assertThat(port.sent).hasSize(2).allMatch(Packet.Subscribe.class::isInstance);
        assertThat(requireNonNull(handle).grantedQos()).isEqualTo(Qos.AT_MOST_ONCE);

        // Act: recreation updates the original grant; the other SUBACK establishes its handle.
        var recreated = (Packet.Subscribe) port.sent.getFirst();
        var pending = (Packet.Subscribe) port.sent.getLast();
        receive(
                2,
                new Packet.SubAck(recreated.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY),
                0
        );
        receive(2, new Packet.SubAck(pending.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);

        // Assert: RecordingPort rejects duplicate operation completion.
        assertThat(requireNonNull(port.outcomes.get(2L)).value()).isSameAs(handle);
        assertThat(handle.grantedQos()).isEqualTo(Qos.AT_LEAST_ONCE);
        assertThat(port.grants).containsExactly(
                handle,
                handle,
                (Subscription) requireNonNull(port.outcomes.get(3L)).value()
        );
        sut.close(null);
        assertThat(port.removed).containsExactly(2L, 3L);
    }

    @Test
    void recreating_the_subscription_limit_waits_for_capacity_and_bounds_the_suback_window() {
        // Arrange: a full registration table is larger than the ordinary send queue.
        connected();
        establishSubscriptions(256);
        sut.failed(1, new IOException("lost connection"));
        port.sent.clear();
        port.sendResult = SendQueue.OfferResult.FULL;
        sut.connect(attempt(1000), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        assertThat(requireNonNull(port.results.get(1000L)).failure()).isNull();
        assertThat(port.sent).isEmpty();

        // Act: old capacity cannot resume work; current capacity opens only the bounded window.
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.sendCapacityAvailable(1);
        assertThat(port.sent).isEmpty();
        sut.sendCapacityAvailable(2);

        // Assert: each SUBACK admits one replacement without registering or completing again.
        for (int i = 0; i < 256; i++) {
            assertThat(port.sent).hasSize(Math.min(256, i + 16));
            var packet = (Packet.Subscribe) port.sent.get(i);
            assertThat(packet.subscriptions().getFirst().filter())
                    .isEqualTo(new TopicFilter("slot/" + i));
            receive(2, new Packet.SubAck(packet.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        }
        assertThat(port.registered).hasSize(256).doesNotHaveDuplicates();
        assertThat(port.grants).hasSize(512);
        assertThat(port.removed).isEmpty();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void a_refused_recreation_ends_only_that_subscription_and_notifies_with_its_original_handle() {
        // Arrange: unsubscribe waits for recreation while another subscription stays active.
        connected();
        establishSubscriptions(2);
        Object refused = requireNonNull(port.outcomes.get(2L)).value();
        sut.failed(1, new IOException("lost connection"));
        port.sent.clear();
        sut.connect(attempt(4), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        var first = (Packet.Subscribe) port.sent.getFirst();
        var second = (Packet.Subscribe) port.sent.getLast();
        sut.unsubscribe(5, 2);

        // Act
        receive(
                2,
                new Packet.SubAck(
                        first.packetId(),
                        List.of(ReasonCode.UNSPECIFIED_ERROR),
                        EMPTY
                ),
                0
        );
        receive(2, new Packet.SubAck(second.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        receive(2, qosOnePublication(7, "slot/1", false), 0);

        // Assert: refusal has one notification and cannot terminate another subscription.
        assertThat(port.removed).containsExactly(2L);
        assertThat(port.removals).singleElement().satisfies(removal -> {
            assertThat(removal.handle()).isSameAs(refused);
            assertThat(removal.cause()).isInstanceOf(MqttSubscriptionRefusedException.class);
        });
        assertThat(port.resubscriptionFailures).containsExactlyElementsOf(port.removals);
        assertThat(requireNonNull(port.results.get(5L)).failure()).isNull();
        assertThat(port.deliveries).singleElement()
                .satisfies(delivery -> assertThat(delivery.ids()).containsExactly(3L));
        assertThat(port.ends).hasSize(1);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void an_interrupted_clean_session_removal_completes_without_restoring_delivery() {
        // Arrange
        connected();
        establishSubscriptions(1);
        sut.unsubscribe(3, 2);

        // Act
        sut.failed(1, new IOException("lost before UNSUBACK"));
        port.sent.clear();
        sut.connect(attempt(4), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        receive(2, qosOnePublication(7, "slot/0", false), 0);

        // Assert: the old clean session has no broker registration to remove.
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        assertThat(port.registered).containsExactly(2L);
        assertThat(port.removed).containsExactly(2L);
        assertThat(port.deliveries).isEmpty();
        assertThat(port.sent).noneMatch(Packet.Subscribe.class::isInstance);
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
    }

    @Test
    void a_subscribe_exception_that_closes_the_client_is_not_a_recreation_refusal() {
        // Arrange
        connected();
        establishSubscriptions(1);
        var failure = new MqttSubscriptionRefusedException(ReasonCode.UNSPECIFIED_ERROR);

        // Act
        sut.close(failure);

        // Assert: an exception's type does not determine why the subscription ended.
        assertThat(port.resubscriptionFailures).isEmpty();
        assertThat(port.removals).singleElement()
                .satisfies(removal -> assertThat(removal.cause()).isSameAs(failure));
        assertThat(sut.state()).isEqualTo(CLOSED);
    }

    @ParameterizedTest(name = "close while connecting: {0}")
    @ValueSource(booleans = {false, true})
    void closing_without_an_established_clean_connection_finishes_immediately(
            boolean connecting
    ) {
        // Arrange
        connected();
        establishSubscriptions(1);
        sut.failed(1, new IOException("lost connection"));
        port.sent.clear();
        if (connecting) {
            sut.connect(attempt(4), 0);
        }

        // Act
        sut.unsubscribe(3, 2);
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        if (!connecting) {
            sut.connect(attempt(4), 0);
        }
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        sut.sendCapacityAvailable(2);

        // Assert: neither SUBSCRIBE nor UNSUBSCRIBE is needed for the closed intention.
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        assertThat(port.sent).isEmpty();
        assertThat(port.removed).containsExactly(2L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "recreation refused: {0}")
    @ValueSource(booleans = {false, true})
    void unsubscribe_during_recreation_waits_for_suback_and_accepts_its_refusal(boolean refused) {
        // Arrange
        connected();
        establishSubscriptions(1);
        Object handle = requireNonNull(port.outcomes.get(2L)).value();
        sut.failed(1, new IOException("lost connection"));
        port.sent.clear();
        sut.connect(attempt(3), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        var recreated = (Packet.Subscribe) port.sent.getLast();

        // Act
        sut.unsubscribe(4, 2);
        assertThat(port.results).doesNotContainKey(4L);
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
        receive(2, new Packet.SubAck(recreated.packetId(), List.of(refused
                ? ReasonCode.UNSPECIFIED_ERROR : ReasonCode.SUCCESS), EMPTY), 0);

        // Assert: refusal already proves that the broker has no matching subscription.
        if (refused) {
            assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
            assertThat(port.removals).singleElement().satisfies(removal ->
                    assertThat(removal.cause())
                            .isInstanceOf(MqttSubscriptionRefusedException.class));
        } else {
            var removal = (Packet.Unsubscribe) port.sent.getLast();
            assertThat(port.results).doesNotContainKey(4L);
            receive(2, new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY), 0);
        }
        assertThat(requireNonNull(port.results.get(4L)).failure()).isNull();
        assertThat(requireNonNull(port.outcomes.get(2L)).value()).isSameAs(handle);
        assertThat(port.removed).containsExactly(2L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "recreation sent: {0}")
    @ValueSource(booleans = {false, true})
    void close_during_recreation_waits_only_for_an_already_sent_subscribe(boolean sent) {
        // Arrange
        connected();
        establishSubscriptions(1);
        sut.failed(1, new IOException("lost connection"));
        port.sent.clear();
        port.sendResult = sent ? SendQueue.OfferResult.ACCEPTED : SendQueue.OfferResult.FULL;
        sut.connect(attempt(3), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);

        // Act
        sut.unsubscribe(4, 2);
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.sendCapacityAvailable(2);

        // Assert
        if (sent) {
            assertThat(port.results).doesNotContainKey(4L);
            assertThat(port.sent).singleElement().isInstanceOf(Packet.Subscribe.class);
            var recreated = (Packet.Subscribe) port.sent.getLast();
            receive(2, new Packet.SubAck(recreated.packetId(),
                    List.of(ReasonCode.SUCCESS), EMPTY), 0);
            var removal = (Packet.Unsubscribe) port.sent.getLast();
            receive(2, new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY), 0);
        } else {
            assertThat(port.sent).isEmpty();
        }
        assertThat(requireNonNull(port.results.get(4L)).failure()).isNull();
        assertThat(port.removed).containsExactly(2L);
    }

    private void establishSubscriptions(int count) {
        for (int i = 0; i < count; i++) {
            sut.subscribe(2 + i, subscription("slot/" + i));
            var packet = (Packet.Subscribe) port.sent.getLast();
            receive(1, new Packet.SubAck(packet.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        }
    }

    @Test
    void connect_waits_for_connack_and_another_request_joins_the_attempt() {
        // Arrange
        sut.connect(attempt(1), 0);

        // Act
        sut.opened(1);
        sut.connect(attempt(2), 0);

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTING);
        assertThat(port.results).doesNotContainKeys(1L, 2L);
        assertThat(port.sent).singleElement().isInstanceOf(Packet.Connect.class);
        assertThat(port.opened).containsExactly(1L);

        accept();
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(requireNonNull(port.results.get(1L)).value()).isEqualTo(ConnAck.of(false, 1));
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(ConnAck.of(false, 1));
        assertThat(port.timers).doesNotContainKey(CONNECT);
    }

    @Test
    void an_established_connection_is_announced_before_the_connect_operation_completes() {
        // Arrange: connection admission must become possible independently of future completion.
        ConnectionAttempt first = attempt(1);
        sut.connect(first, 0);
        port.onConnected = () -> {
            assertThat(sut.state()).isEqualTo(CONNECTED);
            assertThat(port.results).doesNotContainKey(1L);
        };

        // Act
        accept();

        // Assert
        assertThat(port.connections).containsExactly(new Connection(1, ConnAck.of(false, 1)));
        assertThat(requireNonNull(port.results.get(1L)).value()).isEqualTo(ConnAck.of(false, 1));
        assertThat(port.attempts).containsExactly(first);
    }

    @Test
    void a_tokenless_attempt_establishes_a_connection_without_completing_an_operation() {
        // Arrange
        ConnectionAttempt first = attempt(0);
        ConnectionAttempt second = attempt(0);
        sut.connect(first, 0);
        sut.opened(1);

        // Act
        accept();
        sut.failed(1, new IOException("connection ended"));
        sut.connect(second, 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        sut.close(null);

        // Assert: each connection opens for its own attempt despite the shared absent token.
        assertThat(port.results).isEmpty();
        assertThat(port.connections).containsExactly(
                new Connection(1, ConnAck.of(false, 1)),
                new Connection(2, ConnAck.of(false, 2))
        );
        assertThat(port.attempts).containsExactly(first, second);
        assertThat(port.established).containsExactly(true, true);
        assertThat(sut.state()).isEqualTo(CLOSED);
    }

    @Test
    void a_refused_tokenless_attempt_does_not_complete_an_operation_or_announce_a_connection() {
        // Arrange
        sut.connect(attempt(0), 0);

        // Act
        receive(1, new Packet.ConnAck(false, ReasonCode.NOT_AUTHORIZED, EMPTY), 0);

        // Assert
        assertThat(port.results).isEmpty();
        assertThat(port.connections).isEmpty();
        assertThat(port.established).containsExactly(false);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void a_rejected_tokenless_attempt_does_not_replace_the_current_attempt() {
        // Arrange
        sut.connect(attempt(1), 0);

        // Act
        sut.connect(attempt(0), 0);
        accept();

        // Assert
        assertThat(port.results).containsOnlyKeys(1L);
        assertThat(port.opened).containsExactly(1L);
        assertThat(port.connections).containsExactly(new Connection(1, ConnAck.of(false, 1)));
    }

    @Test
    void a_late_connack_cannot_announce_a_connection_after_the_attempt_ends() {
        // Arrange
        sut.connect(attempt(1), 0);
        sut.failed(1, new IOException("first attempt failed"));
        sut.connect(attempt(2), 0);

        // Act & Assert: the ended attempt's CONNACK announces nothing.
        receive(1, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        assertThat(port.connections).isEmpty();

        // Act
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert
        assertThat(port.connections).containsExactly(new Connection(2, ConnAck.of(false, 2)));
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class);
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(ConnAck.of(false, 2));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("attemptOrders")
    void connection_attempt_events_have_one_outcome_in_every_order(AttemptOrder scenario) {
        // Arrange: both public and tokenless attempts share the same connection lifecycle.
        sut.connect(attempt(scenario.operation()), 0);
        sut.opened(1);
        var failure = new IOException("transport failed");
        port.onConnected = () -> assertThat(port.ends).isEmpty();

        // Act: after the first terminal event, the remaining events cannot change its outcome.
        for (ConnectionEvent event : scenario.events()) {
            switch (event) {
                case CONNACK -> accept();
                case FAILURE -> sut.failed(1, failure);
                case DEADLINE -> sut.timer(1, CONNECT, seconds(30));
                case CLOSE -> sut.close(null);
            }
        }

        // Assert: only a CONNACK that precedes all terminal input can establish the connection.
        boolean accepted = scenario.events().getFirst() == ConnectionEvent.CONNACK;
        assertThat(port.connections).hasSize(accepted ? 1 : 0);
        assertThat(port.established).containsExactly(accepted);
        assertThat(port.ends).hasSize(1);
        assertThat(port.opened).containsExactly(1L);
        assertThat(port.timers).isEmpty();
        assertThat(sut.state()).isEqualTo(CLOSED);
        if (scenario.operation() == 0) {
            assertThat(port.results).isEmpty();
        } else {
            assertThat(port.results).containsOnlyKeys(scenario.operation());
            Result result = requireNonNull(port.results.get(scenario.operation()));
            if (accepted) {
                assertThat(result.value()).isEqualTo(ConnAck.of(false, 1));
                assertThat(result.failure()).isNull();
            } else {
                assertThat(result.value()).isNull();
                assertThat(result.failure()).isInstanceOf(MqttConnectException.class);
            }
        }
    }

    private static Stream<AttemptOrder> attemptOrders() {
        List<ConnectionEvent> events = List.of(ConnectionEvent.values());
        List<List<ConnectionEvent>> orders = permutations(events).toList();
        assertThat(orders).hasSize(24).doesNotHaveDuplicates()
                .allSatisfy(order -> assertThat(order)
                        .containsExactlyInAnyOrder(ConnectionEvent.values()));
        List<AttemptOrder> scenarios = orders.stream()
                .flatMap(order -> Stream.of(new AttemptOrder(1, order), new AttemptOrder(0, order)))
                .toList();
        assertThat(scenarios).hasSize(48).doesNotHaveDuplicates();
        return scenarios.stream();
    }

    private enum ConnectionEvent {
        CONNACK,
        FAILURE,
        DEADLINE,
        CLOSE
    }

    private record AttemptOrder(long operation, List<ConnectionEvent> events) {
    }

    @Test
    void refusal_preserves_the_broker_reason_and_does_not_notify_the_listener() {
        // Arrange
        sut.connect(attempt(1), 0);

        // Act
        receive(1, new Packet.ConnAck(false, ReasonCode.NOT_AUTHORIZED, EMPTY), 0);

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .hasNoCause()
                .isInstanceOfSatisfying(
                        MqttConnectException.class,
                        failure -> assertThat(failure.reasonCode())
                                .contains(ReasonCode.NOT_AUTHORIZED)
                );
        assertThat(port.established).containsExactly(false);
        assertThat(port.connections).isEmpty();
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void refusing_connect_for_capacity_fails_only_the_connection_attempt() {
        // Arrange
        sut.connect(attempt(1), 0);
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act
        sut.opened(1);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasCauseInstanceOf(MqttCapacityException.class);
        assertThat(port.established).containsExactly(false);
        assertThat(port.timers).isEmpty();
        assertThat(port.sent).isEmpty();
    }

    @Test
    void refusing_ping_for_capacity_ends_the_connection_without_registering_another_timer() {
        // Arrange
        connected();
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act
        sut.timer(1, KEEP_ALIVE, seconds(10));

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class);
            assertThat(end.userRequested()).isFalse();
        });
        assertThat(port.timers).isEmpty();
        assertThat(port.sent).isEmpty();
    }

    @Test
    void refusing_disconnect_for_capacity_fails_the_operation_and_ends_the_connection() {
        // Arrange
        connected();
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act
        sut.disconnect(2);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        Throwable cause = port.ends.getFirst().cause().orElseThrow();
        assertThat(cause).isInstanceOf(MqttCapacityException.class);
        assertThat(port.ends).containsExactly(Disconnection.of(cause, true, 1));
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(cause);
        assertThat(port.timers).isEmpty();
        assertThat(port.sent).isEmpty();
    }

    @Test
    void a_closed_queue_leaves_the_pending_operation_for_the_original_termination_cause() {
        // Arrange
        connected();
        port.sendResult = SendQueue.OfferResult.CLOSED;
        var failure = new IOException("Peer reset the connection");

        // Act
        sut.disconnect(2);

        // Assert: refusal alone neither fails the operation nor reports a capacity failure.
        assertThat(port.results).doesNotContainKey(2L);
        assertThat(port.ends).isEmpty();
        assertThat(port.sent).isEmpty();

        // Act
        sut.failed(1, failure);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(failure);
        assertThat(port.ends).containsExactly(Disconnection.of(failure, true, 1));
    }

    @Test
    void a_packet_before_connack_and_a_present_clean_session_are_protocol_errors() {
        // Arrange
        sut.connect(attempt(1), 0);

        // Act
        receive(1, new Packet.PingResp(), 0);

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class);
        sut.connect(attempt(2), 0);
        receive(2, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 0);
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectException.class);
        assertThat(port.established).containsExactly(false, false);
    }

    @Test
    void the_connect_deadline_and_old_connection_events_cannot_affect_a_new_attempt() {
        // Arrange
        sut.connect(attempt(1), 0);
        sut.opened(1);

        // Act
        sut.timer(1, CONNECT, seconds(30));

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .hasCauseInstanceOf(TimeoutException.class)
                .hasRootCauseMessage("CONNACK deadline expired")
                .isInstanceOfSatisfying(
                        MqttConnectException.class,
                        failure -> assertThat(failure.reasonCode()).isEmpty()
                );

        // Act: deliver stale events after starting another connection attempt.
        sut.connect(attempt(2), seconds(30));
        sut.opened(1);
        sut.timer(1, CONNECT, seconds(60));
        sut.failed(1, new IllegalStateException("old reader"));
        receive(1, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), seconds(60));

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTING);
        assertThat(port.results).doesNotContainKey(2L);

        // Act: the new transport has not connected, regardless of the old one's phase.
        sut.timer(2, CONNECT, seconds(60));

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasCauseInstanceOf(TimeoutException.class)
                .hasRootCauseMessage("Transport connection deadline expired");
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void sends_move_the_idle_deadline_without_rescheduling_on_every_send() {
        // Arrange: an established connection with a ten-second keep-alive.
        connected();

        // Act
        sut.sent(1, 1, seconds(6));

        // Assert
        assertThat(port.timers.get(KEEP_ALIVE)).isEqualTo(Duration.ofSeconds(10));
        sut.timer(1, KEEP_ALIVE, seconds(10));
        assertThat(port.sent).isEmpty();
        assertThat(port.timers.get(KEEP_ALIVE)).isEqualTo(Duration.ofSeconds(6));
        sut.timer(1, KEEP_ALIVE, seconds(16));
        assertThat(port.sent).singleElement().isInstanceOf(Packet.PingReq.class);
        assertThat(port.timers.get(PING)).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void pingresp_can_arrive_before_the_write_completion() {
        // Arrange: an established connection with a ten-second keep-alive.
        connected();

        // Act
        sut.timer(1, KEEP_ALIVE, seconds(10));
        receive(1, new Packet.PingResp(), seconds(10));
        sut.sent(1, port.ticket, seconds(10));
        sut.timer(1, PING, seconds(20));

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.timers).doesNotContainKey(PING);
    }

    @Test
    void unsolicited_and_duplicate_pingresp_do_not_end_the_connection_or_delay_the_next_ping() {
        // Arrange
        connected();

        // Act: an unsolicited response does not move the send-based keep-alive deadline.
        receive(1, new Packet.PingResp(), seconds(9));
        sut.timer(1, KEEP_ALIVE, seconds(10));

        // Assert
        assertThat(port.sent).singleElement().isInstanceOf(Packet.PingReq.class);
        assertThat(port.timers).containsKey(PING);

        // Act: consume the response once, then tolerate a duplicate.
        receive(1, new Packet.PingResp(), seconds(10));
        receive(1, new Packet.PingResp(), seconds(11));
        sut.timer(1, PING, seconds(20));

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.ends).isEmpty();
        assertThat(port.timers).doesNotContainKey(PING);
        assertThat(port.ignoredResponses).isEqualTo(2);
    }

    @Test
    void missing_pingresp_ends_an_established_connection_once() {
        // Arrange: an established connection with a ten-second keep-alive.
        connected();

        // Act
        sut.timer(1, KEEP_ALIVE, seconds(10));
        sut.timer(1, PING, seconds(20));
        sut.failed(1, new IllegalStateException("reader also ended"));

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause().orElseThrow()).isInstanceOf(TimeoutException.class);
            assertThat(end.userRequested()).isFalse();
        });
        assertThat(port.timers).isEmpty();
    }

    @Test
    void disconnect_waits_for_the_write_then_closes_without_more_sends() {
        // Arrange: an established connection with a ten-second keep-alive.
        connected();

        // Act
        sut.disconnect(2);
        long ticket = port.ticket;

        // Assert
        assertThat(port.results).doesNotContainKey(2L);
        sut.disconnect(3);
        assertThat(port.results).doesNotContainKey(3L);
        sut.timer(1, KEEP_ALIVE, seconds(10));
        assertThat(port.sent).singleElement().isInstanceOf(Packet.Disconnect.class);
        sut.sent(1, ticket, seconds(1));
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(requireNonNull(port.results.get(2L)).failure()).isNull();
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        assertThat(port.ends).containsExactly(Disconnection.of(null, true, 1));
    }

    @Test
    void a_packet_failure_is_shared_by_the_notification_and_the_pending_operation() {
        // Arrange
        connected();
        sut.disconnect(2);
        var original = new PacketDecodeException(ReasonCode.MALFORMED_PACKET, "Invalid packet");

        // Act
        sut.failed(1, original);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        Throwable cause = port.ends.getFirst().cause().orElseThrow();
        assertThat(cause).isInstanceOfSatisfying(MqttProtocolException.class, failure -> {
            assertThat(failure.reasonCode()).isEqualTo(ReasonCode.MALFORMED_PACKET);
            assertThat(failure).hasMessage("Invalid packet").hasCause(original);
        });
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(cause);
    }

    @Test
    void close_is_terminal_idempotent_and_fails_pending_operations() {
        // Arrange
        sut.connect(attempt(1), 0);

        // Act
        sut.close(null);
        sut.close(null);
        sut.connect(attempt(2), 0);

        // Assert
        assertThat(sut.state()).isEqualTo(CLOSED);
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .hasNoCause()
                .isInstanceOfSatisfying(
                        MqttConnectException.class,
                        failure -> assertThat(failure.reasonCode()).isEmpty()
                );
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(IllegalStateException.class);
        assertThat(port.ends).hasSize(1);
    }

    @Test
    void close_fails_a_pending_disconnect_without_inventing_a_cause() {
        // Arrange
        connected();
        sut.disconnect(2);

        // Act
        sut.close(null);

        // Assert
        assertThat(sut.state()).isEqualTo(CLOSED);
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasNoCause();
        assertThat(port.ends).containsExactly(Disconnection.of(null, true, 1));
    }

    @Test
    void publish_passes_the_message_and_completes_only_after_its_write() {
        // Arrange
        connected();
        var payload = new byte[] {1, 2};
        var message = MqttMessage.of(new Topic("a/b"), payload, Qos.AT_MOST_ONCE, true);

        // Act
        sut.publish(2, message);
        long ticket = port.ticket;
        payload[0] = 9;
        message.payload()[1] = 9;

        // Assert: queue admission does not complete a QoS 0 publish or expose its payload.
        assertThat(port.results).doesNotContainKey(2L);
        assertThat(port.publications).singleElement().isSameAs(message);
        assertThat(port.publications.getFirst().payload()).containsExactly(1, 2);
        assertThat(port.sent).isEmpty();

        // Act: an unrelated write completion cannot complete the publication.
        sut.sent(1, ticket + 1, 0);

        // Assert
        assertThat(port.results).doesNotContainKey(2L);

        // Act: the matching write completes once, even when its notification is repeated.
        sut.sent(1, ticket, 0);
        sut.sent(1, ticket, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(PublishResult.of());
    }

    @Test
    void registrations_survive_absent_connections_while_publications_are_rejected() {
        // Act: the client has not established a connection.
        sut.publish(2, message());
        sut.subscribe(3, subscription("a/#"));

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttNotConnectedException.class);
        assertThat(port.outcomes).doesNotContainKey(3L);
        assertThat(port.sent).isEmpty();

        // Arrange: establish a connection, then start its graceful shutdown.
        connected();
        sut.disconnect(4);
        port.sent.clear();

        // Act
        sut.publish(5, message());
        sut.subscribe(6, subscription("next/#"));

        // Assert: the registration is accepted without sending after DISCONNECT.
        assertThat(requireNonNull(port.results.get(5L)).failure())
                .isInstanceOf(MqttNotConnectedException.class);
        assertThat(port.outcomes).doesNotContainKeys(3L, 6L);
        assertThat(port.registered).containsExactly(3L, 6L);
        assertThat(port.sent).isEmpty();
        assertThat(port.publications).isEmpty();
    }

    @ParameterizedTest(name = "register while connecting: {0}")
    @ValueSource(booleans = {false, true})
    void registrations_wait_for_connack_and_accept_delivery_before_suback(boolean connecting) {
        // Arrange
        if (connecting) {
            sut.connect(attempt(1), 0);
        }

        // Act
        sut.subscribe(2, subscription("a/#"));
        assertThat(port.registered).containsExactly(2L);
        assertThat(port.sent).isEmpty();
        assertThat(port.outcomes).doesNotContainKey(2L);
        if (!connecting) {
            sut.connect(attempt(1), 0);
        }
        receive(1, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        receive(1, publication(), 0);

        // Assert
        assertThat(port.sent).singleElement().isInstanceOf(Packet.Subscribe.class);
        assertThat(port.deliveries).singleElement().satisfies(delivery ->
                assertThat(delivery.ids()).containsExactly(2L));
        assertThat(port.outcomes).doesNotContainKey(2L);
        var packet = (Packet.Subscribe) port.sent.getLast();
        receive(1, new Packet.SubAck(packet.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        assertThat(requireNonNull(port.outcomes.get(2L)).failure()).isNull();
    }

    @Test
    void offline_registrations_are_bounded_and_share_the_pending_suback_window() {
        // Arrange: every registration exists before a connection is requested.
        for (int i = 0; i < 256; i++) {
            sut.subscribe(2 + i, subscription("offline/" + i));
        }
        sut.subscribe(300, subscription("overflow"));
        assertThat(port.sent).isEmpty();
        assertThat(port.registered).hasSize(256);
        assertThat(requireNonNull(port.results.get(300L)).failure())
                .isInstanceOf(MqttCapacityException.class);

        // Act
        connected();

        // Assert: initial registrations use the same bounded window as recreated ones.
        for (int i = 0; i < 256; i++) {
            assertThat(port.sent).hasSize(Math.min(256, i + 16));
            var packet = (Packet.Subscribe) port.sent.get(i);
            assertThat(packet.subscriptions().getFirst().filter())
                    .isEqualTo(new TopicFilter("offline/" + i));
            receive(1, new Packet.SubAck(packet.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        }
        assertThat(port.registered).hasSize(256).doesNotHaveDuplicates();
        assertThat(port.grants).hasSize(256);
        assertThat(port.removed).isEmpty();
    }

    @Test
    void a_failed_connection_attempt_does_not_end_an_offline_registration() {
        // Arrange
        sut.subscribe(2, subscription("a/#"));
        sut.connect(attempt(1), 0);

        // Act
        sut.timer(1, CONNECT, Duration.ofSeconds(30).toNanos());
        sut.connect(attempt(3), Duration.ofSeconds(31).toNanos());
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert
        assertThat(port.outcomes).doesNotContainKey(2L);
        assertThat(port.removed).isEmpty();
        assertThat(port.registered).containsExactly(2L);
        assertThat(port.sent).singleElement().isInstanceOf(Packet.Subscribe.class);
        var packet = (Packet.Subscribe) port.sent.getLast();
        receive(2, new Packet.SubAck(packet.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        assertThat(requireNonNull(port.outcomes.get(2L)).failure()).isNull();
    }

    @Test
    void a_removal_awaiting_its_suback_does_not_hold_back_other_removals() {
        // Arrange: b/# is established, while the SUBSCRIBE for a/# still awaits its SUBACK.
        connected();
        sut.subscribe(2, subscription("b/#"));
        var established = (Packet.Subscribe) port.sent.getLast();
        receive(
                1,
                new Packet.SubAck(established.packetId(), List.of(ReasonCode.SUCCESS), EMPTY),
                0
        );
        sut.subscribe(3, subscription("a/#"));
        var pending = (Packet.Subscribe) port.sent.getLast();
        sut.unsubscribe(4, 3);

        // Act
        sut.unsubscribe(5, 2);

        // Assert: b/# leaves at once, and a/# follows its own SUBACK.
        assertThat(port.sent.getLast()).isInstanceOfSatisfying(Packet.Unsubscribe.class, removal ->
                assertThat(removal.filters()).containsExactly(new TopicFilter("b/#")));
        receive(1, new Packet.SubAck(pending.packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0);
        assertThat(port.sent.getLast()).isInstanceOfSatisfying(Packet.Unsubscribe.class, removal ->
                assertThat(removal.filters()).containsExactly(new TopicFilter("a/#")));
    }

    @Test
    void removal_during_initial_subscription_waits_for_its_acknowledgement() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        var registration = (Packet.Subscribe) port.sent.getLast();

        // Act
        sut.unsubscribe(3, 2);
        receive(1, publication(), 0);

        // Assert: removal neither sends before SUBACK nor resumes local delivery.
        assertThat(port.deliveries).isEmpty();
        assertThat(port.results).doesNotContainKey(3L);
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
        receive(
                1,
                new Packet.SubAck(registration.packetId(), List.of(ReasonCode.SUCCESS), EMPTY),
                0
        );
        var removal = (Packet.Unsubscribe) port.sent.getLast();
        receive(1, new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY), 0);
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        assertThat(port.removed).containsExactly(2L);
    }

    @Test
    void a_refused_initial_subscription_confirms_a_pending_removal_without_unsubscribe() {
        // Arrange: removal is requested while the first SUBACK is pending.
        connected();
        sut.subscribe(2, subscription("a/#"));
        var registration = (Packet.Subscribe) port.sent.getLast();
        sut.unsubscribe(3, 2);

        // Act
        receive(
                1,
                new Packet.SubAck(
                        registration.packetId(),
                        List.of(ReasonCode.UNSPECIFIED_ERROR),
                        EMPTY
                ),
                0
        );

        // Assert
        assertThat(port.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
        assertThat(requireNonNull(port.outcomes.get(2L)).failure())
                .isInstanceOf(MqttSubscriptionRefusedException.class);
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        assertThat(port.removed).containsExactly(2L);
    }

    @Test
    void matching_deliveries_reach_all_pending_and_active_subscriptions() {
        // Arrange: overlapping ordinary filters match the same topic.
        connected();
        sut.subscribe(2, subscription("a/#"));
        int firstId = ((Packet.Subscribe) port.sent.getLast()).packetId();
        sut.subscribe(3, subscription("a/+"));
        sut.subscribe(4, subscription("a/b"));
        sut.subscribe(5, subscription("other/#"));

        // Act: MQTT 3.1.1 section 3.8.4 lets the broker deliver a PUBLISH before SUBACK.
        sut.received(1, publication(), 0, 19);

        // Assert
        assertThat(port.registered).containsExactly(2L, 3L, 4L, 5L);
        assertThat(port.deliveries).singleElement().satisfies(delivery -> {
            assertThat(delivery.ids()).containsExactly(2L, 3L, 4L);
            assertThat(delivery.bytes()).isEqualTo(19);
            assertThat(delivery.message()).isEqualTo(message());
        });
        assertThat(port.outcomes).doesNotContainKeys(2L, 3L, 4L, 5L);

        // Act: SUBACK arrives before the writer's completion notification.
        receive(1, new Packet.SubAck(firstId, List.of(ReasonCode.SUCCESS), EMPTY), 0);

        // Assert
        assertThat(requireNonNull(port.outcomes.get(2L)).value())
                .isInstanceOfSatisfying(Subscription.class, value -> {
                    assertThat(value.filter()).isEqualTo(new TopicFilter("a/#"));
                    assertThat(value.grantedQos()).isEqualTo(Qos.AT_MOST_ONCE);
                });

        // Act: later deliveries still reach the established subscription.
        sut.sent(1, 1, 0);
        receive(1, publication(), 0);

        // Assert
        assertThat(port.deliveries).hasSize(2);
    }

    @Test
    void shared_deliveries_match_the_inner_filter_before_and_after_suback() {
        // Arrange: unrelated ordinary and shared subscriptions remain independent.
        connected();
        sut.subscribe(2, subscription("$share/group/a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        sut.subscribe(3, subscription("$share/other/other/#"));
        sut.subscribe(4, subscription("third/#"));

        // Act
        receive(1, publication(), 0);
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        receive(1, publication(), 0);

        // Assert
        assertThat(port.registered).containsExactly(2L, 3L, 4L);
        assertThat(port.deliveries).hasSize(2).allSatisfy(delivery -> {
            assertThat(delivery.ids()).containsExactly(2L);
            assertThat(delivery.message()).isEqualTo(message());
        });
        assertThat(requireNonNull(port.outcomes.get(2L)).value())
                .isInstanceOfSatisfying(Subscription.class, value -> {
                    assertThat(value.filter()).isEqualTo(new TopicFilter("$share/group/a/#"));
                });
    }

    @ParameterizedTest(name = "rejects overlap between {0} and {1}")
    @CsvSource(
            value = {
                    "$share/group/a/# | a/+",
                    "a/+ | $share/group/a/#",
                    "$share/group/a/# | $share/other/a/#",
                    "$share/group/a/# | $share/other/a/+",
                    "$share/other/a/+ | $share/group/a/#",
                    "$share/group/a/# | $share/group/a/+",
                    "$share/group/a/# | a",
                    "$share/group/a/+ | a/",
                    "$share/group/# | /#",
                    "$share/group/+/# | /#",
                    "$share/group/+/+ | /",
                    "$share/group/$SYS/# | $SYS/+",
                    "$share/group/a//+ | a//#",
                    "$share/group/+ | +",
                    "+ | $share/group/#"
            },
            delimiter = '|'
    )
    void shared_overlap_is_rejected_before_reserving_a_lane_or_packet_identifier(
            String first,
            String second
    ) {
        // Arrange
        connected();
        sut.subscribe(2, subscription(first));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act
        sut.subscribe(3, subscription(second));

        // Assert
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(IllegalStateException.class);
        assertThat(port.registered).containsExactly(2L);
        assertThat(port.sent).hasSize(1);
        assertThat(port.removed).isEmpty();
        assertThat(sut.state()).isEqualTo(CONNECTED);

        // Act: the existing subscription still succeeds and gets the next identifier.
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        sut.unsubscribe(4, 2);

        // Assert
        assertThat(requireNonNull(port.outcomes.get(2L)).failure()).isNull();
        assertThat(((Packet.Unsubscribe) port.sent.getLast()).packetId()).isEqualTo(id + 1);
    }

    @ParameterizedTest(name = "allows disjoint filters {0} and {1}")
    @CsvSource(
            value = {
                    "$share/group//# | +",
                    "+ | $share/group//#",
                    "$share/group/a/+ | a",
                    "$share/group/a/+ | b/+",
                    "$share/group/# | $SYS/#",
                    "$share/group/+/x | $SYS/x",
                    "$share/group/$SYS/# | #",
                    "$share/group/$SYS/x | $SYS/y",
                    "$share/group/a//# | a/b/#",
                    "$share/group/a/# | aB/#",
                    "$share/group/a/# | $share/group/b/#",
                    "$share/group/a/# | $share/other/b/#"
            },
            delimiter = '|'
    )
    void shared_filters_with_no_common_valid_topic_are_allowed(String first, String second) {
        // Arrange
        connected();
        sut.subscribe(2, subscription(first));

        // Act
        sut.subscribe(3, subscription(second));

        // Assert
        assertThat(port.registered).containsExactly(2L, 3L);
        assertThat(port.sent).hasSize(2);
        assertThat(port.results).doesNotContainKeys(2L, 3L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "overlap within UTF-8 limit: {0} and {1} repetitions of {2}")
    @CsvSource({"32767, 32767, a", "16383, 16384, é", "8191, 8192, 😀"})
    void shared_overlap_includes_topics_at_the_utf8_length_limit(
            int firstLength,
            int secondLength,
            String character
    ) {
        // Arrange: each filter supplies a different literal level of the common topic.
        connected();
        sut.subscribe(2, subscription("$share/group/" + character.repeat(firstLength) + "/+"));

        // Act
        sut.subscribe(3, subscription("+/" + character.repeat(secondLength)));

        // Assert
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(IllegalStateException.class);
        assertThat(port.registered).containsExactly(2L);
        assertThat(port.sent).hasSize(1);
    }

    @ParameterizedTest(name = "overlap beyond UTF-8 limit: {0} and {1} repetitions of {2}")
    @CsvSource({"32767, 32768, a", "16384, 16384, é", "8192, 8192, 😀"})
    void shared_filters_are_allowed_when_every_common_topic_exceeds_the_utf8_length_limit(
            int firstLength,
            int secondLength,
            String character
    ) {
        // Arrange: both filters are valid, but their shortest common topic is too long.
        connected();
        sut.subscribe(2, subscription("$share/group/" + character.repeat(firstLength) + "/+"));

        // Act
        sut.subscribe(3, subscription("+/" + character.repeat(secondLength)));

        // Assert
        assertThat(port.registered).containsExactly(2L, 3L);
        assertThat(port.sent).hasSize(2);
        assertThat(port.results).doesNotContainKeys(2L, 3L);
    }

    @ParameterizedTest(name = "reserves overlap between {0} and {1} until UNSUBACK")
    @CsvSource(
            value = {
                    "a/# | $share/group/a/+",
                    "$share/group/a/+ | a/#",
                    "$share/group/a/# | $share/other/a/#",
                    "$share/group/a/# | $share/group/a/+"
            },
            delimiter = '|'
    )
    void shared_overlap_remains_reserved_until_unsubscribe_is_acknowledged(
            String first,
            String second
    ) {
        // Arrange
        connected();
        sut.subscribe(2, subscription(first));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        Subscribe candidate = subscription(second);

        // Act: the original filter remains reserved while pending, active, and being removed.
        sut.subscribe(3, candidate);
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        sut.subscribe(4, candidate);
        sut.unsubscribe(5, 2);
        int unsubscribeId = ((Packet.Unsubscribe) port.sent.getLast()).packetId();
        sut.subscribe(6, candidate);
        receive(1, new Packet.UnsubAck(unsubscribeId, List.of(), EMPTY), 0);
        sut.subscribe(7, candidate);

        // Assert
        for (long operation : List.of(3L, 4L, 6L)) {
            assertThat(requireNonNull(port.results.get(operation)).failure())
                    .isInstanceOf(IllegalStateException.class);
        }
        assertThat(requireNonNull(port.results.get(5L)).failure()).isNull();
        assertThat(port.registered).containsExactly(2L, 7L);
        assertThat(port.removed).containsExactly(2L);
        assertThat(port.results).doesNotContainKey(7L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void suback_refusal_removes_the_registration_and_allows_a_new_attempt() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.UNSPECIFIED_ERROR), EMPTY), 0);
        receive(1, publication(), 0);
        sut.subscribe(3, subscription("a/#"));

        // Assert
        assertThat(requireNonNull(port.outcomes.get(2L)).failure())
                .isInstanceOfSatisfying(
                        MqttSubscriptionRefusedException.class,
                        failure -> assertThat(failure.reasonCode())
                                .isEqualTo(ReasonCode.UNSPECIFIED_ERROR)
                );
        assertThat(port.removed).containsExactly(2L);
        assertThat(port.resubscriptionFailures).isEmpty();
        assertThat(port.deliveries).isEmpty();
        assertThat(port.sent).hasSize(2);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void duplicate_filters_are_reserved_until_unsubscribe_is_acknowledged() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("$share/group/a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act: the original filter remains reserved while pending, active, and being removed.
        sut.subscribe(3, subscription("$share/group/a/#"));
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        sut.subscribe(4, subscription("$share/group/a/#"));
        sut.unsubscribe(5, 2);
        int unsubscribeId = ((Packet.Unsubscribe) port.sent.getLast()).packetId();
        sut.subscribe(6, subscription("$share/group/a/#"));

        // A disjoint shared subscription remains independent while the original is removed.
        sut.subscribe(7, subscription("$share/other/other/#"));
        receive(1, publication(), 0);

        // Assert
        assertThat(port.deliveries).isEmpty();

        // Act: the same filter can receive messages again after removal completes.
        receive(1, new Packet.UnsubAck(unsubscribeId, List.of(), EMPTY), 0);
        sut.subscribe(8, subscription("$share/group/a/#"));
        receive(1, publication(), 0);

        // Assert
        for (long operation : List.of(3L, 4L, 6L)) {
            assertThat(requireNonNull(port.results.get(operation)).failure())
                    .isInstanceOf(IllegalStateException.class);
        }
        assertThat(port.deliveries).singleElement()
                .satisfies(delivery -> assertThat(delivery.ids()).containsExactly(8L));
        assertThat(requireNonNull(port.results.get(5L)).failure()).isNull();
        assertThat(port.removed).containsExactly(2L);
        assertThat(port.registered).containsExactly(2L, 7L, 8L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void full_send_capacity_retains_registration_and_removal_requests() {
        // Arrange: an established subscription and a full normal send queue.
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        port.sent.clear();
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act
        sut.publish(3, message());
        long publicationTicket = port.ticket;
        sut.subscribe(4, subscription("new/#"));
        sut.unsubscribe(5, 2);
        sut.sent(1, publicationTicket, 0);
        receive(1, publication(), 0);

        // Assert: publication refusal does not roll back either subscription intent.
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(MqttCapacityException.class);
        assertThat(port.outcomes).doesNotContainKey(4L);
        assertThat(port.results).doesNotContainKey(5L);
        assertThat(port.deliveries).isEmpty();
        assertThat(port.removed).isEmpty();

        // Act: returned capacity resumes both exchanges without another API call.
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.sendCapacityAvailable(1);
        var removal = (Packet.Unsubscribe) port.sent.getFirst();
        var registration = (Packet.Subscribe) port.sent.getLast();
        receive(1, new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY), 0);
        receive(
                1,
                new Packet.SubAck(registration.packetId(), List.of(ReasonCode.SUCCESS), EMPTY),
                0
        );

        // Assert
        assertThat(port.removed).containsExactly(2L);
        assertThat(requireNonNull(port.outcomes.get(4L)).failure()).isNull();
        assertThat(requireNonNull(port.results.get(5L)).failure()).isNull();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest
    @EnumSource(SendQueue.OfferResult.class)
    void publish_passes_the_message_without_copying_the_payload(SendQueue.OfferResult result) {
        // Arrange
        connected();
        port.sendResult = result;
        var message = MqttMessage.of(
                new Topic("a/b"),
                new byte[16 * 1024 * 1024 + 1],
                Qos.AT_MOST_ONCE,
                false
        );
        var memory = (ThreadMXBean) ManagementFactory.getThreadMXBean();

        // Act: MqttMessage.payload() would copy the whole payload.
        long before = memory.getCurrentThreadAllocatedBytes();
        sut.publish(2, message);
        long allocated = memory.getCurrentThreadAllocatedBytes() - before;

        // Assert: permit class-loading overhead, but not a copy of the payload.
        assertThat(before).as("per-thread allocation measurement").isNotNegative();
        assertThat(allocated).isLessThan(1024 * 1024);
        assertThat(port.publications).singleElement().isSameAs(message);
        assertThat(port.sent).isEmpty();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void a_full_subscription_capacity_never_sends_a_subscribe() {
        // Arrange
        connected();
        port.acceptRegistration = false;

        // Act
        sut.subscribe(2, subscription("a/#"));

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttCapacityException.class);
        assertThat(port.sent).isEmpty();

        // Act: releasing capacity allows the same filter to be registered.
        port.acceptRegistration = true;
        sut.subscribe(3, subscription("a/#"));

        // Assert
        assertThat(port.sent).singleElement().isInstanceOf(Packet.Subscribe.class);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void a_suback_for_a_refused_registration_is_a_protocol_error() {
        // Arrange: the refusal returned the first packet identifier without sending it.
        connected();
        port.acceptRegistration = false;
        sut.subscribe(2, subscription("a/#"));

        // Act
        receive(1, new Packet.SubAck(1, List.of(ReasonCode.SUCCESS), EMPTY), 0);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class));
    }

    @Test
    void normal_operations_on_a_closed_send_queue_wait_for_the_terminal_cause() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        port.sendResult = SendQueue.OfferResult.CLOSED;
        var failure = new IOException("Peer reset");

        // Act
        sut.publish(3, message());
        sut.subscribe(4, subscription("new/#"));
        sut.unsubscribe(5, 2);

        // Assert: refusal alone does not fail the operations or end the connection.
        assertThat(port.outcomes).doesNotContainKey(4L);
        assertThat(port.results).doesNotContainKeys(3L, 5L);
        assertThat(port.ends).isEmpty();

        // Act
        sut.failed(1, failure);

        // Assert
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(failure);
        assertThat(port.outcomes).doesNotContainKey(4L);
        assertThat(requireNonNull(port.results.get(5L)).failure()).isNull();
        assertThat(port.removed).containsExactly(2L);
    }

    @ParameterizedTest(name = "invalid SUBACK reasons: {0}")
    @MethodSource("invalidSubackReasons")
    void invalid_subacks_end_the_connection_and_the_registration(List<ReasonCode> reasons) {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act
        receive(1, new Packet.SubAck(id, reasons, EMPTY), 0);

        // Assert: the registration ends with the violation, so no later connection repeats it.
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class));
        assertThat(port.removals).singleElement().satisfies(removal -> assertThat(removal.cause())
                .isInstanceOfSatisfying(MqttProtocolException.class, violation ->
                        assertThat(violation.getMessage()).contains("a/#")));
        port.sent.clear();
        sut.connect(attempt(3), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        assertThat(port.sent).noneMatch(Packet.Subscribe.class::isInstance);
        sut.subscribe(4, subscription("a/#"));
        assertThat(port.sent).last().isInstanceOf(Packet.Subscribe.class);
    }

    @ParameterizedTest(name = "unknown acknowledgement: {0}")
    @MethodSource("unknownSubscriptionAcknowledgements")
    void unknown_suback_and_unsuback_identifiers_are_protocol_errors(Packet acknowledgement) {
        // Arrange
        connected();

        // Act
        receive(1, acknowledgement, 0);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class);
        });
    }

    @Test
    void duplicate_suback_cannot_complete_a_subscription_twice() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        var ack = new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY);

        // Act
        receive(1, ack, 0);
        receive(1, ack, 0);

        // Assert
        assertThat(requireNonNull(port.outcomes.get(2L)).failure()).isNull();
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class);
        });
        assertThat(port.removed).isEmpty();
    }

    @Test
    void duplicate_unsuback_cannot_complete_an_unsubscription_twice() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        sut.unsubscribe(3, 2);
        int unsubscribeId = ((Packet.Unsubscribe) port.sent.getLast()).packetId();
        var ack = new Packet.UnsubAck(unsubscribeId, List.of(), EMPTY);

        // Act
        receive(1, ack, 0);
        receive(1, ack, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class);
        });
        assertThat(port.removed).containsExactly(2L);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("publicationsBeyondLimits")
    void a_publication_beyond_the_brokers_limits_fails_without_being_sent(
            Property limit,
            MqttMessage message,
            ReasonCode reason
    ) {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5(limit);

        // Act
        mqtt5.publish(2, message);

        // Assert: the broker would end the connection with the same reason code, and the
        // client, which refused it, has no reason string to report.
        assertThat(requireNonNull(port.results.get(2L)).failure()).isInstanceOfSatisfying(
                MqttPublicationRefusedException.class,
                refusal -> {
                    assertThat(refusal.reasonCode()).isEqualTo(reason);
                    assertThat(refusal.reasonString()).isEmpty();
                }
        );
        assertThat(port.publications).isEmpty();
        assertThat(mqtt5.state()).isEqualTo(CONNECTED);
    }

    static Stream<Arguments> publicationsBeyondLimits() {
        MqttMessage message = qosOneMessage();
        return Stream.of(
                Arguments.of(
                        Named.of(
                                "QoS 2 above Maximum QoS 1",
                                new Property.ByteValue(PropertyId.MAXIMUM_QOS, 1)
                        ),
                        message.withQos(Qos.EXACTLY_ONCE),
                        ReasonCode.QOS_NOT_SUPPORTED
                ),
                Arguments.of(
                        Named.of(
                                "QoS 1 above Maximum QoS 0",
                                new Property.ByteValue(PropertyId.MAXIMUM_QOS, 0)
                        ),
                        message,
                        ReasonCode.QOS_NOT_SUPPORTED
                ),
                Arguments.of(
                        Named.of(
                                "retain without Retain Available",
                                new Property.ByteValue(PropertyId.RETAIN_AVAILABLE, 0)
                        ),
                        message.withRetain(true),
                        ReasonCode.RETAIN_NOT_SUPPORTED
                ),
                Arguments.of(
                        Named.of(
                                "one byte beyond Maximum Packet Size",
                                new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 64)
                        ),
                        messageWithWireBytes(65, MqttVersion.V5),
                        ReasonCode.PACKET_TOO_LARGE
                ),
                Arguments.of(
                        Named.of(
                                "a payload as large as Maximum Packet Size",
                                new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 64)
                        ),
                        message.withPayload(new byte[64]),
                        ReasonCode.PACKET_TOO_LARGE
                ),
                Arguments.of(
                        Named.of(
                                "properties that take it beyond Maximum Packet Size",
                                new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 64)
                        ),
                        // A User Property of one-byte strings adds seven bytes.
                        messageWithWireBytes(60, MqttVersion.V5)
                                .withUserProperties(List.of(new UserProperty("k", "v"))),
                        ReasonCode.PACKET_TOO_LARGE
                )
        );
    }

    @Test
    void a_publication_within_the_brokers_limits_is_sent() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5(
                new Property.ByteValue(PropertyId.MAXIMUM_QOS, 1),
                new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 64)
        );

        // Act
        mqtt5.publish(2, messageWithWireBytes(64, MqttVersion.V5));

        // Assert
        assertThat(port.publications).hasSize(1);
        assertThat(port.results).doesNotContainKey(2L);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("unsupportedSubscriptions")
    void a_subscription_that_the_broker_does_not_support_fails_without_being_sent(
            String filter,
            Property limit,
            ReasonCode reason
    ) {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5(limit);

        // Act
        mqtt5.subscribe(2, subscription(filter));

        // Assert
        assertThat(port.sent).noneMatch(Packet.Subscribe.class::isInstance);
        assertThat(requireNonNull(port.outcomes.get(2L)).failure()).isInstanceOfSatisfying(
                MqttSubscriptionRefusedException.class,
                refusal -> {
                    assertThat(refusal.reasonCode()).isEqualTo(reason);
                    assertThat(refusal.reasonString()).isEmpty();
                }
        );
        assertThat(mqtt5.state()).isEqualTo(CONNECTED);
    }

    static Stream<Arguments> unsupportedSubscriptions() {
        Named<Property> noWildcards = Named.of(
                "without Wildcard Subscription Available",
                new Property.ByteValue(PropertyId.WILDCARD_SUBSCRIPTION_AVAILABLE, 0)
        );
        Named<Property> noSharing = Named.of(
                "without Shared Subscription Available",
                new Property.ByteValue(PropertyId.SHARED_SUBSCRIPTION_AVAILABLE, 0)
        );
        return Stream.of(
                Arguments.of("a/#", noWildcards, ReasonCode.WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED),
                Arguments.of("a/+/c", noWildcards, ReasonCode.WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED),
                Arguments.of(
                        "$share/group/a/#",
                        noWildcards,
                        ReasonCode.WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED
                ),
                Arguments.of(
                        "$share/group/a/b",
                        noSharing,
                        ReasonCode.SHARED_SUBSCRIPTIONS_NOT_SUPPORTED
                )
        );
    }

    @Test
    void a_subscription_without_wildcards_is_sent_when_the_broker_supports_none() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5(
                new Property.ByteValue(PropertyId.WILDCARD_SUBSCRIPTION_AVAILABLE, 0)
        );

        // Act
        mqtt5.subscribe(2, subscription("a/b"));

        // Assert
        assertThat(port.sent).last().isInstanceOf(Packet.Subscribe.class);
        assertThat(port.outcomes).doesNotContainKey(2L);
    }

    @ParameterizedTest(name = "{0} travels as {1}")
    @CsvSource({
            "SEND, 0",
            "SEND_IF_NEW_SUBSCRIPTION, 1",
            "DO_NOT_SEND, 2"
    })
    void a_subscribe_carries_the_retain_handling_and_by_default_no_other_option(
            RetainHandling handling,
            int value
    ) {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();

        // Act
        mqtt5.subscribe(2, subscription("a/#").withRetainHandling(handling));

        // Assert
        assertThat(((Packet.Subscribe) port.sent.getLast()).subscriptions()).containsExactly(
                new Packet.Subscription(
                        new TopicFilter("a/#"),
                        Qos.AT_MOST_ONCE,
                        false,
                        false,
                        value
                )
        );
    }

    @Test
    void a_recreated_subscription_carries_the_same_options() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        var expected = new Packet.Subscription(
                new TopicFilter("a/#"),
                Qos.AT_MOST_ONCE,
                true,
                true,
                1
        );
        Subscribe request = subscription("a/#")
                .withNoLocal(true)
                .withRetainAsPublished(true)
                .withRetainHandling(RetainHandling.SEND_IF_NEW_SUBSCRIPTION);
        mqtt5.subscribe(2, request);
        var first = (Packet.Subscribe) port.sent.getLast();
        mqtt5.received(
                1,
                new Packet.SubAck(first.packetId(), List.of(ReasonCode.SUCCESS), EMPTY),
                0,
                0
        );

        // Act: the next connection resumes no session, so the subscription is made again.
        mqtt5.failed(1, new IOException("connection lost"));
        port.sent.clear();
        mqtt5.connect(attempt(3), 0);
        mqtt5.opened(2);
        mqtt5.received(2, connAck(), 0, 0);

        // Assert
        assertThat(first.subscriptions()).containsExactly(expected);
        assertThat(port.sent)
                .filteredOn(Packet.Subscribe.class::isInstance)
                .singleElement()
                .isInstanceOfSatisfying(
                        Packet.Subscribe.class,
                        recreated -> assertThat(recreated.subscriptions()).containsExactly(expected)
                );
    }

    @Test
    void a_second_subscription_to_a_filter_is_refused_even_with_other_options() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        int sent = port.sent.size();

        // Act
        mqtt5.subscribe(3, subscription("a/#").withNoLocal(true));

        // Assert
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(IllegalStateException.class);
        assertThat(port.sent).hasSize(sent);
    }

    @Test
    void each_registration_keeps_its_own_identifier_when_it_is_made_again() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        mqtt5.subscribe(3, subscription("b/#"));
        List<Optional<Integer>> first = port.sent.stream()
                .filter(Packet.Subscribe.class::isInstance)
                .map(SessionCoreTest::identifierOf)
                .toList();

        // Act: the next connection resumes no session, so the client makes both again.
        mqtt5.failed(1, new IOException("connection lost"));
        port.sent.clear();
        mqtt5.connect(attempt(4), 0);
        mqtt5.opened(2);
        mqtt5.received(2, connAck(), 0, 0);

        // Assert
        assertThat(first).containsExactly(Optional.of(1), Optional.of(2));
        assertThat(port.sent)
                .filteredOn(Packet.Subscribe.class::isInstance)
                .extracting(SessionCoreTest::identifierOf)
                .containsExactly(Optional.of(1), Optional.of(2));
    }

    @Test
    void a_new_registration_does_not_reuse_the_identifier_of_a_removed_one() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        var subscribe = (Packet.Subscribe) port.sent.getLast();
        mqtt5.received(
                1,
                new Packet.SubAck(subscribe.packetId(), List.of(ReasonCode.SUCCESS), EMPTY),
                0,
                0
        );
        mqtt5.unsubscribe(3, 2);
        var unsubscribe = (Packet.Unsubscribe) port.sent.getLast();
        mqtt5.received(
                1,
                new Packet.UnsubAck(unsubscribe.packetId(), List.of(ReasonCode.SUCCESS), EMPTY),
                0,
                0
        );

        // Act
        mqtt5.subscribe(4, subscription("a/#"));

        // Assert: a message that the broker kept for the removed subscription names only it.
        assertThat(identifierOf(subscribe)).contains(1);
        assertThat(identifierOf(port.sent.getLast())).contains(2);
    }

    @ParameterizedTest(name = "Subscription Identifier Available {0}")
    @ValueSource(ints = {0, 1})
    void a_subscribe_carries_the_identifier_only_when_the_broker_accepts_identifiers(
            int available
    ) {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5(
                new Property.ByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE, available)
        );

        // Act
        mqtt5.subscribe(2, subscription("a/#"));

        // Assert
        assertThat(identifierOf(port.sent.getLast()).isPresent()).isEqualTo(available == 1);
    }

    @Test
    void an_mqtt_3_1_1_subscribe_carries_no_identifier() {
        // Arrange
        connected();

        // Act
        sut.subscribe(2, subscription("a/#"));

        // Assert
        assertThat(identifierOf(port.sent.getLast())).isEmpty();
    }

    @Test
    void a_publication_with_identifiers_goes_only_to_the_registrations_that_they_name() {
        // Arrange: two overlapping registrations, with identifiers 1 and 2, await SUBACK.
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        mqtt5.subscribe(3, subscription("a/b"));

        // Act
        mqtt5.received(1, publication("a/b", 1), 0, 0);
        mqtt5.received(1, publication("a/b", 2, 1), 0, 0);
        mqtt5.received(1, publication("a/b", 2, 2), 0, 0);
        mqtt5.received(1, publication("x/y", 1), 0, 0);

        // Assert: the identifiers decide, even for a topic that the filter does not match.
        assertThat(port.deliveries).extracting(Received::ids).containsExactly(
                List.of(2L),
                List.of(2L, 3L),
                List.of(3L),
                List.of(2L)
        );
    }

    @Test
    void a_publication_whose_identifiers_name_no_live_registration_is_acknowledged_unmatched() {
        // Arrange: the registration with identifier 1 awaits its UNSUBACK.
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        var subscribe = (Packet.Subscribe) port.sent.getLast();
        mqtt5.received(
                1,
                new Packet.SubAck(subscribe.packetId(), List.of(ReasonCode.SUCCESS), EMPTY),
                0,
                0
        );
        mqtt5.unsubscribe(3, 2);

        // Act: one names the registration being removed, the other an unknown identifier.
        mqtt5.received(1, qosOnePublication(7, "a/b", identifierProperties(1)), 0, 0);
        mqtt5.received(1, qosOnePublication(8, "a/b", identifierProperties(99)), 0, 0);

        // Assert
        assertThat(port.deliveries).isEmpty();
        assertThat(pubAckIds()).containsExactly(7, 8);
    }

    @Test
    void a_publication_without_identifiers_goes_to_every_registration_whose_filter_matches() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        mqtt5.subscribe(3, subscription("a/b"));

        // Act
        mqtt5.received(1, publication("a/b"), 0, 0);

        // Assert
        assertThat(port.deliveries).singleElement()
                .extracting(Received::ids)
                .isEqualTo(List.of(2L, 3L));
    }

    @Test
    void a_connection_without_identifiers_matches_filters_whatever_the_publication_carries() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5(
                new Property.ByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE, 0)
        );
        mqtt5.subscribe(2, subscription("a/#"));
        mqtt5.subscribe(3, subscription("a/b"));

        // Act
        mqtt5.received(1, publication("a/b", 1), 0, 0);

        // Assert
        assertThat(port.deliveries).singleElement()
                .extracting(Received::ids)
                .isEqualTo(List.of(2L, 3L));
    }

    @Test
    void a_registration_that_the_broker_holds_without_an_identifier_still_matches_by_topic() {
        // Arrange: a persistent session starts on a connection without identifiers, and
        // continues on one with them.
        SessionCore mqtt5 = mqtt5Core(Duration.ofHours(1));
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);
        mqtt5.received(
                1,
                connAck(new Property.ByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE, 0)),
                0,
                0
        );
        mqtt5.subscribe(2, subscription("a/#"));
        var held = (Packet.Subscribe) port.sent.getLast();
        mqtt5.received(
                1,
                new Packet.SubAck(held.packetId(), List.of(ReasonCode.SUCCESS), EMPTY),
                0,
                0
        );
        mqtt5.failed(1, new IOException("connection lost"));
        mqtt5.connect(attempt(3), 0);
        mqtt5.opened(2);
        mqtt5.received(2, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 0, 0);
        mqtt5.subscribe(4, subscription("a/b"));

        // Act: a broker that sends one copy names only the subscription with an identifier.
        mqtt5.received(2, publication("a/b", 2), 0, 0);

        // Assert
        assertThat(identifierOf(held)).isEmpty();
        assertThat(port.deliveries).singleElement()
                .extracting(Received::ids)
                .isEqualTo(List.of(2L, 4L));
    }

    @Test
    void a_new_session_holds_no_subscription_that_it_has_not_made_again() {
        // Arrange: the subscription was made on a connection without identifiers.
        SessionCore mqtt5 = connectedMqtt5(
                new Property.ByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE, 0)
        );
        mqtt5.subscribe(2, subscription("a/#"));
        var subscribe = (Packet.Subscribe) port.sent.getLast();
        mqtt5.received(
                1,
                new Packet.SubAck(subscribe.packetId(), List.of(ReasonCode.SUCCESS), EMPTY),
                0,
                0
        );

        // Act: the next connection starts a new session, and the send queue holds back the
        // subscription's SUBSCRIBE.
        mqtt5.failed(1, new IOException("connection lost"));
        mqtt5.connect(attempt(3), 0);
        mqtt5.opened(2);
        port.sendResult = SendQueue.OfferResult.FULL;
        mqtt5.received(2, connAck(), 0, 0);
        mqtt5.received(2, publication("a/b", 99), 0, 0);

        // Assert
        assertThat(port.deliveries).isEmpty();
    }

    @Test
    void publications_set_topic_aliases_first_come_first_served_and_then_use_them_alone() {
        // Arrange: the broker takes two aliases.
        SessionCore mqtt5 = connectedMqtt5(topicAliasMaximum(2));

        // Act
        for (String topic : List.of("a", "b", "a", "c", "b")) {
            mqtt5.publish(2, message().withTopic(new Topic(topic)));
        }

        // Assert
        assertThat(port.aliases).containsExactly(
                new TopicAlias(1, true),
                new TopicAlias(2, true),
                new TopicAlias(1, false),
                TopicAlias.NONE,
                new TopicAlias(2, false)
        );
    }

    @Test
    void the_client_sets_at_most_16_aliases_whatever_the_broker_allows() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5(topicAliasMaximum(65_535));

        // Act
        for (int topic = 1; topic <= 17; topic++) {
            mqtt5.publish(2, message().withTopic(new Topic("t/" + topic)));
        }

        // Assert
        assertThat(port.aliases.get(15)).isEqualTo(new TopicAlias(16, true));
        assertThat(port.aliases.getLast()).isEqualTo(TopicAlias.NONE);
    }

    @Test
    void a_broker_that_announces_no_topic_alias_maximum_gets_no_aliases() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();

        // Act
        mqtt5.publish(2, message());
        mqtt5.publish(3, message());

        // Assert
        assertThat(port.aliases).containsOnly(TopicAlias.NONE);
    }

    @Test
    void an_mqtt_3_1_1_publication_uses_no_alias() {
        // Arrange
        connected();

        // Act
        sut.publish(2, message());
        sut.publish(3, message());

        // Assert
        assertThat(port.aliases).containsOnly(TopicAlias.NONE);
    }

    @Test
    void a_publication_that_the_send_queue_refuses_sets_no_alias() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5(topicAliasMaximum(2));
        port.sendResult = SendQueue.OfferResult.FULL;
        mqtt5.publish(2, message());
        port.sendResult = SendQueue.OfferResult.ACCEPTED;

        // Act
        mqtt5.publish(3, message());
        mqtt5.publish(4, message());

        // Assert: the alias is set by the first publication that the queue accepted.
        assertThat(port.aliases).containsExactly(
                new TopicAlias(1, true),
                new TopicAlias(1, true),
                new TopicAlias(1, false)
        );
    }

    @Test
    void a_new_connection_sets_its_aliases_again_also_for_resent_publications() {
        // Arrange: a QoS 1 publication that set an alias awaits PUBACK when the connection ends.
        SessionCore mqtt5 = mqtt5Core(Duration.ofHours(1));
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);
        mqtt5.received(1, connAck(topicAliasMaximum(2)), 0, 0);
        mqtt5.publish(2, qosOneMessage());
        mqtt5.failed(1, new IOException("connection lost"));

        // Act: the session continues on a connection that knows no aliases yet.
        mqtt5.connect(attempt(3), 0);
        mqtt5.opened(2);
        mqtt5.received(2, new Packet.ConnAck(true, ReasonCode.SUCCESS, new Properties(List.of(
                topicAliasMaximum(2)
        ))), 0, 0);
        mqtt5.publish(4, message());

        // Assert
        assertThat(port.publications).hasSize(3);
        assertThat(port.aliases).containsExactly(
                new TopicAlias(1, true),
                new TopicAlias(1, true),
                new TopicAlias(1, false)
        );
    }

    @ParameterizedTest(name = "Maximum Packet Size {0} bytes beyond the publication")
    @CsvSource({"2, false", "3, true"})
    void a_publication_uses_an_alias_only_when_the_alias_fits_the_maximum_packet_size(
            int headroom,
            boolean aliased
    ) {
        // Arrange
        int size = PacketCodec.of(MqttVersion.V5).encode(new Packet.Publish(
                new Topic("a/b"),
                new byte[] {1, 2},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        )).length;
        SessionCore mqtt5 = connectedMqtt5(
                topicAliasMaximum(2),
                new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, size + headroom)
        );

        // Act
        mqtt5.publish(2, message());

        // Assert: the publication itself fits either way and is sent.
        assertThat(port.publications).hasSize(1);
        assertThat(port.aliases.getLast() != TopicAlias.NONE).isEqualTo(aliased);
    }

    @Test
    void a_continued_publication_beyond_the_new_brokers_limits_is_not_replayed() {
        // Arrange: a QoS 2 publication waits for PUBREC when the connection ends.
        SessionCore mqtt5 = mqtt5Core(Duration.ofHours(1));
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);
        mqtt5.received(1, connAck(), 0, 0);
        mqtt5.publish(2, qosOneMessage().withQos(Qos.EXACTLY_ONCE));
        mqtt5.failed(1, new IOException("connection lost"));
        int published = port.publications.size();

        // Act: the session continues on a broker that now allows at most QoS 1.
        mqtt5.connect(attempt(3), 0);
        mqtt5.opened(2);
        mqtt5.received(2, new Packet.ConnAck(true, ReasonCode.SUCCESS, new Properties(List.of(
                new Property.ByteValue(PropertyId.MAXIMUM_QOS, 1)
        ))), 0, 0);

        // Assert: replaying it would end every connection that continues the session.
        assertThat(port.publications).hasSize(published);
        assertThat(mqtt5.state()).isEqualTo(CONNECTED);
    }

    @Test
    void publications_beyond_the_brokers_receive_maximum_wait_for_room() {
        // Arrange: the broker takes two publications at once.
        SessionCore mqtt5 = connectedMqtt5(receiveMaximum(2));
        mqtt5.publish(2, qosOneMessage());
        mqtt5.publish(3, qosOneMessage().withQos(Qos.EXACTLY_ONCE));

        // Act
        mqtt5.publish(4, qosOneMessage().withTopic(new Topic("waiting")));

        // Assert: the third publication waits without failing.
        assertThat(port.publications).hasSize(2);
        assertThat(port.results).doesNotContainKeys(2L, 3L, 4L);

        // Act: the first PUBACK makes room.
        int first = port.publicationIds.getFirst();
        mqtt5.received(1, new Packet.PubAck(first, ReasonCode.SUCCESS, EMPTY), 0, 0);

        // Assert
        assertThat(port.publications).hasSize(3);
        assertThat(port.publications.getLast().topic()).isEqualTo(new Topic("waiting"));
        assertThat(port.results).containsKey(2L).doesNotContainKeys(3L, 4L);
    }

    @Test
    void a_qos_2_exchange_keeps_its_room_until_pubcomp_while_pubrel_still_flows() {
        // Arrange: the broker takes one publication at once, so the second one waits.
        SessionCore mqtt5 = connectedMqtt5(receiveMaximum(1));
        mqtt5.publish(2, qosOneMessage().withQos(Qos.EXACTLY_ONCE));
        int id = port.publicationIds.getLast();
        mqtt5.publish(3, qosOneMessage());

        // Act
        mqtt5.received(1, new Packet.PubRec(id, ReasonCode.SUCCESS, EMPTY), 0, 0);

        // Assert: PUBREL goes out although the broker has no room for another publication.
        assertThat(port.sent).last().isEqualTo(new Packet.PubRel(id, ReasonCode.SUCCESS, EMPTY));
        assertThat(port.publications).hasSize(1);

        // Act
        mqtt5.received(1, new Packet.PubComp(id, ReasonCode.SUCCESS, EMPTY), 0, 0);

        // Assert
        assertThat(port.publications).hasSize(2);
    }

    @Test
    void a_refusing_pubrec_makes_room_for_the_next_publication() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5(receiveMaximum(1));
        mqtt5.publish(2, qosOneMessage().withQos(Qos.EXACTLY_ONCE));
        int id = port.publicationIds.getLast();
        mqtt5.publish(3, qosOneMessage());

        // Act
        mqtt5.received(1, new Packet.PubRec(id, ReasonCode.NOT_AUTHORIZED, EMPTY), 0, 0);

        // Assert
        assertThat(port.publications).hasSize(2);
        assertThat(port.sent).noneMatch(Packet.PubRel.class::isInstance);
    }

    @Test
    void qos_0_does_not_wait_behind_publications_that_the_broker_has_no_room_for() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5(receiveMaximum(1));
        mqtt5.publish(2, qosOneMessage());
        mqtt5.publish(3, qosOneMessage());

        // Act
        mqtt5.publish(4, qosOneMessage().withQos(Qos.AT_MOST_ONCE));

        // Assert
        assertThat(port.publications)
                .extracting(MqttMessage::qos)
                .containsExactly(Qos.AT_LEAST_ONCE, Qos.AT_MOST_ONCE);
    }

    @Test
    void publications_waiting_for_room_hold_back_no_other_packet() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5(receiveMaximum(1));
        mqtt5.publish(2, qosOneMessage());
        mqtt5.publish(3, qosOneMessage());

        // Act
        mqtt5.subscribe(4, subscription("a/#"));

        // Assert
        assertThat(port.sent).last().isInstanceOf(Packet.Subscribe.class);
    }

    @Test
    void waiting_publications_count_toward_the_sixty_four_that_a_session_admits() {
        // Arrange: one publication takes the broker's only room, so 63 wait.
        SessionCore mqtt5 = connectedMqtt5(receiveMaximum(1));
        for (int i = 0; i < 64; i++) {
            mqtt5.publish(2 + i, qosOneMessage());
        }

        // Act
        mqtt5.publish(66, qosOneMessage());

        // Assert
        assertThat(port.publications).hasSize(1);
        assertThat(port.results).doesNotContainKeys(2L, 65L);
        assertThat(requireNonNull(port.results.get(66L)).failure())
                .isInstanceOf(MqttCapacityException.class);
    }

    @Test
    void a_clean_session_counts_waiting_publications_toward_the_byte_budget_until_sent() {
        // Arrange: the first publication is sent, and the next two wait with 32 MiB in all.
        SessionCore mqtt5 = connectedMqtt5(receiveMaximum(1));
        MqttMessage half = messageWithWireBytes(16 * 1024 * 1024, MqttVersion.V5);
        mqtt5.publish(2, half);
        int first = port.publicationIds.getLast();
        mqtt5.publish(3, half);
        mqtt5.publish(4, half);
        mqtt5.publish(5, qosOneMessage());
        assertThat(requireNonNull(port.results.get(5L)).failure())
                .isInstanceOf(MqttCapacityException.class);

        // Act: sending the second publication returns its share of the budget.
        mqtt5.received(1, new Packet.PubAck(first, ReasonCode.SUCCESS, EMPTY), 0, 0);
        mqtt5.publish(6, qosOneMessage());

        // Assert
        assertThat(port.publications).hasSize(2);
        assertThat(port.results).doesNotContainKeys(3L, 4L, 6L);
    }

    @Test
    void a_continued_session_resends_pubrel_first_and_publications_within_the_new_maximum() {
        // Arrange: a QoS 2 exchange awaits PUBCOMP and two QoS 1 publications await PUBACK.
        SessionCore mqtt5 = mqtt5Core(Duration.ofHours(1));
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);
        mqtt5.received(1, connAck(), 0, 0);
        mqtt5.publish(2, qosOneMessage().withQos(Qos.EXACTLY_ONCE));
        int released = port.publicationIds.getLast();
        mqtt5.received(1, new Packet.PubRec(released, ReasonCode.SUCCESS, EMPTY), 0, 0);
        mqtt5.publish(3, qosOneMessage());
        int first = port.publicationIds.getLast();
        mqtt5.publish(4, qosOneMessage());
        int second = port.publicationIds.getLast();
        mqtt5.failed(1, new IOException("connection lost"));
        port.sent.clear();
        port.publicationIds.clear();

        // Act: the session continues where the broker takes one publication at once.
        mqtt5.connect(attempt(5), 0);
        mqtt5.opened(2);
        mqtt5.received(2, new Packet.ConnAck(
                true,
                ReasonCode.SUCCESS,
                new Properties(List.of(receiveMaximum(1)))
        ), 0, 0);

        // Assert: the resent PUBREL holds the only room until PUBCOMP.
        assertThat(port.sent).filteredOn(Packet.PubRel.class::isInstance).containsExactly(
                new Packet.PubRel(released, ReasonCode.SUCCESS, EMPTY)
        );
        assertThat(port.publicationIds).isEmpty();

        // Act & Assert: each answer makes room for the next resent publication.
        mqtt5.received(2, new Packet.PubComp(released, ReasonCode.SUCCESS, EMPTY), 0, 0);
        assertThat(port.publicationIds).containsExactly(first);
        mqtt5.received(2, new Packet.PubAck(first, ReasonCode.SUCCESS, EMPTY), 0, 0);
        assertThat(port.publicationIds).containsExactly(first, second);
    }

    @Test
    void the_response_deadline_of_a_waiting_publication_starts_at_its_write() {
        // Arrange: the second publication waits 50 seconds for room.
        SessionCore mqtt5 = connectedMqtt5(receiveMaximum(1));
        mqtt5.publish(2, qosOneMessage());
        int first = port.publicationIds.getLast();
        mqtt5.sent(1, port.ticket, 0);
        mqtt5.publish(3, qosOneMessage());
        long later = Duration.ofSeconds(50).toNanos();
        mqtt5.received(1, new Packet.PubAck(first, ReasonCode.SUCCESS, EMPTY), later, 0);
        mqtt5.sent(1, port.ticket, later);

        // Act: 60 seconds have passed since the publish, but not since the write.
        mqtt5.timer(1, RESPONSE, Duration.ofSeconds(60).toNanos());

        // Assert
        assertThat(mqtt5.state()).isEqualTo(CONNECTED);
        assertThat(port.results).doesNotContainKey(3L);
    }

    @ParameterizedTest(name = "PUBACK {0}")
    @EnumSource(value = ReasonCode.class, names = {"SUCCESS", "NO_MATCHING_SUBSCRIBERS"})
    void an_mqtt_5_puback_below_0x80_completes_the_publication_with_its_reason_code(
            ReasonCode reason
    ) {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.publish(2, qosOneMessage());
        int id = port.publicationIds.getLast();

        // Act
        mqtt5.received(1, new Packet.PubAck(id, reason, EMPTY), 0, 0);

        // Assert
        assertThat(port.results.get(2L)).isEqualTo(new Result(PublishResult.of(reason), null));
    }

    @Test
    void an_mqtt_5_qos_2_publication_reports_the_reason_code_of_its_pubrec() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.publish(2, qosOneMessage().withQos(Qos.EXACTLY_ONCE));
        int id = port.publicationIds.getLast();
        mqtt5.received(
                1,
                new Packet.PubRec(id, ReasonCode.NO_MATCHING_SUBSCRIBERS, EMPTY),
                0,
                0
        );

        // Act
        mqtt5.received(1, new Packet.PubComp(id, ReasonCode.SUCCESS, EMPTY), 0, 0);

        // Assert
        assertThat(port.results.get(2L)).isEqualTo(new Result(
                PublishResult.of(ReasonCode.NO_MATCHING_SUBSCRIBERS),
                null
        ));
    }

    static Stream<Arguments> refusalsWithReasonStrings() {
        var reason = new Properties(List.of(
                new Property.StringValue(PropertyId.REASON_STRING, "topic is read-only")
        ));
        return Stream.of(
                Arguments.of(
                        Qos.AT_LEAST_ONCE,
                        Named.of("PUBACK", new Packet.PubAck(1, ReasonCode.NOT_AUTHORIZED, reason))
                ),
                Arguments.of(
                        Qos.EXACTLY_ONCE,
                        Named.of("PUBREC", new Packet.PubRec(1, ReasonCode.NOT_AUTHORIZED, reason))
                )
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("refusalsWithReasonStrings")
    void a_refused_publication_carries_the_brokers_reason_string(Qos qos, Packet refusal) {
        // Arrange: the first publication takes packet identifier 1.
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.publish(2, qosOneMessage().withQos(qos));
        assertThat(port.publicationIds.getLast()).isEqualTo(1);

        // Act
        mqtt5.received(1, refusal, 0, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure()).isInstanceOfSatisfying(
                MqttPublicationRefusedException.class,
                failure -> {
                    assertThat(failure.reasonCode()).isEqualTo(ReasonCode.NOT_AUTHORIZED);
                    assertThat(failure.reasonString()).contains("topic is read-only");
                }
        );
    }

    @Test
    void a_refused_subscription_carries_the_brokers_reason_string() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        var reason = new Properties(List.of(
                new Property.StringValue(PropertyId.REASON_STRING, "no access to a/#")
        ));

        // Act
        mqtt5.received(
                1,
                new Packet.SubAck(id, List.of(ReasonCode.NOT_AUTHORIZED), reason),
                0,
                0
        );

        // Assert
        assertThat(requireNonNull(port.outcomes.get(2L)).failure()).isInstanceOfSatisfying(
                MqttSubscriptionRefusedException.class,
                failure -> assertThat(failure.reasonString()).contains("no access to a/#")
        );
    }

    @ParameterizedTest(name = "PUBACK {0}")
    @EnumSource(value = ReasonCode.class, names = {
            "UNSPECIFIED_ERROR",
            "IMPLEMENTATION_SPECIFIC_ERROR",
            "NOT_AUTHORIZED",
            "TOPIC_NAME_INVALID",
            "PACKET_IDENTIFIER_IN_USE",
            "QUOTA_EXCEEDED",
            "PAYLOAD_FORMAT_INVALID"
    })
    void an_mqtt_5_refusing_puback_fails_only_the_publication(ReasonCode reason) {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.publish(2, qosOneMessage());
        int id = port.publicationIds.getLast();

        // Act
        mqtt5.received(1, new Packet.PubAck(id, reason, EMPTY), 0, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure()).isInstanceOfSatisfying(
                MqttPublicationRefusedException.class,
                refusal -> assertThat(refusal.reasonCode()).isEqualTo(reason)
        );
        assertThat(mqtt5.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "SUBACK {0}")
    @EnumSource(value = ReasonCode.class, names = {
            "UNSPECIFIED_ERROR",
            "IMPLEMENTATION_SPECIFIC_ERROR",
            "NOT_AUTHORIZED",
            "TOPIC_FILTER_INVALID",
            "PACKET_IDENTIFIER_IN_USE",
            "QUOTA_EXCEEDED",
            "SHARED_SUBSCRIPTIONS_NOT_SUPPORTED",
            "SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED",
            "WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED"
    })
    void an_mqtt_5_refusal_ends_only_the_subscription_with_its_reason_code(ReasonCode reason) {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act
        mqtt5.received(1, new Packet.SubAck(id, List.of(reason), EMPTY), 0, 0);

        // Assert
        assertThat(mqtt5.state()).isEqualTo(CONNECTED);
        assertThat(requireNonNull(port.outcomes.get(2L)).failure()).isInstanceOfSatisfying(
                MqttSubscriptionRefusedException.class,
                refusal -> assertThat(refusal.reasonCode()).isEqualTo(reason)
        );
        assertThat(port.removed).containsExactly(2L);
    }

    @ParameterizedTest(name = "UNSUBACK {0}")
    @EnumSource(
            value = ReasonCode.class,
            names = {"SUCCESS", "NO_SUBSCRIPTION_EXISTED", "NOT_AUTHORIZED"}
    )
    void any_mqtt_5_unsuback_reason_confirms_the_removal(ReasonCode reason) {
        // Arrange: a refusal leaves the broker's subscription in place, which a log reports.
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        mqtt5.received(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0, 0);
        mqtt5.unsubscribe(3, 2);
        int unsubscribeId = ((Packet.Unsubscribe) port.sent.getLast()).packetId();

        // Act
        mqtt5.received(1, new Packet.UnsubAck(unsubscribeId, List.of(reason), EMPTY), 0, 0);

        // Assert
        assertThat(mqtt5.state()).isEqualTo(CONNECTED);
        assertThat(requireNonNull(port.results.get(3L)).failure()).isNull();
        assertThat(port.removed).containsExactly(2L);
    }

    @Test
    void an_unsuback_with_more_reason_codes_than_filters_breaks_the_protocol() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        mqtt5.received(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0, 0);
        mqtt5.unsubscribe(3, 2);
        int unsubscribeId = ((Packet.Unsubscribe) port.sent.getLast()).packetId();

        // Act
        mqtt5.received(1, new Packet.UnsubAck(
                unsubscribeId,
                List.of(ReasonCode.SUCCESS, ReasonCode.SUCCESS),
                EMPTY
        ), 0, 0);
        mqtt5.sent(1, port.ticket, 0);

        // Assert: the client reports the violation before it closes the connection.
        assertThat(port.sent).last()
                .isEqualTo(new Packet.Disconnect(ReasonCode.PROTOCOL_ERROR, EMPTY));
        assertThat(mqtt5.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class));
    }

    @Test
    void refused_subscribe_and_unsubscribe_release_identifiers_across_allocator_wrap() {
        // Arrange: both operations share one packet-identifier pool [MQTT-2.3.1-2].
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        port.sent.clear();
        port.sendResult = SendQueue.OfferResult.FULL;
        Subscribe request = subscription("new/#");

        // Act: repeated queue refusal returns every identifier before the allocator wraps.
        sut.subscribe(3, request);
        sut.unsubscribe(4, 2);
        for (int i = 0; i < 65535; i++) {
            sut.sendCapacityAvailable(1);
        }

        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.sendCapacityAvailable(1);

        // Assert
        assertThat(port.sent).hasSize(2);
        var unsubscribed = (Packet.Unsubscribe) port.sent.getFirst();
        var subscribed = (Packet.Subscribe) port.sent.getLast();
        assertThat(subscribed.packetId()).isNotEqualTo(unsubscribed.packetId());

        // Act: both exchanges can still complete after capacity returns.
        receive(
                1,
                new Packet.SubAck(subscribed.packetId(), List.of(ReasonCode.SUCCESS), EMPTY),
                0
        );
        receive(1, new Packet.UnsubAck(unsubscribed.packetId(), List.of(), EMPTY), 0);

        // Assert
        assertThat(requireNonNull(port.outcomes.get(3L)).failure()).isNull();
        assertThat(requireNonNull(port.results.get(4L)).failure()).isNull();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void delivery_capacity_failure_ends_the_connection_but_preserves_registrations() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        port.acceptDelivery = false;

        // Act: incoming delivery never waits for an application handler to free capacity. Only a
        // QoS 1 or 2 delivery can find it exhausted; overflow drops QoS 0 instead.
        receive(1, qosOnePublication(7, "a/b", false), 0);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class);
        });
        assertThat(port.removed).isEmpty();
    }

    @Test
    void old_generation_writes_and_acks_cannot_complete_new_operations() {
        // Arrange: lose the first connection with pending operations, then reconnect.
        connected();
        sut.publish(2, message());
        long oldTicket = port.ticket;
        sut.subscribe(3, subscription("a/#"));
        int oldId = ((Packet.Subscribe) port.sent.getLast()).packetId();
        sut.failed(1, new IOException("Peer reset"));
        sut.connect(attempt(4), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        sut.publish(5, message());
        sut.subscribe(6, subscription("new/#"));

        // Act: callbacks from the old connection arrive after the new operations start.
        sut.sent(1, oldTicket, 0);
        receive(1, new Packet.SubAck(oldId, List.of(ReasonCode.SUCCESS), EMPTY), 0);
        receive(1, publication(), 0);

        // Assert
        assertThat(port.results).doesNotContainKey(5L);
        assertThat(port.outcomes).doesNotContainKeys(3L, 6L);
        assertThat(port.deliveries).isEmpty();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    private static Stream<List<ReasonCode>> invalidSubackReasons() {
        return Stream.of(
                List.of(ReasonCode.SUCCESS, ReasonCode.SUCCESS),
                List.of(ReasonCode.GRANTED_QOS_1),
                List.of(ReasonCode.GRANTED_QOS_2)
        );
    }

    private static Stream<Packet> unknownSubscriptionAcknowledgements() {
        return Stream.of(
                new Packet.SubAck(1, List.of(ReasonCode.SUCCESS), EMPTY),
                new Packet.UnsubAck(1, List.of(), EMPTY)
        );
    }

    private static MqttMessage message() {
        return MqttMessage.of(new Topic("a/b"), new byte[] {1, 2}, Qos.AT_MOST_ONCE, false);
    }

    @Test
    void qos_one_publish_waits_for_puback_and_ignores_repeated_acknowledgement() {
        // Arrange
        connected();
        sut.publish(2, qosOneMessage());
        int id = port.publicationIds.getLast();

        // Act: a successful write is insufficient for QoS 1.
        sut.sent(1, port.ticket, 0);

        // Assert
        assertThat(id).isPositive();
        assertThat(port.results).doesNotContainKey(2L);

        // Act
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert: RecordingPort also rejects completing an operation a second time.
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(PublishResult.of());
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void puback_before_write_completion_completes_only_its_publication() {
        // Arrange
        connected();
        sut.publish(2, qosOneMessage());
        int firstId = port.publicationIds.getLast();
        long ticket = port.ticket;

        // Act: inbound and writer notifications may arrive in this order.
        receive(1, new Packet.PubAck(firstId, ReasonCode.SUCCESS, EMPTY), 0);
        sut.publish(3, qosOneMessage());
        sut.sent(1, ticket, 0);
        receive(1, new Packet.PubAck(firstId, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(PublishResult.of());
        assertThat(port.results).doesNotContainKey(3L);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void puback_does_not_release_a_subscription_packet_identifier() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act: an unknown PUBACK is tolerated even if another exchange uses its id.
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY), 0);

        // Assert
        assertThat(requireNonNull(port.outcomes.get(2L)).value()).isInstanceOf(Subscription.class);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void puback_before_close_completes_a_publication_exactly_once() {
        // Arrange
        connected();
        sut.publish(2, qosOneMessage());
        int id = port.publicationIds.getLast();

        // Act
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);
        sut.close(null);
        sut.sent(1, port.ticket, 0);

        // Assert: RecordingPort also rejects completing an operation a second time.
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(PublishResult.of());
        assertThat(sut.state()).isEqualTo(CLOSED);
    }

    @Test
    void close_before_puback_fails_a_publication_exactly_once() {
        // Arrange
        connected();
        sut.publish(2, qosOneMessage());
        int id = port.publicationIds.getLast();

        // Act
        sut.close(null);
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);
        sut.sent(1, port.ticket, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class);
        assertThat(sut.state()).isEqualTo(CLOSED);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void reordered_qos_one_events_complete_once_and_acknowledge_in_receive_order(int seed) {
        // Arrange: each chain keeps its own causal order, and the seed interleaves the chains.
        // A keep-alive exchange always takes part, and some runs hold acknowledgements back for
        // a while. Some runs end the connection at a random point; later events then arrive stale.
        var random = new Random(seed);
        connected();
        sut.subscribe(2, subscription("a/#"));
        var chains = new ArrayList<ArrayDeque<Runnable>>();
        List<Long> operations = List.of(10L, 11L, 12L);
        for (long operation : operations) {
            chains.add(publicationChain(operation, random));
        }
        var admitted = new ArrayList<Integer>();
        for (int id = 100; id < 104; id++) {
            String topic = random.nextBoolean() ? "a/b" : "other";
            chains.add(receptionChain(id, topic, admitted, random));
        }
        chains.add(new ArrayDeque<>(List.of(
                () -> {
                    now = seconds(10);
                    sut.timer(1, KEEP_ALIVE, now);
                },
                () -> receive(1, new Packet.PingResp(), now)
        )));
        if (random.nextBoolean()) {
            chains.add(new ArrayDeque<>(List.of(
                    () -> port.ackSlots = 0,
                    () -> {
                        port.ackSlots = Integer.MAX_VALUE;
                        sut.sendCapacityAvailable(1);
                    }
            )));
        }
        boolean terminates = random.nextBoolean();
        if (terminates) {
            Runnable end = random.nextBoolean()
                    ? () -> sut.close(null)
                    : () -> sut.failed(1, new IOException("Connection reset"));
            chains.add(new ArrayDeque<>(List.of(end)));
        }

        // Act
        while (!chains.isEmpty()) {
            ArrayDeque<Runnable> chain = chains.get(random.nextInt(chains.size()));
            chain.removeFirst().run();
            if (chain.isEmpty()) {
                chains.remove(chain);
            }
        }

        // Assert: RecordingPort rejects a second completion and PacketIds a second release, so
        // reaching here also shows that each happened at most once.
        assertThat(operations).allSatisfy(operation -> {
            Result result = requireNonNull(port.results.get(operation));
            if (!terminates) {
                assertThat(result.value()).isEqualTo(PublishResult.of());
            }
        });
        List<Integer> acknowledged = pubAckIds();
        assertThat(acknowledged).hasSizeLessThanOrEqualTo(admitted.size());
        assertThat(acknowledged).isEqualTo(admitted.subList(0, acknowledged.size()));
        assertThat(port.forgotten).doesNotHaveDuplicates();
        if (!terminates) {
            assertThat(acknowledged).isEqualTo(admitted);
            // Each PUBACK's write completion retires its reception.
            assertThat(port.forgotten).hasSize(admitted.size());
            // The keep-alive exchange sent one PINGREQ, and its PINGRESP ended the wait.
            assertThat(port.sent).filteredOn(Packet.PingReq.class::isInstance).hasSize(1);
            assertThat(port.timers).doesNotContainKey(PING);
        }
        assertThat(port.released).doesNotHaveDuplicates().hasSize(acknowledged.size());
        // A retransmission joins its reception instead of reaching the handler again.
        assertThat(port.deliveries).noneMatch(Received::dup);
    }

    static IntStream seeds() {
        return IntStream.range(0, 100);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void reordered_recreation_removals_and_old_events_preserve_subscription_intent(int seed) {
        // Arrange: one continuing subscription, one close, and one explicit unsubscribe.
        var random = new Random(seed);
        connected();
        establishSubscriptions(3);
        Object continuing = requireNonNull(port.outcomes.get(2L)).value();
        long oldTicket = port.ticket;
        receive(1, qosOnePublication(7, "slot/0", false), 0);
        DeliveryToken old = port.deliveries.getLast().delivery();
        sut.failed(1, new IOException("lost connection"));
        List<Disconnection> expectedEnds = List.copyOf(port.ends);
        port.sent.clear();
        sut.connect(attempt(100), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        List<Packet.Subscribe> recreated = port.sent.stream()
                .map(Packet.Subscribe.class::cast).toList();
        boolean refused = seed % 2 == 0;
        var chains = new ArrayList<ArrayDeque<Runnable>>();
        chains.add(new ArrayDeque<>(List.of(() -> receive(2, new Packet.SubAck(
                recreated.getFirst().packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0))));
        chains.add(new ArrayDeque<>(List.of(() -> {
            sut.unsubscribe(200, 3);
            sut.sendCapacityAvailable(2);
        })));
        chains.add(new ArrayDeque<>(List.of(() -> receive(2, new Packet.SubAck(
                recreated.get(1).packetId(), List.of(ReasonCode.SUCCESS), EMPTY), 0))));
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.unsubscribe(300, 4),
                () -> receive(2, new Packet.SubAck(recreated.getLast().packetId(), List.of(refused
                        ? ReasonCode.UNSPECIFIED_ERROR : ReasonCode.SUCCESS), EMPTY), 0)
        )));
        chains.add(new ArrayDeque<>(List.of(
                () -> receive(1, new Packet.SubAck(recreated.getFirst().packetId(),
                        List.of(ReasonCode.SUCCESS), EMPTY), 0),
                () -> receive(1, new Packet.UnsubAck(recreated.getLast().packetId(),
                        List.of(), EMPTY), 0),
                () -> sut.deliveryReady(old.acknowledgement()),
                () -> sut.deliveryFailed(old, new IllegalStateException("old handler failed")),
                () -> sut.sent(1, oldTicket, 0),
                () -> sut.sendCapacityAvailable(1)
        )));

        // Act & Assert: stale events never complete or terminate current work.
        while (!chains.isEmpty()) {
            ArrayDeque<Runnable> chain = chains.get(random.nextInt(chains.size()));
            chain.removeFirst().run();
            if (chain.isEmpty()) {
                chains.remove(chain);
            }
            assertThat(port.ends).isEqualTo(expectedEnds);
            assertThat(sut.state()).isEqualTo(CONNECTED);
            assertThat(requireNonNull(port.outcomes.get(2L)).value()).isSameAs(continuing);
            assertThat(port.registered).containsExactly(2L, 3L, 4L);
        }
        List<Packet.Unsubscribe> removals = port.sent.stream()
                .filter(Packet.Unsubscribe.class::isInstance)
                .map(Packet.Unsubscribe.class::cast).toList();
        assertThat(removals).hasSize(refused ? 1 : 2);
        for (Packet.Unsubscribe removal : removals) {
            receive(2, new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY), 0);
        }
        assertThat(requireNonNull(port.results.get(200L)).failure()).isNull();
        assertThat(requireNonNull(port.results.get(300L)).failure()).isNull();
        assertThat(port.removed).containsExactlyInAnyOrder(3L, 4L);
        assertThat(port.removals).filteredOn(removal -> removal.id() == 4)
                .singleElement().satisfies(removal -> {
                    if (refused) {
                        assertThat(removal.cause())
                                .isInstanceOf(MqttSubscriptionRefusedException.class);
                    } else {
                        assertThat(removal.cause()).isNull();
                    }
                });
        assertThat(pubAckIds()).isEmpty();
        port.deliveries.clear();
        receive(2, qosOnePublication(7, "slot/0", false), 0);
        assertThat(port.deliveries).singleElement()
                .satisfies(delivery -> assertThat(delivery.ids()).containsExactly(2L));
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void reordered_delivery_failures_stop_only_the_receiving_connection(int seed) {
        // Arrange: an old failure crosses connect and CONNACK, while the current failure may
        // come from QoS 0 or a QoS 1 delivery whose PUBACK has already been written.
        var random = new Random(seed);
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        DeliveryToken old = port.deliveries.getLast().delivery();
        sut.failed(1, new IOException("Lost connection"));
        var expected = new ArrayList<>(port.ends);
        var failure = new IllegalStateException("current handler failed");
        var oldFailure = new IllegalStateException("old handler failed");
        var chains = new ArrayList<ArrayDeque<Runnable>>();
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.connect(attempt(3), 0),
                () -> receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0),
                () -> {
                    receive(2, seed % 2 == 0 ? publication()
                            : qosOnePublication(7, "a/b", false), 0);
                },
                () -> {
                    DeliveryToken current = port.deliveries.getLast().delivery();
                    sut.deliveryReady(current.acknowledgement());
                    if (current.acknowledgement() != 0) {
                        sut.written(2, port.ackTickets.getLast());
                    }
                },
                () -> {
                    expected.add(Disconnection.of(failure, false, 2));
                    sut.deliveryFailed(port.deliveries.getLast().delivery(), failure);
                }
        )));
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.deliveryFailed(old, oldFailure),
                () -> sut.deliveryReady(old.acknowledgement()),
                () -> sut.deliveryFailed(old, oldFailure)
        )));

        // Act & Assert: old failures never add a termination, including during establishment.
        while (!chains.isEmpty()) {
            ArrayDeque<Runnable> chain = chains.get(random.nextInt(chains.size()));
            chain.removeFirst().run();
            if (chain.isEmpty()) {
                chains.remove(chain);
            }
            assertThat(port.ends).isEqualTo(expected);
        }
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(pubAckIds()).isEqualTo(seed % 2 == 0 ? List.of() : List.of(7));
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void reordered_old_readiness_cannot_acknowledge_a_reused_id_after_reconnect(int seed) {
        // Arrange: an old snapshot may reach the core before or after the next CONNACK.
        var random = new Random(seed);
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        long oldToken = port.deliveries.getLast().token();
        List<Long> snapshot = List.of(oldToken);
        var current = new Exchange();
        var expected = new ArrayList<Integer>();
        var chains = new ArrayList<ArrayDeque<Runnable>>();
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.failed(1, new IOException("Lost connection")),
                () -> sut.connect(attempt(3), 0),
                () -> receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0),
                () -> {
                    receive(2, qosOnePublication(7, "a/b", false), 0);
                    current.token = port.deliveries.getLast().token();
                    assertThat(current.token).isGreaterThan(oldToken);
                },
                () -> {
                    expected.add(7);
                    sut.deliveryReady(current.token);
                }
        )));
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.deliveriesReady(snapshot),
                () -> sut.deliveriesReady(snapshot)
        )));
        chains.add(new ArrayDeque<>(List.of(
                () -> sut.sendCapacityAvailable(1),
                () -> sut.written(1, port.ticket)
        )));

        // Act & Assert: readiness before connection loss may finish the original receipt,
        // but only the new delivery's readiness can acknowledge the reused identifier.
        while (!chains.isEmpty()) {
            ArrayDeque<Runnable> chain = chains.get(random.nextInt(chains.size()));
            chain.removeFirst().run();
            if (chain.isEmpty()) {
                chains.remove(chain);
            }
            var acknowledged = new ArrayList<Integer>();
            if (port.released.contains(oldToken)) {
                acknowledged.add(7);
            }
            acknowledged.addAll(expected);
            assertThat(pubAckIds()).isEqualTo(acknowledged);
        }
        assertThat(port.released).contains(current.token).doesNotHaveDuplicates();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void disconnect_completes_publications_acknowledged_before_it_ends_and_not_the_rest() {
        // Arrange
        connected();
        sut.publish(2, qosOneMessage());
        int acknowledgedId = port.publicationIds.getLast();
        sut.publish(3, qosOneMessage());

        // Act: shutdown does not wait for PUBACK, but uses one that arrives before it ends.
        sut.disconnect(4);
        receive(1, new Packet.PubAck(acknowledgedId, ReasonCode.SUCCESS, EMPTY), 0);
        sut.sent(1, port.ticket, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).value()).isEqualTo(PublishResult.of());
        assertThat(requireNonNull(port.results.get(3L)).failure())
                .isInstanceOf(MqttConnectionLostException.class);
        assertThat(requireNonNull(port.results.get(4L)).failure()).isNull();
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.sent).singleElement().isInstanceOf(Packet.Disconnect.class);
    }

    @Test
    void refused_qos_one_publications_return_identifiers_across_allocator_wrap() {
        // Arrange
        connected();
        MqttMessage message = qosOneMessage();
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act: all identifiers are offered and refused once.
        for (int i = 0; i < 65535; i++) {
            sut.publish(i + 2L, message);
        }
        port.sendResult = SendQueue.OfferResult.ACCEPTED;
        sut.publish(65537, message);
        int id = port.publicationIds.getLast();
        sut.subscribe(65538, subscription("other"));
        int subscriptionId = ((Packet.Subscribe) port.sent.getLast()).packetId();
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert: the pool is shared with SUBSCRIBE and no refused operation retained an id.
        assertThat(id).isEqualTo(1);
        assertThat(subscriptionId).isEqualTo(2);
        assertThat(requireNonNull(port.results.get(65536L)).failure())
                .isInstanceOf(MqttCapacityException.class);
        assertThat(requireNonNull(port.results.get(65537L)).value()).isEqualTo(PublishResult.of());
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void acknowledged_qos_one_publications_return_identifiers_across_allocator_wrap() {
        // Arrange
        connected();
        MqttMessage message = qosOneMessage();

        // Act: one connection publishes more QoS 1 messages than there are identifiers.
        for (int i = 0; i < 65536; i++) {
            sut.publish(i + 2L, message);
            int id = port.publicationIds.getLast();
            receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);
        }

        // Assert: the last publication wrapped to an identifier that PUBACK had returned.
        assertThat(port.publicationIds).hasSize(65536);
        assertThat(port.publicationIds.getLast()).isEqualTo(1);
        assertThat(requireNonNull(port.results.get(65537L)).value())
                .isEqualTo(PublishResult.of());
    }

    @Test
    void a_puback_for_a_refused_publication_is_unknown() {
        // Arrange: send admission refuses the publication after it allocated an identifier.
        connected();
        port.sendResult = SendQueue.OfferResult.FULL;
        sut.publish(2, qosOneMessage());
        int id = port.publicationIds.getLast();
        port.sendResult = SendQueue.OfferResult.ACCEPTED;

        // Act
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert: RecordingPort also rejects completing the refused operation a second time.
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttCapacityException.class);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void closed_send_queue_preserves_the_terminal_cause_for_qos_one_publish() {
        // Arrange
        connected();
        port.sendResult = SendQueue.OfferResult.CLOSED;
        var failure = new IOException("Peer reset before PUBACK");

        // Act
        sut.publish(2, qosOneMessage());

        // Assert
        assertThat(port.results).doesNotContainKey(2L);

        // Act
        sut.failed(1, failure);

        // Assert
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(failure);
    }

    @ParameterizedTest(name = "requested {0}, granted {1}")
    @CsvSource({
            "AT_LEAST_ONCE, AT_MOST_ONCE",
            "AT_LEAST_ONCE, AT_LEAST_ONCE",
            "EXACTLY_ONCE, AT_MOST_ONCE",
            "EXACTLY_ONCE, AT_LEAST_ONCE",
            "EXACTLY_ONCE, EXACTLY_ONCE"
    })
    void subscriptions_accept_the_requested_or_lower_grant(Qos requested, Qos granted) {
        // Arrange
        connected();
        sut.subscribe(2, Subscribe.of(new TopicFilter("a/#"), requested));
        int id = ((Packet.Subscribe) port.sent.getLast()).packetId();

        // Act
        receive(1, new Packet.SubAck(id, List.of(ReasonCode.of(granted.level())), EMPTY), 0);

        // Assert
        assertThat(requireNonNull(port.outcomes.get(2L)).value())
                .isInstanceOfSatisfying(Subscription.class, value ->
                        assertThat(value.grantedQos()).isEqualTo(granted));
    }

    @Test
    void qos_one_deliveries_wait_for_all_recipients_and_preserve_receive_order() {
        // Arrange: the lane aggregates each publication's per-recipient completion conditions.
        connected();
        sut.subscribe(2, subscription("a/#"));
        sut.subscribe(3, subscription("a/+"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        receive(1, qosOnePublication(8, "a/b", false), 0);
        long first = port.deliveries.getFirst().token();
        long second = port.deliveries.getLast().token();

        // Act: later recipients are all ready while the first publication is still pending.
        sut.deliveryReady(second);
        sut.deliveryReady(second);

        // Assert
        assertThat(pubAckIds()).isEmpty();
        assertThat(port.released).isEmpty();
        assertThat(port.deliveries).allSatisfy(delivery ->
                assertThat(delivery.ids()).containsExactly(2L, 3L));

        // Act
        sut.deliveryReady(first);
        sut.deliveryReady(first);

        // Assert
        assertThat(pubAckIds()).containsExactly(7, 8);
        assertThat(port.released).containsExactly(first, second);
    }

    @Test
    void unmatched_qos_one_delivery_waits_behind_an_earlier_matching_delivery() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        long first = port.deliveries.getFirst().token();

        // Act
        receive(1, qosOnePublication(8, "other", false), 0);

        // Assert
        assertThat(pubAckIds()).isEmpty();
        assertThat(port.deliveries).hasSize(1);

        // Act
        sut.deliveryReady(first);

        // Assert
        assertThat(pubAckIds()).containsExactly(7, 8);
        assertThat(port.released).hasSize(2);
    }

    @Test
    void qos_zero_delivery_bypasses_a_pending_qos_one_acknowledgement() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);

        // Act
        receive(1, publication(), 0);

        // Assert
        assertThat(port.deliveries).hasSize(2);
        assertThat(port.deliveries.getLast().token()).isZero();
        assertThat(port.deliveries.getLast().message().qos()).isEqualTo(Qos.AT_MOST_ONCE);
        assertThat(pubAckIds()).isEmpty();
    }

    @Test
    void retransmissions_share_delivery_and_ack_until_the_puback_is_written() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        for (int id = 1; id <= 32; id++) {
            receive(1, qosOnePublication(id, "a/b", false), 0);
        }

        // Act: more retransmissions than the acknowledgement limit do not consume new slots.
        for (int round = 0; round < 130; round++) {
            for (int id = 1; id <= 32; id++) {
                receive(1, qosOnePublication(id, "a/b", true), 0);
            }
        }
        sut.deliveriesReady(port.deliveries.stream().map(Received::token).toList());
        receive(1, qosOnePublication(1, "a/b", true), 0);

        // Assert: even an admitted PUBACK has not yet ended the exchange.
        assertThat(port.deliveries).hasSize(32);
        assertThat(pubAckIds()).hasSize(16).doesNotHaveDuplicates();
        assertThat(sut.state()).isEqualTo(CONNECTED);

        // Act
        for (int i = 0; i < 32; i++) {
            sut.written(1, port.ackTickets.get(i));
        }

        // Assert
        assertThat(pubAckIds()).isEqualTo(IntStream.rangeClosed(1, 32).boxed().toList());
        assertThat(port.released).hasSize(32).doesNotHaveDuplicates();
    }

    @ParameterizedTest(name = "reused id has DUP={0}")
    @ValueSource(booleans = {false, true})
    void a_written_puback_allows_id_reuse_regardless_of_dup_and_ignores_the_old_ack(boolean dup) {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", true), 0);
        long first = port.deliveries.getFirst().token();
        sut.deliveryReady(first);
        long ackTicket = port.ackTickets.getFirst();
        sut.written(1, ackTicket);

        // Act: the next message may even have identical topic and payload.
        receive(1, qosOnePublication(7, "a/b", dup), 0);
        sut.deliveryReady(first);
        sut.written(1, ackTicket);

        // Assert: the new exchange has its own readiness condition.
        assertThat(port.deliveries).hasSize(2);
        assertThat(pubAckIds()).containsExactly(7);

        // Act
        sut.deliveryReady(port.deliveries.getLast().token());

        // Assert
        assertThat(pubAckIds()).containsExactly(7, 7);
    }

    @Test
    void reuse_without_dup_before_its_puback_is_queued_withdraws_the_earlier_acknowledgement() {
        // Arrange: the earlier reception is not ready, so its PUBACK is not queued yet.
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        receive(1, qosOnePublication(8, "a/b", false), 0);
        long earlier = port.deliveries.get(0).token();
        long following = port.deliveries.get(1).token();
        sut.deliveryReady(following);

        // Act: a sender reuses an identifier only for an exchange it considers acknowledged.
        receive(1, qosOnePublication(7, "a/b", false), 0);
        long later = port.deliveries.getLast().token();

        // Assert: the earlier reception no longer holds back the ready follower.
        assertThat(port.deliveries).hasSize(3);
        assertThat(port.released).containsExactly(earlier, following);
        assertThat(pubAckIds()).containsExactly(8);
        assertThat(sut.state()).isEqualTo(CONNECTED);

        // Act
        sut.deliveriesReady(List.of(earlier, later));

        // Assert: one PUBACK covers the reused identifier.
        assertThat(pubAckIds()).containsExactly(8, 7);
        assertThat(port.released).containsExactly(earlier, following, later);
    }

    @Test
    void reuse_without_dup_after_its_puback_is_queued_starts_a_new_reception() {
        // Arrange: the earlier PUBACK is queued, but its write has not been reported.
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        sut.deliveryReady(port.deliveries.getFirst().token());
        long earlierAck = port.ackTickets.getFirst();

        // Act: the reported write must not free the identifier of the new reception.
        receive(1, qosOnePublication(7, "a/b", false), 0);
        sut.written(1, earlierAck);
        receive(1, qosOnePublication(7, "a/b", true), 0);

        // Assert: the retransmission joins the new reception.
        assertThat(port.deliveries).hasSize(2);
        assertThat(pubAckIds()).containsExactly(7);

        // Act
        sut.deliveryReady(port.deliveries.getLast().token());

        // Assert
        assertThat(pubAckIds()).containsExactly(7, 7);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void old_generation_readiness_and_puback_do_not_finish_new_connection_work() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(7, "a/b", false), 0);
        long oldToken = port.deliveries.getLast().token();
        sut.failed(1, new IOException("Lost connection"));
        sut.connect(attempt(3), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        sut.subscribe(4, subscription("a/#"));
        receive(2, qosOnePublication(7, "a/b", false), 0);
        long newToken = port.deliveries.getLast().token();
        sut.publish(5, qosOneMessage());
        int id = port.publicationIds.getLast();

        // Act
        sut.deliveryReady(oldToken);
        sut.deliveryReady(oldToken);
        receive(1, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert
        assertThat(newToken).isGreaterThan(oldToken);
        assertThat(pubAckIds()).isEmpty();
        assertThat(port.results).doesNotContainKey(5L);

        // Act
        sut.deliveryReady(newToken);
        receive(2, new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY), 0);

        // Assert
        assertThat(pubAckIds()).containsExactly(7);
        assertThat(requireNonNull(port.results.get(5L)).value()).isEqualTo(PublishResult.of());
    }

    @Test
    void pending_unmatched_acknowledgements_are_bounded_and_cleared_on_connection_end() {
        // Arrange: one unfinished delivery holds the head, including unmatched followers.
        connected();
        sut.subscribe(2, subscription("a/#"));
        receive(1, qosOnePublication(1, "a/b", false), 0);
        for (int id = 2; id <= 4096; id++) {
            receive(1, qosOnePublication(id, "other", false), 0);
        }
        assertThat(sut.state()).isEqualTo(CONNECTED);

        // Act
        receive(1, qosOnePublication(4097, "other", false), 0);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class));
        assertThat(pubAckIds()).isEmpty();

        // Act
        sut.connect(attempt(3), 0);
        receive(2, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        receive(2, qosOnePublication(1, "other", false), 0);

        // Assert
        assertThat(pubAckIds()).containsExactly(1);
    }

    @Test
    void full_puback_reservation_keeps_the_head_and_resumes_only_as_capacity_returns() {
        // Arrange
        connected();
        sut.subscribe(2, subscription("a/#"));
        for (int id = 1; id <= 100; id++) {
            receive(1, qosOnePublication(id, "a/b", false), 0);
        }
        port.ackSlots = 2;

        // Act
        sut.deliveriesReady(port.deliveries.stream().map(Received::token).toList());

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(pubAckIds()).containsExactly(1, 2);
        assertThat(port.released).hasSize(2);
        assertThat(port.ends).isEmpty();

        // Act: write observation precedes budget release; neither event loses the waiting head.
        for (int i = 0; i < 100; i++) {
            sut.written(1, port.ackTickets.get(i));
            port.ackSlots++;
            sut.sendCapacityAvailable(1);
        }

        // Assert: draining requires no new input or delivery notification.
        assertThat(pubAckIds()).isEqualTo(IntStream.rangeClosed(1, 100).boxed().toList());
        assertThat(port.released).hasSize(100).doesNotHaveDuplicates();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void disconnect_prevents_deferred_pubacks_from_refilling_the_queue() {
        // Arrange
        connected();
        receive(1, qosOnePublication(7, "other", false), 0);
        port.ackSlots = 0;
        receive(1, qosOnePublication(8, "other", false), 0);
        sut.disconnect(2);

        // Act
        port.ackSlots = 14;
        sut.written(1, port.ackTickets.getFirst());
        sut.sendCapacityAvailable(1);

        // Assert
        assertThat(pubAckIds()).containsExactly(7);
        assertThat(port.sent.getLast()).isInstanceOf(Packet.Disconnect.class);
    }

    @Test
    void closed_puback_queue_preserves_the_original_terminal_cause() {
        // Arrange
        connected();
        port.sendResult = SendQueue.OfferResult.CLOSED;

        // Act
        receive(1, qosOnePublication(7, "other", false), 0);

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.released).isEmpty();
        assertThat(port.ends).isEmpty();

        // Act
        var failure = new IOException("Transport closed during PUBACK");
        sut.failed(1, failure);

        // Assert
        assertThat(port.ends).containsExactly(Disconnection.of(failure, false, 1));
    }

    /// Publishes, then reports the write completion and PUBACK in a seed-chosen order.
    private ArrayDeque<Runnable> publicationChain(long operation, Random random) {
        var exchange = new Exchange();
        Runnable written = () -> {
            if (exchange.packetId != 0) {
                sut.sent(1, exchange.ticket, now);
            }
        };
        Runnable acknowledged = () -> {
            if (exchange.packetId != 0) {
                receive(1, new Packet.PubAck(exchange.packetId, ReasonCode.SUCCESS, EMPTY), now);
            }
        };
        var chain = new ArrayDeque<Runnable>();
        chain.add(() -> {
            int published = port.publicationIds.size();
            sut.publish(operation, qosOneMessage());
            if (port.publicationIds.size() > published) {
                exchange.packetId = port.publicationIds.getLast();
                exchange.ticket = port.ticket;
            }
        });
        List<Runnable> completion = random.nextBoolean()
                ? List.of(written, acknowledged)
                : List.of(acknowledged, written);
        chain.addAll(completion);
        return chain;
    }

    /// Receives a QoS 1 publication, reports its delivery readiness if it was routed, and then
    /// completes the write of its PUBACK once it is sent. Some publications are retransmitted with
    /// DUP before that write; each retransmission joins the original reception. Receptions while
    /// connected are admitted in this order, which PUBACKs must follow.
    private ArrayDeque<Runnable> receptionChain(
            int packetId,
            String topic,
            List<Integer> admitted,
            Random random
    ) {
        var exchange = new Exchange();
        var chain = new ArrayDeque<Runnable>();
        chain.add(() -> {
            if (sut.state() == CONNECTED) {
                admitted.add(packetId);
            }
            int routed = port.deliveries.size();
            receive(1, qosOnePublication(packetId, topic, false), now);
            if (port.deliveries.size() > routed) {
                exchange.token = port.deliveries.getLast().token();
            }
        });
        Runnable ready = () -> {
            if (exchange.token != 0) {
                sut.deliveryReady(exchange.token);
            }
        };
        Runnable retransmission = () -> receive(1, qosOnePublication(packetId, topic, true), now);
        if (random.nextBoolean()) {
            List<Runnable> steps = random.nextBoolean()
                    ? List.of(retransmission, ready)
                    : List.of(ready, retransmission);
            chain.addAll(steps);
        } else {
            chain.add(ready);
        }
        chain.add(new Runnable() {
            // Bounds the retries, so a PUBACK that is never sent fails the assertions instead of
            // looping.
            private int attempts;

            @Override
            public void run() {
                int position = pubAckIds().indexOf(packetId);
                if (position >= 0) {
                    sut.written(1, port.ackTickets.get(position));
                } else if (sut.state() == CONNECTED && ++attempts < 1_000) {
                    // An earlier reception or the full reserved quota still holds the PUBACK.
                    chain.addFirst(this);
                }
            }
        });
        return chain;
    }

    private List<Integer> pubAckIds() {
        return port.sent.stream()
                .filter(Packet.PubAck.class::isInstance)
                .map(Packet.PubAck.class::cast)
                .map(Packet.PubAck::packetId)
                .toList();
    }

    private static MqttMessage qosOneMessage() {
        return MqttMessage.of(new Topic("a/b"), new byte[] {1, 2}, Qos.AT_LEAST_ONCE, false);
    }

    private static Packet.Publish qosOnePublication(int packetId, String topic, boolean dup) {
        return new Packet.Publish(
                new Topic(topic), new byte[] {1, 2}, Qos.AT_LEAST_ONCE, false, dup, packetId, EMPTY
        );
    }

    private static Packet.Publish publication() {
        return new Packet.Publish(
                new Topic("a/b"),
                new byte[] {1, 2},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        );
    }

    private static Subscribe subscription(String filter) {
        return Subscribe.of(new TopicFilter(filter), Qos.AT_MOST_ONCE).withBufferCapacity(4);
    }

    /// A QoS 0 publication on `topic` that carries `identifiers` as Subscription Identifiers.
    private static Packet.Publish publication(String topic, int... identifiers) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {1, 2},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                identifierProperties(identifiers)
        );
    }

    private static Packet.Publish qosOnePublication(
            int packetId,
            String topic,
            Properties properties
    ) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {1, 2},
                Qos.AT_LEAST_ONCE,
                false,
                false,
                packetId,
                properties
        );
    }

    private static Properties identifierProperties(int... identifiers) {
        return new Properties(IntStream.of(identifiers)
                .<Property>mapToObj(identifier -> new Property.VariableByteValue(
                        PropertyId.SUBSCRIPTION_IDENTIFIER,
                        identifier
                ))
                .toList());
    }

    /// The Subscription Identifier that `packet`, a SUBSCRIBE, carries, or empty without one.
    private static Optional<Integer> identifierOf(Packet packet) {
        return ((Packet.Subscribe) packet).properties()
                .find(PropertyId.SUBSCRIPTION_IDENTIFIER)
                .map(property -> ((Property.VariableByteValue) property).value());
    }

    @ParameterizedTest(name = "session expiry {0} is sent as {1}")
    @CsvSource(nullValues = "none", value = {
            "PT0S, none",
            "PT1S, 1",
            "PT1.5S, 2",
            "PT4294967294.5S, 4294967295",
            "PT4294967295S, 4294967295"
    })
    void an_mqtt_5_connect_sends_the_session_expiry_in_whole_seconds_rounded_up(
            Duration expiry,
            @Nullable Long seconds
    ) {
        // Arrange: no local state yet, so Clean Start goes with every expiry.
        SessionCore mqtt5 = mqtt5Core(expiry);

        // Act
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);

        // Assert
        var connect = (Packet.Connect) port.sent.getLast();
        assertThat(connect.version()).isEqualTo(MqttVersion.V5);
        assertThat(connect.cleanStart()).isTrue();
        assertThat(connect.properties().find(PropertyId.SESSION_EXPIRY_INTERVAL)).isEqualTo(
                Optional.ofNullable(seconds).map(value -> new Property.FourByteValue(
                        PropertyId.SESSION_EXPIRY_INTERVAL,
                        value
                ))
        );
    }

    @Test
    void an_mqtt_5_connect_announces_its_receive_limits_and_topic_aliases_and_nothing_else() {
        // Arrange
        SessionCore mqtt5 = mqtt5Core(Duration.ZERO);

        // Act
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);

        // Assert: room for 4,096 receptions, less 16 acknowledgements that can await their write,
        // and no requests for problem or response information.
        var connect = (Packet.Connect) port.sent.getLast();
        assertThat(connect.properties().entries()).containsExactly(
                new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, 4080),
                new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 1024),
                new Property.TwoByteValue(PropertyId.TOPIC_ALIAS_MAXIMUM, 16)
        );
    }

    @Test
    void an_mqtt_5_session_starts_in_one_connection_and_later_ones_continue_it() {
        // Arrange: a persistent session, started by a connection without local state.
        SessionCore mqtt5 = mqtt5Core(Duration.ofHours(1));
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);
        mqtt5.received(1, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0, 0);
        mqtt5.failed(1, new IOException("connection lost"));

        // Act
        mqtt5.connect(attempt(2), 0);
        mqtt5.opened(2);
        mqtt5.received(2, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 0, 0);

        // Assert: Clean Start went only with the connection that had no local state.
        assertThat(port.sent).filteredOn(Packet.Connect.class::isInstance)
                .extracting(packet -> ((Packet.Connect) packet).cleanStart())
                .containsExactly(true, false);
        assertThat(port.results.get(2L)).isEqualTo(new Result(ConnAck.of(true, 2), null));
    }

    @Test
    void a_broker_beyond_the_announced_receive_maximum_ends_the_connection_with_0x93() {
        // Arrange: a subscription holds every delivery, so no PUBACK returns the broker's quota.
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        int announced = announcedReceiveMaximum();
        receiveQosOne(mqtt5, 1, 1, announced);
        assertThat(port.sent).noneMatch(Packet.Disconnect.class::isInstance);

        // Act
        mqtt5.received(1, qosOnePublication(announced + 1, "a/b", false), 0, 0);

        // Assert: the client found the violation itself, so it may connect again.
        assertThat(port.sent).last()
                .isEqualTo(new Packet.Disconnect(ReasonCode.RECEIVE_MAXIMUM_EXCEEDED, EMPTY));
        mqtt5.sent(1, port.ticket, 0);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.cause()).get().isInstanceOfSatisfying(
                    MqttProtocolException.class,
                    cause -> assertThat(cause.reasonCode())
                            .isEqualTo(ReasonCode.RECEIVE_MAXIMUM_EXCEEDED)
            );
            assertThat(ConnectionRequest.reconnectable(end.cause().orElseThrow())).isTrue();
        });
    }

    @Test
    void a_puback_that_the_send_queue_accepts_returns_its_place_in_the_receive_maximum() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        int announced = announcedReceiveMaximum();
        receiveQosOne(mqtt5, 1, 1, announced);

        // Act: the first delivery is done, and its PUBACK reaches the send queue and the wire.
        mqtt5.deliveryReady(port.deliveries.getFirst().token());
        mqtt5.written(1, port.ticket);
        mqtt5.received(1, qosOnePublication(announced + 1, "a/b", false), 0, 0);

        // Assert
        assertThat(pubAckIds()).containsExactly(1);
        assertThat(port.sent).noneMatch(Packet.Disconnect.class::isInstance);
        mqtt5.received(1, qosOnePublication(announced + 2, "a/b", false), 0, 0);
        assertThat(port.sent).last()
                .isEqualTo(new Packet.Disconnect(ReasonCode.RECEIVE_MAXIMUM_EXCEEDED, EMPTY));
    }

    @Test
    void publications_after_acknowledgements_that_await_their_write_still_fit_the_receive_table() {
        // Arrange: the broker uses its whole quota, and the send queue accepts 16 PUBACKs whose
        // write completions have not reached the core.
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        int announced = announcedReceiveMaximum();
        receiveQosOne(mqtt5, 1, 1, announced);
        for (int index = 0; index < 16; index++) {
            mqtt5.deliveryReady(port.deliveries.get(index).token());
        }

        // Act: the broker uses the quota that those PUBACKs returned.
        receiveQosOne(mqtt5, 1, announced + 1, 16);

        // Assert
        assertThat(pubAckIds()).hasSize(16);
        assertThat(mqtt5.state()).isEqualTo(CONNECTED);
        assertThat(port.ends).isEmpty();
    }

    @ParameterizedTest(name = "released before the QoS 1 publications: {0}")
    @ValueSource(booleans = {false, true})
    void a_qos_2_reception_holds_its_place_until_its_pubcomp_reaches_the_send_queue(
            boolean released
    ) {
        // Arrange: no subscription takes the QoS 2 publication, so its PUBREC goes at once.
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        int announced = announcedReceiveMaximum();
        mqtt5.received(1, new Packet.Publish(
                new Topic("other"),
                new byte[] {1},
                Qos.EXACTLY_ONCE,
                false,
                false,
                1,
                EMPTY
        ), 0, 0);
        mqtt5.written(1, port.ticket);
        if (released) {
            mqtt5.received(1, new Packet.PubRel(1, ReasonCode.SUCCESS, EMPTY), 0, 0);
            mqtt5.written(1, port.ticket);
        }

        // Act: as many QoS 1 publications as the client announced.
        receiveQosOne(mqtt5, 1, 2, announced);

        // Assert: until PUBCOMP, the QoS 2 reception still takes one place.
        assertThat(port.sent.stream().anyMatch(Packet.Disconnect.class::isInstance))
                .isEqualTo(!released);
    }

    @Test
    void a_continued_session_announces_the_room_left_and_counts_no_pubrel_that_it_awaits() {
        // Arrange: a QoS 2 reception awaits PUBREL when the connection ends.
        SessionCore mqtt5 = mqtt5Core(Duration.ofHours(1));
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);
        mqtt5.received(1, connAck(), 0, 0);
        mqtt5.subscribe(2, subscription("a/#"));
        mqtt5.received(1, new Packet.Publish(
                new Topic("other"),
                new byte[] {1},
                Qos.EXACTLY_ONCE,
                false,
                false,
                1,
                EMPTY
        ), 0, 0);
        mqtt5.written(1, port.ticket);
        mqtt5.failed(1, new IOException("connection lost"));
        mqtt5.connect(attempt(3), 0);
        mqtt5.opened(2);
        mqtt5.received(2, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 0, 0);
        int announced = announcedReceiveMaximum();

        // Act
        receiveQosOne(mqtt5, 2, 2, announced);

        // Assert: the receive table keeps the reception aside, and the broker's quota does not
        // count it.
        assertThat(announced).isEqualTo(4079);
        assertThat(port.sent).noneMatch(Packet.Disconnect.class::isInstance);
    }

    @Test
    void a_resent_publication_counts_toward_the_receive_maximum_of_the_new_connection() {
        // Arrange: a QoS 1 delivery is still being processed when the connection ends.
        SessionCore mqtt5 = mqtt5Core(Duration.ofHours(1));
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);
        mqtt5.received(1, connAck(), 0, 0);
        mqtt5.subscribe(2, subscription("a/#"));
        mqtt5.received(1, qosOnePublication(1, "a/b", false), 0, 0);
        mqtt5.failed(1, new IOException("connection lost"));
        mqtt5.connect(attempt(3), 0);
        mqtt5.opened(2);
        mqtt5.received(2, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 0, 0);
        int announced = announcedReceiveMaximum();
        mqtt5.received(2, qosOnePublication(1, "a/b", true), 0, 0);
        receiveQosOne(mqtt5, 2, 2, announced - 1);
        assertThat(port.sent).noneMatch(Packet.Disconnect.class::isInstance);

        // Act
        mqtt5.received(2, qosOnePublication(announced + 1, "a/b", false), 0, 0);

        // Assert
        assertThat(port.sent).last()
                .isEqualTo(new Packet.Disconnect(ReasonCode.RECEIVE_MAXIMUM_EXCEEDED, EMPTY));
    }

    @Test
    void an_auth_on_an_established_mqtt_5_connection_is_a_protocol_error() {
        // Arrange: the client announced no Authentication Method.
        SessionCore mqtt5 = connectedMqtt5();

        // Act
        mqtt5.received(1, new Packet.Auth(ReasonCode.CONTINUE_AUTHENTICATION, EMPTY), 0, 0);

        // Assert
        assertThat(port.sent).last()
                .isEqualTo(new Packet.Disconnect(ReasonCode.PROTOCOL_ERROR, EMPTY));
    }

    @Test
    void a_connack_with_an_authentication_method_fails_the_attempt_as_a_protocol_error() {
        // Arrange: the client announced no Authentication Method.
        SessionCore mqtt5 = mqtt5Core(Duration.ZERO);
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);

        // Act
        mqtt5.received(1, connAck(new Property.StringValue(
                PropertyId.AUTHENTICATION_METHOD,
                "SCRAM-SHA-256"
        )), 0, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class)
                .cause()
                .isInstanceOfSatisfying(
                        MqttProtocolException.class,
                        cause -> assertThat(cause.reasonCode()).isEqualTo(ReasonCode.PROTOCOL_ERROR)
                );
    }

    @Test
    void session_present_in_answer_to_mqtt_5_clean_start_breaks_the_protocol() {
        // Arrange
        SessionCore mqtt5 = mqtt5Core(Duration.ofHours(1));
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);

        // Act
        mqtt5.received(1, new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY), 0, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasCauseInstanceOf(MqttProtocolException.class);
    }

    @Test
    void an_mqtt_5_refusal_fails_the_attempt_with_its_reason_code() {
        // Arrange
        SessionCore mqtt5 = mqtt5Core(Duration.ZERO);
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);

        // Act
        mqtt5.received(1, new Packet.ConnAck(false, ReasonCode.NOT_AUTHORIZED, EMPTY), 0, 0);

        // Assert
        assertThat(requireNonNull(port.results.get(1L)).failure()).isInstanceOfSatisfying(
                MqttConnectException.class,
                failure -> assertThat(failure.reasonCode()).contains(ReasonCode.NOT_AUTHORIZED)
        );
    }

    @Test
    void an_mqtt_5_connack_reports_the_limits_that_the_broker_announced() {
        // Arrange
        SessionCore mqtt5 = mqtt5Core(Duration.ZERO);
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);

        // Act
        mqtt5.received(1, connAck(
                new Property.ByteValue(PropertyId.MAXIMUM_QOS, 1),
                new Property.ByteValue(PropertyId.RETAIN_AVAILABLE, 0),
                new Property.ByteValue(PropertyId.WILDCARD_SUBSCRIPTION_AVAILABLE, 0),
                new Property.ByteValue(PropertyId.SHARED_SUBSCRIPTION_AVAILABLE, 0),
                new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 2048),
                new Property.StringValue(PropertyId.REASON_STRING, "welcome")
        ), 0, 0);

        // Assert
        var expected = ConnAck.of(false, 1, Qos.AT_LEAST_ONCE, false, false, false, 2048);
        assertThat(port.results.get(1L)).isEqualTo(new Result(expected, null));
        assertThat(port.connections).containsExactly(new Connection(1, expected));
    }

    @Test
    void an_mqtt_5_connack_without_limits_reports_what_mqtt_5_assumes() {
        // Arrange
        SessionCore mqtt5 = mqtt5Core(Duration.ZERO);
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);

        // Act
        mqtt5.received(1, connAck(), 0, 0);

        // Assert
        assertThat(port.results.get(1L)).isEqualTo(new Result(ConnAck.of(false, 1), null));
    }

    @Test
    void a_maximum_packet_size_beyond_what_mqtt_carries_reports_the_largest_packet() {
        // Arrange
        SessionCore mqtt5 = mqtt5Core(Duration.ZERO);
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);

        // Act
        mqtt5.received(1, connAck(
                new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 0xFFFF_FFFFL)
        ), 0, 0);

        // Assert
        assertThat(port.connections.getLast().ack().maximumPacketSize()).isEqualTo(268_435_460);
    }

    @Test
    void server_keep_alive_replaces_the_keep_alive_of_its_connection_only() {
        // Arrange: a 10 second keep-alive, which the broker shortens to 3 seconds.
        SessionCore mqtt5 = mqtt5Core(Duration.ZERO);
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);
        mqtt5.received(1, connAck(
                new Property.TwoByteValue(PropertyId.SERVER_KEEP_ALIVE, 3)
        ), 0, 0);
        assertThat(port.timers).containsEntry(KEEP_ALIVE, Duration.ofSeconds(3));

        // Act: the interval passes without a write.
        mqtt5.timer(1, KEEP_ALIVE, seconds(3));

        // Assert: PINGREQ goes out and its answer is due within the same interval.
        assertThat(port.sent.getLast()).isInstanceOf(Packet.PingReq.class);
        assertThat(port.timers).containsEntry(PING, Duration.ofSeconds(3));

        // Act: the next connection's broker sets no keep-alive.
        mqtt5.failed(1, new IOException("connection lost"));
        mqtt5.connect(attempt(2), seconds(4));
        mqtt5.opened(2);
        mqtt5.received(2, connAck(), seconds(4), 0);

        // Assert
        assertThat(port.timers).containsEntry(KEEP_ALIVE, Duration.ofSeconds(10));
    }

    @Test
    void a_server_keep_alive_of_zero_turns_keep_alive_off_for_its_connection() {
        // Arrange
        SessionCore mqtt5 = mqtt5Core(Duration.ZERO);
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);

        // Act
        mqtt5.received(1, connAck(
                new Property.TwoByteValue(PropertyId.SERVER_KEEP_ALIVE, 0)
        ), 0, 0);

        // Assert
        assertThat(port.timers).doesNotContainKey(KEEP_ALIVE);
    }

    @Test
    void an_assigned_client_identifier_does_not_replace_the_configured_one() {
        // Arrange: the broker announces an identifier, as it would for an empty one.
        SessionCore mqtt5 = mqtt5Core(Duration.ZERO);
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);
        mqtt5.received(1, connAck(
                new Property.StringValue(PropertyId.ASSIGNED_CLIENT_IDENTIFIER, "assigned")
        ), 0, 0);
        mqtt5.failed(1, new IOException("connection lost"));

        // Act
        mqtt5.connect(attempt(2), 0);
        mqtt5.opened(2);

        // Assert
        assertThat(port.sent).filteredOn(Packet.Connect.class::isInstance)
                .extracting(packet -> ((Packet.Connect) packet).clientId())
                .containsExactly("client", "client");
    }

    @Test
    void an_mqtt_5_publication_reaches_the_handler_with_its_message_properties() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        var properties = new Properties(List.of(
                new Property.UserProperty("source", "sensor"),
                new Property.ByteValue(PropertyId.PAYLOAD_FORMAT_INDICATOR, 1),
                new Property.FourByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, 60),
                new Property.StringValue(PropertyId.CONTENT_TYPE, "text/plain"),
                new Property.StringValue(PropertyId.RESPONSE_TOPIC, "replies/a"),
                new Property.BinaryValue(PropertyId.CORRELATION_DATA, new byte[] {7}),
                new Property.UserProperty("source", "relay")
        ));

        // Act
        mqtt5.received(1, publication(properties), 0, 19);

        // Assert: the lifetime is what remained when the broker forwarded the message.
        MqttMessage expected = message()
                .withPayloadUtf8(true)
                .withMessageExpiry(Duration.ofSeconds(60))
                .withContentType("text/plain")
                .withResponseTopic(new Topic("replies/a"))
                .withCorrelationData(new byte[] {7})
                .withUserProperties(List.of(
                        new UserProperty("source", "sensor"),
                        new UserProperty("source", "relay")
                ));
        assertThat(port.deliveries).singleElement()
                .satisfies(delivery -> assertThat(delivery.message()).isEqualTo(expected));
    }

    @Test
    void a_payload_declared_as_utf_8_is_delivered_without_checking_it() {
        // Arrange: a lone lead byte of a two-byte UTF-8 sequence.
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.subscribe(2, subscription("a/#"));
        var publish = new Packet.Publish(
                new Topic("a/b"),
                new byte[] {(byte) 0xC3},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                new Properties(List.of(
                        new Property.ByteValue(PropertyId.PAYLOAD_FORMAT_INDICATOR, 1)
                ))
        );

        // Act
        mqtt5.received(1, publish, 0, 8);

        // Assert
        assertThat(port.deliveries).singleElement().satisfies(delivery -> {
            assertThat(delivery.message().payloadUtf8()).isTrue();
            assertThat(delivery.message().payload()).containsExactly(0xC3);
        });
        assertThat(mqtt5.state()).isEqualTo(CONNECTED);
    }

    @Test
    void a_requested_mqtt_5_disconnect_sends_normal_disconnection() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();

        // Act
        mqtt5.disconnect(2);

        // Assert: only this reason code makes the broker discard the Will.
        assertThat(port.sent).last().isEqualTo(new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY));
    }

    @Test
    void an_mqtt_5_broker_learns_of_a_violation_before_the_connection_closes() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();

        // Act: the broker sends a packet that only clients send.
        mqtt5.received(1, new Packet.PingReq(), 0, 2);

        // Assert: DISCONNECT carries the reason code, nothing follows it, and more violations in
        // the same input wait for its write.
        assertThat(port.sent).last()
                .isEqualTo(new Packet.Disconnect(ReasonCode.PROTOCOL_ERROR, EMPTY));
        mqtt5.publish(2, qosOneMessage());
        mqtt5.failed(1, new PacketDecodeException(ReasonCode.MALFORMED_PACKET, "more input"));
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttNotConnectedException.class);
        assertThat(port.publications).isEmpty();
        assertThat(port.ends).isEmpty();

        // Act
        mqtt5.sent(1, port.ticket, 0);

        // Assert: the connection ends with the violation, which the application did not request.
        assertThat(mqtt5.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> {
            assertThat(end.userRequested()).isFalse();
            assertThat(end.cause()).get().isInstanceOfSatisfying(
                    MqttProtocolException.class,
                    cause -> assertThat(cause.reasonCode()).isEqualTo(ReasonCode.PROTOCOL_ERROR)
            );
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("interruptedViolationReports")
    void a_violation_report_that_cannot_finish_ends_the_connection_with_the_violation(
            Consumer<SessionCore> interruption
    ) {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.received(1, new Packet.PingReq(), 0, 2);

        // Act
        interruption.accept(mqtt5);

        // Assert
        assertThat(mqtt5.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> assertThat(end.cause()).get()
                .isInstanceOfSatisfying(MqttProtocolException.class, cause ->
                        assertThat(cause.reasonCode()).isEqualTo(ReasonCode.PROTOCOL_ERROR)));
    }

    static Stream<Named<Consumer<SessionCore>>> interruptedViolationReports() {
        return Stream.of(
                Named.of(
                        "the write deadline",
                        core -> core.timer(1, SessionCore.Timer.DISCONNECT, 0)
                ),
                Named.of(
                        "a transport failure",
                        core -> core.failed(1, new IOException("reset"))
                )
        );
    }

    @Test
    void a_full_send_queue_closes_the_connection_at_once_with_the_violation() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        port.sendResult = SendQueue.OfferResult.FULL;

        // Act
        mqtt5.received(1, new Packet.PingReq(), 0, 2);

        // Assert
        assertThat(mqtt5.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end -> assertThat(end.cause()).get()
                .isInstanceOf(MqttProtocolException.class));
    }

    @Test
    void a_disconnect_while_a_violation_is_reported_fails_without_another_disconnect() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.received(1, new Packet.PingReq(), 0, 2);
        int sent = port.sent.size();

        // Act
        mqtt5.disconnect(2);
        mqtt5.sent(1, port.ticket, 0);

        // Assert: the written DISCONNECT lets the broker publish the Will.
        assertThat(port.sent).hasSize(sent);
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectionLostException.class);
        assertThat(port.ends).singleElement()
                .satisfies(end -> assertThat(end.userRequested()).isTrue());
    }

    @Test
    void a_connect_while_a_violation_is_reported_joins_the_standing_request() {
        // Arrange
        SessionCore mqtt5 = connectedMqtt5();
        mqtt5.received(1, new Packet.PingReq(), 0, 2);

        // Act
        mqtt5.connect(attempt(2), 0);

        // Assert: without reconnection, the joined call fails with the connection's failure.
        assertThat(port.results).doesNotContainKey(2L);
        mqtt5.sent(1, port.ticket, 0);
        assertThat(requireNonNull(port.results.get(2L)).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasCauseInstanceOf(MqttProtocolException.class);
    }

    private void connected() {
        sut.connect(attempt(1), 0);
        accept();
    }

    private static Packet.Publish publication(Properties properties) {
        return new Packet.Publish(
                new Topic("a/b"),
                new byte[] {1, 2},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                properties
        );
    }

    /// An MQTT 5.0 core like `sut`, with `sessionExpiry` and a packet limit of 1 KiB.
    private SessionCore mqtt5Core(Duration sessionExpiry) {
        return new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                MqttVersion.V5,
                sessionExpiry,
                1024,
                null,
                false,
                ReconnectBackoff.disabled(),
                false,
                Duration.ofSeconds(60)
        );
    }

    /// An MQTT 5.0 core like `sut` whose first connection is established.
    private SessionCore connectedMqtt5(Property... limits) {
        SessionCore mqtt5 = mqtt5Core(Duration.ZERO);
        mqtt5.connect(attempt(1), 0);
        mqtt5.opened(1);
        mqtt5.received(1, connAck(limits), 0, 0);
        return mqtt5;
    }

    /// A successful CONNACK without Session Present that carries `properties`.
    private static Packet.ConnAck connAck(Property... properties) {
        return new Packet.ConnAck(false, ReasonCode.SUCCESS, new Properties(List.of(properties)));
    }

    /// The CONNACK property with which the broker takes `publications` at once.
    private static Property receiveMaximum(int publications) {
        return new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, publications);
    }

    /// The CONNACK property with which the broker takes up to `aliases` Topic Aliases.
    private static Property topicAliasMaximum(int aliases) {
        return new Property.TwoByteValue(PropertyId.TOPIC_ALIAS_MAXIMUM, aliases);
    }

    /// The Receive Maximum that the last CONNECT announced.
    private int announcedReceiveMaximum() {
        Packet.Connect connect = port.sent.stream()
                .filter(Packet.Connect.class::isInstance)
                .map(Packet.Connect.class::cast)
                .reduce((earlier, later) -> later)
                .orElseThrow();
        return ((Property.TwoByteValue) connect.properties()
                .find(PropertyId.RECEIVE_MAXIMUM)
                .orElseThrow()).value();
    }

    /// Receives `count` QoS 1 publications on `a/b` over `connection`, from `firstId` on.
    private static void receiveQosOne(SessionCore core, long connection, int firstId, int count) {
        for (int id = firstId; id < firstId + count; id++) {
            core.received(connection, qosOnePublication(id, "a/b", false), 0, 0);
        }
    }

    private static ConnectionAttempt attempt(long operation) {
        return new ConnectionAttempt(operation);
    }

    /// The same configuration as `sut`, except that attempts evaluate a credentials supplier.
    private SessionCore credentialedCore() {
        return new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                MqttVersion.V3_1_1,
                Duration.ZERO,
                16 * 1024 * 1024,
                null,
                true,
                ReconnectBackoff.disabled(),
                false,
                Duration.ofSeconds(60)
        );
    }

    private void accept() {
        receive(1, new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
    }

    /// Production passes the frame size; these tests count only a PUBLISH payload.
    private void receive(long connection, Packet packet, long now) {
        sut.received(connection, packet, now,
                packet instanceof Packet.Publish publish ? publish.payload().length : 0);
    }

    private static long seconds(long value) {
        return Duration.ofSeconds(value).toNanos();
    }

    private record Result(@Nullable Object value, @Nullable Throwable failure) {
    }

    /// State one exchange's later steps read after its first step; all steps run on one thread.
    private static final class Exchange {

        int packetId;
        long ticket;
        long token;
    }

    private record Received(
            DeliveryToken delivery,
            List<Long> ids,
            MqttMessage message,
            boolean dup,
            int bytes
    ) {
        long token() {
            return delivery.acknowledgement();
        }
    }

    private record Removal(long id, Subscription handle, @Nullable Throwable cause) {
    }

    private record Connection(long generation, ConnAck ack) {
    }

    private record TransportOpening(long generation, Duration timeout) {
    }

    private static final class TestSubscription implements Subscription {

        final long id;
        @Nullable TopicFilter filter;
        Qos granted = Qos.AT_MOST_ONCE;

        TestSubscription(long id) {
            this.id = id;
        }

        @Override
        public TopicFilter filter() {
            return requireNonNull(filter);
        }

        @Override
        public Qos grantedQos() {
            return granted;
        }

        @Override
        public CompletableFuture<Qos> established() {
            return CompletableFuture.completedFuture(grantedQos());
        }

        @Override
        public CompletableFuture<Void> unsubscribe() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class RecordingPort implements SessionCore.Port {

        final List<Packet> sent = new ArrayList<>();
        final List<MqttMessage> publications = new ArrayList<>();
        // The Topic Alias of each publication offer, in the order of the offers.
        final List<TopicAlias> aliases = new ArrayList<>();
        final List<Integer> publicationIds = new ArrayList<>();
        final List<Disconnection> ends = new ArrayList<>();
        final List<Boolean> established = new ArrayList<>();
        final Map<Long, Result> results = new HashMap<>();
        // The first outcome each registration reports: its grant or the cause that ended it.
        final Map<Long, Result> outcomes = new HashMap<>();
        final Map<SessionCore.Timer, Duration> timers = new EnumMap<>(SessionCore.Timer.class);
        final List<Long> opened = new ArrayList<>();
        final List<ConnectionAttempt> attempts = new ArrayList<>();
        final List<ConnectionAttempt> evaluations = new ArrayList<>();
        final List<ConnectionAttempt> cancellations = new ArrayList<>();
        final List<TransportOpening> transportOpenings = new ArrayList<>();
        final List<Connection> connections = new ArrayList<>();
        final List<Long> registered = new ArrayList<>();
        final List<Long> removed = new ArrayList<>();
        final List<Removal> removals = new ArrayList<>();
        final List<Removal> resubscriptionFailures = new ArrayList<>();
        final List<Subscription> grants = new ArrayList<>();
        @Nullable TestSubscription registering;
        final List<Received> deliveries = new ArrayList<>();
        final List<Long> released = new ArrayList<>();
        final List<Long> forgotten = new ArrayList<>();
        final List<Long> ackTickets = new ArrayList<>();
        int ackSlots = Integer.MAX_VALUE;
        boolean acceptRegistration = true;
        boolean acceptDelivery = true;
        SendQueue.OfferResult sendResult = SendQueue.OfferResult.ACCEPTED;
        long ticket;
        int ignoredResponses;
        Runnable onConnected = () -> {
        };

        @Override
        public void beginAttempt(
                long generation,
                ConnectionAttempt attempt,
                long attemptNumber
        ) {
            opened.add(generation);
            attempts.add(attempt);
        }

        @Override
        public void continueAttempt(long generation) {
            throw new AssertionError("A clean session never replaces a broker session");
        }

        @Override
        public void connectionRequested(boolean requested) {
        }

        @Override
        public void evaluateCredentials(ConnectionAttempt attempt) {
            assertThat(timers).containsKey(CONNECT);
            evaluations.add(attempt);
        }

        @Override
        public void cancelCredentials(ConnectionAttempt attempt) {
            cancellations.add(attempt);
        }

        @Override
        public void openTransport(long generation, Duration timeout) {
            assertThat(timers).containsKey(CONNECT);
            transportOpenings.add(new TransportOpening(generation, timeout));
        }

        @Override
        public void admit(long generation) {
        }

        @Override
        public void connected(long generation, ConnAck ack) {
            connections.add(new Connection(generation, ack));
            onConnected.run();
        }

        @Override
        public void disconnecting() {
        }

        @Override
        public void stopDiscarded() {
        }

        @Override
        public void resubscribeFailed(
                Subscription handle,
                MqttSubscriptionRefusedException failure
        ) {
            var subscription = (TestSubscription) handle;
            removeSubscription(subscription.id, handle, failure);
            resubscriptionFailures.add(new Removal(subscription.id, handle, failure));
        }

        @Override
        public SendQueue.OfferResult send(
                long generation,
                long id,
                Packet packet,
                @Nullable SendReceipt receipt
        ) {
            ticket = id;
            if (packet instanceof Packet.Subscribe subscribe && registering != null) {
                registering.filter = subscribe.subscriptions().getFirst().filter();
                registering = null;
            }
            if (packet instanceof Packet.PubAck && ackSlots == 0) {
                return SendQueue.OfferResult.FULL;
            }
            if (sendResult == SendQueue.OfferResult.ACCEPTED) {
                if (packet instanceof Packet.PubAck) {
                    ackSlots--;
                    ackTickets.add(id);
                }
                sent.add(packet);
            }
            return sendResult;
        }

        @Override
        public SendQueue.OfferResult publish(
                long generation,
                long id,
                MqttMessage message,
                int packetId,
                @Nullable SendReceipt receipt,
                TopicAlias alias
        ) {
            ticket = id;
            publications.add(message);
            publicationIds.add(packetId);
            aliases.add(alias);
            return sendResult;
        }

        @Override
        public void schedule(long generation, SessionCore.Timer timer, Duration delay) {
            assertThat(opened).contains(generation);
            timers.put(timer, delay);
        }

        @Override
        public void cancel(SessionCore.Timer timer) {
            timers.remove(timer);
        }

        @Override
        public void ignoredPingResponse() {
            ignoredResponses++;
        }

        @Override
        public void complete(long operation, @Nullable Object value, @Nullable Throwable failure) {
            assertThat(operation).isPositive();
            assertThat(results.put(operation, new Result(value, failure))).isNull();
        }

        @Override
        public Optional<Subscription> registerSubscription(long id) {
            if (!acceptRegistration) {
                return Optional.empty();
            }
            registered.add(id);
            var handle = new TestSubscription(id);
            registering = handle;
            return Optional.of(handle);
        }

        @Override
        public void subscribed(Subscription handle, Qos granted) {
            var subscription = (TestSubscription) handle;
            subscription.granted = granted;
            grants.add(handle);
            outcomes.putIfAbsent(subscription.id, new Result(handle, null));
        }

        @Override
        public boolean deliver(
                DeliveryToken token,
                List<Long> ids,
                MqttMessage message,
                boolean dup,
                int bytes
        ) {
            if (acceptDelivery) {
                deliveries.add(new Received(token, List.copyOf(ids), message, dup, bytes));
            }

            return acceptDelivery;
        }

        @Override
        public void releaseDelivery(long token) {
            released.add(token);
        }

        @Override
        public void forgetDelivery(long token) {
            forgotten.add(token);
        }

        @Override
        public Map<Long, Boolean> receiveState() {
            return Map.of();
        }

        @Override
        public boolean readyDelivery(long token) {
            return false;
        }

        @Override
        public void sessionChanged(long session) {
        }

        @Override
        public void removeSubscription(long id, Subscription handle, @Nullable Throwable cause) {
            assertThat(((TestSubscription) handle).id).isEqualTo(id);
            removed.add(id);
            removals.add(new Removal(id, handle, cause));
            if (cause != null) {
                outcomes.putIfAbsent(id, new Result(null, cause));
            }
        }

        @Override
        public Map<Long, Boolean> end(
                long generation,
                boolean connected,
                Disconnection end,
                boolean preserveSession,
                boolean stopped
        ) {
            established.add(connected);
            ends.add(end);
            return Map.of();
        }
    }
}
