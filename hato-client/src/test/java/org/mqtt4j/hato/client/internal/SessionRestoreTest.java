package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.ClientStats;
import org.mqtt4j.hato.DisconnectionAccess;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.RecordingPort;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.DISCONNECTED;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class SessionRestoreTest {

    private static final Duration SESSION_EXPIRY = Duration.ofHours(1);

    private final RecordingPort port = new RecordingPort();
    private SessionCore sut = core(MqttVersion.V5, SESSION_EXPIRY);
    private long nextOperation;
    private long generation;

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_restored_session_continues_on_one_connection(MqttVersion version) {
        // Arrange
        sut = core(version, SESSION_EXPIRY);
        restore(new SessionState(List.of(), 1, List.of()));

        // Act
        connect(true);

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(sent(Packet.Connect.class))
                .singleElement()
                .satisfies(request -> assertThat(request.cleanStart()).isFalse());
        assertThat(sent(Packet.Disconnect.class)).isEmpty();
    }

    @Test
    void registrations_take_the_identifiers_under_which_the_broker_holds_their_filters() {
        // Arrange: the broker holds b/# under 7 and c/# under 3. a/# and b/# are registered
        // before the restore, and d/# after it.
        long unrecorded = subscribe("a/#");
        long recorded = subscribe("b/#");
        restore(new SessionState(
                List.of(registration("b/#", 7), registration("c/#", 3)),
                9,
                List.of()
        ));
        subscribe("d/#");

        // Act
        connect(true);
        receive(identified("b/1", 7));
        receive(identified("c/1", 3));
        receive(identified("a/1", 9));

        // Assert
        assertThat(sent(Packet.Subscribe.class))
                .extracting(
                        request -> request.subscriptions().getFirst().filter().pattern(),
                        SessionRestoreTest::identifier
                )
                .containsExactly(tuple("a/#", 9), tuple("b/#", 7), tuple("d/#", 10));
        assertThat(sent(Packet.Unsubscribe.class))
                .singleElement()
                .extracting(Packet.Unsubscribe::filters)
                .isEqualTo(List.of(new TopicFilter("c/#")));
        assertThat(port.deliveries).containsExactly(List.of(recorded), List.of(unrecorded));
    }

    @Test
    void a_registration_takes_over_a_filter_that_only_the_saved_session_holds() {
        // Arrange: the broker holds c/# under 3, and c/# is registered after the restore.
        restore(new SessionState(List.of(registration("c/#", 3)), 4, List.of()));
        long subscription = subscribe("c/#");

        // Act
        connect(true);
        receive(identified("c/1", 3));

        // Assert
        assertThat(sent(Packet.Unsubscribe.class)).isEmpty();
        assertThat(sent(Packet.Subscribe.class))
                .singleElement()
                .extracting(SessionRestoreTest::identifier)
                .isEqualTo(3);
        assertThat(port.deliveries).containsExactly(List.of(subscription));
    }

    @Test
    void a_filter_taken_over_after_its_unsubscribe_went_out_is_subscribed_after_it() {
        // Arrange: the continued session removes c/#, which no registration took over.
        restore(new SessionState(List.of(registration("c/#", 3)), 4, List.of()));
        connect(true);
        Packet.Unsubscribe removal = sent(Packet.Unsubscribe.class).getFirst();

        // Act
        long subscription = subscribe("c/#");
        receive(new Packet.UnsubAck(removal.packetId(), List.of(ReasonCode.SUCCESS), EMPTY));
        receive(identified("c/1", 3));

        // Assert
        Packet.Subscribe request = sent(Packet.Subscribe.class).getFirst();
        assertThat(identifier(request)).isEqualTo(3);
        assertThat(port.sent.indexOf(request)).isGreaterThan(port.sent.indexOf(removal));
        assertThat(port.removed).isEmpty();
        assertThat(port.deliveries).containsExactly(List.of(subscription));
    }

    @Test
    void a_broker_without_the_session_starts_a_new_one_without_the_saved_state() {
        // Arrange: the saved session holds c/# and an acknowledged QoS 1 reception, id 5.
        restore(new SessionState(
                List.of(registration("c/#", 3)),
                4,
                List.of(new SessionState.Reception(5, Qos.AT_LEAST_ONCE))
        ));
        long subscription = subscribe("t/#");

        // Act
        connect(false);
        receive(publication("t/1", Qos.AT_LEAST_ONCE, true, 5));

        // Assert
        assertThat(sent(Packet.Unsubscribe.class)).isEmpty();
        assertThat(port.deliveries).containsExactly(List.of(subscription));
    }

    @Test
    void a_resend_of_an_acknowledged_qos_1_reception_is_acknowledged_without_a_delivery() {
        // Arrange
        restore(new SessionState(
                List.of(),
                1,
                List.of(new SessionState.Reception(5, Qos.AT_LEAST_ONCE))
        ));
        subscribe("t/#");
        connect(true);

        // Act
        receive(publication("t/1", Qos.AT_LEAST_ONCE, true, 5));

        // Assert
        assertThat(port.deliveries).isEmpty();
        assertThat(port.acknowledgements)
                .containsExactly(new Packet.PubAck(5, ReasonCode.SUCCESS, EMPTY));
    }

    @Test
    void a_new_publication_under_the_identifier_of_a_saved_reception_is_delivered() {
        // Arrange
        restore(new SessionState(
                List.of(),
                1,
                List.of(new SessionState.Reception(5, Qos.AT_LEAST_ONCE))
        ));
        long subscription = subscribe("t/#");
        connect(true);

        // Act
        receive(publication("t/1", Qos.AT_LEAST_ONCE, false, 5));

        // Assert
        assertThat(port.deliveries).containsExactly(List.of(subscription));
        assertThat(port.acknowledgements).isEmpty();
    }

    @Test
    void a_saved_qos_2_reception_answers_its_resend_and_its_release_without_a_delivery() {
        // Arrange
        restore(new SessionState(
                List.of(),
                1,
                List.of(new SessionState.Reception(6, Qos.EXACTLY_ONCE))
        ));
        subscribe("t/#");
        connect(true);

        // Act
        receive(publication("t/1", Qos.EXACTLY_ONCE, true, 6));
        receive(new Packet.PubRel(6, ReasonCode.SUCCESS, EMPTY));

        // Assert
        assertThat(port.deliveries).isEmpty();
        assertThat(port.sent)
                .filteredOn(packet -> packet instanceof Packet.PubRec
                        || packet instanceof Packet.PubComp)
                .containsExactly(
                        new Packet.PubRec(6, ReasonCode.SUCCESS, EMPTY),
                        new Packet.PubComp(6, ReasonCode.SUCCESS, EMPTY)
                );
    }

    @Test
    void the_saved_state_holds_the_filters_and_the_acknowledged_receptions() {
        // Arrange: a/# is granted. The application acknowledges a QoS 1 and a QoS 2 reception
        // whose acknowledgements stay unwritten, and holds another QoS 1 reception.
        subscribe("a/#");
        connect(false);
        Packet.Subscribe request = sent(Packet.Subscribe.class).getFirst();
        receive(new Packet.SubAck(request.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
        port.acknowledgementAllowance = 0;
        receive(publication("a/1", Qos.AT_LEAST_ONCE, false, 1));
        receive(publication("a/2", Qos.AT_LEAST_ONCE, false, 2));
        receive(publication("a/3", Qos.EXACTLY_ONCE, false, 3));
        long acknowledged = port.deliveryTokens.get(0).acknowledgement();
        long held = port.deliveryTokens.get(1).acknowledgement();
        long released = port.deliveryTokens.get(2).acknowledgement();
        port.handed.put(acknowledged, true);
        port.handed.put(held, false);
        port.handed.put(released, true);
        sut.deliveriesReady(new long[] {acknowledged, released});

        // Act
        disconnect();

        // Assert
        assertThat(sut.sessionState()).hasValueSatisfying(saved -> {
            assertThat(saved.registrations()).containsExactly(registration("a/#", 1));
            assertThat(saved.nextIdentifier()).isEqualTo(2);
            assertThat(saved.receptions()).containsExactlyInAnyOrder(
                    new SessionState.Reception(1, Qos.AT_LEAST_ONCE),
                    new SessionState.Reception(3, Qos.EXACTLY_ONCE)
            );
        });
    }

    @Test
    void only_a_session_that_started_and_disconnected_is_handed_over() {
        // Act & Assert: none before the first connection, none while connected, one afterwards.
        assertThat(sut.sessionState()).isEmpty();
        connect(false);
        assertThat(sut.sessionState()).isEmpty();
        disconnect();
        assertThat(sut.sessionState()).isPresent();
    }

    @Test
    void a_nonpersistent_session_is_not_handed_over() {
        // Arrange
        sut = core(MqttVersion.V5, Duration.ZERO);
        connect(false);

        // Act
        disconnect();

        // Assert
        assertThat(sut.sessionState()).isEmpty();
    }

    @Test
    void an_unfinished_publication_keeps_the_session_from_being_handed_over() {
        // Arrange
        connect(false);
        sut.publish(
                operation(),
                MqttMessage.of(new Topic("t"), new byte[] {1}, Qos.AT_LEAST_ONCE, false)
        );

        // Act
        disconnect();

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(sut.sessionState()).isEmpty();
    }

    @Test
    void a_session_that_another_client_took_over_is_not_handed_over() {
        // Arrange
        connect(false);

        // Act
        receive(new Packet.Disconnect(ReasonCode.SESSION_TAKEN_OVER, EMPTY));

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(sut.sessionState()).isEmpty();
    }

    @Test
    void a_client_with_a_session_of_its_own_cannot_take_over_a_saved_one() {
        // Arrange
        connect(false);
        disconnect();

        // Act & Assert
        assertThatThrownBy(() -> sut.restore(new SessionState(List.of(), 1, List.of()), 0))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void a_qos_1_reception_whose_puback_went_to_the_transport_is_left_out() {
        // Arrange: the application acknowledged a QoS 1 reception, and its PUBACK went to the
        // transport, which may still write it to the broker after the connection breaks.
        subscribe("a/#");
        connect(false);
        Packet.Subscribe request = sent(Packet.Subscribe.class).getFirst();
        receive(new Packet.SubAck(request.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
        receive(publication("a/1", Qos.AT_LEAST_ONCE, false, 1));
        long acknowledged = port.deliveryTokens.getFirst().acknowledgement();
        port.handed.put(acknowledged, true);
        sut.deliveriesReady(new long[] {acknowledged});

        // Act
        sut.failed(generation, new IOException("connection lost"));

        // Assert
        assertThat(port.acknowledgements)
                .containsExactly(new Packet.PubAck(1, ReasonCode.SUCCESS, EMPTY));
        assertThat(sut.sessionState())
                .hasValueSatisfying(saved -> assertThat(saved.receptions()).isEmpty());
    }

    @Test
    void a_saved_session_beyond_the_subscription_limit_is_refused_and_changes_nothing() {
        // Arrange: one subscription is allowed and registered, and the saved session holds
        // another.
        sut = core(MqttVersion.V5, SESSION_EXPIRY, new ClientLimits(64, 1));
        subscribe("a/#");
        var saved = new SessionState(List.of(registration("c/#", 3)), 4, List.of());

        // Act & Assert
        assertThatThrownBy(() -> sut.restore(saved, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(sut.values(0).subscribing().registrations())
                .isEqualTo(new ClientStats.Usage(1, 1));
        connect(false);
        assertThat(sent(Packet.Connect.class))
                .singleElement()
                .satisfies(request -> assertThat(request.cleanStart()).isTrue());
    }

    @Test
    void a_saved_session_beyond_the_receive_table_is_refused_and_changes_nothing() {
        // Arrange: the saved session holds a filter and one reception more than the receive
        // table takes.
        subscribe("a/#");
        List<SessionState.Reception> receptions = IntStream.rangeClosed(1, 4_097)
                .mapToObj(packetId -> new SessionState.Reception(packetId, Qos.AT_LEAST_ONCE))
                .toList();
        var saved = new SessionState(List.of(registration("c/#", 3)), 4, receptions);

        // Act & Assert
        assertThatThrownBy(() -> sut.restore(saved, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(sut.values(0).subscribing().registrations().used()).isEqualTo(1);
        connect(false);
        assertThat(sent(Packet.Connect.class))
                .singleElement()
                .satisfies(request -> assertThat(request.cleanStart()).isTrue());
    }

    @Test
    void a_restored_qos_2_reception_awaits_release_from_the_time_of_the_restore() {
        // Arrange
        long restoredAt = Duration.ofDays(10).toNanos();
        var saved = new SessionState(
                List.of(),
                1,
                List.of(new SessionState.Reception(6, Qos.EXACTLY_ONCE))
        );

        // Act
        sut.restore(saved, restoredAt);

        // Assert
        long now = restoredAt + Duration.ofSeconds(1).toNanos();
        assertThat(sut.values(now).receiving().oldestAwaitingRelease())
                .isEqualTo(Duration.ofSeconds(1));
    }

    private SessionCore core(MqttVersion version, Duration sessionExpiry) {
        return core(version, sessionExpiry, ClientLimits.DEFAULT);
    }

    private SessionCore core(MqttVersion version, Duration sessionExpiry, ClientLimits limits) {
        return new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ZERO,
                version,
                sessionExpiry,
                16 * 1024 * 1024,
                null,
                false,
                ReconnectBackoff.disabled(),
                RetryRules.DEFAULT,
                Duration.ofSeconds(60),
                DisconnectionAccess::of,
                limits
        );
    }

    /// Restores `saved` at the start of the client's clock.
    private void restore(SessionState saved) {
        sut.restore(saved, 0);
    }

    private long operation() {
        return ++nextOperation;
    }

    private void connect(boolean sessionPresent) {
        sut.connect(new ConnectionAttempt(operation()), 0);
        generation = port.generation;
        sut.opened(generation);
        receive(new Packet.ConnAck(sessionPresent, ReasonCode.SUCCESS, EMPTY));
    }

    /// Requests a disconnection and completes the write of its DISCONNECT.
    private void disconnect() {
        sut.disconnect(operation());
        sut.sent(generation, port.ticket, 0);
    }

    private void receive(Packet packet) {
        sut.received(
                generation,
                packet,
                0,
                packet instanceof Packet.Publish publish ? publish.payload().length : 0
        );
    }

    private long subscribe(String filter) {
        long operation = operation();
        sut.subscribe(operation, Subscribe.of(new TopicFilter(filter), Qos.AT_LEAST_ONCE));
        return operation;
    }

    private <T extends Packet> List<T> sent(Class<T> type) {
        return port.sent.stream()
                .filter(type::isInstance)
                .map(type::cast)
                .toList();
    }

    private static SessionState.Registration registration(String filter, int identifier) {
        return new SessionState.Registration(new TopicFilter(filter), identifier, false);
    }

    /// A QoS 0 publication to `topic` that carries the Subscription Identifier `identifier`.
    private static Packet.Publish identified(String topic, int identifier) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {1},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                new Properties(List.of(new Property.VariableByteValue(
                        PropertyId.SUBSCRIPTION_IDENTIFIER,
                        identifier
                )))
        );
    }

    /// A publication to `topic` without a Subscription Identifier.
    private static Packet.Publish publication(String topic, Qos qos, boolean dup, int packetId) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {1},
                qos,
                false,
                dup,
                packetId,
                EMPTY
        );
    }

    private static int identifier(Packet.Subscribe request) {
        for (Property property : request.properties().entries()) {
            if (property instanceof Property.VariableByteValue(var id, var value)
                    && id == PropertyId.SUBSCRIPTION_IDENTIFIER) {
                return value;
            }
        }
        throw new AssertionError("No Subscription Identifier in " + request);
    }
}
