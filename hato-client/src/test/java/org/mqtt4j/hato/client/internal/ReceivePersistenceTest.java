package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttSubscriptionRefusedException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.DISCONNECTED;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class ReceivePersistenceTest {

    private final RecordingPort port = new RecordingPort();
    private SessionCore sut = core(MqttVersion.V3_1_1);
    private long nextOperation;
    private long generation;

    @Test
    void an_mqtt_5_reconnection_announces_only_the_room_that_carried_over_receptions_leave() {
        // Arrange: an MQTT 5.0 session whose handlers still hold two receptions when its
        // connection ends.
        sut = core(MqttVersion.V5);
        connectedWithSubscription();
        receive(7, false);
        receive(8, false);
        sut.failed(generation, new IOException("lost"));

        // Act
        connect(true);

        // Assert
        List<Packet.Connect> connects = port.sent.stream()
                .map(Sent::packet)
                .filter(Packet.Connect.class::isInstance)
                .map(Packet.Connect.class::cast)
                .toList();
        assertThat(connects).extracting(connect -> connect.properties()
                        .find(PropertyId.RECEIVE_MAXIMUM)
                        .orElseThrow())
                .containsExactly(
                        new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, 4096),
                        new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, 4094)
                );
    }

    @Test
    void offline_readiness_waits_for_a_retransmission_on_the_new_connection() {
        // Arrange
        connectedWithSubscription();
        receive(7, false);
        DeliveryToken original = port.deliveries.getLast();
        sut.failed(generation, new IOException("lost"));

        // Act
        ready(original);
        connect(true);

        // Assert
        assertThat(acknowledgedIds()).isEmpty();

        // Act
        receive(7, true);

        // Assert
        assertThat(acknowledgedIds()).containsExactly(7);
        assertThat(port.deliveries).extracting(DeliveryToken::acknowledgement)
                .containsOnly(original.acknowledgement());
        assertThat(port.released).containsExactly(original.acknowledgement());
        Sent acknowledgement = lastAcknowledgement();
        requireNonNull(acknowledgement.receipt()).markAccepted();
        sut.written(generation, acknowledgement.ticket());
        assertThat(port.released).containsExactly(original.acknowledgement());
    }

    @Test
    void retained_acknowledgements_follow_the_new_connections_receive_order() {
        // Arrange
        connectedWithSubscription();
        receive(7, false);
        receive(8, false);
        receive(9, false);
        List<DeliveryToken> originals = List.copyOf(port.deliveries);
        sut.failed(generation, new IOException("lost"));
        originals.forEach(this::ready);
        connect(true);

        // Act
        receive(9, true);
        receive(7, true);
        receive(8, true);
        receive(9, true);
        receive(7, true);

        // Assert
        assertThat(acknowledgedIds()).containsExactly(9, 7, 8);
        assertThat(port.deliveries.stream().map(DeliveryToken::acknowledgement).distinct())
                .containsExactlyElementsOf(
                        originals.stream().map(DeliveryToken::acknowledgement).toList()
                );
    }

    @ParameterizedTest
    @EnumSource(value = SendQueue.OfferResult.class, names = {"FULL", "ACCEPTED"})
    void an_unaccepted_puback_is_retained_until_the_broker_retransmits(
            SendQueue.OfferResult admission
    ) {
        // Arrange: queue admission does not imply that Transport accepted the PUBACK.
        connectedWithSubscription();
        receive(7, false);
        DeliveryToken original = port.deliveries.getLast();
        port.ackAdmission = admission;
        ready(original);
        sut.failed(generation, new IOException("PUBACK never reached Transport"));
        port.ackAdmission = SendQueue.OfferResult.ACCEPTED;
        port.sent.clear();

        // Act
        connect(true);

        // Assert
        assertThat(acknowledgedIds()).isEmpty();

        // Act
        receive(7, true);

        // Assert
        assertThat(acknowledgedIds()).containsExactly(7);
        assertThat(port.deliveries).extracting(DeliveryToken::acknowledgement)
                .containsOnly(original.acknowledgement());
    }

    @ParameterizedTest(name = "reused identifier has DUP={0}")
    @ValueSource(booleans = {false, true})
    void an_accepted_puback_does_not_hide_a_new_publication_even_without_written(boolean dup) {
        // Arrange
        connectedWithSubscription();
        receive(7, false);
        DeliveryToken original = port.deliveries.getLast();
        ready(original);
        Sent acknowledgement = lastAcknowledgement();
        requireNonNull(acknowledgement.receipt()).markAccepted();
        sut.failed(generation, new IOException("Transport accepted without reporting the write"));
        port.sent.clear();
        connect(true);

        // Act
        receive(7, dup);
        ready(original);
        sut.written(acknowledgement.generation(), acknowledgement.ticket());

        // Assert
        DeliveryToken current = port.deliveries.getLast();
        assertThat(current.acknowledgement()).isNotEqualTo(original.acknowledgement());
        assertThat(acknowledgedIds()).isEmpty();

        // Act
        ready(current);

        // Assert
        assertThat(acknowledgedIds()).containsExactly(7);
    }

    @Test
    void a_transport_acceptance_after_connection_end_is_seen_before_resuming() {
        // Arrange: inline onClosed can reach the actor before Transport.send returns ACCEPTED.
        connectedWithSubscription();
        receive(7, false);
        DeliveryToken original = port.deliveries.getLast();
        ready(original);
        Sent acknowledgement = lastAcknowledgement();
        sut.failed(generation, new IOException("inline close"));
        port.sent.clear();
        requireNonNull(acknowledgement.receipt()).markAccepted();

        // Act
        connect(true);
        receive(7, true);

        // Assert
        assertThat(port.deliveries.getLast().acknowledgement())
                .isNotEqualTo(original.acknowledgement());
        assertThat(acknowledgedIds()).isEmpty();
        ready(port.deliveries.getLast());
        assertThat(acknowledgedIds()).containsExactly(7);
    }

    @Test
    void reuse_without_dup_supersedes_a_retained_reception() {
        // Arrange
        connectedWithSubscription();
        receive(7, false);
        DeliveryToken original = port.deliveries.getLast();
        sut.failed(generation, new IOException("lost"));
        connect(true);

        // Act
        receive(7, false);
        ready(original);

        // Assert
        DeliveryToken current = port.deliveries.getLast();
        assertThat(current.acknowledgement()).isNotEqualTo(original.acknowledgement());
        assertThat(acknowledgedIds()).isEmpty();

        // Act
        ready(current);

        // Assert
        assertThat(acknowledgedIds()).containsExactly(7);
    }

    @Test
    void session_loss_retires_old_readiness_before_an_identifier_is_reused() {
        // Arrange
        connectedWithSubscription();
        receive(7, false);
        DeliveryToken original = port.deliveries.getLast();
        sut.failed(generation, new IOException("lost"));
        connect(false);
        receive(7, true);
        DeliveryToken current = port.deliveries.getLast();

        // Act
        ready(original);

        // Assert
        assertThat(current.session()).isNotEqualTo(original.session());
        assertThat(current.acknowledgement()).isNotEqualTo(original.acknowledgement());
        assertThat(acknowledgedIds()).isEmpty();

        // Act
        ready(current);

        // Assert
        assertThat(acknowledgedIds()).containsExactly(7);
    }

    @Test
    void a_reception_not_handed_to_any_recipient_is_not_retained() {
        // Arrange
        connectedWithSubscription();
        receive(7, false);
        DeliveryToken original = port.deliveries.getLast();
        port.handed.remove(original.acknowledgement());
        sut.failed(generation, new IOException("ended before handoff"));
        connect(true);

        // Act
        receive(7, true);

        // Assert
        assertThat(port.deliveries.getLast().acknowledgement())
                .isNotEqualTo(original.acknowledgement());
        assertThat(acknowledgedIds()).isEmpty();
    }

    @Test
    void all_four_thousand_and_ninety_six_receive_slots_survive_reconnect_until_session_loss() {
        // Arrange
        connectedWithSubscription();
        for (int id = 1; id <= 4096; id++) {
            receive(id, false);
        }
        sut.failed(generation, new IOException("lost"));
        connect(true);

        // Act
        receive(4097, false);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends.getLast().cause().orElseThrow())
                .isInstanceOf(MqttCapacityException.class);
        assertThat(port.deliveries).hasSize(4096);

        // Act
        connect(false);
        receive(1, true);
        ready(port.deliveries.getLast());

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(acknowledgedIds()).containsExactly(1);
    }

    @Test
    void an_unaccepted_superseded_puback_releases_its_slot_when_the_connection_ends() {
        // Arrange: the old PUBACK counts until the ended connection discards its pending write.
        connectedWithSubscription();
        receive(7, false);
        DeliveryToken original = port.deliveries.getLast();
        ready(original);
        receive(7, false);
        for (int id = 8; id <= 4101; id++) {
            receive(id, false);
        }
        assertThat(sut.state()).isEqualTo(CONNECTED);
        sut.failed(generation, new IOException("superseded PUBACK never reached Transport"));

        // Act
        connect(true);
        receive(4102, false);

        // Assert: only the superseded receipt was removed from the full receive table.
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.handed).doesNotContainKey(original.acknowledgement());
        assertThat(port.deliveries).hasSize(4097);
        receive(4103, false);
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends.getLast().cause().orElseThrow())
                .isInstanceOf(MqttCapacityException.class);
    }

    @ParameterizedTest(name = "session present={0}, receipt QoS={1}")
    @CsvSource({
            "true, AT_MOST_ONCE, DISCONNECTED",
            "true, AT_LEAST_ONCE, DISCONNECTED",
            "false, AT_MOST_ONCE, CONNECTED",
            "false, AT_LEAST_ONCE, CONNECTED"
    })
    void old_handler_failures_follow_the_session_even_without_a_receive_table_entry(
            boolean present,
            Qos qos,
            SessionCore.State expectedState
    ) {
        // Arrange: QoS 0 and a written PUBACK leave no entry with which to match the failure.
        connectedWithSubscription();
        receive(7, qos, false);
        DeliveryToken original = port.deliveries.getLast();
        if (qos == Qos.AT_LEAST_ONCE) {
            ready(original);
            Sent acknowledgement = lastAcknowledgement();
            requireNonNull(acknowledgement.receipt()).markAccepted();
            sut.written(generation, acknowledgement.ticket());
        }
        sut.failed(generation, new IOException("lost"));
        connect(present);
        var failure = new IOException("old handler failed");

        // Act
        sut.deliveryFailed(original, failure);

        // Assert
        assertThat(sut.state()).isEqualTo(expectedState);
        assertThat(port.ends.stream().anyMatch(end -> end.cause().orElse(null) == failure))
                .isEqualTo(present);
    }

    @ParameterizedTest(name = "handler belongs to an earlier connection: {0}")
    @ValueSource(booleans = {false, true})
    void a_handler_failure_during_disconnect_preserves_the_requested_flag(boolean reconnect) {
        // Arrange
        connectedWithSubscription();
        receive(7, Qos.AT_MOST_ONCE, false);
        DeliveryToken original = port.deliveries.getLast();
        if (reconnect) {
            sut.failed(generation, new IOException("lost before the handler finished"));
            connect(true);
        }
        long disconnect = ++nextOperation;
        sut.disconnect(disconnect);
        long ticket = port.sent.getLast().ticket();
        var failure = new IOException("handler failed while DISCONNECT was pending");

        // Act
        sut.deliveryFailed(original, failure);

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends.getLast().cause()).containsSame(failure);
        assertThat(port.ends.getLast().userRequested()).isTrue();
        Result result = requireNonNull(port.results.get(disconnect));
        assertThat(result.failure()).isInstanceOf(MqttConnectionLostException.class)
                .hasCause(failure);
        sut.sent(generation, ticket, 0);
        assertThat(port.results.get(disconnect)).isSameAs(result);
    }

    @Test
    void a_failure_while_offline_does_not_revive_readiness_from_an_older_notification() {
        // Arrange: one recipient survives; another fails after its earlier acknowledgement.
        connectedWithSubscription();
        subscribe("events/+");
        receive(7, false);
        DeliveryToken original = port.deliveries.getLast();
        port.ackAdmission = SendQueue.OfferResult.FULL;
        ready(original);
        sut.failed(generation, new IOException("lost"));
        port.handed.put(original.acknowledgement(), false);
        sut.deliveryFailed(original, new IOException("handler failed while offline"));
        port.ackAdmission = SendQueue.OfferResult.ACCEPTED;

        // Act
        sut.deliveryReady(original.acknowledgement());
        connect(true);
        receive(7, true);

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.ends).hasSize(1);
        assertThat(acknowledgedIds()).isEmpty();

        // Act
        ready(port.deliveries.getLast());

        // Assert
        assertThat(acknowledgedIds()).containsExactly(7);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void reordered_completion_and_reconnection_preserve_receive_identity_and_order(int seed) {
        // Arrange
        var random = new SplittableRandom(seed);
        connectedWithSubscription();
        for (int id = 1; id <= 3; id++) {
            receive(id, false);
        }
        List<DeliveryToken> originals = List.copyOf(port.deliveries);
        originals.forEach(this::ready);
        List<Sent> originalAcks = port.sent.stream()
                .filter(send -> send.packet() instanceof Packet.PubAck)
                .toList();
        long oldGeneration = generation;
        var accepted = new ArrayList<Boolean>();
        var endings = new ArrayList<Runnable>();
        endings.add(() -> sut.failed(oldGeneration, new IOException("seed " + seed)));
        for (int i = 0; i < originals.size(); i++) {
            DeliveryToken token = originals.get(i);
            Sent acknowledgement = originalAcks.get(i);
            boolean handed = random.nextBoolean();
            accepted.add(handed);
            endings.add(() -> ready(token));
            if (handed) {
                endings.add(() -> requireNonNull(acknowledgement.receipt()).markAccepted());
                if (random.nextBoolean()) {
                    endings.add(() -> sut.written(oldGeneration, acknowledgement.ticket()));
                }
            }
        }
        Collections.shuffle(endings, random);

        // Act
        endings.forEach(Runnable::run);
        boolean present = random.nextBoolean();
        port.sent.clear();
        connect(present);
        var receiveOrder = new ArrayList<>(List.of(1, 2, 3));
        Collections.shuffle(receiveOrder, random);
        var expected = new ArrayList<Integer>();
        for (int id : receiveOrder) {
            boolean dup = random.nextBoolean();
            boolean retained = present && dup && !accepted.get(id - 1);
            receive(id, dup);
            DeliveryToken current = port.deliveries.getLast();
            DeliveryToken original = originals.get(id - 1);
            if (retained) {
                expected.add(id);
            }
            ready(original);
            sut.written(oldGeneration, originalAcks.get(id - 1).ticket());

            // Assert: stale conditions and writes cannot acknowledge a new identifier occupant.
            assertThat(current.acknowledgement() == original.acknowledgement())
                    .isEqualTo(retained);
            assertThat(acknowledgedIds()).isEqualTo(expected);
            if (!retained) {
                expected.add(id);
            }
            ready(current);
            receive(id, true);
            assertThat(acknowledgedIds()).isEqualTo(expected);
        }
        assertThat(acknowledgedIds()).isEqualTo(receiveOrder);
        assertThat(sut.state()).isEqualTo(CONNECTED);

        // Arrange: retire the acknowledgements before old handlers finish in another order.
        for (Sent acknowledgement : port.sent.stream()
                .filter(send -> send.packet() instanceof Packet.PubAck).toList()) {
            requireNonNull(acknowledgement.receipt()).markAccepted();
            sut.written(generation, acknowledgement.ticket());
        }
        assertThat(port.handed).isEmpty();
        var handlerFailure = new IOException("late handler failure for seed " + seed);
        var expectedEnds = new ArrayList<>(port.ends);
        if (present) {
            expectedEnds.add(Disconnection.of(handlerFailure, false, port.attempt));
        }
        var completions = new ArrayList<Runnable>();
        for (int i = 0; i < originals.size(); i++) {
            DeliveryToken token = originals.get(i);
            Sent acknowledgement = originalAcks.get(i);
            completions.add(() -> ready(token));
            completions.add(() -> sut.written(oldGeneration, acknowledgement.ticket()));
        }
        DeliveryToken failed = originals.get(random.nextInt(originals.size()));
        completions.add(() -> sut.deliveryFailed(failed, handlerFailure));
        Collections.shuffle(completions, random);

        // Act
        completions.forEach(Runnable::run);

        // Assert: acknowledged work still belongs to the session that handed it to the handler.
        assertThat(sut.state()).isEqualTo(present ? DISCONNECTED : CONNECTED);
        assertThat(port.ends).containsExactlyElementsOf(expectedEnds);
        assertThat(acknowledgedIds()).isEqualTo(receiveOrder);
    }

    private SessionCore core(MqttVersion version) {
        return new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ZERO,
                version,
                Duration.ofHours(1),
                16 * 1024 * 1024,
                null,
                false,
                ReconnectBackoff.disabled(),
                false,
                Duration.ofSeconds(60)
        );
    }

    private void connectedWithSubscription() {
        connect(false);
        subscribe("events/#");
    }

    private void connect(boolean present) {
        long operation = ++nextOperation;
        sut.connect(new ConnectionAttempt(operation), 0);
        generation = port.generation;
        sut.opened(generation);
        sut.received(generation, new Packet.ConnAck(present, ReasonCode.SUCCESS, EMPTY), 0, 0);
        assertThat(port.results.get(operation))
                .isEqualTo(new Result(ConnAck.of(present, port.attempt), null));
    }

    private void subscribe(String filter) {
        long operation = ++nextOperation;
        sut.subscribe(operation, Subscribe.of(new TopicFilter(filter), Qos.AT_LEAST_ONCE));
        var request = (Packet.Subscribe) port.sent.getLast().packet();
        sut.received(
                generation,
                new Packet.SubAck(request.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY),
                0,
                0
        );
    }

    private void receive(int id, boolean dup) {
        receive(id, Qos.AT_LEAST_ONCE, dup);
    }

    private void receive(int id, Qos qos, boolean dup) {
        sut.received(
                generation,
                new Packet.Publish(
                        new Topic("events/value"),
                        new byte[] {1},
                        qos,
                        false,
                        dup,
                        qos == Qos.AT_MOST_ONCE ? 0 : id,
                        EMPTY
                ),
                0,
                1
        );
    }

    private void ready(DeliveryToken token) {
        port.handed.computeIfPresent(token.acknowledgement(), (_, _) -> true);
        sut.deliveryReady(token.acknowledgement());
    }

    private List<Integer> acknowledgedIds() {
        return port.sent.stream()
                .map(Sent::packet)
                .filter(Packet.PubAck.class::isInstance)
                .map(Packet.PubAck.class::cast)
                .map(Packet.PubAck::packetId)
                .toList();
    }

    private Sent lastAcknowledgement() {
        return port.sent.stream()
                .filter(send -> send.packet() instanceof Packet.PubAck)
                .toList().getLast();
    }

    private static IntStream seeds() {
        return IntStream.range(0, 100);
    }

    record Sent(
            long generation,
            long ticket,
            Packet packet,
            @Nullable SendReceipt receipt
    ) {
    }

    record Result(@Nullable Object value, @Nullable Throwable failure) {
    }

    private static final class TestSubscription implements Subscription {

        @Override
        public TopicFilter filter() {
            return new TopicFilter("events/#");
        }

        @Override
        public Qos grantedQos() {
            return Qos.AT_LEAST_ONCE;
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

    static final class RecordingPort implements SessionCore.Port {

        final List<Sent> sent = new ArrayList<>();
        final List<DeliveryToken> deliveries = new ArrayList<>();
        final List<Disconnection> ends = new ArrayList<>();
        final List<Long> released = new ArrayList<>();
        final Map<Long, Boolean> handed = new HashMap<>();
        final Map<Long, Result> results = new HashMap<>();
        SendQueue.OfferResult ackAdmission = SendQueue.OfferResult.ACCEPTED;
        // Runs whenever the core reads the readiness that end() returned. The I/O sender can
        // accept a write while the core reconciles the ended connection.
        Runnable duringEndReconcile = () -> {
        };
        long session;
        long generation;
        long attempt;

        @Override
        public void beginAttempt(
                long generation,
                ConnectionAttempt attempt,
                long attemptNumber
        ) {
            this.generation = generation;
            this.attempt = attemptNumber;
        }

        @Override
        public void continueAttempt(long generation) {
            this.generation = generation;
        }

        @Override
        public void connectionRequested(boolean requested) {
        }

        @Override
        public void evaluateCredentials(ConnectionAttempt attempt) {
        }

        @Override
        public void cancelCredentials(ConnectionAttempt attempt) {
        }

        @Override
        public void openTransport(long generation, Duration timeout) {
        }

        @Override
        public void admit(long generation) {
        }

        @Override
        public void connected(long generation, ConnAck ack) {
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
        }

        @Override
        public SendQueue.OfferResult send(
                long generation,
                long ticket,
                Packet packet,
                @Nullable SendReceipt receipt
        ) {
            if ((packet instanceof Packet.PubAck
                    || packet instanceof Packet.PubRec
                    || packet instanceof Packet.PubComp)
                    && ackAdmission != SendQueue.OfferResult.ACCEPTED) {
                return ackAdmission;
            }
            sent.add(new Sent(generation, ticket, packet, receipt));
            return SendQueue.OfferResult.ACCEPTED;
        }

        @Override
        public SendQueue.OfferResult publish(
                long generation,
                long ticket,
                MqttMessage message,
                int packetId,
                @Nullable SendReceipt receipt
        ) {
            return SendQueue.OfferResult.ACCEPTED;
        }

        @Override
        public void schedule(long generation, SessionCore.Timer timer, Duration delay) {
        }

        @Override
        public void cancel(SessionCore.Timer timer) {
        }

        @Override
        public void ignoredPingResponse() {
        }

        @Override
        public void complete(long operation, @Nullable Object value, @Nullable Throwable failure) {
            assertThat(operation).isPositive();
            assertThat(results.put(operation, new Result(value, failure))).isNull();
        }

        @Override
        public Optional<Subscription> registerSubscription(long id) {
            return Optional.of(new TestSubscription());
        }

        @Override
        public void subscribed(Subscription handle, Qos granted) {
        }

        @Override
        public boolean deliver(
                DeliveryToken token,
                List<Long> ids,
                MqttMessage message,
                boolean dup,
                int bytes
        ) {
            deliveries.add(token);
            if (token.acknowledgement() != 0) {
                handed.putIfAbsent(token.acknowledgement(), false);
            }
            return true;
        }

        @Override
        public void releaseDelivery(long token) {
            released.add(token);
        }

        @Override
        public void forgetDelivery(long token) {
            handed.remove(token);
        }

        @Override
        public Map<Long, Boolean> receiveState() {
            return Map.copyOf(handed);
        }

        @Override
        public boolean readyDelivery(long token) {
            return handed.getOrDefault(token, false);
        }

        @Override
        public void sessionChanged(long session) {
            if (this.session != session) {
                handed.clear();
                this.session = session;
            }
        }

        @Override
        public void removeSubscription(long id, Subscription handle, @Nullable Throwable cause) {
        }

        @Override
        public Map<Long, Boolean> end(
                long generation,
                boolean established,
                Disconnection disconnection,
                boolean preserveSession,
                boolean stopped
        ) {
            ends.add(disconnection);
            if (!preserveSession) {
                handed.clear();
            }
            Runnable concurrently = duringEndReconcile;
            return new HashMap<>(receiveState()) {
                @Override
                public @Nullable Boolean get(@Nullable Object token) {
                    concurrently.run();
                    return super.get(token);
                }
            };
        }
    }
}
