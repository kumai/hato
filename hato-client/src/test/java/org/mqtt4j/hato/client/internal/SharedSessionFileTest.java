package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.mqtt4j.hato.DisconnectionAccess;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.RecordingPort;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// Client objects that overlap on one session file, each a core whose store requests the test
/// hands to its own keeper one step at a time, as a broker would see their packets.
class SharedSessionFileTest {

    private static final SessionCodec codec = new SessionCodec("client", MqttVersion.V5);

    @TempDir
    Path directory;

    private final List<Side> sides = new ArrayList<>();

    @AfterEach
    void closeKeepers() {
        sides.forEach(side -> side.keeper.close());
    }

    @Test
    void a_client_object_whose_transport_opens_after_another_one_saved_continues_that_session()
            throws Exception {
        // Arrange: A starts an attempt whose transport is slow to open. Meanwhile B claims the
        // session, subscribes b/# under identifier 1, and saves the session when it disconnects.
        Path file = directory.resolve("session");
        Side a = side(file);
        Side b = side(file);
        a.subscribe("a/#");
        a.beginConnect();
        b.subscribe("b/#");
        b.beginConnect();
        b.open();
        b.connAck(false);
        b.subAck();
        b.disconnect();
        b.serve();

        // Act
        a.open();
        a.connAck(true);

        // Assert: A continues the session that B saved: it removes b/#, which only B used, and
        // subscribes a/# under an identifier that B did not use.
        assertThat(a.sent(Packet.Connect.class))
                .singleElement()
                .satisfies(connect -> assertThat(connect.cleanStart()).isFalse());
        assertThat(a.sent(Packet.Unsubscribe.class))
                .singleElement()
                .satisfies(request -> assertThat(request.filters())
                        .containsExactly(new TopicFilter("b/#")));
        assertThat(a.sent(Packet.Subscribe.class))
                .singleElement()
                .extracting(SharedSessionFileTest::identifier)
                .isEqualTo(2);
    }

    @Test
    void a_claim_whose_result_waits_for_the_actor_lets_no_other_client_object_connect_meanwhile()
            throws Exception {
        // Arrange: A's claim holds the session file, but A's actor has not taken its result yet.
        Path file = directory.resolve("session");
        Side a = side(file);
        Side b = side(file);
        a.subscribe("a/#");
        a.beginConnect();
        a.core.opened(a.generation);
        List<SessionKeeper.Outcome> waiting = a.claimWithoutTakingTheResult();
        b.subscribe("b/#");
        long refused = b.beginConnect();

        // Act: B tries to connect meanwhile, and then A's actor takes the claim's result.
        b.open();
        a.take(waiting);

        // Assert: B's attempt fails without CONNECT, so A's CONNECT changes no session that B
        // saved.
        assertThat(b.result(refused).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasMessage("Another client object holds the session file");
        assertThat(b.sent(Packet.Connect.class)).isEmpty();
        assertThat(a.sent(Packet.Connect.class)).hasSize(1);
        assertThat(b.port.saves).isEmpty();
    }

    @Test
    void a_client_object_connects_with_the_session_once_the_one_that_holds_it_saved_it()
            throws Exception {
        // Arrange: A owns the session, subscribed to a/# under identifier 1, and loses its
        // connection. B, another instance of the same application, tries to connect meanwhile.
        Path file = directory.resolve("session");
        Side a = side(file);
        Side b = side(file);
        a.subscribe("a/#");
        a.beginConnect();
        a.open();
        a.connAck(false);
        a.subAck();
        a.core.failed(a.generation, new IOException("connection lost"));
        b.subscribe("a/#");
        long refused = b.beginConnect();
        b.open();

        // Act: A reconnects and then disconnects, and B tries again.
        a.beginConnect();
        a.open();
        a.connAck(true);
        a.disconnect();
        a.serve();
        b.beginConnect();
        b.open();
        b.connAck(true);

        // Assert: B's first attempt sent nothing, and A continued its session. B, which never
        // started a session of its own, continues the one that A saved.
        assertThat(b.result(refused).failure())
                .isInstanceOf(MqttConnectException.class)
                .hasMessage("Another client object holds the session file");
        assertThat(a.sent(Packet.Connect.class))
                .hasSize(2)
                .last()
                .satisfies(connect -> assertThat(connect.cleanStart()).isFalse());
        assertThat(b.sent(Packet.Connect.class))
                .singleElement()
                .satisfies(connect -> assertThat(connect.cleanStart()).isFalse());
    }

    private Side side(Path file) {
        var side = new Side(file);
        sides.add(side);
        return side;
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

    /// One client object on the shared file.
    private static final class Side {

        final RecordingPort port = new RecordingPort();
        final SessionCore core = new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ZERO,
                MqttVersion.V5,
                Duration.ofHours(1),
                16 * 1024 * 1024,
                null,
                false,
                ReconnectBackoff.disabled(),
                RetryRules.DEFAULT,
                Duration.ofSeconds(60),
                DisconnectionAccess::of,
                ClientLimits.DEFAULT,
                true
        );
        final Semaphore woken = new Semaphore(0);
        final SessionKeeper keeper;
        long nextOperation;
        long generation;
        // How many of the core's claims, checks, and saves the keeper has served.
        int claims;
        int checks;
        int saves;

        Side(Path file) {
            keeper = new SessionKeeper(
                    "session-test",
                    "client",
                    new FileSessionStore(file),
                    codec,
                    woken::release,
                    () -> {},
                    // The connections of these tests exist only in their cores.
                    () -> false
            );
        }

        SessionPersistenceTest.Result result(long operation) {
            return requireNonNull(port.results.get(operation));
        }

        /// Hands the store requests that the core made since the last call to the keeper and
        /// applies the results. The core makes one request at a time in these tests.
        void serve() throws InterruptedException {
            while (claims < port.claims) {
                claims++;
                keeper.claim();
                take(results());
            }
            while (checks < port.checks.size()) {
                keeper.check(port.checks.get(checks++));
                take(results());
            }
            while (saves < port.saves.size()) {
                keeper.save(port.saves.get(saves), port.savedDisconnections.get(saves));
                saves++;
                take(results());
            }
        }

        /// Hands the core's claim to the keeper and returns its result, which the core has not
        /// taken yet, as when the actor is busy.
        List<SessionKeeper.Outcome> claimWithoutTakingTheResult() throws InterruptedException {
            assertThat(port.claims).isEqualTo(claims + 1);
            claims++;
            keeper.claim();
            return results();
        }

        /// The results that the keeper reports, once it has nothing left to run, when it has given
        /// up the store if it should.
        private List<SessionKeeper.Outcome> results() throws InterruptedException {
            assertThat(woken.tryAcquire(5, SECONDS)).isTrue();
            await().atMost(5, SECONDS).until(keeper::idle);
            return keeper.take();
        }

        /// Applies the store's results to the core, as the actor takes them.
        void take(List<SessionKeeper.Outcome> outcomes) {
            for (SessionKeeper.Outcome outcome : outcomes) {
                switch (outcome) {
                    case SessionKeeper.Outcome.Claimed(var state, var ownSave, var failure) ->
                            core.claimed(state, ownSave, failure, 0);
                    case SessionKeeper.Outcome.Refused _ -> core.claimRefused(0);
                    case SessionKeeper.Outcome.Checked(var connection, var held, var failure) ->
                            core.checked(connection, held, failure, 0);
                    case SessionKeeper.Outcome.Saved(var disconnections) ->
                            core.saved(disconnections);
                }
            }
        }

        long beginConnect() {
            long operation = ++nextOperation;
            core.connect(new ConnectionAttempt(operation), 0);
            generation = port.generation;
            return operation;
        }

        /// Opens the transport of the current attempt and lets the store answer its claim or
        /// check.
        void open() throws InterruptedException {
            core.opened(generation);
            serve();
        }

        void connAck(boolean sessionPresent) {
            receive(new Packet.ConnAck(sessionPresent, ReasonCode.SUCCESS, EMPTY));
        }

        void subscribe(String filter) {
            core.subscribe(
                    ++nextOperation,
                    Subscribe.of(new TopicFilter(filter), Qos.AT_LEAST_ONCE)
            );
        }

        /// Grants the latest SUBSCRIBE.
        void subAck() {
            Packet.Subscribe request = sent(Packet.Subscribe.class).getLast();
            receive(new Packet.SubAck(
                    request.packetId(),
                    List.of(ReasonCode.GRANTED_QOS_1),
                    EMPTY
            ));
        }

        /// Requests a disconnection and completes the write of its DISCONNECT.
        void disconnect() {
            core.disconnect(++nextOperation);
            core.sent(generation, port.ticket, 0);
        }

        void receive(Packet packet) {
            core.received(generation, packet, 0, 0);
        }

        <T extends Packet> List<T> sent(Class<T> type) {
            return port.sent.stream()
                    .filter(type::isInstance)
                    .map(type::cast)
                    .toList();
        }
    }
}
