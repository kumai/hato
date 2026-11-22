package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.broker.ConnectedClient;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Properties;
import org.mqtt4j.hato.internal.protocol.Property;
import org.mqtt4j.hato.internal.protocol.PropertyId;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.internal.protocol.Properties.EMPTY;

/// What the application reads of the clients that a broker serves: each connection that the
/// broker accepted, the subscriptions of its session, the deliveries that wait there, and, on a
/// broker that records it, what the client has done on its connection, read without holding up
/// the broker.
class BrokerClientsTest {

    @Test
    void the_list_shows_each_client_that_the_broker_serves() throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession robot = RawSession.open(sut.port());
                RawSession vehicle = RawSession.open(sut.port(), MqttVersion.V5);
                MqttClient application = MqttClient.builder()
                        .server(sut.uri())
                        .clientId("application")
                        .build()) {
            // Act: an MQTT 3.1.1 client with a user name and a subscription, an MQTT 5.0 client
            // with a Will, and a client in the same JVM.
            robot.send(new Packet.Connect(
                    "robot",
                    MqttVersion.V3_1_1,
                    true,
                    0,
                    null,
                    "fleet",
                    null,
                    EMPTY
            ));
            robot.expect(Packet.ConnAck.class);
            robot.subscribe("orders/robot", AT_LEAST_ONCE);
            vehicle.send(new Packet.Connect(
                    "vehicle",
                    MqttVersion.V5,
                    true,
                    0,
                    new Packet.Will(
                            new Topic("fleet/vehicle/connection"),
                            "lost".getBytes(UTF_8),
                            AT_LEAST_ONCE,
                            false,
                            EMPTY
                    ),
                    null,
                    null,
                    EMPTY
            ));
            vehicle.expect(Packet.ConnAck.class);
            application.connect().get(5, SECONDS);

            // Assert
            await().untilAsserted(() -> assertThat(clientIds(sut))
                    .containsExactlyInAnyOrder("robot", "vehicle", "application"));
            ConnectedClient listedRobot = listed(sut, "robot");
            assertThat(listedRobot.connection().version()).isEqualTo(MqttVersion.V3_1_1);
            assertThat(listedRobot.connection().username()).contains("fleet");
            assertThat(listedRobot.connection().remoteAddress()).isPresent();
            assertThat(listedRobot.connection().willTopic()).isEmpty();
            assertThat(listedRobot.connectedFor().isNegative()).isFalse();
            assertThat(listedRobot.subscriptions())
                    .extracting(
                            ConnectedClient.Subscription::filter,
                            ConnectedClient.Subscription::qos
                    )
                    .containsExactly(tuple(new TopicFilter("orders/robot"), AT_LEAST_ONCE));
            assertThat(listedRobot.queuedDeliveries()).isZero();
            assertThat(listedRobot.activity()).isEmpty();
            ConnectedClient listedVehicle = listed(sut, "vehicle");
            assertThat(listedVehicle.connection().version()).isEqualTo(MqttVersion.V5);
            assertThat(listedVehicle.connection().willTopic())
                    .contains(new Topic("fleet/vehicle/connection"));
            assertThat(listedVehicle.subscriptions()).isEmpty();
            assertThat(listed(sut, "application").connection().remoteAddress()).isEmpty();
        }
    }

    @Test
    void a_client_leaves_the_list_once_its_connection_has_ended() throws Exception {
        try (MqttBroker sut = MqttBroker.start(); RawSession _ = connected(sut.port(), "staying")) {
            // Arrange: a client whose session outlives its connection.
            RawSession leaving = connected(sut.port(), "leaving", false, false);
            await().until(() -> clientIds(sut).size() == 2);

            // Act
            leaving.disconnect();
            leaving.close();

            // Assert: its session waits for it, outside the list.
            await().untilAsserted(() -> assertThat(clientIds(sut)).containsExactly("staying"));
            assertThat(sut.stats().persistentSessions()).isEqualTo(1);
        }
    }

    @Test
    void the_list_shows_the_subscriptions_that_remain_with_the_qos_granted() throws Exception {
        try (BrokerServer sut = BrokerServer.builder().start(loopback());
                RawSession robot = connected(sut.port(), "robot")) {
            // Arrange: the broker grants one of the filters QoS 1 at most.
            sut.grantAtMost("orders/#", 1);
            robot.send(new Packet.Subscribe(
                    1,
                    List.of(
                            subscription("status", AT_MOST_ONCE),
                            subscription("orders/#", EXACTLY_ONCE),
                            subscription("actions", AT_LEAST_ONCE)
                    ),
                    EMPTY
            ));
            robot.expect(Packet.SubAck.class);

            // Act
            robot.send(new Packet.Unsubscribe(2, List.of(new TopicFilter("status")), EMPTY));
            robot.expect(Packet.UnsubAck.class);

            // Assert
            await().untilAsserted(() -> assertThat(sut.clients())
                    .singleElement()
                    .extracting(BrokerServer.ClientValues::subscriptions)
                    .isEqualTo(List.of(
                            subscription("orders/#", AT_LEAST_ONCE),
                            subscription("actions", AT_LEAST_ONCE)
                    )));
        }
    }

    @Test
    void the_list_tells_how_long_ago_the_broker_accepted_each_connection() throws Exception {
        // Arrange: the acceptance of the late client takes 12 seconds by the broker's clock,
        // which the time since its acceptance leaves out.
        var timer = new BrokerDeadlineTest.ManualTimer();
        BrokerServer.Builder slowToAccept = BrokerServer.builder()
                .scheduler(timer)
                .beforeAttach(clientId -> {
                    if (clientId.equals("late")) {
                        timer.clock.advance(Duration.ofSeconds(12));
                    }
                });
        try (BrokerServer sut = slowToAccept.start(loopback());
                RawSession _ = connected(sut.port(), "early")) {
            // One client connects 30 seconds before the other comes.
            await().until(() -> sut.clients().size() == 1);
            timer.clock.advance(Duration.ofSeconds(30));
            try (RawSession _ = connected(sut.port(), "late")) {
                await().until(() -> sut.clients().size() == 2);

                // Act
                timer.clock.advance(Duration.ofSeconds(5));
                Map<String, Duration> connectedFor = sut.clients().stream()
                        .collect(Collectors.toMap(
                                client -> client.connection().clientId(),
                                client -> Duration.ofNanos(client.connectedNanos())
                        ));

                // Assert
                assertThat(connectedFor).containsOnly(
                        entry("early", Duration.ofSeconds(47)),
                        entry("late", Duration.ofSeconds(5))
                );
            }
        }
    }

    @Test
    void the_list_shows_the_deliveries_that_wait_for_a_client() throws Exception {
        // Arrange: the broker keeps its writes to the subscriber back, so that its deliveries
        // wait in its session once its connection has taken what its window lets out.
        try (BrokerServer sut = BrokerServer.builder()
                .holdDeliveryWrite(connection -> connection.clientId.equals("slow"))
                .start(loopback());
                RawSession slow = connected(sut.port(), "slow");
                RawSession publisher = connected(sut.port(), "publisher")) {
            slow.subscribe("orders", AT_LEAST_ONCE);

            // Act
            for (int packetId = 1; packetId <= 200; packetId++) {
                publisher.publish("orders", "order", AT_LEAST_ONCE, packetId);
            }

            // Assert: they are the deliveries that wait in all the sessions.
            await().untilAsserted(() -> {
                long queued = listedValues(sut, "slow").queuedDeliveries();
                assertThat(queued).isPositive().isEqualTo(sut.counts().queuedDeliveries());
            });
        }
    }

    @Test
    void the_list_is_read_while_the_broker_holds_a_session_and_its_connection() throws Exception {
        // Arrange: as the broker is about to write a delivery to the subscriber, a hook takes
        // the subscriber's session and connection, which the broker's threads lock to work on
        // them, and has another thread read the list meanwhile, for two seconds at most.
        var server = new AtomicReference<BrokerServer>();
        var read = new CompletableFuture<List<BrokerServer.ClientValues>>();
        BrokerServer.Builder holding = BrokerServer.builder().holdDeliveryWrite(connection -> {
            Session session = connection.client.session;
            if (session != null && !read.isDone()) {
                synchronized (session) {
                    synchronized (connection) {
                        try {
                            read.complete(CompletableFuture.supplyAsync(server.get()::clients)
                                    .get(2, SECONDS));
                        } catch (Exception failure) {
                            read.completeExceptionally(failure);
                        }
                    }
                }
            }
            return false;
        });
        try (BrokerServer sut = holding.start(loopback());
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            server.set(sut);
            subscriber.subscribe("orders", AT_MOST_ONCE);

            // Act
            publisher.publish("orders", "order", AT_MOST_ONCE, 0);

            // Assert: the list came while the broker's locks were held, with both clients.
            assertThat(read).succeedsWithin(Duration.ofSeconds(5)).satisfies(clients ->
                    assertThat(clients)
                            .extracting(client -> client.connection().clientId())
                            .containsExactlyInAnyOrder("subscriber", "publisher"));
            subscriber.expect(Packet.Publish.class);
        }
    }

    @Test
    void the_list_shows_the_values_of_one_connection_while_another_takes_the_session_over()
            throws Exception {
        var step = new AtomicReference<ListingStep>(_ -> { });
        try (BrokerServer sut = listingWith(step).start(loopback());
                RawSession _ = subscribed(connected(sut.port(), "robot"), "status/robot")) {
            // Arrange: as the list is about to read the client's session, the client connects
            // again, starts the session clean, and subscribes there.
            BrokerServer.ClientValues before = listedValues(sut, "robot");
            var second = new AtomicReference<RawSession>();
            step.set(_ -> {
                step.set(_ -> { });
                second.set(subscribed(connected(sut.port(), "robot"), "orders/robot"));
            });

            // Act
            List<BrokerServer.ClientValues> clients = sut.clients();

            // Assert: the list shows the second connection with the subscription that it made.
            try (RawSession _ = second.get()) {
                assertThat(clients).singleElement().satisfies(listed -> {
                    assertThat(listed.connection()).isNotSameAs(before.connection());
                    assertThat(listed.subscriptions())
                            .containsExactly(subscription("orders/robot", AT_LEAST_ONCE));
                });
            }
        }
    }

    @Test
    void the_list_holds_a_client_once_though_it_connects_again_while_the_list_is_read()
            throws Exception {
        var step = new AtomicReference<ListingStep>(_ -> { });
        var left = new Semaphore(0);
        BrokerServer.Builder builder = listingWith(step).afterLeave(clientId -> {
            if (clientId.equals("agv-1")) {
                left.release();
            }
        });
        // Arrange: the identifiers of the three clients share a bin of the session table, in the
        // order that they connect, so that the table shows the first one again, after the
        // others, once it has a new session.
        try (BrokerServer sut = builder.start(loopback());
                RawSession agv = subscribed(connected(sut.port(), "agv-1"), "orders");
                RawSession _ = subscribed(connected(sut.port(), "robot-13"), "orders");
                RawSession _ = subscribed(connected(sut.port(), "vehicle-11"), "orders")) {
            BrokerServer.ClientValues before = listedValues(sut, "agv-1");
            // As the list is about to read the second client, the first one disconnects, its
            // session leaves the table, and it connects again with a new session.
            var visited = new ArrayList<String>();
            var again = new AtomicReference<RawSession>();
            step.set(clientId -> {
                visited.add(clientId);
                if (clientId.equals("robot-13") && again.get() == null) {
                    agv.disconnect();
                    assertThat(left.tryAcquire(5, SECONDS)).isTrue();
                    again.set(subscribed(connected(sut.port(), "agv-1"), "orders"));
                }
            });

            // Act
            List<BrokerServer.ClientValues> clients = sut.clients();

            // Assert: the table showed the first client twice, and the list holds it once,
            // with its new connection.
            try (RawSession _ = again.get()) {
                assertThat(visited).containsExactly("agv-1", "robot-13", "vehicle-11", "agv-1");
                assertThat(clients)
                        .extracting(client -> client.connection().clientId())
                        .containsExactlyInAnyOrder("agv-1", "robot-13", "vehicle-11");
                assertThat(clients)
                        .filteredOn(client -> client.connection().clientId().equals("agv-1"))
                        .singleElement()
                        .extracting(BrokerServer.ClientValues::connection)
                        .isNotSameAs(before.connection());
            }
        }
    }

    @Test
    void the_list_never_shows_a_client_without_the_subscriptions_that_a_clean_start_clears()
            throws Exception {
        try (BrokerServer sut = BrokerServer.builder().start(loopback());
                RawSession _ = subscribed(connected(sut.port(), "robot"), "commands/robot")) {
            // Arrange: the client connects again and starts its session clean, while the index
            // of subscriptions keeps the clearing of the old session waiting halfway; the list
            // no longer shows the client with its subscription once the clearing has begun.
            var next = new FutureTask<RawSession>(() -> connected(sut.port(), "robot"));
            var during = new AtomicReference<List<BrokerServer.ClientValues>>();
            sut.holdingIndex(() -> {
                Thread.ofPlatform().start(next);
                await().until(() -> sut.clients().stream().noneMatch(client ->
                        client.connection().clientId().equals("robot")
                                && !client.subscriptions().isEmpty()));

                // Act
                during.set(sut.clients());
            });

            // Assert: the list left the client out while its session was cleared, and then
            // shows the new connection with the clean session.
            try (RawSession _ = next.get(5, SECONDS)) {
                assertThat(during.get())
                        .extracting(client -> client.connection().clientId())
                        .doesNotContain("robot");
                await().untilAsserted(() -> assertThat(listedValues(sut, "robot").subscriptions())
                        .isEmpty());
            }
        }
    }

    @Test
    void the_list_leaves_out_a_client_whose_connection_keeps_changing_as_it_is_read()
            throws Exception {
        var step = new AtomicReference<ListingStep>(_ -> { });
        try (BrokerServer sut = listingWith(step).start(loopback())) {
            // Arrange: each time the list is about to read the client's session, the client
            // connects again and resumes the session, five times at most.
            var current = new AtomicReference<RawSession>(
                    subscribed(connected(sut.port(), "robot", false, false), "orders/robot")
            );
            var passes = new AtomicInteger();
            step.set(_ -> {
                if (passes.incrementAndGet() <= 5) {
                    RawSession again = subscribed(
                            connected(sut.port(), "robot", false, true),
                            "orders/robot"
                    );
                    current.getAndSet(again).close();
                }
            });

            // Act
            List<BrokerServer.ClientValues> clients = sut.clients();

            // Assert: the list read the session twice, and left the client out.
            try (RawSession _ = current.get()) {
                assertThat(passes).hasValue(2);
                assertThat(clients).isEmpty();
            }
        }
    }

    @Test
    void a_broker_that_records_activity_shows_what_each_client_published() throws Exception {
        var alias = new Properties(List.of(new Property.TwoByteValue(PropertyId.TOPIC_ALIAS, 1)));
        try (MqttBroker sut = MqttBroker.builder().recordClientActivity().start();
                RawSession robot = connected(sut.port(), "robot");
                RawSession vehicle = connectedV5(sut.port(), "vehicle")) {
            // Act: one publication at each QoS, and two through a Topic Alias, the second of
            // which comes without its topic.
            robot.publish("fleet/robot/state", "1", AT_MOST_ONCE, 0);
            robot.publish("fleet/robot/visualization", "2", AT_LEAST_ONCE, 1);
            robot.publish("fleet/robot/connection", "3", EXACTLY_ONCE, 2);
            vehicle.send(new Packet.Publish(
                    new Topic("fleet/vehicle/state"),
                    "4".getBytes(UTF_8),
                    AT_MOST_ONCE,
                    false,
                    false,
                    0,
                    alias
            ));
            vehicle.send(new Packet.AliasPublish(
                    "5".getBytes(UTF_8),
                    AT_MOST_ONCE,
                    false,
                    false,
                    0,
                    alias
            ));
            vehicle.ping();

            // Assert
            await().untilAsserted(() -> {
                ConnectedClient.Activity robotDid = activityOf(sut, "robot");
                assertThat(robotDid.received()).isEqualTo(3);
                assertThat(robotDid.lastTopic()).contains("fleet/robot/connection");
                assertThat(robotDid.sinceLastReceived()).isPresent();
                ConnectedClient.Activity vehicleDid = activityOf(sut, "vehicle");
                assertThat(vehicleDid.received()).isEqualTo(2);
                assertThat(vehicleDid.lastTopic()).contains("fleet/vehicle/state");
            });
        }
    }

    @Test
    void the_activity_tells_how_long_ago_the_last_publication_came() throws Exception {
        var timer = new BrokerDeadlineTest.ManualTimer();
        try (BrokerServer sut = BrokerServer.builder()
                .scheduler(timer)
                .recordActivity()
                .start(loopback());
                RawSession robot = connected(sut.port(), "robot")) {
            // Arrange
            robot.publish("fleet/robot/state", "1", AT_LEAST_ONCE, 1);

            // Act
            timer.clock.advance(Duration.ofSeconds(7));

            // Assert
            await().untilAsserted(() -> assertThat(activityValuesOf(sut, "robot"))
                    .extracting(BrokerServer.ActivityValues::sinceLastReceivedNanos)
                    .isEqualTo(Duration.ofSeconds(7).toNanos()));
        }
    }

    @Test
    void the_activity_starts_again_with_each_connection_of_a_client() throws Exception {
        try (MqttBroker sut = MqttBroker.builder().recordClientActivity().start()) {
            // Arrange: the client publishes on a connection whose session outlives it.
            try (RawSession first = connected(sut.port(), "robot", false, false)) {
                first.publish("fleet/robot/state", "1", AT_LEAST_ONCE, 1);
                await().until(() -> activityOf(sut, "robot").received() == 1);
                first.disconnect();
            }

            // Act
            try (RawSession _ = connected(sut.port(), "robot", false, true)) {
                // Assert: the list shows the new connection once the broker has accepted it.
                await().until(() -> clientIds(sut).contains("robot"));
                ConnectedClient.Activity again = activityOf(sut, "robot");
                assertThat(again.received()).isZero();
                assertThat(again.lastTopic()).isEmpty();
                assertThat(again.sinceLastReceived()).isEmpty();
            }
        }
    }

    @Test
    void the_activity_shows_the_publications_that_the_broker_sent_a_client() throws Exception {
        try (MqttBroker sut = MqttBroker.builder().recordClientActivity().start();
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            subscriber.subscribe("orders", AT_LEAST_ONCE);

            // Act
            for (int sent = 0; sent < 3; sent++) {
                publisher.publish("orders", "order", AT_MOST_ONCE, 0);
                subscriber.expect(Packet.Publish.class);
            }

            // Assert
            await().untilAsserted(() -> {
                assertThat(activityOf(sut, "subscriber").delivered()).isEqualTo(3);
                assertThat(activityOf(sut, "publisher").delivered()).isZero();
            });
        }
    }

    @Test
    void the_activity_shows_what_the_scope_of_a_client_refused() throws Exception {
        // Arrange: a client that may publish to its own topics and subscribe to its own orders.
        Authenticator scoped = _ -> Authenticator.Decision.accept(Authenticator.Scope.of(
                List.of(new TopicFilter("fleet/robot/#")),
                List.of(new TopicFilter("orders/robot/#"))
        ));
        try (MqttBroker sut = MqttBroker.builder()
                .authenticator(scoped)
                .recordClientActivity()
                .start();
                RawSession robot = connected(sut.port(), "robot")) {
            // Act
            robot.publish("fleet/robot/state", "fine", AT_LEAST_ONCE, 1);
            robot.publish("fleet/other/state", "astray", AT_LEAST_ONCE, 2);
            robot.send(new Packet.Subscribe(
                    3,
                    List.of(subscription("orders/other/#", AT_LEAST_ONCE)),
                    EMPTY
            ));
            robot.expect(Packet.SubAck.class);

            // Assert: the refused publication counts among those received, too.
            await().untilAsserted(() -> {
                ConnectedClient.Activity did = activityOf(sut, "robot");
                assertThat(did.received()).isEqualTo(2);
                assertThat(did.lastTopic()).contains("fleet/other/state");
                assertThat(did.refusedPublications()).isEqualTo(1);
                assertThat(did.lastRefusedTopic()).contains("fleet/other/state");
                assertThat(did.refusedSubscriptions()).isEqualTo(1);
                assertThat(did.lastRefusedFilter()).contains("orders/other/#");
            });
        }
    }

    @Test
    void the_activity_marks_a_long_topic_or_filter_that_it_keeps_as_its_beginning()
            throws Exception {
        // Arrange: a client that may publish to its own topics and subscribe to its own orders.
        Authenticator scoped = _ -> Authenticator.Decision.accept(Authenticator.Scope.of(
                List.of(new TopicFilter("fleet/robot/#")),
                List.of(new TopicFilter("orders/robot/#"))
        ));
        String topic = "fleet/" + "x".repeat(ClientActivity.MAX_TEXT);
        String filter = "orders/" + "y".repeat(ClientActivity.MAX_TEXT);
        try (MqttBroker sut = MqttBroker.builder()
                .authenticator(scoped)
                .recordClientActivity()
                .start();
                RawSession robot = connected(sut.port(), "robot")) {
            // Act: a long topic and a long filter outside the scope, and a topic within it.
            robot.publish(topic, "astray", AT_LEAST_ONCE, 1);
            robot.publish("fleet/robot/state", "fine", AT_LEAST_ONCE, 2);
            robot.send(new Packet.Subscribe(
                    3,
                    List.of(subscription(filter, AT_LEAST_ONCE)),
                    EMPTY
            ));
            robot.expect(Packet.SubAck.class);

            // Assert
            await().untilAsserted(() -> {
                ConnectedClient.Activity did = activityOf(sut, "robot");
                assertThat(did.lastTopic()).contains("fleet/robot/state");
                assertThat(did.lastTopicTruncated()).isFalse();
                assertThat(did.lastRefusedTopic())
                        .contains(topic.substring(0, ClientActivity.MAX_TEXT));
                assertThat(did.lastRefusedTopicTruncated()).isTrue();
                assertThat(did.lastRefusedFilter())
                        .contains(filter.substring(0, ClientActivity.MAX_TEXT));
                assertThat(did.lastRefusedFilterTruncated()).isTrue();
            });
        }
    }

    @Test
    @SuppressWarnings("try") // The test closes the broker itself; closing it again does nothing.
    void a_broker_that_has_ended_serves_no_client() throws Exception {
        try (MqttBroker sut = MqttBroker.start(); RawSession _ = connected(sut.port(), "robot")) {
            // Arrange
            await().until(() -> sut.clients().size() == 1);

            // Act
            sut.close();

            // Assert
            assertThat(sut.clients()).isEmpty();
        }
    }

    /// The identifiers of the clients that the list of `broker` shows.
    private static List<String> clientIds(MqttBroker broker) {
        return broker.clients().stream()
                .map(client -> client.connection().clientId())
                .toList();
    }

    /// The client `clientId` in the list of `broker`, which must show it.
    private static ConnectedClient listed(MqttBroker broker, String clientId) {
        return broker.clients().stream()
                .filter(client -> client.connection().clientId().equals(clientId))
                .findFirst()
                .orElseThrow(() -> new AssertionError(clientId + " is not in the list"));
    }

    /// What the client `clientId` in the list of `broker` has done, which the broker records.
    private static ConnectedClient.Activity activityOf(MqttBroker broker, String clientId) {
        return listed(broker, clientId).activity()
                .orElseThrow(() -> new AssertionError("no activity of " + clientId));
    }

    /// What the server holds of the client `clientId`, which its list must show.
    private static BrokerServer.ClientValues listedValues(BrokerServer server, String clientId) {
        return server.clients().stream()
                .filter(client -> client.connection().clientId().equals(clientId))
                .findFirst()
                .orElseThrow(() -> new AssertionError(clientId + " is not in the list"));
    }

    /// What the client `clientId` in the list of `server` has done, which the server records.
    private static BrokerServer.ActivityValues activityValuesOf(
            BrokerServer server,
            String clientId
    ) {
        BrokerServer.ActivityValues activity = listedValues(server, clientId).activity();
        if (activity == null) {
            throw new AssertionError("no activity of " + clientId);
        }
        return activity;
    }

    /// A builder of a broker that runs the step that `step` holds each time its list is about to
    /// read a session.
    private static BrokerServer.Builder listingWith(AtomicReference<ListingStep> step) {
        return BrokerServer.builder().whileListing(clientId -> {
            try {
                step.get().run(clientId);
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        });
    }

    /// `session`, once the broker has answered its subscription to `filter` at QoS 1, which it
    /// does only after it has listed the connection.
    private static RawSession subscribed(RawSession session, String filter) throws Exception {
        session.subscribe(filter, AT_LEAST_ONCE);
        return session;
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    /// What a test does as the list of a broker is about to read the session of `clientId`.
    @FunctionalInterface
    private interface ListingStep {

        void run(String clientId) throws Exception;
    }
}
