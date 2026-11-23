package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.internal.protocol.Packet;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.ReasonCode.SUCCESS;
import static org.mqtt4j.hato.broker.internal.BrokerRetainedTest.retained;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.internal.protocol.Properties.EMPTY;

/// The scope that the authenticator gives a client: the broker holds the client's publications,
/// subscriptions, and Will to it, and the session that the client resumes.
class BrokerAuthorizationTest {

    // A robot may publish its own topics and subscribe to its orders; the fleet may do anything.
    private static final Authenticator authenticator = request -> Authenticator.Decision.accept(
            request.clientId().equals("robot")
                    ? Authenticator.Scope.of(
                            List.of(new TopicFilter("robots/1/#")),
                            List.of(new TopicFilter("robots/1/order"))
                    )
                    : Authenticator.Scope.all()
    );

    @Test
    void a_publication_within_the_scope_reaches_its_subscribers() throws Exception {
        try (BrokerServer sut = broker();
                RawSession fleet = connected(sut.port(), "fleet");
                RawSession robot = connected(sut.port(), "robot")) {
            fleet.subscribe("robots/#", AT_MOST_ONCE);
            // Act
            robot.publish("robots/1/state", "ok", AT_LEAST_ONCE, 1);
            // Assert
            assertThat(fleet.expect(Packet.Publish.class).topic().name())
                    .isEqualTo("robots/1/state");
        }
    }

    @Test
    void an_mqtt5_publication_outside_the_scope_is_refused_as_not_authorized() throws Exception {
        try (BrokerServer sut = broker();
                RawSession fleet = connected(sut.port(), "fleet");
                RawSession robot = connectedV5(sut.port(), "robot")) {
            fleet.subscribe("robots/#", AT_MOST_ONCE);
            // Act
            robot.send(publication("robots/2/order", "go", AT_LEAST_ONCE, 1));
            robot.send(publication("robots/2/order", "go", EXACTLY_ONCE, 2));
            // Assert
            assertThat(robot.expect(Packet.PubAck.class).reasonCode())
                    .isEqualTo(ReasonCode.NOT_AUTHORIZED);
            assertThat(robot.expect(Packet.PubRec.class).reasonCode())
                    .isEqualTo(ReasonCode.NOT_AUTHORIZED);
            fleet.ping();
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_publication_outside_the_scope_that_cannot_be_refused_is_dropped(MqttVersion version)
            throws Exception {
        try (BrokerServer sut = broker();
                RawSession fleet = connected(sut.port(), "fleet");
                RawSession robot = (version == MqttVersion.V5)
                        ? connectedV5(sut.port(), "robot")
                        : connected(sut.port(), "robot")) {
            fleet.subscribe("robots/#", AT_MOST_ONCE);
            // Act: QoS 0, which no version acknowledges, and QoS 1 of MQTT 3.1.1, which has no
            // refusal to give.
            robot.send(publication("robots/2/order", "go", AT_MOST_ONCE, 0));
            if (version == MqttVersion.V3_1_1) {
                robot.publish("robots/2/order", "go", AT_LEAST_ONCE, 1);
            }
            // Assert: the publisher's connection goes on, and the subscriber gets nothing.
            robot.ping();
            fleet.ping();
        }
    }

    @Test
    void a_retained_publication_outside_the_scope_is_not_kept() throws Exception {
        try (BrokerServer sut = broker();
                RawSession robot = connected(sut.port(), "robot")) {
            // Act
            robot.publish(retained("robots/2/order", "go", AT_LEAST_ONCE));
            // Assert
            try (RawSession fleet = connected(sut.port(), "fleet")) {
                fleet.subscribe("robots/#", AT_MOST_ONCE);
                fleet.ping();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_subscription_outside_or_across_the_scope_subscribes_to_nothing(MqttVersion version)
            throws Exception {
        try (BrokerServer sut = broker();
                RawSession robot = (version == MqttVersion.V5)
                        ? connectedV5(sut.port(), "robot")
                        : connected(sut.port(), "robot")) {
            // Act: a filter within the scope, one outside it, and one that overlaps it.
            robot.send(new Packet.Subscribe(
                    1,
                    List.of(
                            subscription("robots/1/order", AT_MOST_ONCE),
                            subscription("robots/2/order", AT_MOST_ONCE),
                            subscription("robots/+/order", AT_MOST_ONCE)
                    ),
                    EMPTY
            ));
            // Assert
            ReasonCode refused = (version == MqttVersion.V5)
                    ? ReasonCode.NOT_AUTHORIZED
                    : ReasonCode.UNSPECIFIED_ERROR;
            assertThat(robot.expect(Packet.SubAck.class).reasonCodes())
                    .containsExactly(SUCCESS, refused, refused);
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_will_outside_the_scope_refuses_the_connect(MqttVersion version) throws Exception {
        try (BrokerServer sut = broker();
                RawSession robot = RawSession.open(sut.port(), version)) {
            // Act
            robot.send(new Packet.Connect(
                    "robot",
                    version,
                    true,
                    0,
                    new Packet.Will(
                            new Topic("robots/2/state"),
                            "gone".getBytes(StandardCharsets.UTF_8),
                            AT_MOST_ONCE,
                            false,
                            EMPTY
                    ),
                    null,
                    null,
                    EMPTY
            ));
            // Assert
            assertThat(robot.expect(Packet.ConnAck.class).reasonCode())
                    .isEqualTo(ReasonCode.NOT_AUTHORIZED);
            robot.expectClosed();
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_resumed_session_keeps_only_the_subscriptions_within_the_new_scope(MqttVersion version)
            throws Exception {
        // Arrange: a session with two subscriptions, which the robot resumes with a scope that
        // holds one of them.
        var robotScope = new AtomicReference<>(subscribing("orders/#"));
        try (BrokerServer sut = broker(robotScope)) {
            try (RawSession robot = persistent(sut.port(), version, false)) {
                robot.subscribe("orders/1/#", AT_LEAST_ONCE);
                robot.subscribe("orders/2/#", AT_LEAST_ONCE);
                robot.disconnect();
            }
            robotScope.set(subscribing("orders/1/#"));
            try (RawSession robot = persistent(sut.port(), version, true);
                    RawSession fleet = connected(sut.port(), "fleet")) {
                // Act
                fleet.publish("orders/2/a", "outside", AT_LEAST_ONCE, 1);
                fleet.publish("orders/1/a", "within", AT_LEAST_ONCE, 2);
                // Assert: the first publication to arrive is the second one.
                assertThat(robot.expect(Packet.Publish.class).topic().name())
                        .isEqualTo("orders/1/a");
                robot.ping();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_resumed_session_drops_the_waiting_messages_outside_the_new_scope(MqttVersion version)
            throws Exception {
        // Arrange: two messages that wait for the session, which the robot resumes with a scope
        // that allows the topic of one of them.
        var robotScope = new AtomicReference<>(subscribing("orders/#"));
        try (BrokerServer sut = broker(robotScope)) {
            try (RawSession robot = persistent(sut.port(), version, false)) {
                robot.subscribe("orders/#", AT_LEAST_ONCE);
                robot.disconnect();
            }
            try (RawSession fleet = connected(sut.port(), "fleet")) {
                fleet.publish("orders/2/a", "outside", AT_LEAST_ONCE, 1);
                fleet.publish("orders/1/a", "within", AT_LEAST_ONCE, 2);
            }
            robotScope.set(subscribing("orders/1/#"));
            // Act
            try (RawSession robot = persistent(sut.port(), version, true)) {
                // Assert: the one that the scope allows still comes, as messages that wait for a
                // subscription that ends do.
                assertThat(robot.expect(Packet.Publish.class).topic().name())
                        .isEqualTo("orders/1/a");
                robot.ping();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_resumed_session_sends_nothing_unacknowledged_outside_the_new_scope_again(
            MqttVersion version
    ) throws Exception {
        // Arrange: the robot leaves a QoS 1 delivery and a QoS 2 one unanswered, and a QoS 2 one
        // before its PUBCOMP, and resumes the session with a scope that allows nothing.
        var robotScope = new AtomicReference<>(subscribing("orders/#"));
        try (BrokerServer sut = broker(robotScope)) {
            int unanswered;
            int answered;
            try (RawSession robot = persistent(sut.port(), version, false);
                    RawSession fleet = connected(sut.port(), "fleet")) {
                robot.subscribe("orders/#", EXACTLY_ONCE);
                fleet.publish("orders/1/a", "unanswered", AT_LEAST_ONCE, 1);
                fleet.publish("orders/1/b", "unanswered", EXACTLY_ONCE, 2);
                fleet.publish("orders/1/c", "answered", EXACTLY_ONCE, 3);
                robot.expect(Packet.Publish.class);
                unanswered = robot.expect(Packet.Publish.class).packetId();
                answered = robot.expect(Packet.Publish.class).packetId();
                robot.send(new Packet.PubRec(answered, SUCCESS, EMPTY));
                robot.expect(Packet.PubRel.class);
            }
            robotScope.set(Authenticator.Scope.of(List.of(), List.of()));
            // Act
            try (RawSession robot = persistent(sut.port(), version, true)) {
                // Assert: a PUBREL ends each QoS 2 exchange, as the robot may hold their
                // identifiers, and no message comes again.
                List<Integer> released = List.of(
                        robot.expect(Packet.PubRel.class).packetId(),
                        robot.expect(Packet.PubRel.class).packetId()
                );
                assertThat(released).containsExactlyInAnyOrder(unanswered, answered);
                robot.ping();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_delivery_that_the_new_scope_withdraws_is_not_sent_by_the_old_connection(
            MqttVersion version
    ) throws Exception {
        // Arrange: the robot's open connection holds a QoS 2 delivery in a write that it has not
        // marked as sent, and lets that write go just before the takeover attaches the robot's
        // next connection, which has a scope that allows nothing.
        var robotScope = new AtomicReference<>(subscribing("orders/#"));
        var writer = new HeldWrite();
        var atAttach = new AtomicReference<Runnable>(() -> { });
        try (BrokerServer sut = BrokerServer.builder()
                .authenticator(request -> Authenticator.Decision.accept(
                        request.clientId().equals("robot")
                                ? robotScope.get()
                                : Authenticator.Scope.all()
                ))
                .holdDeliveryWrite(writer)
                .beforeAttach(_ -> atAttach.get().run())
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                RawSession fleet = connected(sut.port(), "fleet");
                RawSession first = persistent(sut.port(), version, false)) {
            first.subscribe("orders/#", EXACTLY_ONCE);
            writer.hold();
            fleet.publish("orders/1/a", "withdrawn", EXACTLY_ONCE, 1);
            writer.awaitHeld();
            robotScope.set(Authenticator.Scope.of(List.of(), List.of()));
            var writtenAfter = new CompletableFuture<Object>();
            atAttach.set(() -> {
                writer.release();
                writtenAfter.complete(nextOrClosure(first));
            });
            // Act
            try (RawSession robot = persistent(sut.port(), version, true)) {
                // Assert: the old connection ends without the delivery, and the new one gets
                // nothing of it either, as the client never held its identifier.
                assertThat(writtenAfter.get(5, SECONDS)).isNotInstanceOf(Packet.Publish.class);
                robot.ping();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_shared_subscription_needs_a_shared_one_of_the_scope_to_hold_it(MqttVersion version)
            throws Exception {
        // Arrange: subscribe filters, the shared subscription written as one of them included,
        // allow no shared subscription.
        var robotScope = new AtomicReference<>(Authenticator.Scope.of(
                List.of(),
                List.of(new TopicFilter("jobs/#"), new TopicFilter("$share/workers/jobs/#"))
        ));
        try (BrokerServer sut = broker(robotScope)) {
            try (RawSession robot = robot(sut.port(), version)) {
                // Act
                robot.send(new Packet.Subscribe(
                        1,
                        List.of(subscription("$share/workers/jobs/#", AT_MOST_ONCE)),
                        EMPTY
                ));

                // Assert
                ReasonCode refused = switch (version) {
                    case V3_1_1 -> ReasonCode.UNSPECIFIED_ERROR;
                    case V5 -> ReasonCode.NOT_AUTHORIZED;
                };
                assertThat(robot.expect(Packet.SubAck.class).reasonCodes())
                        .containsExactly(refused);
            }
            // Act & Assert: a shared subscription of any group within `jobs/#` allows it.
            robotScope.set(sharing("$share/+/jobs/#"));
            try (RawSession robot = robot(sut.port(), version)) {
                robot.subscribe("$share/workers/jobs/#", AT_MOST_ONCE);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_resumed_session_keeps_the_waiting_messages_of_a_shared_subscription_within_the_scope(
            MqttVersion version
    ) throws Exception {
        // Arrange: two messages that wait for the robot's shared subscription, which the robot
        // resumes with a scope that allows the shared subscription to the topic of one of them.
        var robotScope = new AtomicReference<>(sharing("$share/+/orders/#"));
        try (BrokerServer sut = broker(robotScope)) {
            try (RawSession robot = persistent(sut.port(), version, false)) {
                robot.subscribe("$share/fleet/orders/#", AT_LEAST_ONCE);
                robot.disconnect();
            }
            try (RawSession fleet = connected(sut.port(), "fleet")) {
                fleet.publish("orders/2/a", "outside", AT_LEAST_ONCE, 1);
                fleet.publish("orders/1/a", "within", AT_LEAST_ONCE, 2);
            }
            robotScope.set(sharing("$share/+/orders/1/#"));
            // Act
            try (RawSession robot = persistent(sut.port(), version, true)) {
                // Assert: the one that the scope allows still comes, as messages that wait for an
                // ordinary subscription that ends do, though its shared subscription has ended.
                assertThat(robot.expect(Packet.Publish.class).topic().name())
                        .isEqualTo("orders/1/a");
                robot.ping();
            }
        }
    }

    @Test
    void a_waiting_message_of_a_long_group_and_a_long_topic_stays_within_the_scope()
            throws Exception {
        // Arrange: a message to a long topic that waits for the robot's shared subscription of a
        // long group, which the robot resumes with a scope that allows the shared subscription
        // to that topic but not the one that it had. No filter could hold both the group and
        // the topic.
        String group = "g".repeat(40_000);
        String topic = "orders/" + "1".repeat(30_000);
        var robotScope = new AtomicReference<>(sharing("$share/+/orders/#"));
        try (BrokerServer sut = broker(robotScope)) {
            try (RawSession robot = persistent(sut.port(), MqttVersion.V5, false)) {
                robot.subscribe("$share/" + group + "/orders/#", AT_LEAST_ONCE);
                robot.disconnect();
            }
            try (RawSession fleet = connected(sut.port(), "fleet")) {
                fleet.publish(topic, "within", AT_LEAST_ONCE, 1);
            }
            robotScope.set(sharing("$share/+/orders/+"));

            // Act
            try (RawSession robot = persistent(sut.port(), MqttVersion.V5, true)) {
                // Assert
                assertThat(robot.expect(Packet.Publish.class).topic().name()).isEqualTo(topic);
                robot.ping();
            }
        }
    }

    @Test
    void a_message_that_a_narrower_scope_keeps_from_a_member_goes_to_another_member()
            throws Exception {
        // Arrange: a message that waits for the robot, the only member while it was away.
        var robotScope = new AtomicReference<>(sharing("$share/+/orders/#"));
        try (BrokerServer sut = broker(robotScope);
                RawSession member = connected(sut.port(), "member")) {
            try (RawSession robot = persistent(sut.port(), MqttVersion.V5, false)) {
                robot.subscribe("$share/fleet/orders/#", AT_LEAST_ONCE);
                robot.disconnect();
            }
            try (RawSession fleet = connected(sut.port(), "fleet")) {
                fleet.publish("orders/2/a", "kept away", AT_LEAST_ONCE, 1);
            }
            member.subscribe("$share/fleet/orders/#", AT_LEAST_ONCE);
            robotScope.set(sharing("$share/+/orders/1/#"));

            // Act
            try (RawSession robot = persistent(sut.port(), MqttVersion.V5, true)) {
                // Assert: the scope keeps the message from the robot, and the other member of the
                // shared subscription gets it instead.
                assertThat(member.expect(Packet.Publish.class).topic().name())
                        .isEqualTo("orders/2/a");
                robot.ping();
            }
        }
    }

    private static BrokerServer broker() throws Exception {
        return BrokerServer.builder()
                .authenticator(authenticator)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    // A scope that lets the client make shared subscriptions within `filter`, a shared
    // subscription, and nothing else.
    private static Authenticator.Scope sharing(String filter) {
        return Authenticator.Scope.of(List.of(), List.of(), List.of(new TopicFilter(filter)));
    }

    // The robot's connection with a session that ends with it.
    private static RawSession robot(int port, MqttVersion version) throws Exception {
        return switch (version) {
            case V3_1_1 -> connected(port, "robot");
            case V5 -> connectedV5(port, "robot");
        };
    }

    // A broker whose authenticator gives the robot the scope that `robotScope` holds as it
    // connects, and the fleet a scope without limits.
    private static BrokerServer broker(AtomicReference<Authenticator.Scope> robotScope)
            throws Exception {
        return BrokerServer.builder()
                .authenticator(request -> Authenticator.Decision.accept(
                        request.clientId().equals("robot")
                                ? robotScope.get()
                                : Authenticator.Scope.all()
                ))
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    // A scope that lets the client subscribe within `filter` and publish nothing.
    private static Authenticator.Scope subscribing(String filter) {
        return Authenticator.Scope.of(List.of(), List.of(new TopicFilter(filter)));
    }

    // The next packet that `session` receives, or the text "closed" if its connection closes
    // instead.
    private static Object nextOrClosure(RawSession session) {
        try {
            return session.expect(Packet.class);
        } catch (Exception | AssertionError _) {
            return "closed";
        }
    }

    // The robot's connection with a session that outlives it, which the broker reports as
    // present or not.
    private static RawSession persistent(int port, MqttVersion version, boolean present)
            throws Exception {
        return switch (version) {
            case V3_1_1 -> connected(port, "robot", false, present);
            case V5 -> connectedV5(port, "robot", false, 60, present);
        };
    }
}
