package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.internal.protocol.Packet;

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
/// subscriptions, and Will to it.
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

    private static BrokerServer broker() throws Exception {
        return BrokerServer.builder()
                .authenticator(authenticator)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }
}
