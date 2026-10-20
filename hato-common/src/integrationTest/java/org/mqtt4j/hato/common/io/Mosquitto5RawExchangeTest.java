package org.mqtt4j.hato.common.io;

import java.util.HexFormat;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.common.protocol.Packet;
import org.mqtt4j.hato.common.protocol.PacketCodec;
import org.mqtt4j.hato.common.protocol.Properties;
import org.mqtt4j.hato.common.protocol.Property;
import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.Topic;
import org.mqtt4j.hato.common.value.TopicFilter;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.protocol.PropertyId.REASON_STRING;
import static org.mqtt4j.hato.common.protocol.PropertyId.SUBSCRIPTION_IDENTIFIER;
import static org.mqtt4j.hato.common.value.ReasonCode.GRANTED_QOS_2;
import static org.mqtt4j.hato.common.value.ReasonCode.NO_MATCHING_SUBSCRIBERS;
import static org.mqtt4j.hato.common.value.ReasonCode.NO_SUBSCRIPTION_EXISTED;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

/// Exercises the common codec directly; the client and embedded broker still select MQTT 3.1.1.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker is not available and hato.docker.required=false"
)
class Mosquitto5RawExchangeTest {

    private static final GenericContainer<?> mosquitto = Mosquitto.container();
    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V5);

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @BeforeAll
    static void startBroker() {
        Docker.start(mosquitto);
    }

    @AfterAll
    static void stopBroker() {
        mosquitto.stop();
    }

    @Test
    void connack_properties_and_disconnect_are_exchanged_with_mosquitto() throws Exception {
        try (var client = RawSession.open(codec, mosquitto)) {
            client.send(connect("hato-five-wire"));

            byte[] wire = client.nextFrame();
            // Recorded from the pinned Mosquitto 2.1.2 image: alias maximum 10,
            // maximum packet size 2,000,000, and receive maximum 20, in this order.
            assertThat(wire).isEqualTo(HexFormat.of().parseHex("200e00000b22000a27001e8480210014"));
            var connAck = (Packet.ConnAck) codec.decode(wire);
            assertThat(connAck.reasonCode()).isEqualTo(SUCCESS);
            assertThat(connAck.sessionPresent()).isFalse();
            assertThat(connAck.properties().entries()).isNotEmpty();
            client.send(new Packet.PingReq());
            assertThat(client.expect(Packet.PingResp.class)).isEqualTo(new Packet.PingResp());
            Properties properties = properties(new Property.StringValue(REASON_STRING, "finished"));
            client.send(new Packet.Disconnect(SUCCESS, properties));
            client.expectEndOfStream();
        }
    }

    @ParameterizedTest
    @EnumSource(Qos.class)
    void publications_and_properties_cross_the_broker_at_every_qos(Qos qos) throws Exception {
        // Arrange: the publisher and subscriber use separate connections and packet identifiers.
        var topic = new Topic("hato/five/" + qos.level());
        var filter = new TopicFilter(topic.name());
        Properties userProperties = properties(
                new Property.UserProperty("key", "first"),
                new Property.UserProperty("key", "second/あ")
        );
        var payload = new byte[] {0, (byte) 0xFF, 42};
        try (
                var subscriber = connected("hato-five-sub-" + qos.level());
                var publisher = connected("hato-five-pub-" + qos.level())
        ) {
            var subscribe = new Packet.Subscribe(
                    1,
                    List.of(new Packet.Subscription(filter, Qos.EXACTLY_ONCE, false, false, 0)),
                    properties(new Property.VariableByteValue(SUBSCRIPTION_IDENTIFIER, 7))
            );
            subscriber.send(subscribe);
            assertThat(subscriber.expect(Packet.SubAck.class))
                    .isEqualTo(new Packet.SubAck(1, List.of(GRANTED_QOS_2), EMPTY));

            // Act
            var publication = new Packet.Publish(
                    topic,
                    payload,
                    qos,
                    false,
                    false,
                    qos == Qos.AT_MOST_ONCE ? 0 : 10,
                    userProperties
            );
            publisher.send(publication);
            switch (qos) {
                case AT_MOST_ONCE -> { }
                case AT_LEAST_ONCE -> assertThat(publisher.expect(Packet.PubAck.class))
                        .isEqualTo(new Packet.PubAck(10, SUCCESS, EMPTY));
                case EXACTLY_ONCE -> {
                    assertThat(publisher.expect(Packet.PubRec.class))
                            .isEqualTo(new Packet.PubRec(10, SUCCESS, EMPTY));
                    publisher.send(new Packet.PubRel(10, SUCCESS, EMPTY));
                    assertThat(publisher.expect(Packet.PubComp.class))
                            .isEqualTo(new Packet.PubComp(10, SUCCESS, EMPTY));
                }
            }
            Packet.Publish delivered = subscriber.expect(Packet.Publish.class);

            // Assert
            assertThat(delivered.topic()).isEqualTo(topic);
            assertThat(delivered.payload()).isEqualTo(payload);
            assertThat(delivered.qos()).isEqualTo(qos);
            assertThat(delivered.properties().entries())
                    .filteredOn(property -> property instanceof Property.UserProperty)
                    .containsExactlyElementsOf(userProperties.entries());
            assertThat(delivered.properties().entries())
                    .contains(new Property.VariableByteValue(SUBSCRIPTION_IDENTIFIER, 7));
            switch (qos) {
                case AT_MOST_ONCE -> { }
                case AT_LEAST_ONCE -> subscriber.send(
                        new Packet.PubAck(delivered.packetId(), SUCCESS, EMPTY)
                );
                case EXACTLY_ONCE -> {
                    subscriber.send(new Packet.PubRec(delivered.packetId(), SUCCESS, EMPTY));
                    assertThat(subscriber.expect(Packet.PubRel.class))
                            .isEqualTo(new Packet.PubRel(delivered.packetId(), SUCCESS, EMPTY));
                    subscriber.send(new Packet.PubComp(delivered.packetId(), SUCCESS, EMPTY));
                }
            }
            var unsubscribe = new Packet.Unsubscribe(
                    2,
                    List.of(filter, new TopicFilter("not-subscribed")),
                    EMPTY
            );
            subscriber.send(unsubscribe);
            assertThat(subscriber.expect(Packet.UnsubAck.class)).isEqualTo(
                    new Packet.UnsubAck(2, List.of(SUCCESS, NO_SUBSCRIPTION_EXISTED), EMPTY)
            );
            subscriber.send(new Packet.PingReq());
            subscriber.expect(Packet.PingResp.class);
        }
    }

    @Test
    void a_publication_without_subscribers_is_answered_with_no_matching_subscribers()
            throws Exception {
        try (var publisher = connected("hato-five-unmatched")) {
            // Act
            publisher.send(new Packet.Publish(
                    new Topic("hato/five/nobody"),
                    new byte[] {1},
                    Qos.AT_LEAST_ONCE,
                    false,
                    false,
                    11,
                    EMPTY
            ));

            // Assert: recorded from the pinned Mosquitto 2.1.2 image.
            assertThat(publisher.expect(Packet.PubAck.class))
                    .isEqualTo(new Packet.PubAck(11, NO_MATCHING_SUBSCRIBERS, EMPTY));
        }
    }

    private static Packet.Connect connect(String clientId) {
        return new Packet.Connect(clientId, MqttVersion.V5, true, 60, null, null, null, EMPTY);
    }

    private static Properties properties(Property... entries) {
        return new Properties(List.of(entries));
    }

    private static RawSession connected(String clientId) throws Exception {
        var session = RawSession.open(codec, mosquitto);
        try {
            session.send(connect(clientId));
            assertThat(session.expect(Packet.ConnAck.class).reasonCode()).isEqualTo(SUCCESS);
            return session;
        } catch (Exception | AssertionError e) {
            session.close();
            throw e;
        }
    }
}
