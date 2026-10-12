package org.mqtt4j.hato.common.io;

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.common.protocol.Packet;
import org.mqtt4j.hato.common.protocol.PacketCodec;
import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.ReasonCode;
import org.mqtt4j.hato.common.value.Topic;
import org.mqtt4j.hato.common.value.TopicFilter;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

/// Exchanges raw MQTT 3.1.1 packets with a real Mosquitto broker using only the protocol and io
/// layers. There is no session, actor, or retry, so each test drives every packet itself.
///
/// Publishing and receiving use separate connections, so the packets on each connection arrive
/// in an order the broker cannot interleave.
///
/// The tests need Docker. Without it they fail with instructions, so no build passes without
/// them. Contributors without Docker opt out with `-Phato.docker.required=false`, or with
/// `hato.docker.required=false` in `~/.gradle/gradle.properties`; the tests are then skipped.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker is not available and hato.docker.required=false"
)
class MosquittoRawExchangeTest {

    private static final GenericContainer<?> mosquitto = Mosquitto.container();
    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    /// Enables the tests when Docker is available, and also when Docker is required, so that
    /// `startBroker` fails with instructions instead of the tests being skipped.
    static boolean dockerIsAvailableOrRequired() {
        return Mosquitto.availableOrRequired();
    }

    @BeforeAll
    static void startBroker() {
        Mosquitto.start(mosquitto);
    }

    @AfterAll
    static void stopBroker() {
        mosquitto.stop();
    }

    @Test
    void a_client_connects_pings_and_disconnects() throws Exception {
        try (var client = RawSession.open(codec, mosquitto)) {
            client.send(connect("hato-lifecycle", 60));
            // [MQTT-3.2.2-1]: a Clean Session connection is accepted without a stored session.
            assertThat(client.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, SUCCESS, EMPTY));

            client.send(new Packet.PingReq());
            // [MQTT-3.12.4-1]
            client.expect(Packet.PingResp.class);

            client.send(new Packet.Disconnect(SUCCESS, EMPTY));
            // The broker closes the connection after DISCONNECT without sending anything else.
            client.expectEndOfStream();
        }
    }

    @Test
    void a_qos_0_publication_reaches_a_subscriber_without_acknowledgements() throws Exception {
        var topic = new Topic("hato/it/qos0");
        byte[] payload = "qos 0".getBytes(UTF_8);
        try (
                var subscriber = connected("hato-qos0-sub");
                var publisher = connected("hato-qos0-pub")
        ) {
            subscribe(subscriber, new TopicFilter(topic.name()));

            publisher.send(publication(topic, payload, Qos.AT_MOST_ONCE, 0));

            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication(topic, payload, Qos.AT_MOST_ONCE, 0));
        }
    }

    @Test
    void a_qos_1_publication_is_acknowledged_by_the_broker_and_by_the_subscriber()
            throws Exception {
        var topic = new Topic("hato/it/qos1");
        byte[] payload = "qos 1".getBytes(UTF_8);
        try (
                var subscriber = connected("hato-qos1-sub");
                var publisher = connected("hato-qos1-pub")
        ) {
            subscribe(subscriber, new TopicFilter(topic.name()));

            publisher.send(publication(topic, payload, Qos.AT_LEAST_ONCE, 7));
            // [MQTT-2.3.1-6]: PUBACK carries the identifier of the PUBLISH it acknowledges.
            assertThat(publisher.expect(Packet.PubAck.class))
                    .isEqualTo(new Packet.PubAck(7, SUCCESS, EMPTY));

            // The broker assigns its own identifier to the delivery; the subscriber acks that one.
            Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
            assertThat(delivery)
                    .isEqualTo(publication(topic, payload, Qos.AT_LEAST_ONCE, delivery.packetId()));
            subscriber.send(new Packet.PubAck(delivery.packetId(), SUCCESS, EMPTY));
            // A PINGRESP shows that the broker accepted the PUBACK and kept the connection.
            subscriber.send(new Packet.PingReq());
            subscriber.expect(Packet.PingResp.class);
        }
    }

    @Test
    void a_qos_2_publication_completes_the_four_step_exchange_on_both_sides() throws Exception {
        var topic = new Topic("hato/it/qos2");
        byte[] payload = "qos 2".getBytes(UTF_8);
        try (
                var subscriber = connected("hato-qos2-sub");
                var publisher = connected("hato-qos2-pub")
        ) {
            subscribe(subscriber, new TopicFilter(topic.name()));

            // Sending side: PUBLISH, PUBREC, PUBREL, and PUBCOMP with the publisher's identifier.
            publisher.send(publication(topic, payload, Qos.EXACTLY_ONCE, 9));
            assertThat(publisher.expect(Packet.PubRec.class))
                    .isEqualTo(new Packet.PubRec(9, SUCCESS, EMPTY));
            publisher.send(new Packet.PubRel(9, SUCCESS, EMPTY));
            assertThat(publisher.expect(Packet.PubComp.class))
                    .isEqualTo(new Packet.PubComp(9, SUCCESS, EMPTY));

            // Receiving side: the same four steps with the broker's identifier [MQTT-4.3.3-2].
            Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
            int id = delivery.packetId();
            assertThat(delivery).isEqualTo(publication(topic, payload, Qos.EXACTLY_ONCE, id));
            subscriber.send(new Packet.PubRec(id, SUCCESS, EMPTY));
            assertThat(subscriber.expect(Packet.PubRel.class))
                    .isEqualTo(new Packet.PubRel(id, SUCCESS, EMPTY));
            subscriber.send(new Packet.PubComp(id, SUCCESS, EMPTY));
            subscriber.send(new Packet.PingReq());
            subscriber.expect(Packet.PingResp.class);
        }
    }

    @Test
    void unsubscribe_is_acknowledged_with_its_packet_identifier() throws Exception {
        var filter = new TopicFilter("hato/it/unsubscribe/#");
        try (var client = connected("hato-unsubscribe")) {
            subscribe(client, filter);

            client.send(new Packet.Unsubscribe(12, List.of(filter), EMPTY));

            // [MQTT-3.10.4-4]; the MQTT 3.1.1 UNSUBACK has no result for each filter.
            assertThat(client.expect(Packet.UnsubAck.class))
                    .isEqualTo(new Packet.UnsubAck(12, List.of(), EMPTY));
        }
    }

    @Test
    void an_unsupported_protocol_level_is_refused_and_the_connection_closed() throws Exception {
        // Encode a valid CONNECT, then change the Protocol Level that follows the fixed header
        // (2 bytes) and the protocol name (6 bytes) from 4 to 3. An older level is refused in
        // the MQTT 3.1.1 format. Mosquitto 2.1 answers levels above 5 with an MQTT 5.0 CONNACK
        // (reason 0x84), which the MQTT 3.1.1 codec rightly rejects as malformed.
        byte[] connect = codec.encode(connect("hato-level-3", 60));
        assertThat(connect[8]).isEqualTo((byte) 4);
        connect[8] = 3;
        try (var client = RawSession.open(codec, mosquitto)) {
            client.sendRaw(connect);

            // [MQTT-3.1.2-2]: CONNACK return code 0x01, then the broker disconnects.
            assertThat(client.expect(Packet.ConnAck.class)).isEqualTo(
                    new Packet.ConnAck(false, ReasonCode.UNSUPPORTED_PROTOCOL_VERSION, EMPTY)
            );
            client.expectEndOfStream();
        }
    }

    @Test
    void the_broker_closes_a_connection_that_stays_silent_past_its_keep_alive() throws Exception {
        try (var client = RawSession.open(codec, mosquitto)) {
            client.send(connect("hato-keep-alive", 2));
            client.expect(Packet.ConnAck.class);

            // [MQTT-3.1.2-24]: one and a half keep-alive periods, about 3 seconds here.
            client.expectEndOfStream();
        }
    }

    private static RawSession connected(String clientId) throws Exception {
        var session = RawSession.open(codec, mosquitto);
        session.send(connect(clientId, 60));
        assertThat(session.expect(Packet.ConnAck.class))
                .isEqualTo(new Packet.ConnAck(false, SUCCESS, EMPTY));
        return session;
    }

    /// Subscribes with maximum QoS 2, so each delivery keeps the QoS it was published with
    /// [MQTT-3.8.4-6].
    private static void subscribe(RawSession session, TopicFilter filter) throws Exception {
        var subscription = new Packet.Subscription(filter, Qos.EXACTLY_ONCE, false, false, 0);
        session.send(new Packet.Subscribe(1, List.of(subscription), EMPTY));
        // [MQTT-3.8.4-2]; Mosquitto grants the requested maximum QoS.
        assertThat(session.expect(Packet.SubAck.class))
                .isEqualTo(new Packet.SubAck(1, List.of(ReasonCode.GRANTED_QOS_2), EMPTY));
    }

    private static Packet.Connect connect(String clientId, int keepAliveSeconds) {
        return new Packet.Connect(
                clientId, MqttVersion.V3_1_1, true, keepAliveSeconds, null, null, null, EMPTY
        );
    }

    private static Packet.Publish publication(Topic topic, byte[] payload, Qos qos, int packetId) {
        return new Packet.Publish(topic, payload, qos, false, false, packetId, EMPTY);
    }
}
