package org.mqtt4j.hato.common.io;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.common.protocol.Packet;
import org.mqtt4j.hato.common.protocol.PacketCodec;
import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.Topic;
import org.mqtt4j.hato.common.value.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

/// Exchanges raw MQTT 3.1.1 packets with a pinned broker image through the common codec alone.
///
/// Every broker keeps a retained message for a new subscription to its topic until an empty one
/// removes it, so that is tested here. Each subclass records how its broker answers what the
/// client's MQTT 3.1.1 connections rely on: a QoS 2 publication, overlapping subscriptions, a
/// retained message for new subscriptions with wildcards, a persistent session that a client
/// resumes, and a connection that another one with the same client identifier takes over. The
/// client's tests take their expectations from these records, so a new image that answers
/// differently fails the record of that answer as well.
///
/// The records about routing subscribe with maximum QoS 0, so deliveries need no
/// acknowledgement.
abstract class MqttRawExchangeTest {

    static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    /// How long [#deliveriesBefore] waits for each later publication from a broker that does not
    /// keep the order of topics.
    private static final Duration STRAGGLER_WAIT = Duration.ofMillis(300);

    private static final byte[] RETAINED = "kept".getBytes(UTF_8);

    private static final byte[] KEPT = "away".getBytes(UTF_8);

    /// Opens a connection to the broker under test.
    abstract RawSession open() throws IOException;

    /// Whether the broker forwards publications on different topics in the order they arrived,
    /// as recorded for it. MQTT requires that order within a topic only. Records that read up to
    /// a publication on another topic wait for later ones when the broker does not keep it.
    boolean keepsOrderAcrossTopics() {
        return true;
    }

    @Test
    void a_new_subscription_receives_the_retained_message_until_an_empty_one_removes_it()
            throws Exception {
        // Arrange
        var topic = new Topic("hato/three/retained");
        var filter = new TopicFilter(topic.name());
        retain(topic, RETAINED);

        // Act
        List<String> kept = arrivalsForNewSubscription("hato-three-kept", topic, filter);
        retain(topic, new byte[0]);
        List<String> removed = arrivalsForNewSubscription("hato-three-removed", topic, filter);

        // Assert: some brokers send the retained message before SUBACK [MQTT-3.8.4-3].
        assertThat(kept).containsExactlyInAnyOrder("SUBACK", "retained");
        assertThat(removed).containsExactly("SUBACK");
    }

    /// Connects with Clean Session and checks that the broker accepts the connection.
    RawSession connected(String clientId) throws Exception {
        return connected(clientId, null);
    }

    /// Connects with Clean Session and `will`, and checks that the broker accepts the
    /// connection.
    RawSession connected(String clientId, Packet.@Nullable Will will) throws Exception {
        RawSession session = open();
        try {
            session.send(new Packet.Connect(
                    clientId,
                    MqttVersion.V3_1_1,
                    true,
                    60,
                    will,
                    null,
                    null,
                    EMPTY
            ));
            assertThat(session.expect(Packet.ConnAck.class))
                    .isEqualTo(new Packet.ConnAck(false, SUCCESS, EMPTY));
            return session;
        } catch (Exception | AssertionError e) {
            session.close();
            throw e;
        }
    }

    /// Publishes at QoS 2 with packet identifier 1 and returns the broker's answer, after
    /// checking that the broker still answers a PINGREQ on that connection.
    Packet answerToQos2Publication() throws Exception {
        try (RawSession publisher = connected("hato-three-qos2")) {
            publisher.send(new Packet.Publish(
                    new Topic("hato/three/qos2"),
                    new byte[] {2},
                    Qos.EXACTLY_ONCE,
                    false,
                    false,
                    1,
                    EMPTY
            ));
            Packet answer = publisher.expect(Packet.class);
            publisher.send(new Packet.PingReq());
            publisher.expect(Packet.PingResp.class);
            return answer;
        }
    }

    /// Subscribes one connection with two overlapping filters, in separate SUBSCRIBE packets,
    /// publishes once to a topic that both filters match, and returns the copies that the
    /// connection receives.
    List<Packet> copiesForOverlappingSubscriptions() throws Exception {
        String prefix = "hato/three/overlap";
        // Only the first filter matches the end marker, so a single copy of it arrives.
        var end = new Topic(prefix + "/end/marker");
        try (
                RawSession subscriber = connected("hato-three-overlap-sub");
                RawSession publisher = connected("hato-three-overlap-pub")
        ) {
            subscribe(subscriber, 1, new TopicFilter(prefix + "/#"));
            subscribe(subscriber, 2, new TopicFilter(prefix + "/+"));
            publisher.send(publication(new Topic(prefix + "/both"), new byte[] {1}, false));
            publisher.send(publication(end, new byte[] {0}, false));
            return deliveriesBefore(subscriber, end);
        }
    }

    /// Retains a message on a topic below `prefix`, and describes what a new subscription with
    /// `filter`, which must match that topic, receives: `SUBACK`, and `retained` for the message,
    /// in the order they arrive. The topic and `prefix` must be distinct to each call.
    List<String> retainedForNewSubscription(String prefix, String filter) throws Exception {
        var topic = new Topic(prefix + "/topic");
        retain(topic, RETAINED);
        try {
            return arrivalsForNewSubscription(
                    "hato-three-" + Integer.toHexString(prefix.hashCode()),
                    topic,
                    new TopicFilter(filter)
            );
        } finally {
            retain(topic, new byte[0]);
        }
    }

    /// Leaves a persistent session with a subscription at `qos`, publishes a message at `qos`
    /// while the client is away, and resumes the session. Records whether the broker resumed it,
    /// and how many copies of that message it delivered before a later publication.
    Resumption resume(Qos qos) throws Exception {
        String clientId = "hato-three-resume-" + qos.level();
        var topic = new Topic("hato/three/resume/" + qos.level());
        try (RawSession publisher = connected(clientId + "-publisher")) {
            try (RawSession first = persistent(clientId)) {
                first.send(new Packet.Subscribe(
                        1,
                        List.of(new Packet.Subscription(
                                new TopicFilter(topic.name()),
                                qos,
                                false,
                                false,
                                0
                        )),
                        EMPTY
                ));
                first.expect(Packet.SubAck.class);
                first.send(new Packet.Disconnect(SUCCESS, EMPTY));
                first.expectEndOfStream();
            }
            publish(publisher, topic, KEPT, qos);
            try (RawSession resumed = open()) {
                resumed.send(persistentConnect(clientId));
                boolean sessionPresent = resumed.expect(Packet.ConnAck.class).sessionPresent();
                int kept = 0;
                if (sessionPresent) {
                    // The later publication follows anything kept for the session.
                    publish(publisher, topic, new byte[] {0}, qos);
                    Packet.Publish delivery = resumed.expect(Packet.Publish.class);
                    while (Arrays.equals(delivery.payload(), KEPT)) {
                        kept++;
                        delivery = resumed.expect(Packet.Publish.class);
                    }
                }
                return new Resumption(sessionPresent, kept);
            } finally {
                // End the session, so that no copy of it remains for a later run.
                connected(clientId).close();
            }
        }
    }

    /// Whether a broker resumed a persistent session, and how many copies it delivered of a
    /// message published to the session while the client was away.
    record Resumption(boolean sessionPresent, int keptMessages) {
    }

    /// Connects with the client identifier of an open connection that has a QoS 0 Will, and
    /// records what the broker does with the open connection.
    Takeover takeOver(String clientId) throws Exception {
        var willTopic = new Topic("hato/three/takeover/" + clientId);
        var will = new Packet.Will(willTopic, new byte[] {9}, Qos.AT_MOST_ONCE, false, EMPTY);
        try (
                RawSession watcher = connected(clientId + "-watcher");
                RawSession first = connected(clientId, will)
        ) {
            subscribe(watcher, 1, new TopicFilter(willTopic.name()));
            try (RawSession _ = connected(clientId)) {
                var received = new ArrayList<Packet>();
                Optional<Packet> packet = first.next();
                while (packet.isPresent()) {
                    received.add(packet.get());
                    packet = first.next();
                }
                boolean willPublished = watcher.poll(Duration.ofSeconds(2)).isPresent();
                return new Takeover(received, willPublished);
            }
        }
    }

    /// What a broker sent a connection that another connection took over, before closing it,
    /// and whether it published the Will of the connection that it closed.
    record Takeover(List<Packet> toTakenOverConnection, boolean willPublished) {
    }

    /// Subscribes a new connection with `filter`, which matches `topic`, and describes what it
    /// receives until a later publication to `topic` that is not retained: `SUBACK`, and
    /// `retained` for a retained message, in order of arrival.
    private List<String> arrivalsForNewSubscription(
            String clientId,
            Topic topic,
            TopicFilter filter
    ) throws Exception {
        Packet.Publish marker = publication(topic, new byte[] {0}, false);
        try (
                RawSession subscriber = connected(clientId);
                RawSession publisher = connected(clientId + "-publisher")
        ) {
            subscriber.send(new Packet.Subscribe(
                    1,
                    List.of(new Packet.Subscription(filter, Qos.AT_MOST_ONCE, false, false, 0)),
                    EMPTY
            ));
            // Only a retained message can arrive before the SUBACK. The marker follows anything
            // that the subscription receives at once.
            var arrivals = new ArrayList<String>();
            Packet packet = subscriber.expect(Packet.class);
            while (!(packet instanceof Packet.SubAck subAck)) {
                arrivals.add(retained(packet));
                packet = subscriber.expect(Packet.class);
            }
            assertThat(subAck).isEqualTo(new Packet.SubAck(1, List.of(SUCCESS), EMPTY));
            arrivals.add("SUBACK");
            publisher.send(marker);
            packet = subscriber.expect(Packet.class);
            while (!packet.equals(marker)) {
                arrivals.add(retained(packet));
                packet = subscriber.expect(Packet.class);
            }
            return arrivals;
        }
    }

    /// Checks that `packet` delivers the retained message, and describes it.
    private static String retained(Packet packet) {
        assertThat(packet).isInstanceOfSatisfying(Packet.Publish.class, publish -> {
            assertThat(publish.retain()).isTrue();
            assertThat(publish.payload()).isEqualTo(RETAINED);
        });
        return "retained";
    }

    private RawSession persistent(String clientId) throws Exception {
        RawSession session = open();
        try {
            session.send(persistentConnect(clientId));
            assertThat(session.expect(Packet.ConnAck.class).reasonCode()).isEqualTo(SUCCESS);
            return session;
        } catch (Exception | AssertionError e) {
            session.close();
            throw e;
        }
    }

    private static Packet.Connect persistentConnect(String clientId) {
        return new Packet.Connect(
                clientId,
                MqttVersion.V3_1_1,
                false,
                60,
                null,
                null,
                null,
                EMPTY
        );
    }

    /// Publishes `payload` to `topic` at `qos`, and for QoS 1 waits for the PUBACK.
    private static void publish(RawSession publisher, Topic topic, byte[] payload, Qos qos)
            throws Exception {
        publisher.send(new Packet.Publish(
                topic,
                payload,
                qos,
                false,
                false,
                qos == Qos.AT_MOST_ONCE ? 0 : 1,
                EMPTY
        ));
        if (qos == Qos.AT_LEAST_ONCE) {
            assertThat(publisher.expect(Packet.PubAck.class))
                    .isEqualTo(new Packet.PubAck(1, SUCCESS, EMPTY));
        }
    }

    /// Publishes `payload` to `topic` with RETAIN set at QoS 1 and waits for the PUBACK, so the
    /// broker has stored it, or removed it for an empty payload, before this returns.
    private void retain(Topic topic, byte[] payload) throws Exception {
        try (RawSession publisher = connected("hato-three-retain-" + topic.name().hashCode())) {
            publisher.send(new Packet.Publish(
                    topic,
                    payload,
                    Qos.AT_LEAST_ONCE,
                    true,
                    false,
                    1,
                    EMPTY
            ));
            assertThat(publisher.expect(Packet.PubAck.class))
                    .isEqualTo(new Packet.PubAck(1, SUCCESS, EMPTY));
        }
    }

    /// Reads what `subscriber` receives until a publication for `end` arrives, and returns the
    /// packets before it, in order. For a broker that does not keep the order of topics, they
    /// also include the publications that arrive soon after it.
    private List<Packet> deliveriesBefore(RawSession subscriber, Topic end) throws Exception {
        return subscriber.packetsBefore(
                end,
                keepsOrderAcrossTopics() ? Duration.ZERO : STRAGGLER_WAIT
        );
    }

    /// Subscribes `session` to `filter` with maximum QoS 0.
    private static void subscribe(RawSession session, int packetId, TopicFilter filter)
            throws Exception {
        var subscription = new Packet.Subscription(filter, Qos.AT_MOST_ONCE, false, false, 0);
        session.send(new Packet.Subscribe(packetId, List.of(subscription), EMPTY));
        assertThat(session.expect(Packet.SubAck.class))
                .isEqualTo(new Packet.SubAck(packetId, List.of(SUCCESS), EMPTY));
    }

    private static Packet.Publish publication(Topic topic, byte[] payload, boolean retain) {
        return new Packet.Publish(topic, payload, Qos.AT_MOST_ONCE, retain, false, 0, EMPTY);
    }
}
