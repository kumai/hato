package org.mqtt4j.mqtj.common.io;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import org.mqtt4j.mqtj.common.protocol.Packet;
import org.mqtt4j.mqtj.common.protocol.PacketCodec;
import org.mqtt4j.mqtj.common.value.MqttVersion;
import org.mqtt4j.mqtj.common.value.Qos;
import org.mqtt4j.mqtj.common.value.ReasonCode;
import org.mqtt4j.mqtj.common.value.Topic;
import org.mqtt4j.mqtj.common.value.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mqtt4j.mqtj.common.protocol.Properties.EMPTY;
import static org.mqtt4j.mqtj.common.value.ReasonCode.SUCCESS;

/// Exchanges raw MQTT 3.1.1 packets with a real Mosquitto broker using only the protocol and io
/// layers. There is no session, actor, or retry, so each test drives every packet itself.
/// Passing these tests is the exit criterion of version 0.1.
///
/// Publishing and receiving use separate connections, so the packets on each connection arrive
/// in an order the broker cannot interleave.
///
/// The tests need Docker. Without it they fail with instructions, so no build passes without
/// them. Contributors without Docker opt out with `-Pmqtj.docker.required=false`, or with
/// `mqtj.docker.required=false` in `~/.gradle/gradle.properties`; the tests are then skipped.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker is not available and mqtj.docker.required=false"
)
class MosquittoRawExchangeTest {

    private static final int MQTT_PORT = 1883;
    private static final int MAX_PACKET_SIZE = 1024 * 1024;
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(10);
    private static final String DOCKER_MISSING = """
            Docker is not available, but the tests against real brokers require it.
            Start Docker, or skip these tests with -Pmqtj.docker.required=false.
            To skip them in every build, put mqtj.docker.required=false
            in ~/.gradle/gradle.properties.""";

    // The version is pinned so that a broker upgrade is a deliberate change. The configuration
    // opens the listener to the host and allows anonymous clients; Mosquitto 2 does neither by
    // default.
    private static final GenericContainer<?> mosquitto =
            new GenericContainer<>(DockerImageName.parse("eclipse-mosquitto:2.1.2-alpine"))
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("mosquitto.conf"),
                            "/mosquitto/config/mosquitto.conf"
                    )
                    .withExposedPorts(MQTT_PORT);
    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    /// Enables the tests when Docker is available, and also when Docker is required, so that
    /// `startBroker` fails with instructions instead of the tests being skipped.
    static boolean dockerIsAvailableOrRequired() {
        return dockerRequired() || DockerClientFactory.instance().isDockerAvailable();
    }

    /// Only the exact value `false` opts out, so a misspelled setting cannot skip the tests.
    private static boolean dockerRequired() {
        return !"false".equals(System.getProperty("mqtj.docker.required"));
    }

    @BeforeAll
    static void startBroker() {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            fail(DOCKER_MISSING);
        }
        mosquitto.start();
    }

    @AfterAll
    static void stopBroker() {
        mosquitto.stop();
    }

    @Test
    void a_client_connects_pings_and_disconnects() throws Exception {
        try (var client = RawSession.open()) {
            client.send(connect("mqtj-lifecycle", 60));
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
        var topic = new Topic("mqtj/it/qos0");
        byte[] payload = "qos 0".getBytes(UTF_8);
        try (
                var subscriber = connected("mqtj-qos0-sub");
                var publisher = connected("mqtj-qos0-pub")
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
        var topic = new Topic("mqtj/it/qos1");
        byte[] payload = "qos 1".getBytes(UTF_8);
        try (
                var subscriber = connected("mqtj-qos1-sub");
                var publisher = connected("mqtj-qos1-pub")
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
        var topic = new Topic("mqtj/it/qos2");
        byte[] payload = "qos 2".getBytes(UTF_8);
        try (
                var subscriber = connected("mqtj-qos2-sub");
                var publisher = connected("mqtj-qos2-pub")
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
        var filter = new TopicFilter("mqtj/it/unsubscribe/#");
        try (var client = connected("mqtj-unsubscribe")) {
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
        byte[] connect = codec.encode(connect("mqtj-level-3", 60));
        assertThat(connect[8]).isEqualTo((byte) 4);
        connect[8] = 3;
        try (var client = RawSession.open()) {
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
        try (var client = RawSession.open()) {
            client.send(connect("mqtj-keep-alive", 2));
            client.expect(Packet.ConnAck.class);

            // [MQTT-3.1.2-24]: one and a half keep-alive periods, about 3 seconds here.
            client.expectEndOfStream();
        }
    }

    private static RawSession connected(String clientId) throws Exception {
        var session = RawSession.open();
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

    /// One TCP connection to the broker that sends and receives whole packets. Each wait for an
    /// incoming packet is bounded, so a missing response fails the test instead of hanging it.
    private static final class RawSession implements AutoCloseable {

        private final TcpConnection connection;
        private final FrameReader reader;
        private final FrameWriter writer;
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        private RawSession(TcpConnection connection) {
            this.connection = connection;
            this.reader = new FrameReader(connection.input(), MAX_PACKET_SIZE);
            this.writer = new FrameWriter(connection.output());
        }

        static RawSession open() throws IOException {
            return new RawSession(TcpConnection.connect(
                    mosquitto.getHost(), mosquitto.getMappedPort(MQTT_PORT), Duration.ofSeconds(5)
            ));
        }

        void send(Packet packet) throws IOException {
            writer.write(codec.encode(packet));
        }

        /// Sends the bytes as given, for packets the model cannot represent.
        void sendRaw(byte[] packet) throws IOException {
            writer.write(packet);
        }

        <T extends Packet> T expect(Class<T> type) throws Exception {
            byte[] frame = nextFrame().orElseThrow(() -> new AssertionError(
                    "The broker closed the connection instead of sending " + type.getSimpleName()
            ));
            Packet packet = codec.decode(frame);
            assertThat(packet).isInstanceOf(type);
            return type.cast(packet);
        }

        void expectEndOfStream() throws Exception {
            assertThat(nextFrame()).as("packet received instead of the end of stream").isEmpty();
        }

        private Optional<byte[]> nextFrame() throws Exception {
            Future<Optional<byte[]>> frame = executor.submit(reader::read);
            try {
                return frame.get(READ_TIMEOUT.toMillis(), MILLISECONDS);
            } catch (TimeoutException e) {
                // Closing the socket releases the blocked read, so close() can end the executor.
                connection.close();
                throw new AssertionError("No packet from the broker within " + READ_TIMEOUT, e);
            }
        }

        @Override
        public void close() throws IOException {
            connection.close();
            executor.close();
        }
    }
}
