package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.broker.BrokerListener;
import org.mqtt4j.hato.broker.BrokerListener.DisconnectReason;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Properties;
import org.mqtt4j.hato.internal.protocol.Property;
import org.mqtt4j.hato.internal.protocol.PropertyId;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.ReasonCode.SESSION_TAKEN_OVER;
import static org.mqtt4j.hato.ReasonCode.SUCCESS;
import static org.mqtt4j.hato.broker.BrokerListener.DisconnectReason.CLIENT_DISCONNECT;
import static org.mqtt4j.hato.broker.internal.RawSession.acceptedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.internal.protocol.Properties.EMPTY;

/// CONNECTs that come to the session before the broker has read what the client sent on the
/// connection that they take over: a client that disconnected cleanly and connected again at once,
/// while the selector of its old connection lagged behind, and the CONNECTs that come meanwhile.
class BrokerTakeOverOrderTest {

    private static final String CLIENT = "robot";

    private static final String WILL_TOPIC = "will/" + CLIENT;

    // What the listener has heard of the client's connections ending, in order.
    private final BlockingQueue<DisconnectReason> ended = new LinkedBlockingQueue<>();

    private final BrokerListener listener = new BrokerListener() {
        @Override
        public void onDisconnected(Connection connection, DisconnectReason reason) {
            if (CLIENT.equals(connection.clientId())) {
                ended.add(reason);
            }
        }
    };

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void what_the_old_connection_sent_comes_before_the_takeover(MqttVersion version)
            throws Exception {
        // Arrange: the actor that takes the session over waits, before it looks at the old
        // connection, until the broker has read the PUBACK and DISCONNECT that the client sends
        // there only after its next CONNECT.
        var armed = new AtomicBoolean();
        var takingOver = new CountDownLatch(1);
        var disconnectRead = new CountDownLatch(1);
        try (BrokerServer sut = BrokerServer.builder()
                .listener(listener)
                .beforeProcess(packet -> {
                    if (armed.get() && packet instanceof Packet.Disconnect) {
                        disconnectRead.countDown();
                    }
                })
                .beforeTakeOver(id -> {
                    if (armed.get() && id.equals(CLIENT)) {
                        takingOver.countDown();
                        awaitQuietly(disconnectRead);
                    }
                })
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                RawSession watcher = connected(sut.port(), "watcher");
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession next = RawSession.open(sut.port(), version)) {
            watcher.subscribe(WILL_TOPIC, AT_LEAST_ONCE);
            try (RawSession old = RawSession.open(sut.port(), version)) {
                old.send(firstConnect(version));
                assertThat(old.expect(Packet.ConnAck.class).reasonCode()).isEqualTo(SUCCESS);
                old.subscribe("work", AT_LEAST_ONCE);
                publisher.publish("work", "job", AT_LEAST_ONCE, 1);
                Packet.Publish job = old.expect(Packet.Publish.class);
                armed.set(true);

                // Act: the next CONNECT comes to the session first; then the old connection
                // acknowledges the job and disconnects cleanly, and the client closes it.
                next.send(nextConnect(version));
                assertThat(takingOver.await(5, SECONDS)).isTrue();
                old.send(new Packet.PubAck(job.packetId(), SUCCESS, EMPTY));
                old.send(new Packet.Disconnect(SUCCESS, EMPTY));
            }

            // Assert: the job does not come again, the Will does not go out, and the listener
            // hears that the client disconnected.
            assertThat(next.expect(Packet.ConnAck.class)).isEqualTo(switch (version) {
                case V3_1_1 -> new Packet.ConnAck(true, SUCCESS, EMPTY);
                case V5 -> acceptedV5(true);
            });
            next.send(new Packet.PingReq());
            Packet afterConnAck = next.expect(Packet.class);
            publisher.publish(WILL_TOPIC, "witness", AT_LEAST_ONCE, 2);
            Packet.Publish watched = watcher.expect(Packet.Publish.class);
            assertThat(new Outcome(
                    afterConnAck.getClass().getSimpleName(),
                    new String(watched.payload(), UTF_8),
                    ended.poll(5, SECONDS)
            )).isEqualTo(new Outcome("PingResp", "witness", CLIENT_DISCONNECT));
        }
    }

    @Test
    void connects_that_come_while_one_waits_take_the_session_over_in_their_order()
            throws Exception {
        // Arrange: three connections of the client, each on a selector of its own. The selector
        // of the first stops on a PINGREQ, so the CONNECT on the second waits for what the first
        // has sent while the CONNECT on the third comes.
        var armed = new AtomicBoolean();
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var takingUp = new Semaphore(0);
        try (BrokerServer sut = BrokerServer.builder()
                .selectors(3)
                .beforeProcess(packet -> {
                    if (armed.get() && packet instanceof Packet.PingReq) {
                        held.countDown();
                        awaitQuietly(release);
                    }
                })
                .beforeTakeOver(_ -> {
                    if (armed.get()) {
                        takingUp.release();
                    }
                })
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                RawSession first = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession second = RawSession.open(sut.port(), MqttVersion.V5);
                RawSession third = RawSession.open(sut.port(), MqttVersion.V5)) {
            first.send(nextConnect(MqttVersion.V5));
            assertThat(first.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(false));
            armed.set(true);
            first.send(new Packet.PingReq());
            assertThat(held.await(5, SECONDS)).isTrue();
            second.send(nextConnect(MqttVersion.V5));
            assertThat(takingUp.tryAcquire(5, SECONDS)).isTrue();
            third.send(nextConnect(MqttVersion.V5));
            assertThat(takingUp.tryAcquire(5, SECONDS)).isTrue();

            // Act
            release.countDown();

            // Assert: the second connection takes the session over after the first has had its
            // answer, and the third takes it from the second.
            first.expect(Packet.PingResp.class);
            assertThat(first.expect(Packet.Disconnect.class).reasonCode())
                    .isEqualTo(SESSION_TAKEN_OVER);
            assertThat(second.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(true));
            assertThat(second.expect(Packet.Disconnect.class).reasonCode())
                    .isEqualTo(SESSION_TAKEN_OVER);
            assertThat(third.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(true));
            third.ping();
        } finally {
            release.countDown();
        }
    }

    // The CONNECT of a client with a session that outlives the connection and a Will.
    private static Packet.Connect firstConnect(MqttVersion version) {
        var will = new Packet.Will(
                new Topic(WILL_TOPIC),
                "gone".getBytes(UTF_8),
                AT_LEAST_ONCE,
                false,
                EMPTY
        );
        return switch (version) {
            case V3_1_1 -> new Packet.Connect(CLIENT, version, false, 0, will, null, null, EMPTY);
            case V5 -> new Packet.Connect(CLIENT, version, true, 0, will, null, null, kept());
        };
    }

    // The CONNECT with which the client continues its session, or starts one that outlives the
    // connection, without a Will.
    private static Packet.Connect nextConnect(MqttVersion version) {
        return switch (version) {
            case V3_1_1 -> new Packet.Connect(CLIENT, version, false, 0, null, null, null, EMPTY);
            case V5 -> new Packet.Connect(CLIENT, version, false, 0, null, null, null, kept());
        };
    }

    // The properties of an MQTT 5.0 CONNECT whose session outlives its connection by a minute.
    private static Properties kept() {
        return new Properties(List.of(
                new Property.FourByteValue(PropertyId.SESSION_EXPIRY_INTERVAL, 60)
        ));
    }

    // Waits for `latch` on the actor, for a test that fails on its own if it never opens.
    private static void awaitQuietly(CountDownLatch latch) {
        try {
            boolean _ = latch.await(5, SECONDS);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    // What the client and the watcher of its Will saw after the takeover: the packet that
    // followed CONNACK on the next connection, the first payload on the Will's topic, and the
    // reason why the listener heard the old connection end.
    private record Outcome(
            String afterConnAck,
            String firstOnWillTopic,
            @Nullable DisconnectReason oldConnectionEnded
    ) {
    }
}
