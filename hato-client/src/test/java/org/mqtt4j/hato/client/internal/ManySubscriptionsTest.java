package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class ManySubscriptionsTest {

    // Far above the default limit, so that work growing with the square of the subscriptions
    // would show.
    private static final int SUBSCRIPTIONS = 20_000;

    private final LinkedBlockingQueue<ScriptTransport> transports = new LinkedBlockingQueue<>();

    @Test
    @Timeout(60)
    void a_client_routes_re_creates_and_removes_many_subscriptions() throws Exception {
        // Arrange: one subscription for each device; the last two take their deliveries
        // through a handler and a hand-off acceptor, the others through receive().
        var handled = new LinkedBlockingQueue<Topic>();
        var handedOff = new LinkedBlockingQueue<Topic>();
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .clientId("many-subscriptions-test")
                .keepAlive(Duration.ZERO)
                .maxSubscriptions(SUBSCRIPTIONS)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                })
                .build()) {
            ScriptTransport first = connect(sut);
            var pulled = new ArrayList<Subscription>();
            for (int device = 0; device < SUBSCRIPTIONS - 2; device++) {
                pulled.add(sut.subscribe(request(device)));
            }
            Subscription _ = sut.subscribe(
                    request(SUBSCRIPTIONS - 2),
                    delivery -> handled.add(delivery.message().topic())
            );
            Subscription _ = sut.handOff(
                    request(SUBSCRIPTIONS - 1),
                    delivery -> handedOff.add(delivery.message().topic())
            );
            acknowledgeSubscribes(first, SUBSCRIPTIONS);

            // Act & Assert: a delivery reaches its device's subscription and no other.
            assertRouted(sut, first, pulled, handled, handedOff);

            // Act & Assert: a new session makes every subscription again.
            first.finishClose();
            ScriptTransport second = connect(sut);
            acknowledgeSubscribes(second, SUBSCRIPTIONS);
            assertRouted(sut, second, pulled, handled, handedOff);

            // Act & Assert: closing every other pulled subscription removes it at the broker,
            // and the rest still take their deliveries.
            for (int device = 0; device < pulled.size(); device += 2) {
                pulled.get(device).close();
            }
            acknowledgeUnsubscribes(second, pulled.size() / 2);
            long before = sut.stats().get(5, SECONDS).receiving().unmatched();
            second.receive(publish(10));
            second.receive(publish(11));
            assertThat(pulled.get(11).receive(Duration.ofSeconds(5)))
                    .isInstanceOf(ReceiveResult.Received.class);
            assertThat(sut.stats().get(5, SECONDS).receiving().unmatched())
                    .isEqualTo(before + 1);
        }
    }

    /// Sends one delivery to each of three devices: a pulled one, the handled one, and the
    /// handed-off one. Each reaches only its own subscription.
    private static void assertRouted(
            MqttClient client,
            ScriptTransport transport,
            List<Subscription> pulled,
            LinkedBlockingQueue<Topic> handled,
            LinkedBlockingQueue<Topic> handedOff
    ) throws Exception {
        long before = client.stats().get(5, SECONDS).receiving().deliveries();
        transport.receive(publish(12_345));
        transport.receive(publish(SUBSCRIPTIONS - 2));
        transport.receive(publish(SUBSCRIPTIONS - 1));
        assertThat(pulled.get(12_345).receive(Duration.ofSeconds(5)))
                .isInstanceOf(ReceiveResult.Received.class);
        assertThat(handled.poll(5, SECONDS)).isEqualTo(topic(SUBSCRIPTIONS - 2));
        assertThat(handedOff.poll(5, SECONDS)).isEqualTo(topic(SUBSCRIPTIONS - 1));
        assertThat(client.stats().get(5, SECONDS).receiving().deliveries()).isEqualTo(before + 3);
    }

    private ScriptTransport connect(MqttClient client) throws Exception {
        CompletableFuture<ConnAck> connected = client.connect();
        ScriptTransport transport = requireNonNull(transports.poll(5, SECONDS));
        assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Connect.class);
        transport.accept();
        connected.get(5, SECONDS);
        return transport;
    }

    /// Answers the next `count` SUBSCRIBE packets with the QoS that each requested.
    private static void acknowledgeSubscribes(ScriptTransport transport, int count)
            throws Exception {
        for (int i = 0; i < count; i++) {
            var packet = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    packet.packetId(),
                    List.of(ReasonCode.of(packet.subscriptions().getFirst().qos().level())),
                    EMPTY
            ));
        }
    }

    /// Answers the next `count` UNSUBSCRIBE packets.
    private static void acknowledgeUnsubscribes(ScriptTransport transport, int count)
            throws Exception {
        for (int i = 0; i < count; i++) {
            var packet = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.UnsubAck(packet.packetId(), List.of(), EMPTY));
        }
    }

    private static Subscribe request(int device) {
        return Subscribe.of(new TopicFilter("devices/" + device + "/#"), Qos.AT_MOST_ONCE);
    }

    private static Topic topic(int device) {
        return new Topic("devices/" + device + "/state");
    }

    private static Packet.Publish publish(int device) {
        return new Packet.Publish(
                topic(device),
                new byte[] {1},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        );
    }
}
