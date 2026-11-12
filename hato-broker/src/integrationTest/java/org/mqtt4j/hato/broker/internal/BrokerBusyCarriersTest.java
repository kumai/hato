package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.testsupport.BusyCarriers;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.internal.RawSession.codec;
import static org.mqtt4j.hato.broker.internal.RawSession.connect;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// A broker keeps serving whatever the application does with its own threads: while its virtual
/// threads keep every carrier busy, as an application that computes on them can, and while its
/// logging backend is held up. No thread of the broker needs a carrier or enters the backend.
///
/// The clients here read and write on the thread of the test, which is a platform thread. A
/// [RawSession] reads on a virtual thread, which would wait for a carrier with the broker.
class BrokerBusyCarriersTest {

    private static final byte[] CONNACK = codec.encode(new Packet.ConnAck(false, SUCCESS, EMPTY));

    private static final byte[] PINGRESP = codec.encode(new Packet.PingResp());

    @Test
    @Timeout(30)
    void a_broker_keeps_a_client_that_keeps_alive_while_virtual_threads_keep_every_carrier_busy()
            throws Exception {
        try (var sut = MqttBroker.start(); Socket client = connected(sut.port(), "client", 1)) {
            // Arrange
            BusyCarriers busy = BusyCarriers.start();
            try {
                // Act & Assert: a keep-alive of one second ends a connection that sends nothing
                // for a second and a half. The broker reads each PINGREQ in time and answers
                // it, well past that.
                for (int ping = 0; ping < 5; ping++) {
                    exchange(client, new Packet.PingReq(), PINGRESP);
                    Thread.sleep(500);
                }
                exchange(client, new Packet.PingReq(), PINGRESP);
            } finally {
                busy.stop();
            }
        }
    }

    @Test
    @Timeout(30)
    void a_broker_accepts_a_connection_while_virtual_threads_keep_every_carrier_busy()
            throws Exception {
        try (var sut = MqttBroker.start()) {
            // Arrange
            BusyCarriers busy = BusyCarriers.start();
            try {
                // Act & Assert: CONNACK comes within the five seconds that a client waits.
                try (Socket client = connected(sut.port(), "late", 0)) {
                    exchange(client, new Packet.PingReq(), PINGRESP);
                }
            } finally {
                busy.stop();
            }
        }
    }

    @Test
    @Timeout(30)
    void a_broker_delivers_a_publication_while_virtual_threads_keep_every_carrier_busy()
            throws Exception {
        try (
                var sut = MqttBroker.start();
                Socket subscriber = connected(sut.port(), "subscriber", 0);
                Socket publisher = connected(sut.port(), "publisher", 0)
        ) {
            // Arrange
            subscribe(subscriber, "news");
            BusyCarriers busy = BusyCarriers.start();
            try {
                // Act & Assert
                deliver(publisher, subscriber, "news");
            } finally {
                busy.stop();
            }
        }
    }

    @Test
    @Timeout(30)
    void a_broker_serves_while_the_logging_backend_is_held_up() throws Exception {
        var handler = new HoldingHandler();
        Logger logs = Logger.getLogger("org.mqtt4j.hato.broker");
        logs.addHandler(handler);
        try (
                var sut = MqttBroker.start();
                Socket subscriber = connected(sut.port(), "subscriber", 0);
                Socket publisher = connected(sut.port(), "publisher", 0)
        ) {
            // Arrange: the broker logs each client that it accepts, and the handler holds the
            // thread that gives it the first of those lines.
            subscribe(subscriber, "news");
            assertThat(handler.entered.await(5, SECONDS)).isTrue();

            // Act & Assert: the broker answers, accepts, and delivers meanwhile.
            exchange(subscriber, new Packet.PingReq(), PINGRESP);
            try (Socket client = connected(sut.port(), "late", 0)) {
                exchange(client, new Packet.PingReq(), PINGRESP);
            }
            deliver(publisher, subscriber, "news");

            // A broker that closes gives the backend a moment for its last lines, so the
            // handler lets go first.
            handler.released.countDown();
        } finally {
            handler.released.countDown();
            logs.removeHandler(handler);
        }
    }

    // Opens a connection on which the broker has accepted an MQTT 3.1.1 client.
    private static Socket connected(int port, String clientId, int keepAlive) throws IOException {
        var socket = new Socket(InetAddress.getLoopbackAddress(), port);
        try {
            socket.setSoTimeout(5_000);
            exchange(socket, connect(clientId, true, keepAlive), CONNACK);
            return socket;
        } catch (IOException | RuntimeException | Error failure) {
            socket.close();
            throw failure;
        }
    }

    // Subscribes the client of `socket` to `filter` at QoS 0.
    private static void subscribe(Socket socket, String filter) throws IOException {
        var request = new Packet.Subscribe(
                1,
                List.of(subscription(filter, AT_MOST_ONCE)),
                EMPTY
        );
        exchange(socket, request, codec.encode(new Packet.SubAck(1, List.of(SUCCESS), EMPTY)));
    }

    // Publishes to `topic` on one connection and expects the delivery on the other, whose
    // client has subscribed to the topic.
    private static void deliver(Socket publisher, Socket subscriber, String topic)
            throws IOException {
        byte[] delivery = codec.encode(publication(topic, "delivered", AT_MOST_ONCE, 0));
        publisher.getOutputStream().write(delivery);
        assertThat(subscriber.getInputStream().readNBytes(delivery.length)).isEqualTo(delivery);
    }

    // Sends `request` and expects the bytes of `response` next, within the socket's timeout.
    private static void exchange(Socket socket, Packet request, byte[] response)
            throws IOException {
        socket.getOutputStream().write(codec.encode(request));
        assertThat(socket.getInputStream().readNBytes(response.length)).isEqualTo(response);
    }

    /// A logging handler that holds the thread that gives it a record until the test releases
    /// it, as a handler does while an application thread keeps its lock.
    private static final class HoldingHandler extends Handler {

        final CountDownLatch entered = new CountDownLatch(1);

        final CountDownLatch released = new CountDownLatch(1);

        @Override
        public void publish(LogRecord record) {
            entered.countDown();
            try {
                // The wait has a limit, so that a test that fails cannot hold the thread for
                // good; whether it was released or the limit came makes no difference here.
                boolean _ = released.await(10, SECONDS);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
