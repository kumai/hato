package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.ClientStats;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.io.FrameReader;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A raw broker for the keep-alive backlog measurements. It accepts one client and establishes its
/// subscriptions, then streams QoS 0 publications at it until the client's first PINGREQ
/// arrives. A platform thread, which keeps the broker's clock independent of virtual threads,
/// answers each PINGREQ and records how long the client goes without sending a packet. Writes
/// stop without an exception once the client has closed the connection, whose reason
/// [#ended()] then reports.
final class KeepAliveBroker implements AutoCloseable {

    static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    // Publications in each write of the stream: few enough that the first PINGRESP waits for
    // little more than what the socket buffers already hold.
    private static final int CHUNK = 1024;

    private final ServerSocket server;

    private final MqttClient client;

    private final LinkedBlockingQueue<Disconnection> ended = new LinkedBlockingQueue<>();

    // Held for each write. It is fair, so that the thread that answers a PINGREQ writes right
    // after the write of the stream in progress: an unfair lock lets the stream take it back
    // again and again while the client reads on, until the client's PINGRESP deadline expires.
    private final ReentrantLock writing = new ReentrantLock(true);

    // Counted down by the thread that answers PINGREQs once it holds the lock for writing, before
    // it answers the first one, so that whatever is sent after the stream ends follows that
    // answer.
    private final CountDownLatch pinged = new CountDownLatch(1);

    private @Nullable Socket socket;

    private @Nullable FrameReader reader;

    private @Nullable OutputStream output;

    // The fields below are guarded by this monitor.
    private long started;

    private long last;

    private long longest;

    private long firstPing = -1;

    private long answerEnded;

    KeepAliveBroker(Duration keepAlive) throws IOException {
        server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        client = MqttClient.builder()
                .server(URI.create("mqtt://127.0.0.1:" + server.getLocalPort()))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .keepAlive(keepAlive)
                .connectionListener(ended::add)
                .build();
    }

    /// The client under test, which closes with this broker.
    MqttClient client() {
        return client;
    }

    /// The ends of the client's connection, which a passing test leaves empty.
    LinkedBlockingQueue<Disconnection> ended() {
        return ended;
    }

    void connect() throws Exception {
        CompletableFuture<ConnAck> connecting = client.connect();
        Socket accepted = server.accept();
        socket = accepted;
        reader = new FrameReader(accepted.getInputStream(), 1024 * 1024);
        output = accepted.getOutputStream();
        assertThat(receive()).isInstanceOf(Packet.Connect.class);
        send(codec.encode(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY)));
        connecting.get(5, SECONDS);
    }

    void subscribe(String filter, Qos qos, Consumer<MqttDelivery> handler) throws Exception {
        Subscription subscription = client.subscribe(
                Subscribe.of(new TopicFilter(filter), qos),
                handler
        );
        var request = (Packet.Subscribe) receive();
        send(codec.encode(new Packet.SubAck(
                request.packetId(),
                List.of(ReasonCode.of(qos.level())),
                EMPTY
        )));
        subscription.established().get(5, SECONDS);
    }

    /// Starts answering PINGREQs, the first one after `beforeFirstAnswer`, and measures the
    /// client's silence from now on.
    void answerPings(byte[] beforeFirstAnswer) {
        synchronized (this) {
            started = System.nanoTime();
            last = started;
        }
        Thread.ofPlatform().daemon().start(() -> answer(beforeFirstAnswer));
    }

    /// Streams `publication` until the client's first PINGREQ arrives, the client closes the
    /// connection, or `limit` passes, and returns how many publications the broker sent.
    long streamUntilPinged(byte[] publication, Duration limit) {
        byte[] chunk = repeat(publication, CHUNK);
        long deadline = System.nanoTime() + limit.toNanos();
        long sent = 0;
        while (pinged.getCount() > 0 && System.nanoTime() < deadline) {
            writing.lock();
            try {
                if (pinged.getCount() == 0 || !write(chunk)) {
                    break;
                }
            } finally {
                writing.unlock();
            }
            sent += CHUNK;
        }
        return sent;
    }

    /// How long after the start of [#answerPings(byte[])] the client's first PINGREQ arrived.
    synchronized Duration firstPing() {
        assertThat(firstPing).as("first PINGREQ").isNotNegative();
        return Duration.ofNanos(firstPing - started);
    }

    /// How long after the client's first PINGREQ arrived the broker finished sending the answer,
    /// which the client reads only as fast as it reads what lies before it; until now if the
    /// connection ended first.
    synchronized Duration firstAnswer() {
        long end = answerEnded == 0 ? System.nanoTime() : answerEnded;
        return Duration.ofNanos(end - started).minus(firstPing());
    }

    /// The longest time without a packet from the client since [#answerPings(byte[])], counting
    /// the time since its last packet.
    synchronized Duration longestSilence() {
        return Duration.ofNanos(Math.max(longest, System.nanoTime() - last));
    }

    void send(byte[] bytes) {
        writing.lock();
        try {
            boolean _ = write(bytes);
        } finally {
            writing.unlock();
        }
    }

    static byte[] publication(String topic) {
        return codec.encode(new Packet.Publish(
                new Topic(topic),
                new byte[1],
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        ));
    }

    /// Each of the 256 filters keeps or replaces each of eight levels with +, so all of them
    /// match the topic a/b/c/d/e/f/g/h.
    static String filter(int index) {
        var filter = new StringBuilder();
        for (int level = 0; level < 8; level++) {
            if (level > 0) {
                filter.append('/');
            }
            filter.append((index & 1 << level) != 0 ? "+" : String.valueOf((char) ('a' + level)));
        }
        return filter.toString();
    }

    /// The QoS 0 deliveries that the client has accounted for: the `handled` ones, those that it
    /// dropped because its buffers or a subscription's buffer were full, and one for each of
    /// `subscriptions` for every publication that it dropped before routing, as it does while it
    /// reads on towards a PINGRESP. A delivery dropped for any other reason does not count, so it
    /// fails a test that compares the result with the deliveries sent.
    static long accounted(MqttClient client, long handled, int subscriptions) throws Exception {
        // The client takes a request for its stats after the packets that it received before,
        // which a busy machine can take many seconds to route.
        ClientStats.Receiving receiving = client.stats().get(60, SECONDS).receiving();
        Map<ClientStats.DropReason, Long> dropped = receiving.dropped();
        return handled
                + dropped.getOrDefault(ClientStats.DropReason.CLIENT_FULL, 0L)
                + dropped.getOrDefault(ClientStats.DropReason.SUBSCRIPTION_FULL, 0L)
                + receiving.unrouted() * subscriptions;
    }

    static byte[] repeat(byte[] bytes, int count) {
        var repeated = new byte[bytes.length * count];
        for (int index = 0; index < count; index++) {
            System.arraycopy(bytes, 0, repeated, index * bytes.length, bytes.length);
        }
        return repeated;
    }

    @Override
    public void close() throws IOException {
        Socket accepted = socket;
        try {
            if (accepted != null) {
                accepted.close();
            }
        } finally {
            try {
                client.close();
            } finally {
                server.close();
            }
        }
    }

    private void answer(byte[] beforeFirstAnswer) {
        try {
            while (true) {
                Packet packet = receive();
                received(packet);
                if (packet instanceof Packet.PingReq) {
                    writing.lock();
                    try {
                        OutputStream out = requireOutput();
                        if (pinged.getCount() > 0) {
                            pinged.countDown();
                            out.write(beforeFirstAnswer);
                        }
                        out.write(codec.encode(new Packet.PingResp()));
                        synchronized (this) {
                            if (answerEnded == 0) {
                                answerEnded = System.nanoTime();
                            }
                        }
                    } finally {
                        writing.unlock();
                    }
                }
            }
        } catch (IOException | RuntimeException _) {
            // The test closes the socket when it is done.
        }
    }

    private synchronized void received(Packet packet) {
        long now = System.nanoTime();
        longest = Math.max(longest, now - last);
        last = now;
        if (packet instanceof Packet.PingReq && firstPing < 0) {
            firstPing = now;
        }
    }

    // Called with the lock for writing held.
    private boolean write(byte[] bytes) {
        try {
            requireOutput().write(bytes);
            return true;
        } catch (IOException _) {
            return false;
        }
    }

    private Packet receive() throws IOException {
        return codec.decode(requireNonNull(reader, "connection").read().orElseThrow());
    }

    private OutputStream requireOutput() {
        return requireNonNull(output, "connection");
    }
}
