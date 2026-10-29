package org.mqtt4j.hato.client.internal;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.UserProperty;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.RemainingLength;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mqtt4j.hato.client.internal.Measurements.percentile;
import static org.mqtt4j.hato.client.internal.Measurements.print;
import static org.mqtt4j.hato.client.internal.Measurements.runtime;
import static org.mqtt4j.hato.client.internal.Measurements.sorted;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// Measures how the cost of publishing and of receiving grows with the payload, and the cost of
/// publishing with message properties, and prints the results instead of asserting limits: per
/// message, the elapsed time, the processor time of the whole process, and the heap that every
/// thread allocated. A raw peer reads and writes the bytes without decoding them and allocates
/// nothing for each message, so beyond the socket the processor time and the allocation are
/// nearly all the client's. Waits block instead of spinning, so that they do not count as
/// processor time. Each MQTT version is measured; under MQTT 5.0 the peer gives each publication
/// that it sends the Subscription Identifier of the subscription, as a broker that takes
/// identifiers does. Runs only with `-Phato.measure=true`; the integrationTestSingleCarrier task
/// measures the worst case of one carrier thread.
@EnabledIfSystemProperty(named = "hato.measure", matches = "true")
@Timeout(600)
class PayloadMeasurementTest {

    private static final List<MqttVersion> VERSIONS = List.of(MqttVersion.V3_1_1, MqttVersion.V5);

    private static final int RUNS = 5;

    private static final List<Integer> SIZES = List.of(16, 1024, 64 * 1024, 1024 * 1024);

    // A run moves about this many payload bytes, within the message counts below, so that it
    // lasts long enough to measure whatever the payload size.
    private static final long RUN_BYTES = 512L * 1024 * 1024;

    private static final int MOST_MESSAGES = 100_000;

    private static final int LEAST_MESSAGES = 512;

    // The messages unfinished at once: at most the client's default limit of inflight
    // publications, or the receive window that the delivery measurement uses, and at most this
    // many payload bytes, well within the client's send and receive budgets.
    private static final int PUBLISH_WINDOW = 64;

    private static final int RECEIVE_WINDOW = 500;

    private static final int WINDOW_BYTES = 8 * 1024 * 1024;

    // The peer writes publications to the client in batches of at most this many, or of this many
    // bytes, whichever comes first, as a broker writes what it has ready.
    private static final int RECEIVE_BATCH = 50;

    private static final int BATCH_BYTES = 64 * 1024;

    // The payload of the publications that measure message properties.
    private static final int PROPERTIES_PAYLOAD = 16;

    @Test
    void publishing_costs_grow_with_the_payload() throws Exception {
        print(
                "%s: per publication through the client, each a new message, median of %d runs;"
                        + " at most %d or %d MiB outstanding",
                runtime(),
                RUNS,
                PUBLISH_WINDOW,
                WINDOW_BYTES / (1024 * 1024)
        );
        printHeading();
        for (MqttVersion version : VERSIONS) {
            for (Qos qos : List.of(Qos.AT_MOST_ONCE, Qos.AT_LEAST_ONCE)) {
                for (int size : SIZES) {
                    printRow(version, qos, size, median(() -> publishing(size, qos, version)));
                }
            }
        }
    }

    @Test
    void publishing_costs_grow_with_the_message_properties() throws Exception {
        print(
                "%s: per MQTT 5.0 publication of %d bytes through the client, the same message"
                        + " each time so that building it does not count, median of %d runs; at"
                        + " most %d outstanding",
                runtime(),
                PROPERTIES_PAYLOAD,
                RUNS,
                PUBLISH_WINDOW
        );
        print(
                "%-20s | %-13s | %10s | %10s | %12s",
                "properties",
                "qos",
                "ns",
                "cpu ns",
                "allocated B"
        );
        for (Qos qos : List.of(Qos.AT_MOST_ONCE, Qos.AT_LEAST_ONCE)) {
            var plain = MqttMessage.of(
                    new Topic("publish"),
                    new byte[PROPERTIES_PAYLOAD],
                    qos,
                    false
            );
            for (PropertySet set : List.of(
                    new PropertySet("none", plain),
                    new PropertySet("request", request(plain)),
                    new PropertySet("100 user properties", userProperties(plain, 100))
            )) {
                Cost cost = median(() -> publishing(
                        MOST_MESSAGES,
                        PROPERTIES_PAYLOAD,
                        MqttVersion.V5,
                        set::message
                ));
                print(
                        "%-20s | %-13s | %10d | %10d | %12d",
                        set.name(),
                        qos,
                        cost.nanos(),
                        cost.processorNanos(),
                        cost.allocatedBytes()
                );
            }
        }
    }

    @Test
    void receiving_costs_grow_with_the_payload() throws Exception {
        print(
                "%s: per publication received through the client, no-op handler, median of %d"
                        + " runs; at most %d or %d MiB unfinished",
                runtime(),
                RUNS,
                RECEIVE_WINDOW,
                WINDOW_BYTES / (1024 * 1024)
        );
        printHeading();
        for (MqttVersion version : VERSIONS) {
            for (Qos qos : List.of(Qos.AT_MOST_ONCE, Qos.AT_LEAST_ONCE)) {
                for (int size : SIZES) {
                    printRow(version, qos, size, median(() -> receiving(size, qos, version)));
                }
            }
        }
    }

    /// What a run cost per message.
    private record Cost(long nanos, long processorNanos, long allocatedBytes) {}

    /// A message that carries a set of properties, and the name of the set.
    private record PropertySet(String name, MqttMessage message) {}

    /// A message like `plain` with the properties of a request that asks for a reply: a UTF-8
    /// payload, a minute's expiry, a content type, a response topic, 16 bytes of Correlation
    /// Data, and three User Properties.
    private static MqttMessage request(MqttMessage plain) {
        return plain.withPayloadUtf8(true)
                .withMessageExpiry(Duration.ofMinutes(1))
                .withContentType("application/json")
                .withResponseTopic(new Topic("replies/client-1"))
                .withCorrelationData(new byte[16])
                .withUserProperties(List.of(
                        new UserProperty("trace-id", "4bf92f3577b34da6a3ce929d0e0e4736"),
                        new UserProperty("source", "sensor-12"),
                        new UserProperty("unit", "celsius")
                ));
    }

    /// A message like `plain` with `count` User Properties, each a short name and a value of 32
    /// characters.
    private static MqttMessage userProperties(MqttMessage plain, int count) {
        var properties = new ArrayList<UserProperty>();
        for (int index = 0; index < count; index++) {
            properties.add(new UserProperty("key-" + index, "v".repeat(32)));
        }
        return plain.withUserProperties(properties);
    }

    @FunctionalInterface
    private interface CostRun {

        Cost run() throws Exception;
    }

    @FunctionalInterface
    private interface Body {

        void run() throws Exception;
    }

    /// Publishes the run's messages of `size` bytes at `qos` over `version`, each a new message
    /// as an application would make it, keeping a window outstanding, and returns the cost per
    /// publication until the last has completed: QoS 0 once written and QoS 1 at PUBACK.
    private static Cost publishing(int size, Qos qos, MqttVersion version) throws Exception {
        var topic = new Topic("publish");
        var payload = new byte[size];
        return publishing(
                messages(size),
                size,
                version,
                () -> MqttMessage.of(topic, payload, qos, false)
        );
    }

    /// Publishes `messages` messages of `size` payload bytes that `next` gives over `version`,
    /// keeping a window outstanding, and returns the cost per publication until the last has
    /// completed: QoS 0 once written and QoS 1 at PUBACK.
    private static Cost publishing(
            int messages,
            int size,
            MqttVersion version,
            Supplier<MqttMessage> next
    ) throws Exception {
        var outstanding = new Semaphore(Math.clamp(WINDOW_BYTES / size, 1, PUBLISH_WINDOW));
        var completed = new CountDownLatch(messages);
        var failure = new AtomicReference<@Nullable Throwable>();
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = ScriptedBroker.client(server, version);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut, version)) {
            Thread draining = Thread.ofPlatform().start(() -> drain(peer, messages, version));
            Cost cost = measure(messages, () -> {
                for (int index = 0; index < messages; index++) {
                    if (!outstanding.tryAcquire(1, TimeUnit.MINUTES)) {
                        throw new AssertionError("No publication completed for a minute");
                    }
                    MqttMessage message = next.get();
                    CompletableFuture<PublishResult> publication = sut.publish(message);
                    // A publication's operation slot returns once the continuations of its
                    // future have returned, just after the one below has made room here, so a
                    // refusal for operation capacity is tried again.
                    while (refusedForCapacity(publication)) {
                        Thread.onSpinWait();
                        publication = sut.publish(message);
                    }
                    publication.whenComplete((_, cause) -> {
                        if (cause != null) {
                            failure.compareAndSet(null, cause);
                        }
                        outstanding.release();
                        completed.countDown();
                    });
                }
                if (!completed.await(1, TimeUnit.MINUTES)) {
                    throw new AssertionError("Not every publication completed");
                }
            });
            draining.join();
            Throwable failed = failure.get();
            if (failed != null) {
                throw new AssertionError("A publication failed", failed);
            }
            return cost;
        }
    }

    /// Sends the run's messages of `size` bytes at `qos` over `version` from the peer to a
    /// subscription whose handler does nothing, keeping a window unfinished, and returns the cost
    /// per message until the last has finished: QoS 0 once the handler has it and QoS 1 at
    /// PUBACK. The publications are encoded before the clock starts and written again and again,
    /// so the time is mostly the client's.
    private static Cost receiving(int size, Qos qos, MqttVersion version) throws Exception {
        int batch = Math.clamp(BATCH_BYTES / size, 1, RECEIVE_BATCH);
        int window = Math.max(batch, Math.min(RECEIVE_WINDOW, WINDOW_BYTES / size));
        int writes = Math.ceilDiv(messages(size), batch);
        int messages = writes * batch;
        var room = new Semaphore(window);
        var finished = new CountDownLatch(messages);
        Runnable finish = () -> {
            room.release();
            finished.countDown();
        };
        Consumer<MqttDelivery> handler = qos == Qos.AT_MOST_ONCE ? _ -> finish.run() : _ -> { };
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = ScriptedBroker.client(server, version);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut, version)) {
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("receive"), qos),
                    handler
            );
            var request = (Packet.Subscribe) peer.receive();
            peer.send(new Packet.SubAck(
                    request.packetId(),
                    List.of(ReasonCode.of(qos.level())),
                    EMPTY
            ));
            subscription.established().get(5, SECONDS);
            // Under QoS 1 a write reuses the packet identifiers of the write that used the same
            // batch before it. More batches than the window holds leave that one finished.
            byte[][] batches = batches(
                    size,
                    qos,
                    batch,
                    window / batch + 2,
                    PacketCodec.of(version),
                    ScriptedBroker.forwarding(request)
            );
            Thread counting = Thread.ofPlatform().start(() -> acknowledgements(
                    peer,
                    qos == Qos.AT_MOST_ONCE ? 0 : messages,
                    finish
            ));
            OutputStream output = peer.output();
            Cost cost = measure(messages, () -> {
                for (int write = 0; write < writes; write++) {
                    if (!room.tryAcquire(batch, 1, TimeUnit.MINUTES)) {
                        throw new AssertionError("No message finished for a minute");
                    }
                    output.write(batches[write % batches.length]);
                }
                if (!finished.await(1, TimeUnit.MINUTES)) {
                    throw new AssertionError("Not every message finished");
                }
            });
            counting.join();
            return cost;
        }
    }

    /// The prepared writes: `count` batches of `batch` publications of `size` bytes at `qos`,
    /// encoded by `codec` with `properties`. Under QoS 1 every publication in them has its own
    /// packet identifier.
    private static byte[][] batches(
            int size,
            Qos qos,
            int batch,
            int count,
            PacketCodec codec,
            Properties properties
    ) {
        var payload = new byte[size];
        var batches = new byte[count][];
        for (int index = 0; index < count; index++) {
            var bytes = new ByteArrayOutputStream();
            for (int position = 0; position < batch; position++) {
                int packetId = qos == Qos.AT_MOST_ONCE ? 0 : index * batch + position + 1;
                bytes.writeBytes(codec.encode(new Packet.Publish(
                        new Topic("receive"),
                        payload,
                        qos,
                        false,
                        false,
                        packetId,
                        properties
                )));
            }
            batches[index] = bytes.toByteArray();
        }
        return batches;
    }

    /// Reads the client's `messages` publications in `version` without decoding them, so that
    /// the peer allocates nothing for each, and answers QoS 1 with PUBACK. The answers to
    /// publications that arrived together go out in one write.
    private static void drain(ScriptedBroker peer, int messages, MqttVersion version) {
        try {
            InputStream input = peer.input();
            OutputStream output = peer.output();
            var scratch = new byte[64 * 1024];
            var answers = new byte[4 * 1024];
            int answered = 0;
            for (int received = 0; received < messages; received++) {
                int header = read(input);
                int length = remainingLength(input);
                int qos = (header >>> 1) & 3;
                if (header >>> 4 != 3 || qos > 1) {
                    throw new AssertionError("Unexpected packet: " + Integer.toHexString(header));
                }
                // A PUBLISH names its topic, and gives a packet identifier at QoS 1, before the
                // payload. A PUBACK of two bytes after its fixed header means success in both
                // versions.
                int topicLength = (read(input) << 8) | read(input);
                skip(input, scratch, topicLength);
                length -= 2 + topicLength;
                if (qos == 1) {
                    answers[answered++] = 0x40;
                    answers[answered++] = 2;
                    answers[answered++] = (byte) read(input);
                    answers[answered++] = (byte) read(input);
                    length -= 2;
                }
                // An MQTT 5.0 PUBLISH then gives the length of its properties, which the skip
                // below passes over together with the payload.
                if (version == MqttVersion.V5) {
                    length -= RemainingLength.encodedSize(remainingLength(input));
                }
                skip(input, scratch, length);
                if (answered > 0 && (answered == answers.length || input.available() == 0)) {
                    output.write(answers, 0, answered);
                    answered = 0;
                }
            }
        } catch (IOException cause) {
            throw new UncheckedIOException(cause);
        }
    }

    /// Reads the client's packets without decoding them and finishes a message for each of the
    /// `messages` PUBACKs.
    private static void acknowledgements(ScriptedBroker peer, int messages, Runnable finish) {
        try {
            InputStream input = peer.input();
            var scratch = new byte[16];
            for (int acknowledged = 0; acknowledged < messages; acknowledged++) {
                int header = read(input);
                if (header != 0x40) {
                    throw new AssertionError("Unexpected packet: " + Integer.toHexString(header));
                }
                skip(input, scratch, remainingLength(input));
                finish.run();
            }
        } catch (IOException cause) {
            throw new UncheckedIOException(cause);
        }
    }

    private static int read(InputStream input) throws IOException {
        int value = input.read();
        if (value == -1) {
            throw new EOFException("The client closed the connection");
        }
        return value;
    }

    private static int remainingLength(InputStream input) throws IOException {
        int length = 0;
        int shift = 0;
        int digit;
        do {
            digit = read(input);
            length |= (digit & 0x7F) << shift;
            shift += 7;
        } while ((digit & 0x80) != 0);
        return length;
    }

    /// Reads and drops `count` bytes into the reused `scratch` array.
    private static void skip(InputStream input, byte[] scratch, int count) throws IOException {
        for (int left = count; left > 0; ) {
            int read = input.readNBytes(scratch, 0, Math.min(scratch.length, left));
            if (read == 0) {
                throw new EOFException("The client closed the connection");
            }
            left -= read;
        }
    }

    /// Runs `body` once and returns its cost per message.
    private static Cost measure(int messages, Body body) throws Exception {
        long allocated = Measurements.allocatedBytes();
        long processor = Measurements.processorNanos();
        long start = System.nanoTime();
        body.run();
        long elapsed = System.nanoTime() - start;
        return new Cost(
                elapsed / messages,
                (Measurements.processorNanos() - processor) / messages,
                (Measurements.allocatedBytes() - allocated) / messages
        );
    }

    /// The median of each part of the cost over [#RUNS] runs of `run`, after one more run that
    /// warms up the code paths.
    private static Cost median(CostRun run) throws Exception {
        Cost _ = run.run();
        var nanos = new long[RUNS];
        var processorNanos = new long[RUNS];
        var allocatedBytes = new long[RUNS];
        for (int index = 0; index < RUNS; index++) {
            Cost cost = run.run();
            nanos[index] = cost.nanos();
            processorNanos[index] = cost.processorNanos();
            allocatedBytes[index] = cost.allocatedBytes();
        }
        return new Cost(
                percentile(sorted(nanos), 50),
                percentile(sorted(processorNanos), 50),
                percentile(sorted(allocatedBytes), 50)
        );
    }

    private static int messages(int size) {
        return Math.clamp(RUN_BYTES / size, LEAST_MESSAGES, MOST_MESSAGES);
    }

    private static boolean refusedForCapacity(CompletableFuture<PublishResult> publication) {
        return publication.state() == Future.State.FAILED
                && publication.exceptionNow() instanceof MqttCapacityException;
    }

    private static void printHeading() {
        print(
                "%-7s | %-13s | %9s | %8s | %10s | %10s | %12s | %8s",
                "version",
                "qos",
                "payload",
                "messages",
                "ns",
                "cpu ns",
                "allocated B",
                "x payload"
        );
    }

    private static void printRow(MqttVersion version, Qos qos, int size, Cost cost) {
        print(
                "%-7s | %-13s | %9d | %8d | %10d | %10d | %12d | %8.2f",
                version,
                qos,
                size,
                messages(size),
                cost.nanos(),
                cost.processorNanos(),
                cost.allocatedBytes(),
                (double) cost.allocatedBytes() / size
        );
    }
}
