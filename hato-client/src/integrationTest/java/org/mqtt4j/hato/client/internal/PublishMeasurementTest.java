package org.mqtt4j.hato.client.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mqtt4j.hato.client.internal.Measurements.median;
import static org.mqtt4j.hato.client.internal.Measurements.percentile;
import static org.mqtt4j.hato.client.internal.Measurements.print;
import static org.mqtt4j.hato.client.internal.Measurements.runtime;
import static org.mqtt4j.hato.client.internal.Measurements.sorted;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// Measures publishing through the client and prints the results instead of asserting limits:
/// what a publication costs at each QoS while many are outstanding, with and without a metrics
/// listener, and how long a publication takes to complete when it is the only one outstanding.
/// A raw peer answers at once, writing its answers to what has arrived together. Runs only with
/// `-Phato.measure=true`; the integrationTestSingleCarrier task measures the worst case of one
/// carrier thread.
@EnabledIfSystemProperty(named = "hato.measure", matches = "true")
@Timeout(600)
class PublishMeasurementTest {

    private static final int RUNS = 5;

    private static final int PUBLICATIONS = 100_000;

    // The publications outstanding at once: the client's default limit of inflight publications.
    private static final int WINDOW = 64;

    private static final int LATENCIES = 10_000;

    @Test
    void publishing_through_the_client_costs() throws Exception {
        print(
                "%s: nanoseconds per publication through the client with %d outstanding, median"
                        + " of %d runs of %d publications of 16 bytes",
                runtime(),
                WINDOW,
                RUNS,
                PUBLICATIONS
        );
        print(
                "%-16s | %12s | %13s | %12s",
                "client",
                Qos.AT_MOST_ONCE,
                Qos.AT_LEAST_ONCE,
                Qos.EXACTLY_ONCE
        );
        for (boolean metrics : List.of(false, true)) {
            var nanos = new long[Qos.values().length];
            for (Qos qos : Qos.values()) {
                nanos[qos.ordinal()] = median(RUNS, () -> publishNanos(qos, metrics));
            }
            print(
                    "%-16s | %12d | %13d | %12d",
                    metrics ? "metrics listener" : "no listener",
                    nanos[0],
                    nanos[1],
                    nanos[2]
            );
        }
    }

    @Test
    void a_publication_alone_completes_within() throws Exception {
        print(
                "%s: microseconds from publish to completion with one publication outstanding,"
                        + " %d publications of 16 bytes",
                runtime(),
                LATENCIES
        );
        print("%-14s | %s", "qos", "p50 / p99 / max");
        for (Qos qos : Qos.values()) {
            long[] latencies = sorted(latencies(qos));
            print(
                    "%-14s | %.1f / %.1f / %.1f",
                    qos,
                    micros(percentile(latencies, 50)),
                    micros(percentile(latencies, 99)),
                    micros(latencies[latencies.length - 1])
            );
        }
    }

    /// Publishes [#PUBLICATIONS] messages at `qos`, keeping at most [#WINDOW] outstanding, and
    /// returns the time per publication until the last has completed: QoS 0 once written, QoS 1
    /// at PUBACK, and QoS 2 at PUBCOMP.
    private static long publishNanos(Qos qos, boolean metrics) throws Exception {
        MqttMessage message = MqttMessage.of(new Topic("publish"), new byte[16], qos, false);
        var outstanding = new Semaphore(WINDOW);
        var completed = new CountDownLatch(PUBLICATIONS);
        var failure = new AtomicReference<@Nullable Throwable>();
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = metrics
                        ? ScriptedBroker.client(server, Measurements.countingListener())
                        : ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            Thread answering = Thread.ofPlatform().start(() -> answer(peer, PUBLICATIONS));
            long start = System.nanoTime();
            for (int index = 0; index < PUBLICATIONS; index++) {
                if (!outstanding.tryAcquire(1, TimeUnit.MINUTES)) {
                    throw new AssertionError("No publication completed for a minute");
                }
                CompletableFuture<PublishResult> publication = sut.publish(message);
                // A publication's operation slot returns once the continuations of its future
                // have returned, just after the one below has made room here, so a refusal for
                // operation capacity is tried again.
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
            long elapsed = System.nanoTime() - start;
            answering.join();
            Throwable failed = failure.get();
            if (failed != null) {
                throw new AssertionError("A publication failed", failed);
            }
            return elapsed / PUBLICATIONS;
        }
    }

    /// Publishes [#LATENCIES] messages at `qos` one at a time and returns the time from each
    /// publish to its completion.
    private static long[] latencies(Qos qos) throws Exception {
        MqttMessage message = MqttMessage.of(new Topic("publish"), new byte[16], qos, false);
        try (ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            Thread answering = Thread.ofPlatform().start(() -> answer(peer, 2 * LATENCIES));
            // The first round warms up the code paths.
            for (int index = 0; index < LATENCIES; index++) {
                sut.publish(message).get(5, SECONDS);
            }
            var latencies = new long[LATENCIES];
            for (int index = 0; index < LATENCIES; index++) {
                long start = System.nanoTime();
                sut.publish(message).get(5, SECONDS);
                latencies[index] = System.nanoTime() - start;
            }
            answering.join();
            return latencies;
        }
    }

    /// Answers the client until `publications` have finished: QoS 1 with PUBACK, QoS 2 with
    /// PUBREC and then PUBCOMP for its PUBREL. QoS 0 needs no answer. The answers to packets
    /// that arrived together go out in one write.
    private static void answer(ScriptedBroker peer, int publications) {
        var codec = PacketCodec.of(MqttVersion.V3_1_1);
        var answers = new ByteArrayOutputStream();
        int finished = 0;
        try {
            while (finished < publications) {
                switch (peer.receive()) {
                    case Packet.Publish publication -> {
                        int id = publication.packetId();
                        switch (publication.qos()) {
                            case AT_MOST_ONCE -> finished++;
                            case AT_LEAST_ONCE -> {
                                answers.writeBytes(codec.encode(
                                        new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY)
                                ));
                                finished++;
                            }
                            case EXACTLY_ONCE -> answers.writeBytes(codec.encode(
                                    new Packet.PubRec(id, ReasonCode.SUCCESS, EMPTY)
                            ));
                        }
                    }
                    case Packet.PubRel release -> {
                        answers.writeBytes(codec.encode(
                                new Packet.PubComp(release.packetId(), ReasonCode.SUCCESS, EMPTY)
                        ));
                        finished++;
                    }
                    case Packet.PingReq _ -> answers.writeBytes(
                            codec.encode(new Packet.PingResp())
                    );
                    case Packet other -> throw new AssertionError("Unexpected packet: " + other);
                }
                if (answers.size() > 0 && !peer.hasPending()) {
                    peer.send(answers.toByteArray());
                    answers.reset();
                }
            }
        } catch (IOException cause) {
            throw new UncheckedIOException(cause);
        }
    }

    private static boolean refusedForCapacity(CompletableFuture<PublishResult> publication) {
        return publication.state() == Future.State.FAILED
                && publication.exceptionNow() instanceof MqttCapacityException;
    }

    private static double micros(long nanos) {
        return nanos / 1e3;
    }
}
