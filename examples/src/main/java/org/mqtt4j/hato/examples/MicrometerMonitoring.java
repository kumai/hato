package org.mqtt4j.hato.examples;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.mqtt4j.hato.CapacityLimit;
import org.mqtt4j.hato.ClientStats;
import org.mqtt4j.hato.MetricsListener;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/// Records Hato's timings and snapshots in Micrometer while exchanging a reading with an
/// embedded broker. Uses a local registry; an application can use its existing registry and
/// backend instead. The example owns the registry and closes it after all callbacks return.
public final class MicrometerMonitoring {

    void main() throws IOException, InterruptedException {
        var registry = new SimpleMeterRegistry();
        try (MqttBroker broker = MqttBroker.start()) {
            MqttClient client = MqttClient.builder()
                    .server(URI.create("mqtt://localhost:" + broker.port()))
                    .metricsListener(timings(registry))
                    .build();
            try {
                monitor(client, registry);
            } finally {
                client.close();
                // The listener may still have queued timing events when close() returns.
                client.terminated().join();
            }

            System.out.println("Timed publications: " + registry.get("hato.publish")
                    .tag("outcome", "success").timer().count());
            System.out.println("Timed deliveries: " + registry.get("hato.delivery.wait")
                    .timer().count());
            System.out.println("Timed handlers: " + registry.get("application.mqtt.processing")
                    .timer().count());
        } finally {
            registry.close();
        }
    }

    private static MetricsListener timings(MeterRegistry registry) {
        Timer deliveryWait = registry.timer("hato.delivery.wait", "client", "telemetry");
        return new MetricsListener() {
            @Override
            public void onPublishCompleted(PublishCompletion completion) {
                registry.timer(
                        "hato.publish",
                        "client",
                        "telemetry",
                        "qos",
                        completion.qos().name(),
                        "outcome",
                        completion.failure().isPresent() ? "failure" : "success"
                ).record(completion.duration());
            }

            @Override
            public void onDeliveryStarted(DeliveryStart start) {
                deliveryWait.record(start.waited());
            }
        };
    }

    private static void monitor(MqttClient client, MeterRegistry registry)
            throws InterruptedException {
        var latest = new AtomicReference<>(client.stats().join());
        var collectedAt = new AtomicLong(System.nanoTime());
        Counter failures = registry.counter("hato.stats.failures", "client", "telemetry");
        bindSnapshots(registry, latest, collectedAt);

        // One collector waits for one snapshot at a time. Scrapes read the cached values and
        // never wait for the client. Keep the holders referenced for the meters' lifetime.
        try (ScheduledExecutorService collector = Executors.newSingleThreadScheduledExecutor()) {
            collector.scheduleWithFixedDelay(
                    () -> collect(client, latest, collectedAt, failures),
                    0,
                    1,
                    SECONDS
            );
            exchangeReading(client, registry);
        }
        // Closing the executor stops periodic collection and waits for the pending call.
        collect(client, latest, collectedAt, failures);
        System.out.println("Held deliveries: " + registry.get("hato.receive.held").gauge().value());
        System.out.println("Collection failures: " + failures.count());
    }

    private static void collect(
            MqttClient client,
            AtomicReference<ClientStats> latest,
            AtomicLong collectedAt,
            Counter failures
    ) {
        try {
            latest.set(client.stats().join());
            collectedAt.set(System.nanoTime());
        } catch (RuntimeException failure) {
            // Retain the last good snapshot. Its age and this counter reveal stale data while
            // allowing the next scheduled collection to try again.
            failures.increment();
            System.err.println("Could not collect telemetry client stats: " + failure);
        }
    }

    private static void bindSnapshots(
            MeterRegistry registry,
            AtomicReference<ClientStats> latest,
            AtomicLong collectedAt
    ) {
        Gauge.builder(
                "hato.receive.held",
                latest,
                value -> value.get().receiving().heldDeliveries().used()
        ).tag("client", "telemetry").register(registry);
        Gauge.builder(
                "hato.receive.bytes",
                latest,
                value -> value.get().receiving().heldBytes().used()
        ).tag("client", "telemetry").baseUnit("bytes").register(registry);
        TimeGauge.builder(
                "hato.stats.age",
                collectedAt,
                NANOSECONDS,
                value -> System.nanoTime() - value.get()
        ).tag("client", "telemetry").register(registry);
        FunctionCounter.builder(
                "hato.metrics.dropped",
                latest,
                value -> value.get().droppedMetricEvents()
        ).tag("client", "telemetry").register(registry);

        // Enum-valued reasons keep the number of series bounded. Snapshot counts are already
        // cumulative: adding them to a Counter on each poll would count events repeatedly.
        for (ClientStats.DropReason reason : ClientStats.DropReason.values()) {
            FunctionCounter.builder(
                    "hato.receive.dropped",
                    latest,
                    value -> value.get().receiving().dropped().getOrDefault(reason, 0L)
            ).tag("client", "telemetry").tag("reason", reason.name()).register(registry);
        }
        for (CapacityLimit limit : CapacityLimit.values()) {
            FunctionCounter.builder(
                    "hato.operations.refused",
                    latest,
                    value -> value.get().refusals().getOrDefault(limit, 0L)
            ).tag("client", "telemetry").tag("limit", limit.name()).register(registry);
        }
        for (ClientStats.OverloadCause cause : ClientStats.OverloadCause.values()) {
            FunctionCounter.builder(
                    "hato.overloads",
                    latest,
                    value -> value.get().overloads().getOrDefault(cause, 0L)
            ).tag("client", "telemetry").tag("cause", cause.name()).register(registry);
        }
    }

    private static void exchangeReading(MqttClient client, MeterRegistry registry)
            throws InterruptedException {
        Timer processing = registry.timer("application.mqtt.processing", "client", "telemetry");
        var received = new CountDownLatch(1);
        client.connect().join();
        Subscription subscription = client.subscribe(
                Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_LEAST_ONCE),
                delivery -> {
                    processing.record(() -> System.out.println(
                            delivery.message().topic() + ": "
                                    + new String(delivery.message().payload(), UTF_8)
                    ));
                    received.countDown();
                }
        );
        subscription.established().join();
        client.publish(MqttMessage.of(
                new Topic("sensors/kitchen/temperature"),
                "21.5".getBytes(UTF_8),
                Qos.AT_LEAST_ONCE,
                false
        )).join();
        if (!received.await(5, SECONDS)) {
            throw new IllegalStateException("The reading did not reach the handler");
        }
        subscription.unsubscribe().join();
        subscription.terminated().join();
        client.disconnect().join();
    }
}
