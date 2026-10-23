package org.mqtt4j.hato.comparison;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;

/// Samples how many messages one queue of a RabbitMQ container holds while a run goes on, and
/// keeps the most: ready messages and messages delivered but not yet acknowledged together.
///
/// For an MQTT subscriber with a clean session, RabbitMQ names the queue
/// `mqtt-subscription-<client id>qos1` for its QoS 1 subscriptions. Messages beyond the
/// subscriber's prefetch wait there, so the samples show how much a burst backed up in the broker.
/// Each sample runs `rabbitmqctl` in the container, which takes a while; samples miss what comes
/// and goes between them.
final class QueueProbe implements AutoCloseable {

    private static final Duration PERIOD = Duration.ofMillis(250);

    private final GenericContainer<?> broker;

    private final String queue;

    // The most messages seen in the queue, or -1 before any sample has found it.
    private final AtomicLong peak = new AtomicLong(-1);

    private final Thread sampler;

    private volatile boolean stopped;

    QueueProbe(GenericContainer<?> broker, String queue) {
        this.broker = broker;
        this.queue = queue;
        this.sampler = Thread.ofPlatform().name("queue-probe").daemon().unstarted(this::sample);
        sampler.start();
    }

    /// The most messages that a sample found in the queue so far, or -1 when no sample found it.
    long peak() {
        return peak.get();
    }

    private void sample() {
        while (!stopped) {
            try {
                ExecResult result = broker.execInContainer(
                        "rabbitmqctl",
                        "-q",
                        "list_queues",
                        "--no-table-headers",
                        "name",
                        "messages"
                );
                for (String line : result.getStdout().split("\n")) {
                    String[] columns = line.trim().split("\\s+");
                    if (columns.length == 2 && columns[0].equals(queue)) {
                        peak.accumulateAndGet(Long.parseLong(columns[1]), Math::max);
                    }
                }
                Thread.sleep(PERIOD);
            } catch (InterruptedException _) {
                return;
            } catch (Exception e) {
                System.err.println("queue probe: " + e);
                return;
            }
        }
    }

    /// Stops sampling, waiting a little for a sample in progress.
    @Override
    public void close() {
        stopped = true;
        try {
            sampler.join(Duration.ofSeconds(10));
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }
}
