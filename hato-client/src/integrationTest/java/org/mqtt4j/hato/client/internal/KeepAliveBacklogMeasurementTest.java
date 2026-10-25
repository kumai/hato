package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.Qos;

import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.client.internal.KeepAliveBroker.accounted;
import static org.mqtt4j.hato.client.internal.KeepAliveBroker.filter;
import static org.mqtt4j.hato.client.internal.KeepAliveBroker.publication;
import static org.mqtt4j.hato.client.internal.KeepAliveBroker.repeat;
import static org.mqtt4j.hato.client.internal.Measurements.percentile;
import static org.mqtt4j.hato.client.internal.Measurements.print;
import static org.mqtt4j.hato.client.internal.Measurements.runtime;
import static org.mqtt4j.hato.client.internal.Measurements.sorted;

/// Measures how much of its keep-alive margins the client uses during a receive backlog, at the
/// shortest interval of one second, and prints the results instead of asserting limits: how late
/// the first PINGREQ goes out while a fan-out to 256 subscriptions keeps the client busy, against
/// the half interval that a broker allows, and how long the client takes to read through a
/// backlog to the PINGRESP behind it, against its own deadline of one interval. The scheduling of
/// the client's threads sets both, so they vary with the machine and its load, which is why no
/// build runs them; `MailboxTest` checks without real time that keep-alive events overtake a
/// backlog. Runs only with `-Phato.measure=true`; the integrationTestSingleCarrier task
/// measures the worst case of one carrier thread.
@EnabledIfSystemProperty(named = "hato.measure", matches = "true")
@Timeout(600)
class KeepAliveBacklogMeasurementTest {

    private static final Duration KEEP_ALIVE = Duration.ofSeconds(1);

    private static final int RUNS = 5;

    private static final Duration STREAM_LIMIT = Duration.ofSeconds(30);

    // The publications that lay before the PINGRESP when the client came to read on in the first
    // version of the test of a paused reader: about 4.5 MB.
    private static final int BEHIND = 225_000;

    @Test
    void a_fan_out_to_256_subscriptions_delays_the_pingreq() throws Exception {
        var late = new long[RUNS];
        var silences = new long[RUNS];
        int lost = 0;
        for (int run = 0; run < RUNS; run++) {
            var handled = new AtomicLong();
            try (var broker = new KeepAliveBroker(KEEP_ALIVE)) {
                MqttClient client = broker.client();
                broker.connect();
                for (int i = 0; i < 256; i++) {
                    broker.subscribe(filter(i), Qos.AT_MOST_ONCE, _ -> handled.incrementAndGet());
                }
                broker.answerPings(new byte[0]);
                long published = broker.streamUntilPinged(
                        publication("a/b/c/d/e/f/g/h"),
                        STREAM_LIMIT
                );
                long deliveries = published * 256;
                await().atMost(Duration.ofSeconds(60)).until(() ->
                        accounted(client, handled.get(), 256) == deliveries
                                || !broker.ended().isEmpty());
                late[run] = broker.firstPing().minus(KEEP_ALIVE).toMillis();
                silences[run] = broker.longestSilence().toMillis();
                lost += broker.ended().isEmpty() ? 0 : 1;
            }
        }
        print("%s: a fan-out to 256 subscriptions until the first PINGREQ, keep-alive %d s",
                runtime(), KEEP_ALIVE.toSeconds());
        print("%-34s | %6s | %6s", "", "p50", "max");
        row("first PINGREQ after it was due, ms", late);
        row("longest silence, ms (broker: 1500)", silences);
        print("connections lost: %d of %d", lost, RUNS);
    }

    @Test
    void reading_through_a_backlog_delays_the_pingresp() throws Exception {
        byte[] message = publication("a/b/c/d/e/f/g/h");
        var reads = new long[RUNS];
        var rates = new long[RUNS];
        int lost = 0;
        for (int run = 0; run < RUNS; run++) {
            try (var broker = new KeepAliveBroker(KEEP_ALIVE)) {
                broker.connect();
                for (int i = 0; i < 32; i++) {
                    broker.subscribe(filter(i), Qos.AT_MOST_ONCE, _ -> { });
                }
                broker.answerPings(repeat(message, BEHIND));
                long _ = broker.streamUntilPinged(message, STREAM_LIMIT);
                // One more interval: a PINGRESP that came too late has ended the connection.
                Thread.sleep(KEEP_ALIVE.toMillis() * 3 / 2);
                Duration read = broker.firstAnswer();
                reads[run] = read.toMillis();
                rates[run] = (long) message.length * BEHIND * 1_000_000_000L
                        / Math.max(1, read.toNanos()) / 1_000_000;
                lost += broker.ended().isEmpty() ? 0 : 1;
            }
        }
        print("%s: reading through %d publications (%.1f MB) to the PINGRESP after a paused "
                + "reader, keep-alive %d s", runtime(), BEHIND,
                (double) message.length * BEHIND / 1_000_000, KEEP_ALIVE.toSeconds());
        print("%-34s | %6s | %6s", "", "p50", "max");
        row("read through, ms (deadline 1000)", reads);
        print("%-34s | %6d | %6d", "read rate, MB/s (p50, min)",
                percentile(sorted(rates), 50), sorted(rates)[0]);
        print("connections lost: %d of %d", lost, RUNS);
    }

    private static void row(String name, long[] values) {
        long[] sorted = sorted(values);
        print("%-34s | %6d | %6d", name, percentile(sorted, 50), sorted[sorted.length - 1]);
    }
}
