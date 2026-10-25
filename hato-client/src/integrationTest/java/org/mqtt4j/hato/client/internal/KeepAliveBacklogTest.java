package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.client.internal.KeepAliveBroker.codec;
import static org.mqtt4j.hato.client.internal.KeepAliveBroker.filter;
import static org.mqtt4j.hato.client.internal.KeepAliveBroker.publication;
import static org.mqtt4j.hato.client.internal.KeepAliveBroker.repeat;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A raw broker streams QoS 0 publications that the client routes more slowly than it reads them,
/// until the client's first PINGREQ arrives, and checks that a reader paused by the backlog reads
/// on towards the PINGRESP. Streaming until then makes keep-alive come due during a backlog
/// however fast the machine runs, and reading on shows as publications dropped before routing,
/// whatever the client's speed. The time bounds leave room for the client's scheduling delays,
/// which `KeepAliveBacklogMeasurementTest` measures; `ClientActorTest` checks without real time
/// that keep-alive events overtake a backlog.
class KeepAliveBacklogTest {

    private static final Duration KEEP_ALIVE = Duration.ofSeconds(2);

    // A broker closes a connection that sends nothing for one and a half intervals; this bound
    // adds room for the client's scheduling delays.
    private static final Duration SILENCE_LIMIT = KEEP_ALIVE.multipliedBy(5).dividedBy(2);

    // How long the broker streams without a PINGREQ before it gives up, which fails the test.
    private static final Duration STREAM_LIMIT = Duration.ofSeconds(30);

    // More publications than the 65,536 that the client's queue of received packets holds, so a
    // client that reads on towards the PINGRESP after them overflows the queue and drops some.
    private static final int FILLER = 100_000;

    @Test
    @Timeout(120)
    void a_backlog_that_pauses_reading_does_not_hold_back_keep_alive() throws Exception {
        // Arrange: every publication matches 32 filters, so the client routes more slowly than it
        // reads, fills its queue of received packets, and pauses reading. The broker sends the
        // first PINGRESP only after FILLER more publications, and then a QoS 1 publication that
        // marks the end.
        var marked = new CountDownLatch(1);
        try (var broker = new KeepAliveBroker(KEEP_ALIVE)) {
            MqttClient sut = broker.client();
            broker.connect();
            for (int i = 0; i < 32; i++) {
                broker.subscribe(filter(i), Qos.AT_MOST_ONCE, _ -> { });
            }
            broker.subscribe("marker", Qos.AT_LEAST_ONCE, _ -> marked.countDown());
            byte[] message = publication("a/b/c/d/e/f/g/h");
            broker.answerPings(repeat(message, FILLER));

            // Act
            long _ = broker.streamUntilPinged(message, STREAM_LIMIT);
            broker.send(codec.encode(new Packet.Publish(
                    new Topic("marker"),
                    new byte[1],
                    Qos.AT_LEAST_ONCE,
                    false,
                    false,
                    1,
                    EMPTY
            )));
            await().atMost(Duration.ofSeconds(60))
                    .until(() -> marked.getCount() == 0 || !broker.ended().isEmpty());

            // Assert: the client read on past its full queue towards the PINGRESP, which a
            // paused reader never does, so it dropped publications before routing them, and it
            // reached the PINGRESP before its deadline.
            assertThat(broker.ended()).isEmpty();
            assertThat(marked.getCount()).isZero();
            assertThat(sut.stats().get(5, SECONDS).receiving().unrouted()).isPositive();
            assertThat(broker.longestSilence()).isLessThanOrEqualTo(SILENCE_LIMIT);
        }
    }
}
