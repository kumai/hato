package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.internal.inproc.InprocBrokers;
import org.mqtt4j.hato.internal.inproc.InprocLink;

import static java.util.Objects.requireNonNull;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;

/// The broker's side of in-process connections, where a client's link meets the limits that
/// connections over TCP share.
class BrokerInprocConnectionTest {

    @Test
    void a_client_that_ends_before_its_acceptance_gives_its_place_back_at_once() throws Exception {
        // Arrange: room for one connection that waits for its CONNECT, and an in-process client
        // that ends before the broker accepts its link.
        try (BrokerServer sut = BrokerServer.builder()
                .name("ended-before-acceptance-test")
                .maxAwaitingConnect(1)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))) {
            var link = new InprocLink();
            link.endClient();
            // Act
            requireNonNull(InprocBrokers.find("ended-before-acceptance-test")).connect(link);
            // Assert: the broker ends that connection long before its CONNECT deadline, and a
            // TCP client takes the place that it had.
            await().atMost(Duration.ofSeconds(5)).until(link::brokerEnded);
            try (RawSession next = connected(sut.port(), "next")) {
                next.ping();
            }
        }
    }
}
