package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.testsupport.BusyCarriers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// A raw broker checks that a client keeps answering while the application's virtual threads keep
/// every carrier busy, as an application that computes on virtual threads of its own can. Only a
/// client whose core runs on platform threads can; the single-carrier suite runs every core on
/// virtual threads, so these tests run only where clients take platform cores.
class BusyCarriersTest {

    @Test
    @Timeout(30)
    void a_client_keeps_alive_while_virtual_threads_keep_every_carrier_busy() throws Exception {
        assumeTrue(platformCores());
        try (
                ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = MqttClient.builder()
                        .server(ScriptedBroker.endpoint(server))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .keepAlive(Duration.ofSeconds(1))
                        .build();
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)
        ) {
            // Arrange
            BusyCarriers busy = BusyCarriers.start();
            try {
                // Act & Assert: the PINGREQ goes out after a second of silence, as without the
                // busy carriers.
                assertThat(peer.receive()).isInstanceOf(Packet.PingReq.class);
            } finally {
                busy.stop();
            }
        }
    }

    @Test
    @Timeout(30)
    void a_client_completes_a_publication_while_virtual_threads_keep_every_carrier_busy()
            throws Exception {
        assumeTrue(platformCores());
        try (
                ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)
        ) {
            // Arrange
            BusyCarriers busy = BusyCarriers.start();
            try {
                // Act & Assert: the PUBLISH goes out, and the PUBACK completes it within five
                // seconds.
                peer.roundTripBarrier(sut);
            } finally {
                busy.stop();
            }
        }
    }

    @Test
    @Timeout(30)
    void a_client_completes_observations_while_virtual_threads_keep_every_carrier_busy()
            throws Exception {
        assumeTrue(platformCores());
        try (
                ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)
        ) {
            // Arrange
            BusyCarriers busy = BusyCarriers.start();
            try {
                // Act & Assert: the SUBACK completes established() within five seconds, and the
                // client's next snapshot completes stats() within five seconds too.
                peer.subscribe(sut, "busy", AckMode.ON_RECEIVE, _ -> { });
                assertThat(sut.stats()).succeedsWithin(Duration.ofSeconds(5));
            } finally {
                busy.stop();
            }
        }
    }

    @Test
    @Timeout(30)
    @SuppressWarnings("try") // Explicit close is under test; the resource closes on failure.
    void a_closed_client_ends_its_connection_while_virtual_threads_keep_every_carrier_busy()
            throws Exception {
        assumeTrue(platformCores());
        try (
                ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)
        ) {
            // Arrange
            BusyCarriers busy = BusyCarriers.start();
            try {
                // Act: closing the client ends its output gracefully, and the peer then closes its
                // end of the connection too.
                sut.close();
                peer.expectClosed();
                peer.close();

                // Assert: the transport reports its close within five seconds. Termination also
                // waits for the connection listener's last notification, which runs on a virtual
                // thread like other application code.
                var client = (ClientActor) sut;
                await().atMost(Duration.ofSeconds(5))
                        .untilAsserted(() -> assertThat(client.terminationPending())
                                .doesNotContain(ClientTermination.Pending.TRANSPORTS));
            } finally {
                busy.stop();
            }
        }
    }

    @Test
    @Timeout(30)
    void a_client_with_a_session_file_connects_while_virtual_threads_keep_every_carrier_busy(
            @TempDir Path directory
    ) throws Exception {
        assumeTrue(platformCores());
        try (
                ServerSocket server = ScriptedBroker.listen();
                MqttClient sut = MqttClient.builder()
                        .server(ScriptedBroker.endpoint(server))
                        .clientId("busy-carriers")
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .sessionExpiry(Duration.ofMinutes(1))
                        .sessionFile(directory.resolve("session"))
                        .build()
        ) {
            // Arrange
            BusyCarriers busy = BusyCarriers.start();
            try {
                // Act & Assert: the client claims the file, sends CONNECT, and takes the CONNACK
                // within five seconds.
                ScriptedBroker.connect(server, sut).close();
            } finally {
                busy.stop();
            }
        }
    }

    // Whether this JVM gives clients platform cores; the single-carrier suite gives none.
    private static boolean platformCores() {
        return Integer.getInteger("hato.client.platformCores", CoreThreads.PLATFORM_CORES) > 0;
    }
}
