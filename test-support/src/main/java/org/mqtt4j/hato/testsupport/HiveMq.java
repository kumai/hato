package org.mqtt4j.hato.testsupport;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/// Repository-private HiveMQ Community Edition fixture, started with
/// [Docker#start(GenericContainer)]; independent of every Hato module.
public final class HiveMq {

    /// The listener that the image opens to every client: its bundled extension allows all
    /// connections and topics.
    public static final int MQTT_PORT = 1883;

    private static final DockerImageName IMAGE = DockerImageName.parse("hivemq/hivemq-ce:2026.5");

    private HiveMq() {
    }

    public static GenericContainer<?> container() {
        // HiveMQ logs this line once its listeners accept connections.
        return new GenericContainer<>(IMAGE)
                .withExposedPorts(MQTT_PORT)
                .waitingFor(Wait.forLogMessage(".*Started HiveMQ in .*", 1));
    }
}
