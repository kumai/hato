package org.mqtt4j.hato.testsupport;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/// Repository-private HiveMQ Community Edition fixture, started with
/// [Docker#start(GenericContainer)]; independent of every Hato module.
public final class HiveMq {

    /// The listener that the image opens to every client: its bundled extension allows all
    /// connections and topics.
    public static final int MQTT_PORT = 1883;

    private static final DockerImageName IMAGE = DockerImageName.parse("hivemq/hivemq-ce:2026.5");

    private HiveMq() {
    }

    /// HiveMQ CE on [#MQTT_PORT] that announces smaller limits than the defaults: Maximum QoS 1,
    /// no retained messages, no wildcard or shared subscriptions, no Subscription Identifiers, a
    /// Maximum Packet Size of 512 bytes, a Receive Maximum of 2, and no Topic Aliases.
    public static GenericContainer<?> restrictedContainer() {
        return container().withCopyFileToContainer(
                MountableFile.forClasspathResource("hivemq-restricted.xml"),
                "/opt/hivemq/conf/config.xml"
        );
    }

    public static GenericContainer<?> container() {
        // HiveMQ logs this line once its listeners accept connections.
        return new GenericContainer<>(IMAGE)
                .withExposedPorts(MQTT_PORT)
                .waitingFor(Wait.forLogMessage(".*Started HiveMQ in .*", 1));
    }
}
