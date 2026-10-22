package org.mqtt4j.hato.testsupport;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/// Repository-private RabbitMQ fixture with the MQTT plugins that the image bundles, started with
/// [Docker#start(GenericContainer)]; independent of every Hato module.
public final class RabbitMq {

    /// The listener of the MQTT plugin. A client that connects without a user name logs in as
    /// the image's `guest` user, which the image lets connect from other hosts.
    public static final int MQTT_PORT = 1883;

    /// The listener of the Web MQTT plugin for MQTT over WebSocket, on the path `/ws`.
    public static final int WEBSOCKET_PORT = 15675;

    private static final DockerImageName IMAGE = DockerImageName.parse("rabbitmq:4.4.0-alpine");

    private RabbitMq() {
    }

    public static GenericContainer<?> container() {
        // RabbitMQ opens the MQTT listeners while it is still starting, and logs this line once
        // it has started every plugin.
        return new GenericContainer<>(IMAGE)
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("rabbitmq-enabled-plugins"),
                        "/etc/rabbitmq/enabled_plugins"
                )
                .withExposedPorts(MQTT_PORT, WEBSOCKET_PORT)
                .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1));
    }
}
