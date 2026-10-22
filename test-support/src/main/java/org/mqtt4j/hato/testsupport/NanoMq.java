package org.mqtt4j.hato.testsupport;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/// Repository-private NanoMQ fixture, started with [Docker#start(GenericContainer)]; independent
/// of every Hato module.
public final class NanoMq {

    /// The listener that the image's default configuration opens to anonymous clients.
    public static final int MQTT_PORT = 1883;

    /// The listener for MQTT over WebSocket that the image's default configuration also opens,
    /// on the path `/mqtt`.
    public static final int WEBSOCKET_PORT = 8083;

    private static final DockerImageName IMAGE = DockerImageName.parse("emqx/nanomq:0.25.7");

    private NanoMq() {
    }

    public static GenericContainer<?> container() {
        return new GenericContainer<>(IMAGE).withExposedPorts(MQTT_PORT, WEBSOCKET_PORT);
    }
}
