package org.mqtt4j.hato.testsupport;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/// Repository-private EMQX fixture, started with [Docker#start(GenericContainer)]; independent of
/// every Hato module.
///
/// The image runs a single node under the license that it prints at startup, which permits free
/// use of a single node in an internal environment. The tests use it only to check
/// interoperability, and no build output contains it.
public final class Emqx {

    /// The listener that the image opens to anonymous clients without further configuration.
    public static final int MQTT_PORT = 1883;

    private static final DockerImageName IMAGE = DockerImageName.parse("emqx/emqx:6.3.2");

    private Emqx() {
    }

    public static GenericContainer<?> container() {
        // EMQX opens its listeners about a second before the node finishes starting, so the
        // container is ready when EMQX says so rather than when the port accepts connections.
        return new GenericContainer<>(IMAGE)
                .withExposedPorts(MQTT_PORT)
                .waitingFor(Wait.forLogMessage(".* is running now!.*", 1));
    }
}
