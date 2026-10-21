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

    /// EMQX on [#MQTT_PORT] that announces smaller limits than the defaults: Maximum QoS 1, no
    /// retained messages, no wildcard or shared subscriptions, a Maximum Packet Size of 512
    /// bytes, a Receive Maximum of 2, and no Topic Aliases from clients. EMQX has no setting that
    /// withdraws Subscription Identifiers.
    public static GenericContainer<?> restrictedContainer() {
        return container()
                .withEnv("EMQX_MQTT__MAX_QOS_ALLOWED", "1")
                .withEnv("EMQX_MQTT__RETAIN_AVAILABLE", "false")
                .withEnv("EMQX_MQTT__WILDCARD_SUBSCRIPTION", "false")
                .withEnv("EMQX_MQTT__SHARED_SUBSCRIPTION", "false")
                .withEnv("EMQX_MQTT__MAX_PACKET_SIZE", "512")
                .withEnv("EMQX_MQTT__MAX_INFLIGHT", "2")
                .withEnv("EMQX_MQTT__MAX_TOPIC_ALIAS", "0");
    }

    public static GenericContainer<?> container() {
        // EMQX opens its listeners about a second before the node finishes starting, so the
        // container is ready when EMQX says so rather than when the port accepts connections.
        return new GenericContainer<>(IMAGE)
                .withExposedPorts(MQTT_PORT)
                .waitingFor(Wait.forLogMessage(".* is running now!.*", 1));
    }
}
