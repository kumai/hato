package org.mqtt4j.mqtj.testsupport;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/// Repository-private Docker policy and broker fixture; independent of every MQTJ module.
public final class Mosquitto {

    public static final int MQTT_PORT = 1883;

    private Mosquitto() {
    }

    /// Required Docker keeps tests enabled so a missing daemon fails visibly at startup.
    public static boolean availableOrRequired() {
        return !"false".equals(System.getProperty("mqtj.docker.required"))
                || DockerClientFactory.instance().isDockerAvailable();
    }

    public static GenericContainer<?> container() {
        return new GenericContainer<>(DockerImageName.parse("eclipse-mosquitto:2.1.2-alpine"))
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("mosquitto.conf"),
                        "/mosquitto/config/mosquitto.conf"
                )
                .withExposedPorts(MQTT_PORT);
    }

    public static void start(GenericContainer<?> container) {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new AssertionError("""
                    Docker is required for the broker integration tests.
                    Start Docker, or opt out with -Pmqtj.docker.required=false.
                    To opt out for every build, set mqtj.docker.required=false
                    in ~/.gradle/gradle.properties.
                    """);
        }
        container.start();
    }
}
