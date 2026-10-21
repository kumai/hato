package org.mqtt4j.hato.testsupport;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/// Repository-private Mosquitto fixture, started with [Docker#start(GenericContainer)];
/// independent of every Hato module.
public final class Mosquitto {

    public static final int MQTT_PORT = 1883;

    /// The [#container()] listener for MQTT over WebSocket, which serves every path.
    public static final int WEBSOCKET_PORT = 8080;

    /// The [#tlsContainer()] listener that authenticates only the broker, with the certificate
    /// that [Tls#clientContext()] trusts.
    public static final int MQTTS_PORT = 8883;

    /// The [#tlsContainer()] listener that also requires a client certificate signed by the test
    /// CA, such as the one [Tls#clientContextWithCertificate()] presents.
    public static final int MUTUAL_TLS_PORT = 8884;

    private static final DockerImageName IMAGE =
            DockerImageName.parse("eclipse-mosquitto:2.1.2-alpine");

    private static final String CONFIGURATION = "/mosquitto/config/mosquitto.conf";

    private Mosquitto() {
    }

    /// Mosquitto on [#MQTT_PORT] and on [#WEBSOCKET_PORT].
    public static GenericContainer<?> container() {
        return new GenericContainer<>(IMAGE)
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("mosquitto.conf"),
                        CONFIGURATION
                )
                .withExposedPorts(MQTT_PORT, WEBSOCKET_PORT);
    }

    /// Mosquitto on [#MQTT_PORT] that announces smaller limits than the defaults: Maximum QoS 1,
    /// no retained messages, a Maximum Packet Size of 512 bytes, a Receive Maximum of 2, and no
    /// Topic Aliases from clients. Mosquitto has no setting that withdraws wildcard
    /// subscriptions, shared subscriptions, or Subscription Identifiers.
    public static GenericContainer<?> restrictedContainer() {
        return new GenericContainer<>(IMAGE)
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("mosquitto-restricted.conf"),
                        CONFIGURATION
                )
                .withExposedPorts(MQTT_PORT);
    }

    /// Mosquitto with only the TLS listeners [#MQTTS_PORT] and [#MUTUAL_TLS_PORT], which present
    /// the server identity of [Tls#serverContext()]. That certificate names `localhost` and no IP
    /// address, so clients that verify the host name must reach a Docker daemon on this machine
    /// as `localhost`.
    public static GenericContainer<?> tlsContainer() {
        // Mosquitto reads PEM files only. They are exported from the bundled PKCS12 identities
        // here, so the repository holds no PEM private key. Mosquitto reads the key after giving
        // up root, and copied files belong to root, so the key keeps the default mode 0644.
        try {
            return new GenericContainer<>(IMAGE)
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("mosquitto-tls.conf"),
                            CONFIGURATION
                    )
                    .withCopyToContainer(
                            Transferable.of(Tls.certificatePem("ca")),
                            "/mosquitto/config/tls/ca.crt"
                    )
                    .withCopyToContainer(
                            Transferable.of(Tls.certificatePem("server")),
                            "/mosquitto/config/tls/server.crt"
                    )
                    .withCopyToContainer(
                            Transferable.of(Tls.privateKeyPem("server")),
                            "/mosquitto/config/tls/server.key"
                    )
                    .withExposedPorts(MQTTS_PORT, MUTUAL_TLS_PORT);
        } catch (IOException | GeneralSecurityException failure) {
            throw new IllegalStateException("The bundled test identities are unreadable", failure);
        }
    }
}
