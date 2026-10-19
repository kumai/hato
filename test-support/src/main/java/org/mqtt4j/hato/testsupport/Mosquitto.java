package org.mqtt4j.hato.testsupport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/// Repository-private Docker policy and broker fixture; independent of every Hato module.
public final class Mosquitto {

    public static final int MQTT_PORT = 1883;

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

    /// Required Docker keeps tests enabled so a missing daemon fails visibly at startup.
    public static boolean availableOrRequired() {
        return !"false".equals(System.getProperty("hato.docker.required"))
                || DockerClientFactory.instance().isDockerAvailable();
    }

    public static GenericContainer<?> container() {
        return new GenericContainer<>(IMAGE)
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("mosquitto.conf"),
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

    public static void start(GenericContainer<?> container) {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new AssertionError("""
                    Docker is required for the broker integration tests.
                    Start Docker, or opt out with -Phato.docker.required=false.
                    To opt out for every build, set hato.docker.required=false
                    in ~/.gradle/gradle.properties.
                    """);
        }
        container.setPortBindings(unusedHostPorts(container.getExposedPorts()));
        container.start();
    }

    /// Publishes each container port on a host port that no socket on this machine uses.
    ///
    /// Left to choose, Docker Desktop on macOS takes the system's next ephemeral port and listens
    /// on every address, even when another process, such as an IDE, already listens on 127.0.0.1
    /// with that number. macOS hands connections to 127.0.0.1 to the more specific listener, so
    /// every test using the container would reach that process instead. A bind without
    /// SO_REUSEADDR skips numbers in use on any address, and macOS hands out ephemeral ports in
    /// order, so the number goes to no other socket before Docker binds it. Where ports are
    /// random, as on Linux, another socket can take it, and the container then fails to start
    /// rather than share the port. Publishing on 127.0.0.1 alone would also prevent the sharing,
    /// but Docker Desktop then stalls every burst of small writes for about 50 ms; switch to it
    /// once that forwarding no longer stalls.
    private static List<String> unusedHostPorts(List<Integer> containerPorts) {
        var bindings = new ArrayList<String>();
        for (int containerPort : containerPorts) {
            try (var probe = new ServerSocket()) {
                // Java server sockets enable SO_REUSEADDR by default.
                probe.setReuseAddress(false);
                probe.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                bindings.add(probe.getLocalPort() + ":" + containerPort);
            } catch (IOException failure) {
                throw new UncheckedIOException("No unused port on the loopback address", failure);
            }
        }
        return bindings;
    }
}
