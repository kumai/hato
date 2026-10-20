package org.mqtt4j.hato.testsupport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

/// Repository-private Docker policy shared by the broker fixtures; independent of every Hato
/// module.
public final class Docker {

    private Docker() {
    }

    /// Required Docker keeps tests enabled so a missing daemon fails visibly at startup.
    public static boolean availableOrRequired() {
        return !"false".equals(System.getProperty("hato.docker.required"))
                || DockerClientFactory.instance().isDockerAvailable();
    }

    /// Starts `container` with each of its ports published on a host port that no socket on this
    /// machine uses, or fails with instructions when Docker is not running.
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
