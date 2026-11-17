package org.mqtt4j.hato.broker.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/// Neither endpoint uses the Hato codec: both programs come from the Mosquitto image.
@EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
class MosquittoClientTest {

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @ParameterizedTest(name = "{0} QoS {1}")
    @CsvSource({
            "mqttv311, 0",
            "mqttv311, 1",
            "mqttv311, 2",
            "mqttv5, 0",
            "mqttv5, 1",
            "mqttv5, 2"
    })
    void independent_mosquitto_clients_exchange_the_requested_qos(String version, int qos)
            throws Exception {
        // Arrange
        try (
                var sut = MqttBroker.start();
                GenericContainer<?> clientTools = Mosquitto.container()
        ) {
            Testcontainers.exposeHostPorts(sut.port());
            Docker.start(clientTools);
            String port = Integer.toString(sut.port());
            String level = Integer.toString(qos);
            String topic = "interop/qos" + qos;
            try (
                    var subscriber = Program.start(
                            clientTools,
                            "mosquitto_sub",
                            "-h",
                            "host.testcontainers.internal",
                            "-p",
                            port,
                            "-V",
                            version,
                            "-q",
                            level,
                            "-t",
                            topic,
                            "-C",
                            "1",
                            "-W",
                            "10",
                            "-d",
                            "-F",
                            "%q %p"
                    )
            ) {
                subscriber.awaitOutput("Subscribed (mid: 1): " + qos);

                // Act
                publish(clientTools, version, port, level, topic, "independent exchange");

                // Assert
                assertThat(subscriber.awaitSuccess()).contains(level + " independent exchange");
            }
        }
    }

    @ParameterizedTest(name = "{0} QoS {1}")
    @CsvSource({"mqttv311, 1", "mqttv311, 2", "mqttv5, 1", "mqttv5, 2"})
    void independent_mosquitto_client_resumes_queued_qos_one_and_two_deliveries(
            String version,
            int qos
    ) throws Exception {
        // Arrange
        try (
                var sut = MqttBroker.start();
                GenericContainer<?> clientTools = Mosquitto.container()
        ) {
            Testcontainers.exposeHostPorts(sut.port());
            Docker.start(clientTools);
            String port = Integer.toString(sut.port());
            String topic = "interop/persistent";
            String level = Integer.toString(qos);
            // Act & Assert: register, disconnect, publish offline, and resume the subscription.
            for (int attempt = 0; attempt < 2; attempt++) {
                if (attempt == 1) {
                    for (int publishedQos : List.of(1, 2)) {
                        publish(
                                clientTools,
                                version,
                                port,
                                Integer.toString(publishedQos),
                                topic,
                                "offline qos " + publishedQos
                        );
                    }
                }
                try (
                        var subscriber = Program.start(
                                clientTools,
                                "mosquitto_sub",
                                "-h",
                                "host.testcontainers.internal",
                                "-p",
                                port,
                                "-V",
                                version,
                                "-c",
                                "-i",
                                "persistent-subscriber",
                                "-q",
                                level,
                                "-t",
                                topic,
                                "-C",
                                attempt == 0 ? "1" : "2",
                                "-W",
                                "10",
                                "-d",
                                "-F",
                                "%q %p"
                        )
                ) {
                    if (attempt == 0) {
                        subscriber.awaitOutput("Subscribed (mid: 1): " + qos);
                        publish(clientTools, version, port, level, topic, "registered");
                    }
                    List<String> payloads = subscriber.awaitSuccess().stream()
                            .filter(line -> line.startsWith("1 ") || line.startsWith("2 "))
                            .toList();
                    assertThat(payloads).containsExactlyElementsOf(
                            attempt == 0
                                    ? List.of(level + " registered")
                                    : List.of("1 offline qos 1", level + " offline qos 2")
                    );
                }
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"mqttv311", "mqttv5"})
    void independent_mosquitto_client_receives_a_retained_message_as_it_subscribes(
            String version
    ) throws Exception {
        // Arrange
        try (
                var sut = MqttBroker.start();
                GenericContainer<?> clientTools = Mosquitto.container()
        ) {
            Testcontainers.exposeHostPorts(sut.port());
            Docker.start(clientTools);
            String port = Integer.toString(sut.port());
            String topic = "interop/retained";
            // -r asks the broker to retain the message. mosquitto_pub ends on the PUBACK, which
            // the broker sends once it has retained the message.
            publish(clientTools, version, port, "1", topic, "retained state", "-r");

            // Act
            String received = receiveFirst(clientTools, version, port, topic);

            // Assert: the RETAIN flag, the QoS, and the payload.
            assertThat(received).isEqualTo("1 1 retained state");
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"mqttv311", "mqttv5"})
    void independent_mosquitto_client_leaves_its_will_when_it_vanishes(String version)
            throws Exception {
        // Arrange
        try (
                var sut = MqttBroker.start();
                GenericContainer<?> clientTools = Mosquitto.container()
        ) {
            Testcontainers.exposeHostPorts(sut.port());
            Docker.start(clientTools);
            String port = Integer.toString(sut.port());
            String topic = "interop/will";
            try (
                    // A retained Will at QoS 1, as a device announces a broken connection. The
                    // client only has to stay connected, which it does on any topic.
                    var vanishing = Program.start(
                            clientTools,
                            "mosquitto_sub",
                            "-h",
                            "host.testcontainers.internal",
                            "-p",
                            port,
                            "-V",
                            version,
                            "-i",
                            "vanishing-client",
                            "-t",
                            "interop/unused",
                            "-d",
                            "--will-topic",
                            topic,
                            "--will-payload",
                            "connection broken",
                            "--will-qos",
                            "1",
                            "--will-retain"
                    );
                    var watcher = Program.start(
                            clientTools,
                            "mosquitto_sub",
                            "-h",
                            "host.testcontainers.internal",
                            "-p",
                            port,
                            "-V",
                            version,
                            "-q",
                            "1",
                            "-t",
                            topic,
                            "-C",
                            "1",
                            "-W",
                            "10",
                            "-d",
                            "-F",
                            "%r %q %p"
                    )
            ) {
                vanishing.awaitOutput("Subscribed (mid: 1): 0");
                watcher.awaitOutput("Subscribed (mid: 1): 1");

                // Act: the client ends without a DISCONNECT, as one whose device loses power.
                Container.ExecResult killed = clientTools.execInContainer(
                        "pkill",
                        "-KILL",
                        "-f",
                        "vanishing-client"
                );

                // Assert: the subscriber receives the Will at its QoS, with RETAIN 0, since its
                // subscription was there before. The broker retains a Will before it delivers
                // it, so a later subscriber finds it.
                assertThat(killed.getExitCode()).as(killed.getStderr()).isZero();
                assertThat(watcher.awaitSuccess()).contains("0 1 connection broken");
                assertThat(receiveFirst(clientTools, version, port, topic))
                        .isEqualTo("1 1 connection broken");
            }
        }
    }

    /// Publishes `message` with the image's `mosquitto_pub`, with `options` added to its command
    /// line, and checks that the command succeeded.
    private static void publish(
            GenericContainer<?> clientTools,
            String version,
            String port,
            String qos,
            String topic,
            String message,
            String... options
    ) throws Exception {
        var command = new ArrayList<>(List.of(
                "mosquitto_pub",
                "-h",
                "host.testcontainers.internal",
                "-p",
                port,
                "-V",
                version,
                "-q",
                qos,
                "-t",
                topic,
                "-m",
                message
        ));
        command.addAll(List.of(options));
        Container.ExecResult published = clientTools.execInContainer(
                command.toArray(String[]::new)
        );
        assertThat(published.getExitCode()).as(published.getStderr()).isZero();
    }

    /// Subscribes to `topic` at QoS 1 with the image's `mosquitto_sub` and returns the first
    /// message that arrives, as its RETAIN flag, its QoS, and its payload.
    private static String receiveFirst(
            GenericContainer<?> clientTools,
            String version,
            String port,
            String topic
    ) throws Exception {
        Container.ExecResult received = clientTools.execInContainer(
                "mosquitto_sub",
                "-h",
                "host.testcontainers.internal",
                "-p",
                port,
                "-V",
                version,
                "-q",
                "1",
                "-t",
                topic,
                "-C",
                "1",
                "-W",
                "10",
                "-F",
                "%r %q %p"
        );
        assertThat(received.getExitCode()).as(received.getStderr()).isZero();
        return received.getStdout().strip();
    }

    /// A program of the image that runs with a terminal and collects what it prints. The
    /// terminal makes the debug output of Mosquitto's programs line-buffered, so that a test sees
    /// a step such as a SUBACK as it happens, without a guessed delay.
    private static final class Program extends ResultCallback.Adapter<Frame> {

        private final DockerClient docker;
        private final String exec;
        private final StringBuffer output = new StringBuffer();

        private Program(DockerClient docker, String exec) {
            this.docker = docker;
            this.exec = exec;
        }

        /// Starts `command` in `clientTools`.
        static Program start(GenericContainer<?> clientTools, String... command) {
            DockerClient docker = DockerClientFactory.instance().client();
            String exec = docker.execCreateCmd(clientTools.getContainerId())
                    .withCmd(command)
                    .withTty(true)
                    .withAttachStdout(true)
                    .withAttachStderr(true)
                    .exec()
                    .getId();
            var program = new Program(docker, exec);
            docker.execStartCmd(exec).withTty(true).exec(program);
            return program;
        }

        @Override
        public void onNext(Frame frame) {
            output.append(new String(frame.getPayload(), UTF_8));
        }

        /// Waits until the program has printed `text`.
        void awaitOutput(String text) {
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(output.toString()).contains(text));
        }

        /// Waits until the program has ended with exit code zero, and returns the lines that it
        /// printed. A failure shows what it printed.
        List<String> awaitSuccess() throws InterruptedException {
            assertThat(awaitCompletion(10, SECONDS)).as(output.toString()).isTrue();
            assertThat(docker.inspectExecCmd(exec).exec().getExitCodeLong())
                    .as(output.toString())
                    .isZero();
            return output.toString().lines().toList();
        }
    }
}
