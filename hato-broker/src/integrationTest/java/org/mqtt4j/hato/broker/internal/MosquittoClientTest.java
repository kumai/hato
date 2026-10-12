package org.mqtt4j.hato.broker.internal;

import java.time.Duration;
import java.util.List;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/// Neither endpoint uses the Hato codec: both programs come from the Mosquitto image.
@EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
class MosquittoClientTest {

    static boolean dockerIsAvailableOrRequired() {
        return Mosquitto.availableOrRequired();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void independent_mosquitto_clients_exchange_the_requested_qos(int qos) throws Exception {
        // Arrange
        try (
                var sut = MqttBroker.start();
                GenericContainer<?> clientTools = Mosquitto.container()
        ) {
            Testcontainers.exposeHostPorts(sut.port());
            Mosquitto.start(clientTools);
            String port = Integer.toString(sut.port());
            String level = Integer.toString(qos);
            String topic = "interop/qos" + qos;
            var output = new StringBuffer();
            DockerClient docker = DockerClientFactory.instance().client();
            String subscription = docker.execCreateCmd(clientTools.getContainerId())
                    .withCmd(
                            "mosquitto_sub",
                            "-h",
                            "host.testcontainers.internal",
                            "-p",
                            port,
                            "-V",
                            "mqttv311",
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
                    // A terminal makes the CLI's debug output line-buffered, so SUBACK is
                    // observed before publishing without a guessed startup delay.
                    .withTty(true)
                    .withAttachStdout(true)
                    .withAttachStderr(true)
                    .exec()
                    .getId();
            try (var callback = new ResultCallback.Adapter<Frame>() {
                @Override
                public void onNext(Frame frame) {
                    output.append(new String(frame.getPayload(), UTF_8));
                }
            }) {
                docker.execStartCmd(subscription).withTty(true).exec(callback);
                await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                        assertThat(output.toString()).contains("Subscribed (mid: 1): " + qos));

                // Act
                publish(clientTools, port, level, topic, "independent exchange");

                // Assert
                assertThat(callback.awaitCompletion(10, SECONDS)).isTrue();
                assertThat(docker.inspectExecCmd(subscription).exec().getExitCodeLong()).isZero();
                assertThat(output.toString().lines().toList())
                        .contains(level + " independent exchange");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void independent_mosquitto_client_resumes_queued_qos_one_and_two_deliveries(int qos)
            throws Exception {
        // Arrange
        try (
                var sut = MqttBroker.start();
                GenericContainer<?> clientTools = Mosquitto.container()
        ) {
            Testcontainers.exposeHostPorts(sut.port());
            Mosquitto.start(clientTools);
            String port = Integer.toString(sut.port());
            String topic = "interop/persistent";
            String level = Integer.toString(qos);
            DockerClient docker = DockerClientFactory.instance().client();
            // Act & Assert: register, disconnect, publish offline, and resume the subscription.
            for (int attempt = 0; attempt < 2; attempt++) {
                if (attempt == 1) {
                    for (int publishedQos : List.of(1, 2)) {
                        publish(
                                clientTools,
                                port,
                                Integer.toString(publishedQos),
                                topic,
                                "offline qos " + publishedQos
                        );
                    }
                }
                var output = new StringBuffer();
                String subscription = docker.execCreateCmd(clientTools.getContainerId())
                        .withCmd(
                                "mosquitto_sub",
                                "-h",
                                "host.testcontainers.internal",
                                "-p",
                                port,
                                "-V",
                                "mqttv311",
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
                        .withTty(true)
                        .withAttachStdout(true)
                        .withAttachStderr(true)
                        .exec()
                        .getId();
                try (var callback = new ResultCallback.Adapter<Frame>() {
                    @Override
                    public void onNext(Frame frame) {
                        output.append(new String(frame.getPayload(), UTF_8));
                    }
                }) {
                    docker.execStartCmd(subscription).withTty(true).exec(callback);
                    if (attempt == 0) {
                        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                                assertThat(output.toString())
                                        .contains("Subscribed (mid: 1): " + qos));
                        publish(clientTools, port, level, topic, "registered");
                    }
                    assertThat(callback.awaitCompletion(10, SECONDS)).isTrue();
                    assertThat(docker.inspectExecCmd(subscription).exec().getExitCodeLong())
                            .isZero();
                    List<String> payloads = output.toString().lines()
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

    /// Publishes `message` with the image's `mosquitto_pub` and checks that the command succeeded.
    private static void publish(
            GenericContainer<?> clientTools,
            String port,
            String qos,
            String topic,
            String message
    ) throws Exception {
        Container.ExecResult published = clientTools.execInContainer(
                "mosquitto_pub",
                "-h",
                "host.testcontainers.internal",
                "-p",
                port,
                "-V",
                "mqttv311",
                "-q",
                qos,
                "-t",
                topic,
                "-m",
                message
        );
        assertThat(published.getExitCode()).as(published.getStderr()).isZero();
    }
}
