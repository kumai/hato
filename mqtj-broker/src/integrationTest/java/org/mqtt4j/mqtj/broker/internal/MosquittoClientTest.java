package org.mqtt4j.mqtj.broker.internal;

import java.time.Duration;
import java.util.List;

import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;

import org.mqtt4j.mqtj.broker.MqttBroker;
import org.mqtt4j.mqtj.testsupport.Mosquitto;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/// Neither endpoint uses the MQTJ codec: both programs come from the Mosquitto image.
@EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
class MosquittoClientTest {

    static boolean dockerIsAvailableOrRequired() {
        return Mosquitto.availableOrRequired();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void independent_mosquitto_clients_exchange_the_requested_qos(int qos) throws Exception {
        try (var sut = MqttBroker.start(); var clientTools = Mosquitto.container()) {
            Testcontainers.exposeHostPorts(sut.port());
            Mosquitto.start(clientTools);
            String port = Integer.toString(sut.port());
            String level = Integer.toString(qos);
            String topic = "interop/qos" + qos;
            var output = new StringBuffer();
            var docker = DockerClientFactory.instance().client();
            String subscription = docker.execCreateCmd(clientTools.getContainerId())
                    .withCmd(
                            "mosquitto_sub", "-h", "host.testcontainers.internal", "-p", port,
                            "-V", "mqttv311", "-q", level, "-t", topic,
                            "-C", "1", "-W", "10", "-d", "-F", "%q %p"
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

                Container.ExecResult published = clientTools.execInContainer(
                        "mosquitto_pub", "-h", "host.testcontainers.internal", "-p", port,
                        "-V", "mqttv311", "-q", level, "-t", topic, "-m", "independent exchange"
                );
                assertThat(published.getExitCode()).as(published.getStderr()).isZero();
                assertThat(callback.awaitCompletion(10, SECONDS)).isTrue();
                assertThat(docker.inspectExecCmd(subscription).exec().getExitCodeLong()).isZero();
                assertThat(output.toString().lines().toList())
                        .contains(level + " independent exchange");
            }
        }
    }

    @Test
    void independent_mosquitto_client_resumes_queued_qos_one_deliveries() throws Exception {
        try (var sut = MqttBroker.start(); var clientTools = Mosquitto.container()) {
            Testcontainers.exposeHostPorts(sut.port());
            Mosquitto.start(clientTools);
            String port = Integer.toString(sut.port());
            String topic = "interop/persistent";
            var docker = DockerClientFactory.instance().client();
            for (int attempt = 0; attempt < 2; attempt++) {
                if (attempt == 1) {
                    for (String payload : List.of("offline first", "offline second")) {
                        Container.ExecResult published = clientTools.execInContainer(
                                "mosquitto_pub", "-h", "host.testcontainers.internal", "-p", port,
                                "-V", "mqttv311", "-q", "1", "-t", topic, "-m", payload
                        );
                        assertThat(published.getExitCode()).as(published.getStderr()).isZero();
                    }
                }
                var output = new StringBuffer();
                String subscription = docker.execCreateCmd(clientTools.getContainerId())
                        .withCmd(
                                "mosquitto_sub", "-h", "host.testcontainers.internal", "-p", port,
                                "-V", "mqttv311", "-c", "-i", "persistent-subscriber",
                                "-q", "1", "-t", topic, "-C", attempt == 0 ? "1" : "2",
                                "-W", "10", "-d", "-F", "%q %p"
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
                                assertThat(output.toString()).contains("Subscribed (mid: 1): 1"));
                        Container.ExecResult published = clientTools.execInContainer(
                                "mosquitto_pub", "-h", "host.testcontainers.internal", "-p", port,
                                "-V", "mqttv311", "-q", "1", "-t", topic, "-m", "registered"
                        );
                        assertThat(published.getExitCode()).as(published.getStderr()).isZero();
                    }
                    assertThat(callback.awaitCompletion(10, SECONDS)).isTrue();
                    assertThat(docker.inspectExecCmd(subscription).exec().getExitCodeLong())
                            .isZero();
                    var payloads = output.toString().lines()
                            .filter(line -> line.startsWith("1 ")).toList();
                    assertThat(payloads).containsExactlyElementsOf(attempt == 0
                            ? List.of("1 registered")
                            : List.of("1 offline first", "1 offline second"));
                }
            }
        }
    }
}
