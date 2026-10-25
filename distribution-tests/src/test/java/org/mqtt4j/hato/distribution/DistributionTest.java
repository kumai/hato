package org.mqtt4j.hato.distribution;

import java.io.File;
import java.io.IOException;
import java.lang.classfile.Attributes;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.reflect.AccessFlag;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class DistributionTest {

    private static final Path CLIENT = Path.of(System.getProperty("client.jar"));
    private static final Path BROKER = Path.of(System.getProperty("broker.jar"));
    private static final String TESTKIT_JARS = System.getProperty("testkit.jars");
    private static final String CLIENT_PROBE = """
            if (org.mqtt4j.hato.MqttVersion.of(4) != org.mqtt4j.hato.MqttVersion.V3_1_1
                    || org.mqtt4j.hato.MqttVersion.V5.protocolLevel() != 5
                    || org.mqtt4j.hato.Qos.of(2) != org.mqtt4j.hato.Qos.EXACTLY_ONCE
                    || org.mqtt4j.hato.ReasonCode.of(0x80)
                            != org.mqtt4j.hato.ReasonCode.UNSPECIFIED_ERROR
                    || org.mqtt4j.hato.ReasonCode.WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED.value()
                            != 0xA2) {
                throw new AssertionError("Client wire values are not usable");
            }
            var filter = new org.mqtt4j.hato.TopicFilter("a/#");
            if (!filter.matches(new org.mqtt4j.hato.Topic("a/b"))
                    || filter.matches(new org.mqtt4j.hato.Topic("b/a"))) {
                throw new AssertionError("Client topic values are not usable");
            }
            org.mqtt4j.hato.MqttClient client = org.mqtt4j.hato.MqttClient.builder()
                    .server(java.net.URI.create("mqtt://localhost"))
                    .version(org.mqtt4j.hato.MqttVersion.V3_1_1)
                    .connectionListener(end -> {
                        end.cause().ifPresent(cause -> cause.getMessage());
                    })
                    .build();
            client.close();
            if (!client.connect().isCompletedExceptionally()) {
                throw new AssertionError("Closed client accepted a connection");
            }
            var transport = org.mqtt4j.hato.Transport.tcp("probe");
            transport.close();
            if (transport.send(1, new byte[0])
                    != org.mqtt4j.hato.Transport.SendResult.CLOSED) {
                throw new AssertionError("Closed transport accepted a write");
            }
            var cause = new org.mqtt4j.hato.MqttProtocolException(
                    "Invalid packet field",
                    org.mqtt4j.hato.ReasonCode.MALFORMED_PACKET
            );
            var failure = new org.mqtt4j.hato.MqttProtocolException(
                    "Invalid packet",
                    org.mqtt4j.hato.ReasonCode.MALFORMED_PACKET,
                    cause
            );
            if (failure.reasonCode() != org.mqtt4j.hato.ReasonCode.MALFORMED_PACKET
                    || failure.getCause() != cause
                    || cause.getCause() != null) {
                throw new AssertionError("Public protocol failure is not usable");
            }
            var disconnection = org.mqtt4j.hato.Disconnection.of(cause, false, 1);
            var message = org.mqtt4j.hato.MqttMessage.of(
                    new org.mqtt4j.hato.Topic("a/b"),
                    new byte[0],
                    org.mqtt4j.hato.Qos.AT_MOST_ONCE,
                    false
            ).withTopic(new org.mqtt4j.hato.Topic("a/c"))
                    .withPayload(new byte[] {42})
                    .withQos(org.mqtt4j.hato.Qos.AT_LEAST_ONCE)
                    .withRetain(true);
            if (disconnection.cause().orElseThrow() != cause
                    || disconnection.userRequested()
                    || org.mqtt4j.hato.Disconnection.of(null, true, 1).cause().isPresent()
                    || !org.mqtt4j.hato.Disconnection.of(
                            null,
                            false,
                            2,
                            java.time.Duration.ofSeconds(1)
                    ).reconnectDelay().isPresent()
                    || !org.mqtt4j.hato.ConnAck.of(true, 1).sessionPresent()
                    || !org.mqtt4j.hato.ReconnectPolicy.exponentialBackoff(
                            java.time.Duration.ofSeconds(1),
                            java.time.Duration.ofSeconds(2)
                    ).withInitialRetry().equals(org.mqtt4j.hato.ReconnectPolicy.exponentialBackoff(
                            java.time.Duration.ofSeconds(1),
                            java.time.Duration.ofSeconds(2)
                    ).withInitialRetry())
                    || !org.mqtt4j.hato.PublishResult.of()
                            .equals(org.mqtt4j.hato.PublishResult.of())
                    || !message.equals(org.mqtt4j.hato.MqttMessage.of(
                            new org.mqtt4j.hato.Topic("a/c"),
                            new byte[] {42},
                            org.mqtt4j.hato.Qos.AT_LEAST_ONCE,
                            true
                    ))) {
                throw new AssertionError("Client value factories are not usable");
            }
            var property = new org.mqtt4j.hato.UserProperty("source", "probe");
            var described = message.withUserProperties(java.util.List.of(property));
            if (!described.userProperties().equals(java.util.List.of(property))
                    || described.equals(message)) {
                throw new AssertionError("Message properties are not usable");
            }
            // A WebSocket attempt runs the JDK's HTTP client, which the module requires, and the
            // client's termination waits until that client has ended.
            org.mqtt4j.hato.MqttClient webSocket = org.mqtt4j.hato.MqttClient.builder()
                    .server(java.net.URI.create("ws://127.0.0.1:1/mqtt"))
                    .reconnectPolicy(org.mqtt4j.hato.ReconnectPolicy.disabled())
                    .build();
            try {
                webSocket.connect().get(10, java.util.concurrent.TimeUnit.SECONDS);
                throw new AssertionError("A closed port accepted a WebSocket connection");
            } catch (java.util.concurrent.ExecutionException refused) {
                if (!(refused.getCause() instanceof org.mqtt4j.hato.MqttConnectException)) {
                    throw new AssertionError("WebSocket attempt failed oddly", refused);
                }
            } finally {
                webSocket.close();
            }
            webSocket.terminated().get(10, java.util.concurrent.TimeUnit.SECONDS);
            """;
    private static final String BROKER_PROBE = """
            try (var broker = org.mqtt4j.hato.broker.MqttBroker.start()) {
                if (broker.port() <= 0) {
                    throw new AssertionError("Broker did not bind a port");
                }
            }
            """;
    private static final String CONNECTION_PROBE = """
            // Both protocol versions run, so that each codec in the jars is used.
            for (var version : org.mqtt4j.hato.MqttVersion.values()) {
                var ended = new java.util.concurrent.LinkedBlockingQueue<
                        org.mqtt4j.hato.Disconnection
                >();
                try (var broker = org.mqtt4j.hato.broker.MqttBroker.start();
                        org.mqtt4j.hato.MqttClient sut = org.mqtt4j.hato.MqttClient.builder()
                                .server(java.net.URI.create("mqtt://localhost:" + broker.port()))
                                .version(version)
                                .connectionListener(ended::add)
                                .build()) {
                    org.mqtt4j.hato.ConnAck connAck = sut.connect()
                            .get(5, java.util.concurrent.TimeUnit.SECONDS);
                    if (connAck.sessionPresent()) {
                        throw new AssertionError(version + " clean connection resumed a session");
                    }

                    // Arrange: use the public messaging roles from the distributed client jar,
                    // with one handler and one pull subscription.
                    var handled = new java.util.concurrent.LinkedBlockingQueue<
                            org.mqtt4j.hato.MqttMessage
                    >();
                    org.mqtt4j.hato.MqttSubscriber subscriber = sut;
                    org.mqtt4j.hato.MqttPublisher publisher = sut;
                    org.mqtt4j.hato.Subscription push = subscriber.subscribe(
                            org.mqtt4j.hato.Subscribe.of(
                                    new org.mqtt4j.hato.TopicFilter("probe/push"),
                                    org.mqtt4j.hato.Qos.EXACTLY_ONCE
                            ),
                            delivery -> handled.add(delivery.message())
                    );
                    org.mqtt4j.hato.PullSubscription pull = subscriber.subscribe(
                            org.mqtt4j.hato.Subscribe.of(
                                    new org.mqtt4j.hato.TopicFilter("probe/pull"),
                                    org.mqtt4j.hato.Qos.EXACTLY_ONCE
                            ).withAckMode(org.mqtt4j.hato.AckMode.MANUAL)
                    );
                    push.established().get(5, java.util.concurrent.TimeUnit.SECONDS);
                    pull.established().get(5, java.util.concurrent.TimeUnit.SECONDS);

                    // Act & Assert: every QoS level reaches the handler and the receiving thread
                    // intact, QoS included, because the subscriptions request QoS 2 and the broker
                    // grants it.
                    for (var qos : org.mqtt4j.hato.Qos.values()) {
                        var pushed = org.mqtt4j.hato.MqttMessage.of(
                                new org.mqtt4j.hato.Topic("probe/push"),
                                new byte[] {42},
                                qos,
                                false
                        );
                        publisher.publish(pushed).get(5, java.util.concurrent.TimeUnit.SECONDS);
                        var handledMessage =
                                handled.poll(5, java.util.concurrent.TimeUnit.SECONDS);
                        if (!pushed.equals(handledMessage)) {
                            throw new AssertionError(version + " " + qos + " handler changed it");
                        }
                        var pulled = org.mqtt4j.hato.MqttMessage.of(
                                new org.mqtt4j.hato.Topic("probe/pull"),
                                new byte[] {7},
                                qos,
                                false
                        );
                        publisher.publish(pulled).get(5, java.util.concurrent.TimeUnit.SECONDS);
                        if (!(pull.receive(java.time.Duration.ofSeconds(5))
                                instanceof org.mqtt4j.hato.ReceiveResult.Received(var delivery))
                                || !pulled.equals(delivery.message())) {
                            throw new AssertionError(version + " " + qos + " receiver changed it");
                        }
                        delivery.ack();
                    }

                    // Act & Assert: lifecycle operations must finish within the deadline.
                    push.unsubscribe().get(5, java.util.concurrent.TimeUnit.SECONDS);
                    pull.unsubscribe().get(5, java.util.concurrent.TimeUnit.SECONDS);
                    if (!(pull.receive() instanceof org.mqtt4j.hato.ReceiveResult.Ended)) {
                        throw new AssertionError(version + " removed subscription kept receiving");
                    }
                    sut.disconnect().get(5, java.util.concurrent.TimeUnit.SECONDS);
                    // Removal and disconnect also complete without a connection, so only the
                    // listener shows that the connection lasted until the requested disconnect.
                    if (!org.mqtt4j.hato.Disconnection.of(null, true, 1)
                            .equals(ended.poll(5, java.util.concurrent.TimeUnit.SECONDS))) {
                        throw new AssertionError(version + " connection ended before disconnect");
                    }
                }
            }
            """;

    @TempDir
    Path temporaryDirectory;

    @Test
    void client_runs_with_only_its_distributed_jar() throws Exception {
        runClasspathProbe("client", CLIENT.toString(), CLIENT_PROBE);
    }

    @Test
    void broker_runs_with_only_its_distributed_jar() throws Exception {
        runClasspathProbe("broker", BROKER.toString(), BROKER_PROBE);
    }

    @Test
    void client_and_broker_can_share_a_classpath() throws Exception {
        runClasspathProbe(
                "both",
                CLIENT + File.pathSeparator + BROKER,
                CLIENT_PROBE + BROKER_PROBE + CONNECTION_PROBE
        );
    }

    @Test
    void client_runs_as_a_standalone_named_module() throws Exception {
        runModuleProbe(
                "clientModule",
                CLIENT.toString(),
                List.of("org.mqtt4j.hato.client"),
                CLIENT_PROBE
        );
    }

    @Test
    void broker_runs_as_a_standalone_named_module() throws Exception {
        runModuleProbe(
                "brokerModule",
                BROKER.toString(),
                List.of("org.mqtt4j.hato.broker"),
                BROKER_PROBE
        );
    }

    @Test
    void client_and_broker_connect_as_independent_named_modules() throws Exception {
        runModuleProbe(
                "bothModules",
                CLIENT + File.pathSeparator + BROKER,
                List.of("org.mqtt4j.hato.client", "org.mqtt4j.hato.broker"),
                CONNECTION_PROBE
        );
    }

    @Test
    void testkit_resolves_both_independent_modules_transitively() throws Exception {
        assertThat(paths(TESTKIT_JARS)).hasSize(3);
        runModuleProbe("testkitModule", TESTKIT_JARS, List.of("org.mqtt4j.hato.testkit"),
                CLIENT_PROBE + CONNECTION_PROBE + """
                var scheduler = new org.mqtt4j.hato.testkit.ManualTimerScheduler();
                var ran = new java.util.concurrent.atomic.AtomicBoolean();
                scheduler.schedule(java.time.Duration.ofSeconds(1), () -> ran.set(true));
                scheduler.advance(java.time.Duration.ofSeconds(1));
                if (!ran.get()) {
                    throw new AssertionError("Public timer SPI is not usable");
                }
                if (ModuleLayer.boot().findModule("org.mqtt4j.hato.client").isEmpty()
                        || ModuleLayer.boot().findModule("org.mqtt4j.hato.broker").isEmpty()) {
                    throw new AssertionError("Testkit did not resolve both libraries");
                }
                """);
    }

    @Test
    void embedded_packages_are_disjoint_and_unexported() throws Exception {
        Path testkit = testkitJar();
        Set<String> clientClasses = classEntries(CLIENT);
        Set<String> brokerClasses = classEntries(BROKER);
        Set<String> testkitClasses = classEntries(testkit);
        for (String type : List.of("MqttVersion", "Qos", "ReasonCode", "Topic", "TopicFilter")) {
            assertThat(clientClasses).contains("org/mqtt4j/hato/" + type + ".class");
            assertThat(clientClasses)
                    .noneMatch(name -> name.endsWith("/value/" + type + ".class"));
            assertThat(brokerClasses)
                    .contains("org/mqtt4j/hato/broker/internal/common/value/" + type + ".class");
            assertThat(testkitClasses)
                    .contains("org/mqtt4j/hato/testkit/internal/common/value/" + type + ".class");
        }
        for (Path jar : List.of(CLIENT, BROKER, testkit)) {
            assertThat(classEntries(jar))
                    .contains("org/mqtt4j/hato/" + ownerOf(jar) + "/internal/common/log/Log.class")
                    .noneMatch(name -> name.endsWith("/ProtocolVersion.class"));
        }
        assertThat(clientClasses).doesNotContainAnyElementsOf(brokerClasses);
        assertThat(clientClasses).doesNotContainAnyElementsOf(testkitClasses);
        assertThat(brokerClasses).doesNotContainAnyElementsOf(testkitClasses);

        for (Path jar : List.of(CLIENT, BROKER, testkit)) {
            ModuleDescriptor descriptor = ModuleFinder.of(jar).findAll().iterator().next()
                    .descriptor();
            assertThat(descriptor.isAutomatic()).isFalse();
            assertThat(descriptor.isOpen()).isFalse();
            assertThat(descriptor.opens()).isEmpty();
            assertThat(descriptor.requires()).allMatch(r -> r.name().startsWith("java.")
                    || (jar.equals(testkit) && r.name().startsWith("org.mqtt4j.hato."))
                    || isCompileTimeOnlyAnnotations(r));
            assertThat(descriptor.requires())
                    .anyMatch(DistributionTest::isCompileTimeOnlyAnnotations);
            assertThat(descriptor.exports())
                    .filteredOn(e -> !e.isQualified())
                    .noneMatch(e -> List.of(e.source().split("\\.")).contains("internal"));
            // The one export to a single module: the broker's test controls, for the testkit.
            assertThat(descriptor.exports())
                    .filteredOn(ModuleDescriptor.Exports::isQualified)
                    .extracting(e -> e.source() + " to " + e.targets())
                    .containsExactlyElementsOf(jar.equals(BROKER)
                            ? List.of("org.mqtt4j.hato.broker.internal.control"
                                    + " to [org.mqtt4j.hato.testkit]")
                            : List.of());
            assertThat(descriptor.packages())
                    .noneMatch(p -> p.startsWith("org.mqtt4j.hato.common"));
        }

        Path root = temporaryDirectory.resolve("privateApi");
        Path descriptor = write(
                root.resolve("src/module-info.java"),
                "module probe { requires org.mqtt4j.hato.client; requires org.mqtt4j.hato.broker; "
                        + "requires org.mqtt4j.hato.testkit; }"
        );
        for (String owner : List.of("client", "broker", "testkit")) {
            Path source = write(root.resolve("src/probe/Probe.java"), """
                    package probe;
                    import org.mqtt4j.hato.%s.internal.common.log.Log;
                    public class Probe { Log value; }
                    """.formatted(owner));
            ProcessResult result = execute(
                    root,
                    "javac",
                    "-XDrawDiagnostics",
                    "--module-path",
                    TESTKIT_JARS,
                    "-d",
                    root.resolve("classes").toString(),
                    descriptor.toString(),
                    source.toString()
            );
            assertThat(result.exitCode()).isNotZero();
            assertThat(result.output()).contains("compiler.err.package.not.visible");
        }
        Path controls = write(root.resolve("src/probe/Probe.java"), """
                package probe;
                import org.mqtt4j.hato.broker.internal.control.ControlPort;
                public class Probe { ControlPort value; }
                """);
        ProcessResult result = execute(
                root,
                "javac",
                "-XDrawDiagnostics",
                "--module-path",
                TESTKIT_JARS,
                "-d",
                root.resolve("classes").toString(),
                descriptor.toString(),
                controls.toString()
        );
        assertThat(result.exitCode()).isNotZero();
        assertThat(result.output()).contains("compiler.err.package.not.visible");
    }

    @Test
    void each_jar_exports_exactly_the_types_of_its_public_api() throws Exception {
        // A change here is a change to the public API; update the list only on purpose.
        assertThat(publicTypes(CLIENT)).containsExactlyInAnyOrder(
                "org.mqtt4j.hato.AckMode",
                "org.mqtt4j.hato.CapacityLimit",
                "org.mqtt4j.hato.ClientStats",
                "org.mqtt4j.hato.ClientStats$Connection",
                "org.mqtt4j.hato.ClientStats$ConnectionState",
                "org.mqtt4j.hato.ClientStats$DropReason",
                "org.mqtt4j.hato.ClientStats$OverloadCause",
                "org.mqtt4j.hato.ClientStats$Receiving",
                "org.mqtt4j.hato.ClientStats$Sending",
                "org.mqtt4j.hato.ClientStats$Subscribing",
                "org.mqtt4j.hato.ClientStats$SubscriptionStats",
                "org.mqtt4j.hato.ClientStats$Usage",
                "org.mqtt4j.hato.ConnAck",
                "org.mqtt4j.hato.ConnectionListener",
                "org.mqtt4j.hato.DeliveryOptions",
                "org.mqtt4j.hato.Disconnection",
                "org.mqtt4j.hato.FailurePolicy",
                "org.mqtt4j.hato.MetricsListener",
                "org.mqtt4j.hato.MetricsListener$DeliveryStart",
                "org.mqtt4j.hato.MetricsListener$PublishCompletion",
                "org.mqtt4j.hato.MqttBrokerDisconnectException",
                "org.mqtt4j.hato.MqttCapacityException",
                "org.mqtt4j.hato.MqttClient",
                "org.mqtt4j.hato.MqttClientBuilder",
                "org.mqtt4j.hato.MqttConnectException",
                "org.mqtt4j.hato.MqttConnectionLostException",
                "org.mqtt4j.hato.MqttCredentials",
                "org.mqtt4j.hato.MqttDelivery",
                "org.mqtt4j.hato.MqttException",
                "org.mqtt4j.hato.MqttMessage",
                "org.mqtt4j.hato.MqttNotConnectedException",
                "org.mqtt4j.hato.MqttProtocolException",
                "org.mqtt4j.hato.MqttPublicationRefusedException",
                "org.mqtt4j.hato.MqttPublisher",
                "org.mqtt4j.hato.MqttSubscriber",
                "org.mqtt4j.hato.MqttSubscriptionRefusedException",
                "org.mqtt4j.hato.MqttVersion",
                "org.mqtt4j.hato.PublishResult",
                "org.mqtt4j.hato.PullSubscription",
                "org.mqtt4j.hato.Qos",
                "org.mqtt4j.hato.ReasonCode",
                "org.mqtt4j.hato.ReconnectPolicy",
                "org.mqtt4j.hato.ReceiveResult",
                "org.mqtt4j.hato.ReceiveResult$Ended",
                "org.mqtt4j.hato.ReceiveResult$Received",
                "org.mqtt4j.hato.ReceiveResult$TimedOut",
                "org.mqtt4j.hato.RetainHandling",
                "org.mqtt4j.hato.Subscribe",
                "org.mqtt4j.hato.Subscription",
                "org.mqtt4j.hato.TimerScheduler",
                "org.mqtt4j.hato.TimerScheduler$Cancellable",
                "org.mqtt4j.hato.Topic",
                "org.mqtt4j.hato.TopicFilter",
                "org.mqtt4j.hato.Transport",
                "org.mqtt4j.hato.Transport$Factory",
                "org.mqtt4j.hato.Transport$Listener",
                "org.mqtt4j.hato.Transport$SendResult",
                "org.mqtt4j.hato.UserProperty"
        );
        assertThat(publicTypes(BROKER)).containsExactly("org.mqtt4j.hato.broker.MqttBroker");
        assertThat(publicTypes(testkitJar())).containsExactlyInAnyOrder(
                "org.mqtt4j.hato.testkit.BrokerCapabilities",
                "org.mqtt4j.hato.testkit.BrokerControls",
                "org.mqtt4j.hato.testkit.BrokerReason",
                "org.mqtt4j.hato.testkit.FakeMqttPublisher",
                "org.mqtt4j.hato.testkit.FakeMqttPublisher$HeldPublication",
                "org.mqtt4j.hato.testkit.FakeMqttSubscriber",
                "org.mqtt4j.hato.testkit.FakeMqttSubscriber$FakeMqttDelivery",
                "org.mqtt4j.hato.testkit.FakeMqttSubscriber$FakeSubscription",
                "org.mqtt4j.hato.testkit.FaultyTransport",
                "org.mqtt4j.hato.testkit.HeldResponse",
                "org.mqtt4j.hato.testkit.ManualTimerScheduler",
                "org.mqtt4j.hato.testkit.ResponseKind"
        );
    }

    @Test
    void testkit_signatures_name_no_internal_type() throws Exception {
        // The testkit reaches the broker's test controls, but hands applications only public
        // types of the client, the broker, the testkit, and the JDK.
        Path testkit = testkitJar();
        try (var archive = new JarFile(testkit.toFile())) {
            for (String type : publicTypes(testkit)) {
                assertThat(publicSignatures(archive, type))
                        .as(type)
                        .noneMatch(name -> name.contains("/internal/"));
            }
        }
    }

    @Test
    void testkit_types_in_the_compatibility_promise_name_no_broker_type() throws Exception {
        // Only the testkit types that are experimental like the broker may show broker types;
        // showing one would tie the promised types to the broker's experimental API.
        var likeTheBroker = Set.of(
                "org.mqtt4j.hato.testkit.BrokerControls",
                "org.mqtt4j.hato.testkit.HeldResponse",
                "org.mqtt4j.hato.testkit.ResponseKind"
        );
        Path testkit = testkitJar();
        try (var archive = new JarFile(testkit.toFile())) {
            for (String type : publicTypes(testkit)) {
                if (!likeTheBroker.contains(type)) {
                    assertThat(publicSignatures(archive, type))
                            .as(type)
                            .noneMatch(name -> name.contains("org/mqtt4j/hato/broker/"));
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "Disconnection", "ConnAck", "PublishResult", "MqttMessage", "MqttCredentials",
            "DeliveryOptions", "Subscribe", "ClientStats", "ClientStats$Connection",
            "ClientStats$Receiving", "ClientStats$Sending", "ClientStats$Subscribing",
            "ClientStats$SubscriptionStats", "MetricsListener$PublishCompletion",
            "MetricsListener$DeliveryStart"
    })
    void evolving_value_types_are_final_classes_with_private_constructors(String type)
            throws Exception {
        try (var archive = new JarFile(CLIENT.toFile())) {
            JarEntry entry = archive.getJarEntry("org/mqtt4j/hato/" + type + ".class");
            assertThat(entry).as(type).isNotNull();
            ClassModel model = ClassFile.of().parse(archive.getInputStream(entry).readAllBytes());
            assertThat(model.flags().has(AccessFlag.FINAL)).as(type).isTrue();
            assertThat(model.findAttribute(Attributes.record())).as(type).isEmpty();
            assertThat(model.methods())
                    .filteredOn(method -> method.methodName().equalsString("<init>"))
                    .as(type).isNotEmpty()
                    .allSatisfy(method -> assertThat(method.flags().has(AccessFlag.PRIVATE))
                            .as(type).isTrue());
        }
    }

    @Test
    void source_jars_match_the_embedded_packages() throws Exception {
        for (Path path : paths(System.getProperty("source.jars"))) {
            String owner = ownerOf(path);
            try (var archive = new ZipFile(path.toFile())) {
                String valuePackage = owner.equals("client") ? "org.mqtt4j.hato"
                        : "org.mqtt4j.hato." + owner + ".internal.common.value";
                for (String type : List.of(
                        "MqttVersion",
                        "Qos",
                        "ReasonCode",
                        "Topic",
                        "TopicFilter"
                )) {
                    String name = valuePackage.replace('.', '/') + "/" + type + ".java";
                    ZipEntry entry = archive.getEntry(name);
                    assertThat(entry).as("%s in %s", name, path).isNotNull();
                    var source = new String(archive.getInputStream(entry).readAllBytes(), UTF_8);
                    assertThat(source).contains("package " + valuePackage + ";");
                    assertThat(source).doesNotContain("org.mqtt4j.hato.common");
                }
                assertThat(archive.stream().map(e -> e.getName()))
                        .noneMatch(name -> name.startsWith("org/mqtt4j/hato/common/"));
            }
        }
    }

    @Test
    void publication_metadata_has_only_the_intended_dependencies() throws Exception {
        List<Path> metadata = paths(System.getProperty("publication.metadata"));
        assertThat(metadata).hasSize(6);
        for (Path path : metadata) {
            String contents = Files.readString(path);
            assertThat(contents).doesNotContain(
                    "hato-common",
                    "jspecify"
            );
            if (path.toString().endsWith(".xml")) {
                Document document = readPom(path);
                NodeList groups = document.getElementsByTagName("groupId");
                assertThat(groups.getLength()).isPositive();
                for (int index = 0; index < groups.getLength(); index++) {
                    assertThat(groups.item(index).getTextContent()).isEqualTo("org.mqtt4j");
                }
                NodeList dependencies = document.getElementsByTagName("dependency");
                var artifacts = new HashSet<String>();
                for (int index = 0; index < dependencies.getLength(); index++) {
                    var dependency = (org.w3c.dom.Element) dependencies.item(index);
                    artifacts.add(dependency.getElementsByTagName("artifactId")
                            .item(0).getTextContent());
                }
                if (document.getDocumentElement().getElementsByTagName("artifactId")
                        .item(0).getTextContent().equals("hato-testkit")) {
                    assertThat(artifacts).containsExactlyInAnyOrder("hato-client", "hato-broker");
                } else {
                    assertThat(artifacts).isEmpty();
                }
            } else {
                List<String> groups = Pattern.compile("\"group\"\\s*:\\s*\"([^\"]+)\"")
                        .matcher(contents)
                        .results()
                        .map(m -> m.group(1))
                        .toList();
                assertThat(groups).isNotEmpty().containsOnly("org.mqtt4j");
                List<String> modules = Pattern.compile("\"module\"\\s*:\\s*\"([^\"]+)\"")
                        .matcher(contents)
                        .results()
                        .map(m -> m.group(1))
                        .toList();
                assertThat(modules).isNotEmpty();
                if (modules.getFirst().equals("hato-testkit")) {
                    assertThat(new HashSet<>(modules)).containsExactlyInAnyOrder(
                            "hato-testkit",
                            "hato-client",
                            "hato-broker"
                    );
                } else {
                    // No published variant may acquire a dependency, including an internal one.
                    assertThat(contents).doesNotContain("\"dependencies\"");
                }
            }
        }
    }

    @Test
    void publication_poms_identify_the_project_license_developer_and_sources() throws Exception {
        List<Path> poms = paths(System.getProperty("publication.metadata")).stream()
                .filter(path -> path.toString().endsWith(".xml"))
                .toList();
        assertThat(poms).hasSize(3);
        XPath xpath = XPathFactory.newInstance().newXPath();
        var artifacts = new HashSet<String>();
        for (Path pom : poms) {
            Document document = readPom(pom);
            artifacts.add(xpath.evaluate("/project/artifactId", document));
            assertThat(Files.readString(pom)).as("POM %s", pom).doesNotContainIgnoringCase("TODO");
            for (String field : List.of(
                    "name",
                    "description",
                    "url",
                    "licenses/license/name",
                    "licenses/license/url",
                    "developers/developer/id",
                    "developers/developer/name",
                    "developers/developer/url",
                    "scm/connection",
                    "scm/developerConnection",
                    "scm/url"
            )) {
                assertThat(xpath.evaluate("/project/" + field, document))
                        .as("%s in %s", field, pom)
                        .isNotBlank();
            }
        }
        assertThat(artifacts)
                .containsExactlyInAnyOrder("hato-client", "hato-broker", "hato-testkit");
    }

    @Test
    void javadoc_jars_document_only_the_public_api_in_english() throws Exception {
        List<Path> javadocs = paths(System.getProperty("javadoc.jars"));
        assertThat(javadocs).extracting(DistributionTest::ownerOf)
                .containsExactlyInAnyOrder("client", "broker", "testkit");
        for (Path javadoc : javadocs) {
            Path library = List.of(CLIENT, BROKER, testkitJar()).stream()
                    .filter(path -> ownerOf(path).equals(ownerOf(javadoc)))
                    .findFirst()
                    .orElseThrow();
            ModuleDescriptor descriptor = ModuleFinder.of(library).findAll().iterator().next()
                    .descriptor();
            String modulePrefix = descriptor.name() + "/";
            Set<String> packages = descriptor.exports().stream()
                    .filter(export -> !export.isQualified())
                    .map(export -> export.source().replace('.', '/'))
                    .collect(Collectors.toSet());
            try (var archive = new ZipFile(javadoc.toFile())) {
                List<String> pages = archive.stream()
                        .map(ZipEntry::getName)
                        .filter(name -> name.endsWith(".html"))
                        .toList();
                assertThat(pages).as("Javadoc pages in %s", javadoc).isNotEmpty()
                        .noneMatch(name -> List.of(name.split("/")).contains("internal"));
                for (String page : pages) {
                    var contents = new String(
                            archive.getInputStream(archive.getEntry(page)).readAllBytes(),
                            UTF_8
                    );
                    assertThat(contents).as("%s in %s", page, javadoc)
                            .contains("<html lang=\"en\">");
                    if (page.contains("/")) {
                        assertThat(page).startsWith(modulePrefix);
                        String relative = page.substring(modulePrefix.length());
                        if (relative.contains("/")) {
                            assertThat(packages).as("Package of %s", page)
                                    .contains(relative.substring(0, relative.lastIndexOf('/')));
                        }
                    }
                }
                for (String packagePath : packages) {
                    assertThat(pages)
                            .contains(modulePrefix + packagePath + "/package-summary.html");
                }
                for (String type : publicTypes(library)) {
                    assertThat(pages)
                            .contains(modulePrefix + type.replace('.', '/').replace('$', '.')
                                    + ".html");
                }
            }
        }
    }

    @Test
    void runtime_and_source_jars_include_the_repository_license() throws Exception {
        byte[] license = Files.readAllBytes(Path.of(System.getProperty("repository.license")));
        List<Path> sources = paths(System.getProperty("source.jars"));
        assertThat(sources).extracting(DistributionTest::ownerOf)
                .containsExactlyInAnyOrder("client", "broker", "testkit");
        var archives = new ArrayList<>(List.of(CLIENT, BROKER, testkitJar()));
        archives.addAll(sources);
        for (Path path : archives) {
            try (var archive = new ZipFile(path.toFile())) {
                ZipEntry entry = archive.getEntry("META-INF/LICENSE");
                assertThat(entry).as("LICENSE in %s", path).isNotNull();
                assertThat(archive.getInputStream(entry).readAllBytes())
                        .as("LICENSE in %s", path)
                        .isEqualTo(license);
            }
        }
    }

    private static Document readPom(Path path) throws Exception {
        var xml = DocumentBuilderFactory.newInstance();
        xml.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        xml.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        xml.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        return xml.newDocumentBuilder().parse(path.toFile());
    }

    private void runClasspathProbe(String name, String classpath, String body) throws Exception {
        Path root = temporaryDirectory.resolve(name);
        Path source = write(root.resolve("src/probe/Probe.java"), probeSource(body));
        Path classes = Files.createDirectories(root.resolve("classes"));
        // Every lint category on: the JSpecify annotations in our class files must not produce
        // warnings for consumers that do not have the annotation library.
        succeed(
                root,
                "javac",
                "-Xlint:all",
                "-Werror",
                "-cp",
                classpath,
                "-d",
                classes.toString(),
                source.toString()
        );
        succeed(root, "java", "-cp", classes + File.pathSeparator + classpath, "probe.Probe");
    }

    private void runModuleProbe(
            String name,
            String modulepath,
            List<String> requiredModules,
            String body
    ) throws Exception {
        Path root = temporaryDirectory.resolve(name);
        Path source = write(root.resolve("src/probe/Probe.java"), probeSource(body));
        Path descriptor = write(
                root.resolve("src/module-info.java"),
                "module probe { requires " + String.join("; requires ", requiredModules) + "; }"
        );
        Path classes = Files.createDirectories(root.resolve("classes"));
        succeed(
                root,
                "javac",
                "-Xlint:all",
                "-Werror",
                "--module-path",
                modulepath,
                "-d",
                classes.toString(),
                descriptor.toString(),
                source.toString()
        );
        succeed(
                root,
                "java",
                "--module-path",
                classes + File.pathSeparator + modulepath,
                "-m",
                "probe/probe.Probe"
        );
    }

    private static String probeSource(String body) {
        return "package probe; public class Probe { public static void main(String[] args)"
                + " throws Exception {"
                + body + " System.out.println(\"OK\"); } }";
    }

    private static Set<String> classEntries(Path jar) throws IOException {
        try (var archive = new JarFile(jar.toFile())) {
            List<String> entries = archive.stream().map(e -> e.getName()).toList();
            assertThat(entries).contains("module-info.class");
            assertThat(entries).noneMatch(name -> name.endsWith(".jar")
                    || name.startsWith("org/mqtt4j/hato/common/"));
            var classes = new HashSet<String>();
            entries.stream().filter(name -> name.endsWith(".class"))
                    .filter(name -> !name.equals("module-info.class")).forEach(classes::add);
            return classes;
        }
    }

    /// Public types in the jar's exported packages, nested ones by binary name.
    private static Set<String> publicTypes(Path jar) throws IOException {
        Set<String> exported = ModuleFinder.of(jar).findAll().iterator().next().descriptor()
                .exports().stream()
                .filter(export -> !export.isQualified())
                .map(ModuleDescriptor.Exports::source)
                .collect(Collectors.toSet());
        var types = new HashSet<String>();
        try (var archive = new JarFile(jar.toFile())) {
            for (JarEntry entry : archive.stream().toList()) {
                String name = entry.getName();
                if (!name.endsWith(".class") || name.endsWith("module-info.class")
                        || name.endsWith("package-info.class")) {
                    continue;
                }
                String type = name.substring(0, name.length() - ".class".length())
                        .replace('/', '.');
                if (!exported.contains(type.substring(0, type.lastIndexOf('.')))) {
                    continue;
                }
                ClassModel model = ClassFile.of()
                        .parse(archive.getInputStream(entry).readAllBytes());
                if (isPublic(model)) {
                    types.add(type);
                }
            }
        }
        return types;
    }

    /// The descriptors and generic signatures of the public and protected methods and fields of
    /// `type`, one of the public types in `archive`.
    private static List<String> publicSignatures(JarFile archive, String type)
            throws IOException {
        JarEntry entry = archive.getJarEntry(type.replace('.', '/') + ".class");
        ClassModel model = ClassFile.of()
                .parse(archive.getInputStream(entry).readAllBytes());
        var signatures = new ArrayList<String>();
        for (var method : model.methods()) {
            if (method.flags().has(AccessFlag.PUBLIC)
                    || method.flags().has(AccessFlag.PROTECTED)) {
                signatures.add(method.methodType().stringValue());
                method.findAttribute(Attributes.signature())
                        .ifPresent(signature -> signatures.add(
                                signature.signature().stringValue()
                        ));
            }
        }
        for (var field : model.fields()) {
            if (field.flags().has(AccessFlag.PUBLIC)
                    || field.flags().has(AccessFlag.PROTECTED)) {
                signatures.add(field.fieldType().stringValue());
            }
        }
        return signatures;
    }

    /// A nested type's declared access is recorded in the InnerClasses attribute. Enclosing
    /// types are not checked, so a public type nested in a hidden one is listed as well.
    private static boolean isPublic(ClassModel model) {
        String self = model.thisClass().asInternalName();
        return model.flags().has(AccessFlag.PUBLIC)
                && model.findAttribute(Attributes.innerClasses()).stream()
                        .flatMap(attribute -> attribute.classes().stream())
                        .filter(inner -> inner.innerClass().asInternalName().equals(self))
                        .allMatch(inner -> inner.flags().contains(AccessFlag.PUBLIC));
    }

    /// JSpecify may only be `requires static`, never `transitive`: a transitive static requires
    /// makes javac demand the module from module-path consumers at compile time.
    private static boolean isCompileTimeOnlyAnnotations(ModuleDescriptor.Requires requires) {
        return requires.name().equals("org.jspecify")
                && requires.modifiers().contains(ModuleDescriptor.Requires.Modifier.STATIC)
                && !requires.modifiers().contains(ModuleDescriptor.Requires.Modifier.TRANSITIVE);
    }

    private static Path testkitJar() {
        return paths(TESTKIT_JARS).stream()
                .filter(path -> path.getFileName().toString().startsWith("hato-testkit"))
                .findFirst()
                .orElseThrow();
    }

    private static String ownerOf(Path jar) {
        String name = jar.getFileName().toString();
        for (String owner : List.of("client", "broker", "testkit")) {
            if (name.startsWith("hato-" + owner)) {
                return owner;
            }
        }
        throw new AssertionError("Unexpected jar: " + jar);
    }

    private static List<Path> paths(String path) {
        return Arrays.stream(path.split(Pattern.quote(File.pathSeparator))).map(Path::of).toList();
    }

    private static Path write(Path path, String contents) throws IOException {
        Files.createDirectories(path.getParent());
        return Files.writeString(path, contents);
    }

    private static void succeed(Path root, String tool, String... arguments) throws Exception {
        ProcessResult result = execute(root, tool, arguments);
        assertThat(result.exitCode()).as(result.output()).isZero();
        if (tool.equals("java")) {
            assertThat(result.output()).contains("OK");
        }
    }

    private static ProcessResult execute(Path root, String tool, String... arguments)
            throws Exception {
        Files.createDirectories(root);
        Path log = Files.createTempFile(root, tool, ".log");
        String executable = tool + (File.separatorChar == '\\' ? ".exe" : "");
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", executable).toString());
        command.addAll(List.of(arguments));
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        boolean finished = process.waitFor(
                Duration.ofSeconds(30).toMillis(),
                TimeUnit.MILLISECONDS
        );
        if (!finished) {
            process.destroyForcibly();
        }
        assertThat(finished).as("Command timed out: %s", command).isTrue();
        return new ProcessResult(process.exitValue(), Files.readString(log));
    }

    private record ProcessResult(int exitCode, String output) {
    }
}
