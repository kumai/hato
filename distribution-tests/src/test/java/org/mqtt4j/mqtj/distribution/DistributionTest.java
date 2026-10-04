package org.mqtt4j.mqtj.distribution;

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
import java.util.zip.ZipFile;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class DistributionTest {

    private static final Path CLIENT = Path.of(System.getProperty("client.jar"));
    private static final Path BROKER = Path.of(System.getProperty("broker.jar"));
    private static final String TESTKIT_JARS = System.getProperty("testkit.jars");
    private static final String CLIENT_PROBE = """
            if (org.mqtt4j.mqtj.MqttVersion.of(4) != org.mqtt4j.mqtj.MqttVersion.V3_1_1
                    || org.mqtt4j.mqtj.MqttVersion.V5.protocolLevel() != 5
                    || org.mqtt4j.mqtj.Qos.of(2) != org.mqtt4j.mqtj.Qos.EXACTLY_ONCE
                    || org.mqtt4j.mqtj.ReasonCode.of(0x80)
                            != org.mqtt4j.mqtj.ReasonCode.UNSPECIFIED_ERROR
                    || org.mqtt4j.mqtj.ReasonCode.WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED.value()
                            != 0xA2) {
                throw new AssertionError("Client wire values are not usable");
            }
            var filter = new org.mqtt4j.mqtj.TopicFilter("a/#");
            if (!filter.matches(new org.mqtt4j.mqtj.Topic("a/b"))
                    || filter.matches(new org.mqtt4j.mqtj.Topic("b/a"))) {
                throw new AssertionError("Client topic values are not usable");
            }
            org.mqtt4j.mqtj.MqttClient client = org.mqtt4j.mqtj.MqttClient.builder()
                    .server(java.net.URI.create("mqtt://localhost"))
                    .version(org.mqtt4j.mqtj.MqttVersion.V3_1_1)
                    .connectionListener(end -> {
                        if (end.cause() != null) {
                            end.cause().getMessage();
                        }
                    })
                    .build();
            client.close();
            if (!client.connect().isCompletedExceptionally()) {
                throw new AssertionError("Closed client accepted a connection");
            }
            var transport = org.mqtt4j.mqtj.spi.Transport.tcp("probe");
            transport.close();
            if (transport.send(1, new byte[0])
                    != org.mqtt4j.mqtj.spi.Transport.SendResult.CLOSED) {
                throw new AssertionError("Closed transport accepted a write");
            }
            var cause = new org.mqtt4j.mqtj.MqttProtocolException(
                    "Invalid packet field",
                    org.mqtt4j.mqtj.ReasonCode.MALFORMED_PACKET
            );
            var failure = new org.mqtt4j.mqtj.MqttProtocolException(
                    "Invalid packet",
                    org.mqtt4j.mqtj.ReasonCode.MALFORMED_PACKET,
                    cause
            );
            if (failure.reasonCode() != org.mqtt4j.mqtj.ReasonCode.MALFORMED_PACKET
                    || failure.getCause() != cause
                    || cause.getCause() != null) {
                throw new AssertionError("Public protocol failure is not usable");
            }
            """;
    private static final String BROKER_PROBE = """
            try (var broker = org.mqtt4j.mqtj.broker.MqttBroker.start()) {
                if (broker.port() <= 0) {
                    throw new AssertionError("Broker did not bind a port");
                }
            }
            """;
    private static final String CONNECTION_PROBE = """
            try (var broker = org.mqtt4j.mqtj.broker.MqttBroker.start();
                    org.mqtt4j.mqtj.MqttClient sut = org.mqtt4j.mqtj.MqttClient.builder()
                            .server(java.net.URI.create("mqtt://localhost:" + broker.port()))
                            .version(org.mqtt4j.mqtj.MqttVersion.V3_1_1)
                            .build()) {
                org.mqtt4j.mqtj.ConnAck connAck = sut.connect()
                        .get(5, java.util.concurrent.TimeUnit.SECONDS);
                if (connAck.sessionPresent()) {
                    throw new AssertionError("Clean connection resumed a session");
                }

                // Arrange: use the public messaging roles from the distributed client jar,
                // with one handler and one pull subscription.
                var handled =
                        new java.util.concurrent.LinkedBlockingQueue<org.mqtt4j.mqtj.MqttMessage>();
                org.mqtt4j.mqtj.MqttSubscriber subscriber = sut;
                org.mqtt4j.mqtj.MqttPublisher publisher = sut;
                org.mqtt4j.mqtj.Subscription push = subscriber.subscribe(
                        org.mqtt4j.mqtj.Subscribe.of(
                                new org.mqtt4j.mqtj.TopicFilter("probe/push"),
                                org.mqtt4j.mqtj.Qos.AT_LEAST_ONCE
                        ),
                        delivery -> handled.add(delivery.message())
                ).get(5, java.util.concurrent.TimeUnit.SECONDS);
                org.mqtt4j.mqtj.Subscription pull = subscriber.subscribe(
                        org.mqtt4j.mqtj.Subscribe.of(
                                new org.mqtt4j.mqtj.TopicFilter("probe/pull"),
                                org.mqtt4j.mqtj.Qos.AT_LEAST_ONCE
                        ).withAckMode(org.mqtt4j.mqtj.AckMode.MANUAL)
                ).get(5, java.util.concurrent.TimeUnit.SECONDS);

                // Act & Assert: QoS 0 and 1 reach the handler and the receiving thread intact.
                for (var qos : java.util.List.of(
                        org.mqtt4j.mqtj.Qos.AT_MOST_ONCE, org.mqtt4j.mqtj.Qos.AT_LEAST_ONCE
                )) {
                    var pushed = new org.mqtt4j.mqtj.MqttMessage(
                            new org.mqtt4j.mqtj.Topic("probe/push"), new byte[] {42}, qos, false
                    );
                    publisher.publish(pushed).get(5, java.util.concurrent.TimeUnit.SECONDS);
                    if (!pushed.equals(handled.poll(5, java.util.concurrent.TimeUnit.SECONDS))) {
                        throw new AssertionError(qos + " delivery to a handler changed");
                    }
                    var pulled = new org.mqtt4j.mqtj.MqttMessage(
                            new org.mqtt4j.mqtj.Topic("probe/pull"), new byte[] {7}, qos, false
                    );
                    publisher.publish(pulled).get(5, java.util.concurrent.TimeUnit.SECONDS);
                    if (!(pull.receive(java.time.Duration.ofSeconds(5))
                            instanceof org.mqtt4j.mqtj.ReceiveResult.Received(var delivery))
                            || !pulled.equals(delivery.message())) {
                        throw new AssertionError(qos + " delivery to a receiver changed");
                    }
                    delivery.ack();
                }

                // Act & Assert: lifecycle operations must finish within the deadline.
                push.unsubscribe().get(5, java.util.concurrent.TimeUnit.SECONDS);
                pull.unsubscribe().get(5, java.util.concurrent.TimeUnit.SECONDS);
                if (!(pull.receive() instanceof org.mqtt4j.mqtj.ReceiveResult.Ended)) {
                    throw new AssertionError("A removed subscription kept receiving");
                }
                sut.disconnect().get(5, java.util.concurrent.TimeUnit.SECONDS);
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
                List.of("org.mqtt4j.mqtj.client"),
                CLIENT_PROBE
        );
    }

    @Test
    void broker_runs_as_a_standalone_named_module() throws Exception {
        runModuleProbe(
                "brokerModule",
                BROKER.toString(),
                List.of("org.mqtt4j.mqtj.broker"),
                BROKER_PROBE
        );
    }

    @Test
    void client_and_broker_connect_as_independent_named_modules() throws Exception {
        runModuleProbe(
                "bothModules",
                CLIENT + File.pathSeparator + BROKER,
                List.of("org.mqtt4j.mqtj.client", "org.mqtt4j.mqtj.broker"),
                CONNECTION_PROBE
        );
    }

    @Test
    void testkit_resolves_both_independent_modules_transitively() throws Exception {
        assertThat(paths(TESTKIT_JARS)).hasSize(3);
        runModuleProbe("testkitModule", TESTKIT_JARS, List.of("org.mqtt4j.mqtj.testkit"),
                CLIENT_PROBE + CONNECTION_PROBE + """
                var scheduler = new org.mqtt4j.mqtj.testkit.ManualTimerScheduler();
                var ran = new java.util.concurrent.atomic.AtomicBoolean();
                scheduler.schedule(java.time.Duration.ofSeconds(1), () -> ran.set(true));
                scheduler.advance(java.time.Duration.ofSeconds(1));
                if (!ran.get()) {
                    throw new AssertionError("Public timer SPI is not usable");
                }
                if (ModuleLayer.boot().findModule("org.mqtt4j.mqtj.client").isEmpty()
                        || ModuleLayer.boot().findModule("org.mqtt4j.mqtj.broker").isEmpty()) {
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
            assertThat(clientClasses).contains("org/mqtt4j/mqtj/" + type + ".class");
            assertThat(clientClasses)
                    .noneMatch(name -> name.endsWith("/value/" + type + ".class"));
            assertThat(brokerClasses)
                    .contains("org/mqtt4j/mqtj/broker/internal/common/value/" + type + ".class");
            assertThat(testkitClasses)
                    .contains("org/mqtt4j/mqtj/testkit/internal/common/value/" + type + ".class");
        }
        for (Path jar : List.of(CLIENT, BROKER, testkit)) {
            assertThat(classEntries(jar))
                    .contains("org/mqtt4j/mqtj/" + ownerOf(jar) + "/internal/common/log/Log.class")
                    .noneMatch(name -> name.endsWith("/ProtocolVersion.class"));
        }
        assertThat(clientClasses).doesNotContainAnyElementsOf(brokerClasses);
        assertThat(clientClasses).doesNotContainAnyElementsOf(testkitClasses);
        assertThat(brokerClasses).doesNotContainAnyElementsOf(testkitClasses);

        for (Path jar : List.of(CLIENT, BROKER, testkit)) {
            var descriptor = ModuleFinder.of(jar).findAll().iterator().next().descriptor();
            assertThat(descriptor.isAutomatic()).isFalse();
            assertThat(descriptor.isOpen()).isFalse();
            assertThat(descriptor.opens()).isEmpty();
            assertThat(descriptor.requires()).allMatch(r -> r.name().startsWith("java.")
                    || (jar.equals(testkit) && r.name().startsWith("org.mqtt4j.mqtj."))
                    || isCompileTimeOnlyAnnotations(r));
            assertThat(descriptor.requires())
                    .anyMatch(DistributionTest::isCompileTimeOnlyAnnotations);
            assertThat(descriptor.exports())
                    .noneMatch(e -> List.of(e.source().split("\\.")).contains("internal"));
            assertThat(descriptor.packages())
                    .noneMatch(p -> p.startsWith("org.mqtt4j.mqtj.common"));
        }

        Path root = temporaryDirectory.resolve("privateApi");
        Path descriptor = write(root.resolve("src/module-info.java"),
                "module probe { requires org.mqtt4j.mqtj.client; requires org.mqtt4j.mqtj.broker; "
                        + "requires org.mqtt4j.mqtj.testkit; }"
        );
        for (String owner : List.of("client", "broker", "testkit")) {
            Path source = write(root.resolve("src/probe/Probe.java"), """
                    package probe;
                    import org.mqtt4j.mqtj.%s.internal.common.log.Log;
                    public class Probe { Log value; }
                    """.formatted(owner));
            var result = execute(root, "javac", "-XDrawDiagnostics", "--module-path",
                    TESTKIT_JARS, "-d", root.resolve("classes").toString(),
                    descriptor.toString(), source.toString()
            );
            assertThat(result.exitCode()).isNotZero();
            assertThat(result.output()).contains("compiler.err.package.not.visible");
        }
    }

    @Test
    void each_jar_exports_exactly_the_types_of_its_public_api() throws Exception {
        // A change here is a change to the public API; update the list only on purpose.
        assertThat(publicTypes(CLIENT)).containsExactlyInAnyOrder(
                "org.mqtt4j.mqtj.AckMode",
                "org.mqtt4j.mqtj.ConnAck",
                "org.mqtt4j.mqtj.ConnectionListener",
                "org.mqtt4j.mqtj.Disconnection",
                "org.mqtt4j.mqtj.MqttCapacityException",
                "org.mqtt4j.mqtj.MqttClient",
                "org.mqtt4j.mqtj.MqttClientBuilder",
                "org.mqtt4j.mqtj.MqttConnectException",
                "org.mqtt4j.mqtj.MqttConnectionLostException",
                "org.mqtt4j.mqtj.MqttDelivery",
                "org.mqtt4j.mqtj.MqttException",
                "org.mqtt4j.mqtj.MqttMessage",
                "org.mqtt4j.mqtj.MqttNotConnectedException",
                "org.mqtt4j.mqtj.MqttProtocolException",
                "org.mqtt4j.mqtj.MqttPublisher",
                "org.mqtt4j.mqtj.MqttSubscribeException",
                "org.mqtt4j.mqtj.MqttSubscriber",
                "org.mqtt4j.mqtj.MqttVersion",
                "org.mqtt4j.mqtj.PublishResult",
                "org.mqtt4j.mqtj.Qos",
                "org.mqtt4j.mqtj.ReasonCode",
                "org.mqtt4j.mqtj.ReceiveResult",
                "org.mqtt4j.mqtj.ReceiveResult$Ended",
                "org.mqtt4j.mqtj.ReceiveResult$Received",
                "org.mqtt4j.mqtj.ReceiveResult$TimedOut",
                "org.mqtt4j.mqtj.Subscribe",
                "org.mqtt4j.mqtj.Subscription",
                "org.mqtt4j.mqtj.Topic",
                "org.mqtt4j.mqtj.TopicFilter",
                "org.mqtt4j.mqtj.spi.TimerScheduler",
                "org.mqtt4j.mqtj.spi.TimerScheduler$Cancellable",
                "org.mqtt4j.mqtj.spi.Transport",
                "org.mqtt4j.mqtj.spi.Transport$Factory",
                "org.mqtt4j.mqtj.spi.Transport$Listener",
                "org.mqtt4j.mqtj.spi.Transport$SendResult"
        );
        assertThat(publicTypes(BROKER)).containsExactly("org.mqtt4j.mqtj.broker.MqttBroker");
        assertThat(publicTypes(testkitJar())).containsExactlyInAnyOrder(
                "org.mqtt4j.mqtj.testkit.FaultyTransport",
                "org.mqtt4j.mqtj.testkit.ManualTimerScheduler"
        );
    }

    @Test
    void source_jars_match_the_embedded_packages() throws Exception {
        for (Path path : paths(System.getProperty("source.jars"))) {
            String owner = ownerOf(path);
            try (var archive = new ZipFile(path.toFile())) {
                String valuePackage = owner.equals("client") ? "org.mqtt4j.mqtj"
                        : "org.mqtt4j.mqtj." + owner + ".internal.common.value";
                for (String type : List.of(
                        "MqttVersion", "Qos", "ReasonCode", "Topic", "TopicFilter"
                )) {
                    String name = valuePackage.replace('.', '/') + "/" + type + ".java";
                    var entry = archive.getEntry(name);
                    assertThat(entry).as("%s in %s", name, path).isNotNull();
                    String source = new String(archive.getInputStream(entry).readAllBytes(), UTF_8);
                    assertThat(source).contains("package " + valuePackage + ";");
                    assertThat(source).doesNotContain("org.mqtt4j.mqtj.common");
                }
                assertThat(archive.stream().map(e -> e.getName()))
                        .noneMatch(name -> name.startsWith("org/mqtt4j/mqtj/common/"));
            }
        }
    }

    @Test
    void publication_metadata_has_only_the_intended_dependencies() throws Exception {
        List<Path> metadata = paths(System.getProperty("publication.metadata"));
        assertThat(metadata).hasSize(6);
        var xml = DocumentBuilderFactory.newInstance();
        xml.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        xml.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        xml.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        for (Path path : metadata) {
            String contents = Files.readString(path);
            assertThat(contents).doesNotContain("mqtj-common", "mqtj-protocol", "mqtj-core",
                    "jspecify");
            if (path.toString().endsWith(".xml")) {
                var document = xml.newDocumentBuilder().parse(path.toFile());
                var groups = document.getElementsByTagName("groupId");
                assertThat(groups.getLength()).isPositive();
                for (int index = 0; index < groups.getLength(); index++) {
                    assertThat(groups.item(index).getTextContent()).isEqualTo("org.mqtt4j");
                }
                var dependencies = document.getElementsByTagName("dependency");
                var artifacts = new HashSet<String>();
                for (int index = 0; index < dependencies.getLength(); index++) {
                    var dependency = (org.w3c.dom.Element) dependencies.item(index);
                    artifacts.add(dependency.getElementsByTagName("artifactId")
                            .item(0).getTextContent());
                }
                if (document.getDocumentElement().getElementsByTagName("artifactId")
                        .item(0).getTextContent().equals("mqtj-testkit")) {
                    assertThat(artifacts).containsExactlyInAnyOrder("mqtj-client", "mqtj-broker");
                } else {
                    assertThat(artifacts).isEmpty();
                }
            } else {
                var groups = Pattern.compile("\"group\"\\s*:\\s*\"([^\"]+)\"")
                        .matcher(contents).results().map(m -> m.group(1)).toList();
                assertThat(groups).isNotEmpty().containsOnly("org.mqtt4j");
                var modules = Pattern.compile("\"module\"\\s*:\\s*\"([^\"]+)\"")
                        .matcher(contents).results().map(m -> m.group(1)).toList();
                assertThat(modules).isNotEmpty();
                if (modules.getFirst().equals("mqtj-testkit")) {
                    assertThat(new HashSet<>(modules)).containsExactlyInAnyOrder(
                            "mqtj-testkit", "mqtj-client", "mqtj-broker"
                    );
                } else {
                    // No published variant may acquire a dependency, including an internal one.
                    assertThat(contents).doesNotContain("\"dependencies\"");
                }
            }
        }
    }

    private void runClasspathProbe(String name, String classpath, String body) throws Exception {
        Path root = temporaryDirectory.resolve(name);
        Path source = write(root.resolve("src/probe/Probe.java"), probeSource(body));
        Path classes = Files.createDirectories(root.resolve("classes"));
        // Every lint category on: the JSpecify annotations in our class files must not produce
        // warnings for consumers that do not have the annotation library.
        succeed(root, "javac", "-Xlint:all", "-Werror", "-cp", classpath, "-d", classes.toString(),
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
        succeed(root, "javac", "-Xlint:all", "-Werror", "--module-path", modulepath, "-d",
                classes.toString(), descriptor.toString(), source.toString()
        );
        succeed(root, "java", "--module-path", classes + File.pathSeparator + modulepath,
                "-m", "probe/probe.Probe"
        );
    }

    private static String probeSource(String body) {
        return "package probe; public class Probe { public static void main(String[] args)"
                + " throws Exception {"
                + body + " System.out.println(\"OK\"); } }";
    }

    private static Set<String> classEntries(Path jar) throws IOException {
        try (var archive = new JarFile(jar.toFile())) {
            var entries = archive.stream().map(e -> e.getName()).toList();
            assertThat(entries).contains("module-info.class");
            assertThat(entries).noneMatch(name -> name.endsWith(".jar")
                    || name.startsWith("org/mqtt4j/mqtj/common/") || name.startsWith("dev/mqtj/"));
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
                .filter(path -> path.getFileName().toString().startsWith("mqtj-testkit"))
                .findFirst()
                .orElseThrow();
    }

    private static String ownerOf(Path jar) {
        String name = jar.getFileName().toString();
        for (String owner : List.of("client", "broker", "testkit")) {
            if (name.startsWith("mqtj-" + owner)) {
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
        Process process = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(log.toFile()).start();
        boolean finished = process.waitFor(
                Duration.ofSeconds(30).toMillis(), TimeUnit.MILLISECONDS
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
