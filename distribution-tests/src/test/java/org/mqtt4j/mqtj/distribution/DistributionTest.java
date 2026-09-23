package org.mqtt4j.mqtj.distribution;

import java.io.File;
import java.io.IOException;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
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
            """;
    private static final String BROKER_PROBE = """
            var version = org.mqtt4j.mqtj.broker.internal.common.value.MqttVersion.of(5);
            if (version.protocolLevel() != 5) {
                throw new AssertionError("Broker wire values are not usable");
            }
            org.mqtt4j.mqtj.broker.internal.common.log.Log.of("org.mqtt4j.mqtj.broker.probe")
                    .info("probe {}", 5);
            """;

    @TempDir
    Path temporaryDirectory;

    @Test
    void client_runs_with_only_its_distributed_jar() throws Exception {
        runClasspathProbe("client", CLIENT.toString(), CLIENT_PROBE);
    }

    @Test
    void broker_runs_with_only_its_distributed_jar() throws Exception {
        // White-box until there is a public broker API. There is no client or protocol jar here.
        runClasspathProbe("broker", BROKER.toString(), BROKER_PROBE);
    }

    @Test
    void client_and_broker_can_share_a_classpath() throws Exception {
        runClasspathProbe("both", CLIENT + File.pathSeparator + BROKER,
                CLIENT_PROBE + BROKER_PROBE
        );
    }

    @Test
    void client_runs_as_a_standalone_named_module() throws Exception {
        runModuleProbe("clientModule", CLIENT.toString(), "org.mqtt4j.mqtj.client", CLIENT_PROBE);
    }

    @Test
    void broker_runs_as_a_standalone_named_module() throws Exception {
        // White-box until there is a public broker API: the probe is an ordinary consumer module
        // that is granted access to the internal packages with --add-exports. Nothing is patched
        // into the jar, so it is resolved exactly as an application resolves it, and its
        // `requires static` on the annotation module stays optional.
        Path root = temporaryDirectory.resolve("brokerModule");
        Path source = write(root.resolve("src/probe/Probe.java"), probeSource(BROKER_PROBE));
        Path descriptor = write(root.resolve("src/module-info.java"),
                "module probe { requires org.mqtt4j.mqtj.broker; }"
        );
        Path classes = Files.createDirectories(root.resolve("classes"));
        List<String> exports = List.of(
                "--add-exports",
                "org.mqtt4j.mqtj.broker/org.mqtt4j.mqtj.broker.internal.common.value=probe",
                "--add-exports",
                "org.mqtt4j.mqtj.broker/org.mqtt4j.mqtj.broker.internal.common.log=probe"
        );
        var compile = new ArrayList<>(List.of("-Xlint:all", "-Werror", "--module-path",
                BROKER.toString(), "-d", classes.toString()));
        compile.addAll(exports);
        compile.addAll(List.of(descriptor.toString(), source.toString()));
        succeed(root, "javac", compile.toArray(String[]::new));
        var run = new ArrayList<>(exports);
        run.addAll(List.of("--module-path", classes + File.pathSeparator + BROKER,
                "-m", "probe/probe.Probe"));
        succeed(root, "java", run.toArray(String[]::new));
    }

    @Test
    void testkit_resolves_both_independent_modules_transitively() throws Exception {
        assertThat(paths(TESTKIT_JARS)).hasSize(3);
        runModuleProbe("testkitModule", TESTKIT_JARS, "org.mqtt4j.mqtj.testkit", CLIENT_PROBE + """
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
        for (String type : List.of("MqttVersion", "Qos", "ReasonCode")) {
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
            assertThat(descriptor.exports()).noneMatch(e -> e.source().contains(".internal."));
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
    void source_jars_match_the_embedded_packages() throws Exception {
        for (Path path : paths(System.getProperty("source.jars"))) {
            String owner = ownerOf(path);
            try (var archive = new ZipFile(path.toFile())) {
                String valuePackage = owner.equals("client") ? "org.mqtt4j.mqtj"
                        : "org.mqtt4j.mqtj." + owner + ".internal.common.value";
                for (String type : List.of("MqttVersion", "Qos", "ReasonCode")) {
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

    private void runModuleProbe(String name, String modulepath, String requiredModule, String body)
            throws Exception {
        Path root = temporaryDirectory.resolve(name);
        Path source = write(root.resolve("src/probe/Probe.java"), probeSource(body));
        Path descriptor = write(root.resolve("src/module-info.java"),
                "module probe { requires " + requiredModule + "; }"
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
        return "package probe; public class Probe { public static void main(String[] args) {"
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
