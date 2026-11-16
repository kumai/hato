package org.mqtt4j.hato.internal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// The module is layered. The client, in the public package and in
/// `org.mqtt4j.hato.client.internal`, imports nothing of the broker, and the broker imports
/// nothing of the client's internals. The shared implementation in `org.mqtt4j.hato.internal`
/// imports neither and is layered in turn: each of its packages may import `java.*`, the JSpecify
/// annotations, and the layers listed here, nothing else. The lowest layer, `value`, is the
/// values that the public package shares with the broker, and they import nothing of Hato. Fully
/// qualified references without an import are not detected; review covers those.
class LayeringTest {

    private static final Path SOURCES = Path.of("src/main/java/org/mqtt4j/hato");
    private static final String ROOT = "org.mqtt4j.hato.internal.";
    private static final Set<String> VALUES = Set.of(
            "org.mqtt4j.hato.MqttVersion",
            "org.mqtt4j.hato.Qos",
            "org.mqtt4j.hato.ReasonCode",
            "org.mqtt4j.hato.Topic",
            "org.mqtt4j.hato.TopicFilter",
            "org.mqtt4j.hato.TopicText"
    );
    private static final Map<String, Set<String>> ALLOWED_IMPORTS = Map.of(
            "value", Set.of(),
            "util", Set.of(),
            "log", Set.of("util"),
            "protocol", Set.of("value", "util"),
            "concurrent", Set.of("util", "log"),
            "io", Set.of("value", "util", "log", "concurrent", "protocol"),
            "inproc", Set.of("protocol")
    );
    private static final Pattern IMPORT = Pattern.compile("^import\\s+(?:static\\s+)?([\\w.]+)");

    @Test
    void each_layer_imports_only_the_layers_below_it() throws IOException {
        Path internal = SOURCES.resolve("internal");
        for (Path source : sources(internal, Integer.MAX_VALUE)) {
            Path relative = internal.relativize(source);
            assertThat(relative.getNameCount())
                    .as("%s must be in a layer", source)
                    .isGreaterThan(1);
            checkLayer(source, relative.getName(0).toString());
        }
        for (String value : VALUES) {
            String file = value.substring(value.lastIndexOf('.') + 1) + ".java";
            checkLayer(SOURCES.resolve(file), "value");
        }
    }

    @Test
    void the_client_imports_nothing_of_the_broker() throws IOException {
        // The public package itself, without its subpackages, and the client's internals.
        List<Path> client = Stream.concat(
                sources(SOURCES, 1).stream(),
                sources(SOURCES.resolve("client"), Integer.MAX_VALUE).stream()
        ).toList();
        for (Path source : client) {
            assertThat(imports(source))
                    .as("imports of %s", source)
                    .noneMatch(imported -> imported.startsWith("org.mqtt4j.hato.broker."));
        }
    }

    @Test
    void the_broker_imports_nothing_of_the_client_internals() throws IOException {
        for (Path source : sources(SOURCES.resolve("broker"), Integer.MAX_VALUE)) {
            assertThat(imports(source))
                    .as("imports of %s", source)
                    .noneMatch(imported -> imported.startsWith("org.mqtt4j.hato.client."));
        }
    }

    private static void checkLayer(Path source, String layer) throws IOException {
        assertThat(ALLOWED_IMPORTS).as("unknown layer in %s", source).containsKey(layer);
        for (String imported : imports(source)) {
            if (imported.startsWith("java.") || imported.startsWith("org.jspecify.annotations.")) {
                continue;
            }
            String target;
            if (VALUES.stream().anyMatch(value -> imported.equals(value)
                    || imported.startsWith(value + "."))) {
                target = "value";
            } else {
                assertThat(imported).as("%s imports %s", source, imported).startsWith(ROOT);
                target = imported.substring(ROOT.length()).split("\\.")[0];
            }
            if (target.equals(layer)) {
                continue;
            }
            assertThat(ALLOWED_IMPORTS.get(layer))
                    .as("%s is in layer %s and must not import %s", source, layer, imported)
                    .contains(target);
        }
    }

    // The Java sources under `directory`, to `depth` levels of directories down.
    private static List<Path> sources(Path directory, int depth) throws IOException {
        try (Stream<Path> files = Files.walk(directory, depth)) {
            List<Path> sources = files.filter(path -> path.toString().endsWith(".java")).toList();
            assertThat(sources).as("sources in %s", directory).isNotEmpty();
            return sources;
        }
    }

    private static List<String> imports(Path source) throws IOException {
        return Files.readAllLines(source).stream()
                .map(IMPORT::matcher)
                .filter(matcher -> matcher.find())
                .map(matcher -> matcher.group(1))
                .toList();
    }
}
