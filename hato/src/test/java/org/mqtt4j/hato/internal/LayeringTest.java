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

/// The shared implementation is layered. Each package of `org.mqtt4j.hato.internal` may import
/// `java.*`, the JSpecify annotations, and the layers listed here, nothing else. The lowest
/// layer, `value`, is the values that the public package shares with the broker, and they import
/// nothing of Hato. Fully qualified references without an import are not detected; review covers
/// those.
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
            "io", Set.of("value", "util", "log", "concurrent", "protocol")
    );
    private static final Pattern IMPORT = Pattern.compile("^import\\s+(?:static\\s+)?([\\w.]+)");

    @Test
    void each_layer_imports_only_the_layers_below_it() throws IOException {
        Path internal = SOURCES.resolve("internal");
        try (Stream<Path> files = Files.walk(internal)) {
            List<Path> sources = files.filter(path -> path.toString().endsWith(".java")).toList();
            assertThat(sources).isNotEmpty();
            for (Path source : sources) {
                Path relative = internal.relativize(source);
                assertThat(relative.getNameCount())
                        .as("%s must be in a layer", source)
                        .isGreaterThan(1);
                checkImports(source, relative.getName(0).toString());
            }
        }
        for (String value : VALUES) {
            String file = value.substring(value.lastIndexOf('.') + 1) + ".java";
            checkImports(SOURCES.resolve(file), "value");
        }
    }

    private static void checkImports(Path source, String layer) throws IOException {
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

    private static List<String> imports(Path source) throws IOException {
        return Files.readAllLines(source).stream()
                .map(IMPORT::matcher)
                .filter(matcher -> matcher.find())
                .map(matcher -> matcher.group(1))
                .toList();
    }
}
