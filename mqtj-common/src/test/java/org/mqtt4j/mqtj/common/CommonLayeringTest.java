package org.mqtt4j.mqtj.common;

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

/// The shared implementation is layered. Each subpackage may import `java.*`, the JSpecify
/// annotations, and the layers listed here, nothing else. Fully qualified references without an
/// import are not detected; review covers those.
class CommonLayeringTest {

    private static final Path SOURCES = Path.of("src/main/java/org/mqtt4j/mqtj/common");
    private static final String ROOT = "org.mqtt4j.mqtj.common.";
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
        try (Stream<Path> files = Files.walk(SOURCES)) {
            List<Path> sources = files.filter(path -> path.toString().endsWith(".java")).toList();
            assertThat(sources).isNotEmpty();
            for (Path source : sources) {
                Path relative = SOURCES.relativize(source);
                assertThat(relative.getNameCount())
                        .as("%s must be in a layer", source)
                        .isGreaterThan(1);
                String layer = relative.getName(0).toString();
                assertThat(ALLOWED_IMPORTS).as("unknown layer in %s", source).containsKey(layer);
                for (String imported : imports(source)) {
                    if (imported.startsWith("java.")
                            || imported.startsWith("org.jspecify.annotations.")) {
                        continue;
                    }
                    assertThat(imported).as("%s imports %s", source, imported).startsWith(ROOT);
                    String target = imported.substring(ROOT.length()).split("\\.")[0];
                    if (target.equals(layer)) {
                        continue;
                    }
                    assertThat(ALLOWED_IMPORTS.get(layer))
                            .as("%s is in layer %s and must not import %s", source, layer, imported)
                            .contains(target);
                }
            }
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
