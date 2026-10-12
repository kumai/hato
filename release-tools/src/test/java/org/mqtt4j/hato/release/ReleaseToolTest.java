package org.mqtt4j.hato.release;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReleaseToolTest {

    @TempDir
    Path directory;

    @Test
    void the_version_command_passes_the_version_to_publish_to_later_jobs() throws Exception {
        // Arrange
        Path output = directory.resolve("github-output");
        Map<String, String> environment = tagPush("v0.3.0", output);

        // Act
        ReleaseTool.run(List.of("version", properties("0.3.0").toString()), environment);

        // Assert
        assertThat(Files.readString(output)).isEqualTo("version=0.3.0\n");
    }

    @Test
    void the_version_command_rejects_a_tag_of_another_committed_version() throws Exception {
        // Arrange
        Path output = directory.resolve("github-output");
        Map<String, String> environment = tagPush("v0.3.0", output);
        List<String> args = List.of("version", properties("0.3.0-SNAPSHOT").toString());

        // Act & Assert
        assertThatThrownBy(() -> ReleaseTool.run(args, environment))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("gradle.properties");
        assertThat(output).doesNotExist();
    }

    @Test
    void an_upload_without_the_portal_token_fails_before_any_request() {
        assertThatThrownBy(() -> ReleaseTool.run(List.of("upload", "bundle.zip"), Map.of()))
                .isInstanceOf(ReleaseException.class)
                .hasMessage("Missing CENTRAL_USERNAME");
    }

    @ParameterizedTest(name = "\"{0}\"")
    @ValueSource(strings = {"", "publish", "bundle 0.3.0", "upload a.zip b.zip"})
    void unknown_commands_and_wrong_arguments_show_the_usage(String arguments) {
        List<String> args = arguments.isEmpty() ? List.of() : List.of(arguments.split(" "));

        assertThatThrownBy(() -> ReleaseTool.run(args, Map.of()))
                .isInstanceOf(ReleaseException.class)
                .hasMessageStartingWith("Usage:");
    }

    private Path properties(String version) throws Exception {
        return Files.writeString(
                directory.resolve("gradle.properties"),
                "group=org.mqtt4j\nversion=" + version + "\n"
        );
    }

    private static Map<String, String> tagPush(String tag, Path output) {
        return Map.ofEntries(
                Map.entry("GITHUB_EVENT_NAME", "push"),
                Map.entry("GITHUB_REF_TYPE", "tag"),
                Map.entry("GITHUB_REF_NAME", tag),
                Map.entry("GITHUB_OUTPUT", output.toString())
        );
    }
}
