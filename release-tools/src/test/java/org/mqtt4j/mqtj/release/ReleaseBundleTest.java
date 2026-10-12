package org.mqtt4j.mqtj.release;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReleaseBundleTest {

    private static final String CLIENT_JAR = "org/mqtt4j/mqtj-client/0.3.0/mqtj-client-0.3.0.jar";

    private static final List<String> ARTIFACT_SUFFIXES = List.of(
            ".jar",
            "-sources.jar",
            "-javadoc.jar",
            ".pom",
            ".module"
    );

    @TempDir
    Path directory;

    /// The files of a complete signed bundle for 0.3.0, in the layout that Gradle stages.
    private final Map<String, byte[]> files = completeBundle();

    @Test
    void a_complete_signed_bundle_passes_with_optional_checksums() {
        // Arrange: Gradle also writes SHA-256 and SHA-512 checksums of each artifact.
        for (Map.Entry<String, byte[]> file : List.copyOf(files.entrySet())) {
            if (ARTIFACT_SUFFIXES.stream().anyMatch(file.getKey()::endsWith)) {
                files.put(file.getKey() + ".sha256", checksum("SHA-256", file.getValue()));
                files.put(file.getKey() + ".sha512", checksum("SHA-512", file.getValue()));
            }
        }

        // Act & Assert
        assertThatCode(() -> ReleaseBundle.verify(write(zip(files)), "0.3.0"))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("requiredFiles")
    void every_artifact_signature_and_required_checksum_must_be_present(String name) {
        files.remove(name);

        assertThatThrownBy(() -> ReleaseBundle.verify(write(zip(files)), "0.3.0"))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("missing=[" + name + "]");
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            "org/mqtt4j/mqtj-client/maven-metadata.xml",
            "org/mqtt4j/mqtj-client/0.2.0/mqtj-client-0.2.0.jar",
            "org/mqtt4j/mqtj-client/0.3.0/other.txt",
            CLIENT_JAR + ".asc.md5"
    })
    void other_files_and_other_versions_are_rejected(String name) {
        files.put(name, "extra".getBytes(UTF_8));

        assertThatThrownBy(() -> ReleaseBundle.verify(write(zip(files)), "0.3.0"))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("extra=[" + name + "]");
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {".md5", ".sha1"})
    void a_checksum_that_does_not_match_its_file_is_rejected(String suffix) {
        files.put(CLIENT_JAR + suffix, "0123456789abcdef".getBytes(UTF_8));

        assertThatThrownBy(() -> ReleaseBundle.verify(write(zip(files)), "0.3.0"))
                .isInstanceOf(ReleaseException.class)
                .hasMessage("Checksum mismatch: " + CLIENT_JAR + suffix);
    }

    @Test
    void an_empty_signature_is_rejected() {
        files.put(CLIENT_JAR + ".asc", new byte[0]);

        assertThatThrownBy(() -> ReleaseBundle.verify(write(zip(files)), "0.3.0"))
                .isInstanceOf(ReleaseException.class)
                .hasMessage("Empty release file: " + CLIENT_JAR + ".asc");
    }

    @Test
    void duplicate_entries_are_rejected() throws IOException {
        // Arrange: ZipOutputStream refuses duplicate names. Add the jar again under a placeholder
        // name of the same length, then rename it in the local header and the central directory.
        String placeholder = CLIENT_JAR.replace("client", "CLIENT");
        files.put(placeholder, requireNonNull(files.get(CLIENT_JAR)));
        byte[] bundle = replaceAll(
                zip(files),
                placeholder.getBytes(UTF_8),
                CLIENT_JAR.getBytes(UTF_8)
        );
        Path path = write(bundle);

        // Act & Assert
        assertThatThrownBy(() -> ReleaseBundle.verify(path, "0.3.0"))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("duplicate");
    }

    static Stream<String> requiredFiles() {
        return completeBundle().keySet().stream();
    }

    private static Map<String, byte[]> completeBundle() {
        var files = new LinkedHashMap<String, byte[]>();
        for (String module : List.of("mqtj-client", "mqtj-broker", "mqtj-testkit")) {
            for (String suffix : ARTIFACT_SUFFIXES) {
                String name = "org/mqtt4j/%s/0.3.0/%s-0.3.0%s".formatted(module, module, suffix);
                byte[] content = name.getBytes(UTF_8);
                files.put(name, content);
                files.put(name + ".asc", "test signature".getBytes(UTF_8));
                files.put(name + ".md5", checksum("MD5", content));
                files.put(name + ".sha1", checksum("SHA-1", content));
            }
        }
        return files;
    }

    private static byte[] checksum(String algorithm, byte[] content) {
        try {
            byte[] digest = MessageDigest.getInstance(algorithm).digest(content);
            return HexFormat.of().formatHex(digest).getBytes(UTF_8);
        } catch (NoSuchAlgorithmException failure) {
            throw new AssertionError(failure);
        }
    }

    private static byte[] zip(Map<String, byte[]> files) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var archive = new ZipOutputStream(bytes)) {
            for (Map.Entry<String, byte[]> file : files.entrySet()) {
                archive.putNextEntry(new ZipEntry(file.getKey()));
                archive.write(file.getValue());
                archive.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static byte[] replaceAll(byte[] data, byte[] target, byte[] replacement) {
        byte[] result = data.clone();
        for (int index = 0; index + target.length <= result.length; index++) {
            if (Arrays.equals(result, index, index + target.length, target, 0, target.length)) {
                System.arraycopy(replacement, 0, result, index, replacement.length);
            }
        }
        return result;
    }

    private Path write(byte[] bundle) throws IOException {
        return Files.write(directory.resolve("bundle.zip"), bundle);
    }
}
