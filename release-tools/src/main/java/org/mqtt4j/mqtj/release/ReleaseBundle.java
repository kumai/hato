package org.mqtt4j.mqtj.release;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static java.nio.charset.StandardCharsets.US_ASCII;

/// Checks a Central Portal bundle before it is uploaded. The Portal validates the signatures
/// themselves; this check makes sure that every file it requires is present and intact.
final class ReleaseBundle {

    private static final List<String> MODULES = List.of(
            "mqtj-client",
            "mqtj-broker",
            "mqtj-testkit"
    );

    private static final List<String> ARTIFACT_SUFFIXES = List.of(
            ".jar",
            "-sources.jar",
            "-javadoc.jar",
            ".pom",
            ".module"
    );

    /// Central requires a signature and MD5 and SHA-1 checksums for each artifact.
    private static final List<String> REQUIRED_SIDECARS = List.of("", ".asc", ".md5", ".sha1");

    /// Gradle also writes SHA-256 and SHA-512 checksums. It writes none for the signatures.
    private static final Map<String, String> CHECKSUM_ALGORITHMS = Map.ofEntries(
            Map.entry(".md5", "MD5"),
            Map.entry(".sha1", "SHA-1"),
            Map.entry(".sha256", "SHA-256"),
            Map.entry(".sha512", "SHA-512")
    );

    private ReleaseBundle() {
    }

    /// Fails unless `bundle` holds exactly the release files of the three modules at `version`:
    /// each artifact with its signature and its MD5 and SHA-1 checksums, optionally its SHA-256
    /// and SHA-512 checksums, no other or duplicate file, no empty file, and only checksums that
    /// match.
    static void verify(Path bundle, String version) throws IOException {
        ReleaseVersion.check(version);
        var required = new TreeSet<String>();
        var allowed = new HashSet<String>();
        for (String module : MODULES) {
            for (String suffix : ARTIFACT_SUFFIXES) {
                String artifact = "org/mqtt4j/%s/%s/%s-%s%s"
                        .formatted(module, version, module, version, suffix);
                for (String sidecar : REQUIRED_SIDECARS) {
                    required.add(artifact + sidecar);
                }
                for (String checksum : CHECKSUM_ALGORITHMS.keySet()) {
                    allowed.add(artifact + checksum);
                }
            }
        }
        allowed.addAll(required);

        try (var archive = new ZipFile(bundle.toFile())) {
            List<String> names = archive.stream()
                    .filter(entry -> !entry.isDirectory())
                    .map(ZipEntry::getName)
                    .toList();
            if (Set.copyOf(names).size() != names.size()) {
                throw new ReleaseException("The release bundle contains duplicate entries");
            }
            var missing = new TreeSet<>(required);
            missing.removeAll(names);
            var extra = new TreeSet<>(names);
            extra.removeAll(allowed);
            if (!missing.isEmpty() || !extra.isEmpty()) {
                throw new ReleaseException(
                        "Invalid bundle contents: missing=" + missing + ", extra=" + extra
                );
            }

            for (String name : names) {
                byte[] content = read(archive, name);
                if (content.length == 0) {
                    throw new ReleaseException("Empty release file: " + name);
                }
                // A checksum file names the file it covers plus the algorithm's extension.
                String extension = name.substring(name.lastIndexOf('.'));
                String algorithm = CHECKSUM_ALGORITHMS.get(extension);
                if (algorithm != null) {
                    String covered = name.substring(0, name.length() - extension.length());
                    String expected = HexFormat.of()
                            .formatHex(digest(algorithm, read(archive, covered)));
                    String actual = new String(content, US_ASCII).strip().toLowerCase(Locale.ROOT);
                    if (!actual.equals(expected)) {
                        throw new ReleaseException("Checksum mismatch: " + name);
                    }
                }
            }
        }
    }

    private static byte[] read(ZipFile archive, String name) throws IOException {
        ZipEntry entry = archive.getEntry(name);
        if (entry == null) {
            throw new ReleaseException("Missing release file: " + name);
        }
        try (InputStream input = archive.getInputStream(entry)) {
            return input.readAllBytes();
        }
    }

    private static byte[] digest(String algorithm, byte[] content) {
        try {
            return MessageDigest.getInstance(algorithm).digest(content);
        } catch (NoSuchAlgorithmException failure) {
            // The JDK provides all four algorithms.
            throw new IllegalStateException(failure);
        }
    }
}
