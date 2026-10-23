package org.mqtt4j.hato.release;

import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// The rules that decide which version a release run publishes.
final class ReleaseVersion {

    private static final Pattern FORMAT = Pattern.compile(
            "[0-9]+\\.[0-9]+\\.[0-9]+(?:-[A-Za-z0-9]+(?:[.-][A-Za-z0-9]+)*)?"
    );

    // The three numbers at the start of a version; any suffix, such as -SNAPSHOT, is ignored.
    private static final Pattern NUMBERS = Pattern.compile(
            "([0-9]+)\\.([0-9]+)\\.([0-9]+)(?:-.*)?"
    );

    /// The GitHub event and ref that started a release run.
    record Ref(
            String event,
            String type,
            String name
    ) {}

    private ReleaseVersion() {
    }

    /// Fails unless `version` can be published: three numeric parts, an optional pre-release
    /// suffix such as `-rc.1`, and no SNAPSHOT in any letter case.
    static void check(String version) {
        if (!FORMAT.matcher(version).matches()) {
            throw new ReleaseException("Expected a release version such as 0.3.0 or 0.3.0-rc.1");
        }
        if (version.toUpperCase(Locale.ROOT).contains("SNAPSHOT")) {
            throw new ReleaseException("SNAPSHOT versions cannot be released");
        }
    }

    /// Returns the version that a run started by `ref` publishes, which its tag names.
    ///
    /// The tag may name the committed version or an earlier one, such as `0.7.1` while
    /// `gradle.properties` already has `0.8.0-SNAPSHOT`, but nothing above it: a typo in the tag
    /// would otherwise publish a number that Central never removes. Only the three numbers
    /// count, so `0.8.0` and `0.8.0-rc.1` are both within `0.8.0-SNAPSHOT`.
    ///
    /// A published GitHub Release can publish any such version. A manual trial takes only a
    /// pre-release version, because Central may not accept a final version number again after
    /// the trial is dropped.
    static String of(Ref ref, String committedVersion) {
        if (!ref.type().equals("tag") || !ref.name().startsWith("v")) {
            throw new ReleaseException("Select a v-prefixed tag, including for a manual trial");
        }
        String version = ref.name().substring(1);
        check(version);
        switch (ref.event()) {
            case "release" -> {
                // Any checked version, including a pre-release, can be published.
            }
            case "workflow_dispatch" -> {
                if (!version.contains("-")) {
                    throw new ReleaseException(
                            "Manual trials need a pre-release version such as 0.3.0-rc.1"
                    );
                }
            }
            // No other event, such as a tag push or a scheduled run, decides what to publish.
            default -> throw new ReleaseException(
                    "Releases start from a published GitHub Release or a manual trial, not "
                            + ref.event()
            );
        }
        if (Arrays.compare(numbers(version), numbers(committedVersion)) > 0) {
            throw new ReleaseException(
                    "%s is above %s in gradle.properties; raise the version there to release it"
                            .formatted(version, committedVersion)
            );
        }
        return version;
    }

    /// Returns the three numbers at the start of `version`, such as 0, 8, and 0 for
    /// `0.8.0-SNAPSHOT`.
    private static int[] numbers(String version) {
        Matcher numbers = NUMBERS.matcher(version);
        if (!numbers.matches()) {
            throw new ReleaseException(
                    "Expected a version such as 0.8.0-SNAPSHOT in gradle.properties, not "
                            + version
            );
        }
        return new int[] {
                Integer.parseInt(numbers.group(1)),
                Integer.parseInt(numbers.group(2)),
                Integer.parseInt(numbers.group(3))
        };
    }
}
