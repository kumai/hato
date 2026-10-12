package org.mqtt4j.hato.release;

import java.util.Locale;
import java.util.regex.Pattern;

/// The rules that decide which version a release run publishes.
final class ReleaseVersion {

    private static final Pattern FORMAT = Pattern.compile(
            "[0-9]+\\.[0-9]+\\.[0-9]+(?:-[A-Za-z0-9]+(?:[.-][A-Za-z0-9]+)*)?"
    );

    /// The GitHub event and ref that started a release run. `trialVersion` is the version that a
    /// manual trial asks for; it is empty for a tag push.
    record Ref(
            String event,
            String type,
            String name,
            String trialVersion
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

    /// Returns the version that a run started by `ref` publishes.
    ///
    /// A tag push publishes the committed version, which the tag must name. A manual trial
    /// replaces the committed version with the pre-release version that its tag names. A trial
    /// never takes a final version number, because Central may not accept that number again
    /// after the trial is dropped.
    static String of(Ref ref, String committedVersion) {
        if (!ref.type().equals("tag") || !ref.name().startsWith("v")) {
            throw new ReleaseException("Select a v-prefixed tag, including for a manual trial");
        }
        String version = ref.name().substring(1);
        check(version);
        switch (ref.event()) {
            case "push" -> {
                if (!version.equals(committedVersion)) {
                    throw new ReleaseException("The tag must match version in gradle.properties");
                }
            }
            case "workflow_dispatch" -> {
                if (!version.equals(ref.trialVersion())) {
                    throw new ReleaseException("The trial version must match the selected tag");
                }
                if (!version.contains("-")) {
                    throw new ReleaseException(
                            "Manual trials need a pre-release version such as 0.3.0-rc.1"
                    );
                }
            }
            // No other event, such as a scheduled run, decides what to publish.
            default -> throw new ReleaseException(
                    "Releases start from a tag push or a manual trial, not " + ref.event()
            );
        }
        return version;
    }
}
