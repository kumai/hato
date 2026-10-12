package org.mqtt4j.mqtj.release;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

/// The commands of the release workflow, in the order a release runs them:
///
/// - `version <gradle.properties>` checks the GitHub ref of the run and writes the version to
///   publish to `GITHUB_OUTPUT`, or prints it outside GitHub Actions.
/// - `bundle <version> <bundle.zip>` checks the signed bundle before the upload.
/// - `upload <bundle.zip>` uploads the bundle for manual publication with the Portal user token
///   in `CENTRAL_USERNAME` and `CENTRAL_PASSWORD`, and waits until the Portal validates it.
///
/// A failure that the maintainer can act on ends with exit status 1 and a message that says
/// what to do.
public final class ReleaseTool {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(10);

    private static final String USAGE = """
            Usage: release-tools version <gradle.properties>
                   release-tools bundle <version> <bundle.zip>
                   release-tools upload <bundle.zip>""";

    private ReleaseTool() {
    }

    public static void main(String[] args) throws InterruptedException {
        try {
            run(List.of(args), System.getenv());
        } catch (ReleaseException failure) {
            System.err.println(failure.getMessage());
            System.exit(1);
        } catch (IOException failure) {
            System.err.println(failure);
            System.exit(1);
        }
    }

    static void run(List<String> args, Map<String, String> environment)
            throws IOException, InterruptedException {
        String command = args.isEmpty() ? "" : args.getFirst();
        if (command.equals("version") && args.size() == 2) {
            version(Path.of(args.get(1)), environment);
        } else if (command.equals("bundle") && args.size() == 3) {
            ReleaseBundle.verify(Path.of(args.get(2)), args.get(1));
        } else if (command.equals("upload") && args.size() == 2) {
            upload(Path.of(args.get(1)), environment);
        } else {
            throw new ReleaseException(USAGE);
        }
    }

    private static void version(Path properties, Map<String, String> environment)
            throws IOException {
        var committed = new Properties();
        try (BufferedReader reader = Files.newBufferedReader(properties)) {
            committed.load(reader);
        }
        String committedVersion = committed.getProperty("version");
        if (committedVersion == null) {
            throw new ReleaseException("No version in " + properties);
        }
        var ref = new ReleaseVersion.Ref(
                required(environment, "GITHUB_EVENT_NAME"),
                required(environment, "GITHUB_REF_TYPE"),
                required(environment, "GITHUB_REF_NAME"),
                environment.getOrDefault("INPUT_VERSION", "")
        );
        String version = ReleaseVersion.of(ref, committedVersion.strip());
        String output = environment.get("GITHUB_OUTPUT");
        if (output == null) {
            System.out.println(version);
        } else {
            Files.writeString(Path.of(output), "version=" + version + "\n", UTF_8, CREATE, APPEND);
        }
    }

    private static void upload(Path bundle, Map<String, String> environment)
            throws IOException, InterruptedException {
        String credentials = required(environment, "CENTRAL_USERNAME") + ":"
                + required(environment, "CENTRAL_PASSWORD");
        String token = Base64.getEncoder().encodeToString(credentials.getBytes(UTF_8));
        if ("true".equals(environment.get("GITHUB_ACTIONS"))) {
            // GitHub Actions masks the secrets themselves, but not this value derived from them.
            System.out.println("::add-mask::" + token);
        }
        String summary = environment.get("GITHUB_STEP_SUMMARY");
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build()) {
            var central = new CentralUpload(
                    client,
                    CentralUpload.PORTAL,
                    token,
                    POLL_INTERVAL,
                    System.out,
                    summary == null ? null : Path.of(summary)
            );
            central.upload(bundle);
        }
    }

    private static String required(Map<String, String> environment, String name) {
        String value = environment.get(name);
        if (value == null || value.isEmpty()) {
            throw new ReleaseException("Missing " + name);
        }
        return value;
    }
}
