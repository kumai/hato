package org.mqtt4j.mqtj.release;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

/// Uploads a release bundle to the Central Publisher API for manual publication, then waits
/// until the Portal has validated it. The maintainer publishes or drops the deployment in the
/// Portal; this class never does either.
///
/// See the [Central Publisher API](https://central.sonatype.org/publish/publish-portal-api/).
final class CentralUpload {

    /// The Publisher API of the Central Portal.
    static final URI PORTAL = URI.create("https://central.sonatype.com/api/v1/publisher/");

    /// Where the maintainer reviews the deployment, then publishes or drops it.
    private static final String DEPLOYMENTS_PAGE = "https://central.sonatype.com/publishing";

    private static final Duration UPLOAD_TIMEOUT = Duration.ofSeconds(120);

    private static final Duration STATUS_TIMEOUT = Duration.ofSeconds(10);

    private static final int STATUS_CHECKS = 30;

    private static final Pattern DEPLOYMENT_ID = Pattern.compile(
            "[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
    );

    // The status is JSON, but only this one top-level string is needed, so a pattern stands in
    // for a JSON parser and the tool needs no library. Use a parser before reading nested or
    // repeated fields.
    private static final Pattern DEPLOYMENT_STATE = Pattern.compile(
            "\"deploymentState\"\\s*:\\s*\"([A-Za-z_]+)\""
    );

    private final HttpClient client;
    private final URI api;
    private final String authorization;
    private final Duration pollInterval;
    private final PrintStream out;
    private final @Nullable Path summary;

    /// `api` is the Publisher API's base URI, ending with a slash. `token` is the Base64 form of
    /// the Portal user token. `summary` is the Markdown file that GitHub Actions shows for the
    /// step, or null outside GitHub Actions.
    CentralUpload(
            HttpClient client,
            URI api,
            String token,
            Duration pollInterval,
            PrintStream out,
            @Nullable Path summary
    ) {
        this.client = client;
        this.api = api;
        authorization = "Bearer " + token;
        this.pollInterval = pollInterval;
        this.out = out;
        this.summary = summary;
    }

    /// Uploads `bundle` once, then checks its status at most thirty times, `pollInterval` apart,
    /// and returns once the Portal reports it as validated. Any other outcome throws a
    /// [ReleaseException] that says what to inspect in the Portal. The upload is never retried:
    /// a lost response may already have created a deployment.
    void upload(Path bundle) throws IOException, InterruptedException {
        String boundary = "mqtj-" + UUID.randomUUID();
        // Multipart lines end with CRLF.
        String partHeader = """
                --%s\r
                Content-Disposition: form-data; name="bundle"; filename="%s"\r
                Content-Type: application/octet-stream\r
                \r
                """.formatted(boundary, bundle.getFileName());
        // USER_MANAGED leaves publishing to the maintainer, after reviewing the deployment.
        URI target = api.resolve("upload?publishingType=USER_MANAGED");
        HttpRequest upload = HttpRequest.newBuilder(target)
                .timeout(UPLOAD_TIMEOUT)
                .header("Authorization", authorization)
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(BodyPublishers.concat(
                        BodyPublishers.ofString(partHeader),
                        BodyPublishers.ofFile(bundle),
                        BodyPublishers.ofString("\r\n--" + boundary + "--\r\n")
                ))
                .build();
        String deployment = send(upload, "Upload failed; check the Portal before retrying.")
                .body()
                .strip();
        if (!DEPLOYMENT_ID.matcher(deployment).matches()) {
            throw new ReleaseException(
                    "The upload did not return a deployment ID; check the Portal before retrying.\n"
                            + deployment
            );
        }
        out.println("Deployment: " + deployment);
        summarize("Deployment **%s**: [review in Central Portal](%s)."
                .formatted(deployment, DEPLOYMENTS_PAGE));

        HttpRequest status = HttpRequest.newBuilder(api.resolve("status?id=" + deployment))
                .timeout(STATUS_TIMEOUT)
                .header("Authorization", authorization)
                .POST(BodyPublishers.noBody())
                .build();
        for (int check = 1; check <= STATUS_CHECKS; check++) {
            if (check > 1) {
                Thread.sleep(pollInterval);
            }
            String body = send(status, "Status check failed; inspect the deployment in the Portal.")
                    .body();
            Matcher state = DEPLOYMENT_STATE.matcher(body);
            if (!state.find()) {
                throw new ReleaseException(
                        "Unexpected status response; inspect the deployment in the Portal.\n" + body
                );
            }
            switch (state.group(1)) {
                case "VALIDATED" -> {
                    out.println(
                            "Validated. Review the deployment in the Portal before publishing or"
                                    + " dropping it."
                    );
                    summarize("Status: **VALIDATED**. Publish or drop manually in the Portal.");
                    return;
                }
                case "PENDING", "VALIDATING" -> {
                    // Validation is still running; check again after the interval.
                }
                // FAILED, PUBLISHING, PUBLISHED, and any state added later need the maintainer.
                default -> throw new ReleaseException(
                        "Unexpected deployment state: " + state.group(1) + "\n" + body
                );
            }
        }
        throw new ReleaseException(
                "Validation did not finish within thirty checks;"
                        + " inspect the deployment in the Portal."
        );
    }

    /// Sends `request` and returns its successful response. A failure throws a
    /// [ReleaseException] that starts with `failure` and carries the response body, if any.
    private HttpResponse<String> send(HttpRequest request, String failure)
            throws InterruptedException {
        HttpResponse<String> response;
        try {
            response = client.send(request, BodyHandlers.ofString());
        } catch (IOException exception) {
            throw new ReleaseException(failure + "\n" + exception);
        }
        if (response.statusCode() / 100 != 2) {
            throw new ReleaseException(
                    failure + "\nHTTP " + response.statusCode() + "\n" + response.body()
            );
        }
        return response;
    }

    private void summarize(String line) throws IOException {
        if (summary != null) {
            Files.writeString(summary, line + "\n", UTF_8, CREATE, APPEND);
        }
    }
}
