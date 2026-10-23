package org.mqtt4j.hato.release;

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

/// Uploads a release bundle to the Central Publisher API and waits until the Portal has
/// validated it. The [PublishingType] decides whether the Portal then publishes the deployment
/// or leaves it to the maintainer; this class never publishes or drops a deployment through the
/// API.
///
/// See the [Central Publisher API](https://central.sonatype.org/publish/publish-portal-api/).
final class CentralUpload {

    /// How the Portal publishes a deployment that passes validation. The names are the values
    /// of the API's `publishingType` parameter.
    enum PublishingType {

        /// The maintainer reviews the deployment in the Portal, then publishes or drops it.
        USER_MANAGED,

        /// The Portal publishes the deployment without review.
        AUTOMATIC
    }

    /// The Publisher API of the Central Portal.
    static final URI PORTAL = URI.create("https://central.sonatype.com/api/v1/publisher/");

    /// Where the maintainer follows deployments and publishes or drops user-managed ones.
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

    /// Uploads `bundle` once as `type`, then checks its status at most thirty times,
    /// `pollInterval` apart. A user-managed upload returns once the Portal has validated the
    /// deployment, and an automatic one once the Portal has started publishing it. Any other
    /// outcome throws a [ReleaseException] that says what to inspect in the Portal. The upload is
    /// never retried: a lost response may already have created a deployment.
    void upload(Path bundle, PublishingType type) throws IOException, InterruptedException {
        String boundary = "hato-" + UUID.randomUUID();
        // Multipart lines end with CRLF.
        String partHeader = """
                --%s\r
                Content-Disposition: form-data; name="bundle"; filename="%s"\r
                Content-Type: application/octet-stream\r
                \r
                """.formatted(boundary, bundle.getFileName());
        URI target = api.resolve("upload?publishingType=" + type.name());
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
        summarize("Deployment **%s**, %s: [open in Central Portal](%s)."
                .formatted(deployment, type, DEPLOYMENTS_PAGE));

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
            String current = state.group(1);
            switch (current) {
                case "PENDING", "VALIDATING" -> {
                    // Validation is still running; check again after the interval.
                }
                case "VALIDATED" -> {
                    if (type == PublishingType.USER_MANAGED) {
                        out.println(
                                "Validated. Review the deployment in the Portal before publishing"
                                        + " or dropping it."
                        );
                        summarize("Status: **VALIDATED**. Publish or drop manually in the Portal.");
                        return;
                    }
                    // The API does not say whether an automatic deployment reports this state on
                    // its way to publishing, so keep waiting until publishing starts.
                }
                case "PUBLISHING", "PUBLISHED" -> {
                    if (type == PublishingType.USER_MANAGED) {
                        // Only the maintainer publishes a user-managed deployment, from the
                        // Portal, and the wait does not expect that.
                        throw unexpectedState(current, body);
                    }
                    out.println("Validated. The Portal publishes the deployment without review.");
                    summarize("Status: **%s**. Publishing needs no action in the Portal."
                            .formatted(current));
                    return;
                }
                // FAILED, and any state added later, need the maintainer.
                default -> throw unexpectedState(current, body);
            }
        }
        throw new ReleaseException(
                "The wait ended after thirty checks; inspect the deployment in the Portal."
        );
    }

    private static ReleaseException unexpectedState(String state, String body) {
        return new ReleaseException("Unexpected deployment state: " + state + "\n" + body);
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
