package org.mqtt4j.mqtj.release;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Runs the upload against a local stand-in for the Central Publisher API.
class CentralUploadTest {

    private static final String DEPLOYMENT = "12345678-abcd-abcd-abcd-123456789abc";

    private static final String TOKEN = "dGVzdC11c2VyOnRlc3QtcGFzc3dvcmQ=";

    private static final String BUNDLE = "bundle bytes";

    @TempDir
    Path directory;

    private final Portal portal = new Portal();

    private final HttpClient client = HttpClient.newHttpClient();

    CentralUploadTest() throws IOException {
    }

    @AfterEach
    void stop() {
        client.close();
        portal.close();
    }

    @Test
    void a_bundle_is_uploaded_for_manual_publication_and_waits_until_validated() throws Exception {
        // Arrange
        portal.statuses.addAll(List.of(state("PENDING"), state("VALIDATING"), state("VALIDATED")));
        Path summary = directory.resolve("summary.md");
        CentralUpload sut = upload(summary);

        // Act
        sut.upload(bundle());

        // Assert: one multipart upload with the token, then status checks until VALIDATED.
        assertThat(portal.requests).hasSize(4);
        Request upload = portal.requests.getFirst();
        assertThat(upload.target())
                .isEqualTo("/api/v1/publisher/upload?publishingType=USER_MANAGED");
        assertThat(upload.authorization()).isEqualTo("Bearer " + TOKEN);
        assertThat(upload.contentType()).startsWith("multipart/form-data; boundary=");
        String boundary = upload.contentType().substring(upload.contentType().indexOf('=') + 1);
        assertThat(upload.body()).isEqualTo(
                "--" + boundary + "\r\n"
                        + "Content-Disposition: form-data; name=\"bundle\";"
                        + " filename=\"mqtj-0.3.0.zip\"\r\n"
                        + "Content-Type: application/octet-stream\r\n"
                        + "\r\n"
                        + BUNDLE + "\r\n"
                        + "--" + boundary + "--\r\n"
        );
        assertThat(portal.requests.subList(1, 4))
                .extracting(Request::target)
                .containsOnly("/api/v1/publisher/status?id=" + DEPLOYMENT);
        assertThat(Files.readString(summary)).contains(DEPLOYMENT).contains("VALIDATED");
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"FAILED", "PUBLISHING", "PUBLISHED", "UNKNOWN"})
    void any_state_but_validation_stops_the_wait_for_the_maintainer(String state) {
        // Arrange
        portal.statuses.add(state(state));
        CentralUpload sut = upload(null);

        // Act & Assert
        assertThatThrownBy(() -> sut.upload(bundle()))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("Unexpected deployment state: " + state);
        assertThat(portal.requests).hasSize(2);
    }

    @Test
    void the_wait_for_validation_ends_after_thirty_status_checks() {
        // Arrange
        portal.statuses.add(state("VALIDATING"));
        CentralUpload sut = upload(null);

        // Act & Assert
        assertThatThrownBy(() -> sut.upload(bundle()))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("thirty checks");
        assertThat(portal.requests).hasSize(31);
    }

    @Test
    void an_upload_response_without_a_deployment_id_is_not_retried() {
        // Arrange
        portal.upload = new Response(200, "not a deployment ID");
        Path summary = directory.resolve("summary.md");
        CentralUpload sut = upload(summary);

        // Act & Assert
        assertThatThrownBy(() -> sut.upload(bundle()))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("did not return a deployment ID");
        assertThat(portal.requests).hasSize(1);
        assertThat(summary).doesNotExist();
    }

    @Test
    void a_rejected_upload_reports_the_response_without_retrying_or_the_token() {
        // Arrange
        portal.upload = new Response(401, "Invalid token");
        CentralUpload sut = upload(null);

        // Act & Assert
        assertThatThrownBy(() -> sut.upload(bundle()))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("check the Portal before retrying")
                .hasMessageContaining("HTTP 401")
                .hasMessageContaining("Invalid token")
                .hasMessageNotContaining(TOKEN);
        assertThat(portal.requests).hasSize(1);
    }

    @Test
    void a_failed_status_check_reports_the_response_and_keeps_the_deployment_in_the_summary()
            throws Exception {
        // Arrange
        portal.statuses.add(new Response(503, "Service unavailable"));
        Path summary = directory.resolve("summary.md");
        CentralUpload sut = upload(summary);

        // Act & Assert
        assertThatThrownBy(() -> sut.upload(bundle()))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("inspect the deployment in the Portal")
                .hasMessageContaining("HTTP 503")
                .hasMessageContaining("Service unavailable");
        assertThat(portal.requests).hasSize(2);
        assertThat(Files.readString(summary)).contains(DEPLOYMENT);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"<html>maintenance</html>", "{}"})
    void an_unreadable_status_reports_the_response(String body) {
        // Arrange
        portal.statuses.add(new Response(200, body));
        CentralUpload sut = upload(null);

        // Act & Assert
        assertThatThrownBy(() -> sut.upload(bundle()))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("Unexpected status response")
                .hasMessageContaining(body);
        assertThat(portal.requests).hasSize(2);
    }

    private CentralUpload upload(@Nullable Path summary) {
        return new CentralUpload(
                client,
                portal.api(),
                TOKEN,
                Duration.ZERO,
                new PrintStream(OutputStream.nullOutputStream()),
                summary
        );
    }

    private Path bundle() throws IOException {
        return Files.writeString(directory.resolve("mqtj-0.3.0.zip"), BUNDLE);
    }

    private static Response state(String state) {
        return new Response(200, "{\"deploymentState\":\"%s\"}".formatted(state));
    }

    private record Request(
            String target,
            String authorization,
            String contentType,
            String body
    ) {}

    private record Response(int status, String body) {}

    /// Answers uploads and status checks from scripts and records every request. The last
    /// scripted status answers all later checks.
    private static final class Portal implements AutoCloseable {

        final List<Request> requests = new CopyOnWriteArrayList<>();

        // Tests script the answers before the upload; the server thread consumes them.
        final Deque<Response> statuses = new LinkedBlockingDeque<>();

        volatile Response upload = new Response(200, DEPLOYMENT);

        private final HttpServer server;

        Portal() throws IOException {
            server = HttpServer.create(
                    new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0),
                    0
            );
            server.createContext("/api/v1/publisher/", this::answer);
            server.start();
        }

        URI api() {
            return URI.create(
                    "http://127.0.0.1:" + server.getAddress().getPort() + "/api/v1/publisher/"
            );
        }

        private void answer(HttpExchange exchange) throws IOException {
            requests.add(new Request(
                    exchange.getRequestURI().toString(),
                    header(exchange, "Authorization"),
                    header(exchange, "Content-Type"),
                    new String(exchange.getRequestBody().readAllBytes(), UTF_8)
            ));
            Response response = exchange.getRequestURI().getPath().endsWith("/upload")
                    ? upload
                    : nextStatus();
            byte[] body = response.body().getBytes(UTF_8);
            // A length of zero would select chunked encoding; -1 sends no body.
            exchange.sendResponseHeaders(response.status(), body.length == 0 ? -1 : body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        }

        // The server answers one request at a time, so this check and removal do not race.
        private Response nextStatus() {
            return statuses.size() > 1 ? statuses.removeFirst() : statuses.getFirst();
        }

        private static String header(HttpExchange exchange, String name) {
            List<String> values = exchange.getRequestHeaders().get(name);
            return values == null || values.isEmpty() ? "" : values.getFirst();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
