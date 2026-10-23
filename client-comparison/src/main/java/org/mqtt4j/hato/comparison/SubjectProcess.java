package org.mqtt4j.hato.comparison;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/// A [Subject] running in a JVM of its own for one run. The subject reports, on its output, when
/// it has subscribed and then its result; any other line it prints is passed through, with the
/// library's name in front.
final class SubjectProcess implements AutoCloseable {

    private final Process process;

    private final Library library;

    private final CompletableFuture<Boolean> ready = new CompletableFuture<>();

    private final CompletableFuture<Arguments> result = new CompletableFuture<>();

    /// Starts a subject of `library` with `jvmOptions` and `arguments`, on the class path of this
    /// JVM.
    SubjectProcess(Library library, List<String> jvmOptions, List<String> arguments)
            throws IOException {
        this.library = library;
        var command = new ArrayList<String>();
        command.add(ProcessHandle.current().info().command().orElseThrow());
        command.addAll(jvmOptions);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(Subject.class.getName());
        command.add("library=" + library.label());
        command.addAll(arguments);
        process = new ProcessBuilder(command)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
        Thread.ofPlatform()
                .name("subject-output")
                .daemon()
                .start(this::readOutput);
    }

    /// Waits until the subject has subscribed.
    void awaitReady(Duration timeout) throws Exception {
        ready.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /// Waits for the subject's result, and then for its JVM to exit.
    Arguments awaitResult(Duration timeout) throws Exception {
        Arguments outcome = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!process.waitFor(1, TimeUnit.MINUTES)) {
            throw new IllegalStateException(library.label() + " did not exit");
        }
        return outcome;
    }

    private void readOutput() {
        try (var lines = new BufferedReader(
                new InputStreamReader(process.getInputStream(), UTF_8)
        )) {
            String line;
            while ((line = lines.readLine()) != null) {
                if (line.equals(Subject.READY)) {
                    ready.complete(true);
                } else if (line.startsWith(Subject.RESULT_PREFIX)) {
                    String words = line.substring(Subject.RESULT_PREFIX.length());
                    result.complete(Arguments.parse(words));
                } else {
                    System.out.println(library.label() + ": " + line);
                }
            }
        } catch (IOException e) {
            result.completeExceptionally(e);
        }
        var ended = new IllegalStateException(library.label() + " ended without a result");
        ready.completeExceptionally(ended);
        result.completeExceptionally(ended);
    }

    /// Ends the subject's JVM if it is still running.
    @Override
    public void close() {
        process.destroyForcibly();
    }
}
