package org.mqtt4j.hato.testkit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttPublisher;
import org.mqtt4j.hato.PublishResult;

import static java.util.Objects.requireNonNull;

/// A stand-in for the publishing role of a client, for tests of code that takes an
/// [MqttPublisher]. It records each message in the order published and decides each
/// publication's result without a broker: by default it completes the publication at once with
/// [PublishResult#of()]. A test can make later publications fail with an exception of its choice,
/// or hold them until it completes or fails each one, to check how the code under test handles
/// those results.
///
/// It is not a client. It sends nothing, and checks nothing that a client checks, such as its
/// connection, its capacity, or MQTT 5.0 properties on an MQTT 3.1.1 connection. All methods may
/// be called from any thread.
public final class FakeMqttPublisher implements MqttPublisher {

    private final List<MqttMessage> published = new ArrayList<>();
    private final List<HeldPublication> held = new ArrayList<>();
    private Outcome outcome = new Complete(PublishResult.of());

    /// Creates a fake that completes each publication at once with [PublishResult#of()].
    public FakeMqttPublisher() {
    }

    /// Records `message` and returns the result that the fake decides now: completed or failed
    /// at once, or held until the test completes or fails it through [#heldPublications()].
    ///
    /// @param message
    ///     the message to record
    /// @return
    ///     the result of the publication
    /// @throws NullPointerException
    ///     if `message` is null
    @Override
    public synchronized CompletableFuture<PublishResult> publish(MqttMessage message) {
        published.add(requireNonNull(message, "message must not be null"));
        return switch (outcome) {
            case Complete(var result) -> CompletableFuture.completedFuture(result);
            case Fail(var failure) -> CompletableFuture.failedFuture(failure);
            case Hold _ -> {
                var publication = new HeldPublication(this, message);
                held.add(publication);
                yield publication.result;
            }
        };
    }

    /// {@return the messages published so far, held and failed ones included, in the order
    /// published, as an unmodifiable list that later publications do not affect}
    public synchronized List<MqttMessage> published() {
        return List.copyOf(published);
    }

    /// Completes later publications at once with `result`, as the fake does by default with
    /// [PublishResult#of()]. Publications held before stay held.
    ///
    /// @param result
    ///     the result to complete them with
    /// @throws NullPointerException
    ///     if `result` is null
    public synchronized void completePublications(PublishResult result) {
        outcome = new Complete(requireNonNull(result, "result must not be null"));
    }

    /// Fails later publications at once with `failure`, such as
    /// [org.mqtt4j.hato.MqttNotConnectedException].
    /// Publications held before stay held.
    ///
    /// @param failure
    ///     the exception to fail them with
    /// @throws NullPointerException
    ///     if `failure` is null
    public synchronized void failPublications(Throwable failure) {
        outcome = new Fail(requireNonNull(failure, "failure must not be null"));
    }

    /// Holds later publications until the test completes or fails each one.
    public synchronized void holdPublications() {
        outcome = new Hold();
    }

    /// {@return the publications held now, in the order published, as an unmodifiable list that
    /// later changes do not affect}
    public synchronized List<HeldPublication> heldPublications() {
        return List.copyOf(held);
    }

    private synchronized boolean release(HeldPublication publication) {
        return held.remove(publication);
    }

    /// A publication that the fake holds until the test completes or fails it.
    public static final class HeldPublication {

        private final FakeMqttPublisher publisher;
        private final MqttMessage message;
        private final CompletableFuture<PublishResult> result = new CompletableFuture<>();

        private HeldPublication(FakeMqttPublisher publisher, MqttMessage message) {
            this.publisher = publisher;
            this.message = message;
        }

        /// {@return the message that the code under test published}
        public MqttMessage message() {
            return message;
        }

        /// Completes this publication with `result`.
        ///
        /// @param result
        ///     the result to complete it with
        /// @throws NullPointerException
        ///     if `result` is null
        /// @throws IllegalStateException
        ///     if the test already completed or failed it
        public void complete(PublishResult result) {
            requireNonNull(result, "result must not be null");
            release();
            this.result.complete(result);
        }

        /// Fails this publication with `failure`.
        ///
        /// @param failure
        ///     the exception to fail it with
        /// @throws NullPointerException
        ///     if `failure` is null
        /// @throws IllegalStateException
        ///     if the test already completed or failed it
        public void fail(Throwable failure) {
            requireNonNull(failure, "failure must not be null");
            release();
            result.completeExceptionally(failure);
        }

        // Takes this publication out of the held ones under the fake's lock. The callers settle
        // the result after that, outside the lock, because settling it runs the code under test
        // that waits for it.
        private void release() {
            if (!publisher.release(this)) {
                throw new IllegalStateException("The publication is no longer held");
            }
        }
    }

    // How the fake settles a publication when it receives it. The alternatives are private
    // members of the class, not of the interface, whose members would be public.
    private sealed interface Outcome {}

    private record Complete(PublishResult result) implements Outcome {}

    private record Fail(Throwable failure) implements Outcome {}

    private record Hold() implements Outcome {}
}
