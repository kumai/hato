package org.mqtt4j.hato.client.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jspecify.annotations.Nullable;

/// A future that the client returns. Waiting for it with `join()` or `get()` inside one of the
/// client's inline handlers throws [IllegalStateException], even once it has completed; see
/// [InlineHandlers]. The futures that the methods of [CompletableFuture] make from it, such as
/// `thenApply`, are of this class too.
final class ClientFuture<T extends @Nullable Object> extends CompletableFuture<T> {

    private final InlineHandlers inlineHandlers;

    ClientFuture(InlineHandlers inlineHandlers) {
        this.inlineHandlers = inlineHandlers;
    }

    /// A future completed with `value`, which is null for a future of `Void`.
    static <T extends @Nullable Object> ClientFuture<T> completed(
            InlineHandlers inlineHandlers,
            @Nullable T value
    ) {
        var future = new ClientFuture<T>(inlineHandlers);
        future.complete(value);
        return future;
    }

    static <T extends @Nullable Object> ClientFuture<T> failed(
            InlineHandlers inlineHandlers,
            Throwable failure
    ) {
        var future = new ClientFuture<T>(inlineHandlers);
        future.completeExceptionally(failure);
        return future;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        inlineHandlers.refuseWait();
        return super.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        inlineHandlers.refuseWait();
        return super.get(timeout, unit);
    }

    @Override
    public T join() {
        inlineHandlers.refuseWait();
        return super.join();
    }

    @Override
    public <U extends @Nullable Object> CompletableFuture<U> newIncompleteFuture() {
        return new ClientFuture<>(inlineHandlers);
    }
}
