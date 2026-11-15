package org.mqtt4j.hato.internal.concurrent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;

/// Loads and initializes every class that the client, or the broker, may use, once for each and
/// on a platform thread, before it runs on virtual threads.
///
/// The JDK keeps a virtual thread on its carrier while the JVM loads a class for it, and while it
/// initializes a class or waits for another thread to initialize one. If such a thread waits for
/// a class-loading lock or a jar that an unmounted virtual thread holds, the carrier waits with
/// it, and once every carrier waits like this, no virtual thread in the JVM runs again. JEP 491
/// leaves these cases to future work. Without this class, the threads of the client or the
/// broker and their calls on application threads would load classes at first use, at moments
/// that the application cannot foresee, such as a client's first `stats()` call.
///
/// The entry points of the client and the broker call [#awaitLoaded()] on their preload before
/// anything else, so application threads load only the classes that they use before that call.
/// Every class loads, including those of features that the application does not use, at a
/// one-time cost of some tens of milliseconds. Revisit this once every Java release that Hato
/// supports lets a virtual thread unmount while it loads or initializes a class.
///
/// This is the one use of `Class.forName` in the project. The build writes the class names to an
/// index for each preload next to this class, because no explicit reference can name the classes
/// that are private to their packages. The client's index leaves out the broker's packages and
/// the broker's leaves out the client's internals, so an application that runs only clients
/// loads no class of the broker. It hinders neither native images, which hold every class from
/// their build and skip this, nor the AOT cache, which keeps the classes that this loads in a
/// training run.
public final class ClassPreload {

    /// The classes that a client may use.
    public static final ClassPreload client = new ClassPreload("client");

    /// The classes that a broker may use.
    public static final ClassPreload broker = new ClassPreload("broker");

    private final String owner;

    // The binary names of the classes, one per line, written by the build.
    private final String index;

    // The first caller starts the loading; every caller waits for it to finish.
    private final AtomicBoolean started = new AtomicBoolean();

    private final FutureTask<@Nullable Void> loading = new FutureTask<>(this::loadAll, null);

    private ClassPreload(String owner) {
        this.owner = owner;
        index = owner + "-classes.txt";
    }

    /// Returns once the preload's classes are loaded and initialized, or at once when the
    /// calling thread is interrupted, keeping its interrupt status. The first call loads them on
    /// the calling thread when it is a platform thread, and otherwise on a new platform thread that
    /// it waits for. A class that fails to load or initialize is left to the code that uses it,
    /// which then reports the failure. When that platform thread cannot be started, the first call
    /// throws what starting it threw, and later calls return at once.
    public void awaitLoaded() {
        // A native image holds every class from its build.
        if (System.getProperty("org.graalvm.nativeimage.imagecode") != null) {
            return;
        }
        if (started.compareAndSet(false, true)) {
            if (Thread.currentThread().isVirtual()) {
                try {
                    Thread.ofPlatform()
                            .daemon()
                            .name("hato-" + owner + "-preload")
                            .start(loading);
                } catch (Throwable e) {
                    // Without the thread, nothing would release the other callers.
                    loading.cancel(false);
                    throw e;
                }
            } else {
                loading.run();
            }
        }
        try {
            loading.get();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | CancellationException _) {
            // Loading stopped early or never ran; the code that uses each remaining class loads it.
        }
    }

    // Initializes the classes in the order of the index rather than in the order of first use.
    // Both orders give every static field the same value while no static initializer of Hato
    // depends, through the classes that it uses, on a class whose initialization depends back on
    // it.
    private void loadAll() {
        ClassLoader loader = ClassPreload.class.getClassLoader();
        InputStream classes = ClassPreload.class.getResourceAsStream(index);
        if (classes == null) {
            return;
        }
        try (var names = new BufferedReader(new InputStreamReader(classes, UTF_8))) {
            for (String name = names.readLine(); name != null; name = names.readLine()) {
                try {
                    Class<?> _ = Class.forName(name, true, loader);
                } catch (ClassNotFoundException | LinkageError _) {
                    // The code that uses the class tries again and reports the failure.
                }
            }
        } catch (IOException _) {
            // The code that uses each remaining class loads it.
        }
    }
}
