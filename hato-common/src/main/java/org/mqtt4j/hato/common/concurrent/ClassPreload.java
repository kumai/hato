package org.mqtt4j.hato.common.concurrent;

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

/// Loads and initializes every class of the distribution that embeds this copy, once and on a
/// platform thread, before the distribution runs on virtual threads.
///
/// The JDK keeps a virtual thread on its carrier while the JVM loads a class for it, and while it
/// initializes a class or waits for another thread to initialize one. If such a thread waits for
/// a class-loading lock or a jar that an unmounted virtual thread holds, the carrier waits with
/// it, and once every carrier waits like this, no virtual thread in the JVM runs again. JEP 491
/// leaves these cases to future work. Without this class, the threads of a distribution and its
/// calls on application threads would load its classes at first use, at moments that the
/// application cannot foresee, such as a client's first `stats()` call.
///
/// The entry point of each distribution calls [#awaitLoaded()] before anything else, so
/// application threads load only the classes that they use before that call. Every class loads,
/// including those of features that the application does not use, at a one-time cost of some
/// tens of milliseconds. Revisit this once every Java release that the distributions support lets
/// a virtual thread unmount while it loads or initializes a class.
///
/// This is the one use of `Class.forName` in the project. The build writes the class names to an
/// index next to each embedded copy, because no explicit reference can name the classes that are
/// private to their packages. It hinders neither native images, which hold every class from
/// their build and skip this, nor the AOT cache, which keeps the classes that this loads in a
/// training run.
public final class ClassPreload {

    // The binary name of every class in the distribution, one per line, written by the build.
    private static final String INDEX = "classes.txt";

    // The first caller starts the loading; every caller waits for it to finish.
    private static final AtomicBoolean started = new AtomicBoolean();

    private static final FutureTask<@Nullable Void> loading =
            new FutureTask<>(ClassPreload::loadAll, null);

    private ClassPreload() {
    }

    /// Returns once the distribution's classes are loaded and initialized, or at once when the
    /// calling thread is interrupted, keeping its interrupt status. The first call loads them on
    /// the calling thread when it is a platform thread, and otherwise on a new platform thread that
    /// it waits for. A class that fails to load or initialize is left to the code that uses it,
    /// which then reports the failure. When that platform thread cannot be started, the first call
    /// throws what starting it threw, and later calls return at once.
    public static void awaitLoaded() {
        // A native image holds every class from its build.
        if (System.getProperty("org.graalvm.nativeimage.imagecode") != null) {
            return;
        }
        if (started.compareAndSet(false, true)) {
            if (Thread.currentThread().isVirtual()) {
                // Each distribution embeds a copy under org.mqtt4j.hato.<distribution>; naming the
                // thread after it tells the copies apart in one JVM.
                String distribution = ClassPreload.class.getPackageName().split("\\.")[3];
                try {
                    Thread.ofPlatform()
                            .daemon()
                            .name("hato-" + distribution + "-preload")
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
    // Both orders give every static field the same value while no static initializer of the
    // distribution depends, through the classes that it uses, on a class whose initialization
    // depends back on it.
    private static void loadAll() {
        ClassLoader loader = ClassPreload.class.getClassLoader();
        InputStream index = ClassPreload.class.getResourceAsStream(INDEX);
        if (index == null) {
            return;
        }
        try (var names = new BufferedReader(new InputStreamReader(index, UTF_8))) {
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
