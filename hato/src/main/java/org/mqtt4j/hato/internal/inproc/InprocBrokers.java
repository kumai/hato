package org.mqtt4j.hato.internal.inproc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/// The names of the brokers that run in this JVM, by which clients reach them in process. The
/// table is static, so each class loader that loads these classes has a table of its own.
///
/// A broker takes its name as it starts and gives it back as it ends. The table takes no lock:
/// a broker's own threads give names back as the broker ends, and must not wait for a client's
/// thread, which may be virtual, that looks a name up meanwhile.
public final class InprocBrokers {

    // A name is a host name in lower case: labels of letters, digits, and hyphens, separated by
    // periods, as DNS has them, so that it is the host of a URI as it stands.
    private static final int MAX_LABEL_LENGTH = 63;

    private static final int MAX_NAME_LENGTH = 253;

    // The names that brokers take when the application gives none: this prefix and a number.
    private static final String AUTOMATIC_PREFIX = "hato-";

    private static final ConcurrentHashMap<String, Registration> registrations =
            new ConcurrentHashMap<>();

    // The number that the next automatic name tries.
    private static final AtomicLong next = new AtomicLong(1);

    private InprocBrokers() {
    }

    /// Takes `name` for a broker that starts.
    ///
    /// @throws IllegalArgumentException if `name` is no valid broker name
    /// @throws IllegalStateException if another broker holds `name`
    public static Registration take(String name) {
        check(name);
        var registration = new Registration(name);
        if (registrations.putIfAbsent(name, registration) != null) {
            throw new IllegalStateException("A broker named " + name + " runs already");
        }
        return registration;
    }

    /// Takes the first automatic name that no broker holds, for a broker that starts without a
    /// name of its own.
    public static Registration takeNext() {
        while (true) {
            String name = AUTOMATIC_PREFIX + next.getAndIncrement();
            var registration = new Registration(name);
            if (registrations.putIfAbsent(name, registration) == null) {
                return registration;
            }
        }
    }

    /// Checks that `name` can name a broker.
    ///
    /// @throws IllegalArgumentException if it cannot
    public static void check(String name) {
        if (!valid(name)) {
            throw new IllegalArgumentException(
                    "A broker name is lower-case labels of letters, digits, and hyphens, each of"
                            + " 1 to 63 characters, separated by periods, at most 253 characters"
                            + " in all, with no label that starts or ends with a hyphen: " + name
            );
        }
    }

    private static boolean valid(String name) {
        if (name.isEmpty() || name.length() > MAX_NAME_LENGTH) {
            return false;
        }
        int labelStart = 0;
        for (int i = 0; i <= name.length(); i++) {
            char c = i < name.length() ? name.charAt(i) : '.';
            if (c == '.') {
                int length = i - labelStart;
                if (length == 0
                        || length > MAX_LABEL_LENGTH
                        || name.charAt(labelStart) == '-'
                        || name.charAt(i - 1) == '-') {
                    return false;
                }
                labelStart = i + 1;
            } else if (!(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && c != '-') {
                return false;
            }
        }
        return true;
    }

    /// A name that a broker holds, until it gives it back.
    public static final class Registration {

        private final String name;

        private Registration(String name) {
            this.name = name;
        }

        /// {@return the name}
        public String name() {
            return name;
        }

        /// Gives the name back, so that another broker can take it. Later calls do nothing.
        public void release() {
            registrations.remove(name, this);
        }
    }
}
