package org.mqtt4j.hato.broker.internal;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.broker.Authenticator;

import static java.util.Objects.requireNonNull;

// The checks of an authenticator's scope that the broker makes beside the public ones, which the
// scope's class hands over here as it initializes. A scope that the broker holds has initialized
// its class, so the checks are here whenever the broker asks. The package is not exported, so
// applications see only the public checks.
public final class ScopeChecks {

    // Whether a scope would let its client make a shared subscription of a group to a filter
    // that matches a topic alone.
    @FunctionalInterface
    public interface Share {

        boolean permits(Authenticator.Scope scope, String group, Topic topic);
    }

    private static volatile @Nullable Share share;

    private ScopeChecks() {
    }

    // Takes the checks of Authenticator.Scope, which calls this alone, as its class initializes.
    public static void register(Share checks) {
        share = checks;
    }

    // Whether `scope` would let its client make a shared subscription of `group` to `topic`,
    // which the public checks cannot say for a long group and a long topic: no filter can hold
    // both (see Session.receives).
    static boolean permitsShare(Authenticator.Scope scope, String group, Topic topic) {
        return requireNonNull(share, "the checks of Authenticator.Scope")
                .permits(scope, group, topic);
    }
}
