package org.mqtt4j.hato.client.internal;

/// The parts of a client's reconnect policy that decide which failures it retries: the failures
/// before a connection request's first successful connection, and the two broker reason codes
/// whose classification the policy can change, 0x98 (Administrative action) and 0x80
/// (Unspecified error). [ConnectionRequest] applies them; every other reason code follows its
/// fixed table.
public record RetryRules(
        boolean initialRetry,
        boolean administrativeActionRetry,
        boolean unspecifiedErrorRetry
) {

    /// The rules of a policy that changes nothing: initial failures end the request, an
    /// administrative action stops reconnecting, and an unspecified error is retried.
    public static final RetryRules DEFAULT = new RetryRules(false, false, true);

    public RetryRules withInitialRetry() {
        return new RetryRules(true, administrativeActionRetry, unspecifiedErrorRetry);
    }

    public RetryRules withAdministrativeActionRetry() {
        return new RetryRules(initialRetry, true, unspecifiedErrorRetry);
    }

    public RetryRules withoutUnspecifiedErrorRetry() {
        return new RetryRules(initialRetry, administrativeActionRetry, false);
    }
}
