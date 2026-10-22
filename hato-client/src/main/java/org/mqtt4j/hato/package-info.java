/// The public API of the Hato client.
///
/// Applications use values and client interfaces, and supply listeners. [Transport] and
/// [TimerScheduler] let tests and integrations replace the client's connections and clock:
/// [Transport] is experimental and outside the client's compatibility promise, while
/// [TimerScheduler] is covered by the same compatibility policy as the rest of the API.
///
/// Everything is non-null unless marked `@Nullable`.
@NullMarked
package org.mqtt4j.hato;

import org.jspecify.annotations.NullMarked;
