/// The public API of the Hato client.
///
/// Applications use values and client interfaces, and supply listeners. [Transport] and
/// [TimerScheduler] let tests and integrations replace the client's connections and clock.
///
/// Everything is non-null unless marked `@Nullable`.
@NullMarked
package org.mqtt4j.hato;

import org.jspecify.annotations.NullMarked;
