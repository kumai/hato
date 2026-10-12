/// The Hato client. Applications see only the exported packages; everything else is internal
/// and may change without notice.
module org.mqtt4j.hato.client {
    // Nullness annotations are compile-time only. Never 'transitive': that would force module-path
    // consumers to have org.jspecify at compile time.
    requires static org.jspecify;

    exports org.mqtt4j.hato;
    exports org.mqtt4j.hato.spi;
}
