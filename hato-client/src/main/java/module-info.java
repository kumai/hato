/// The client's module name of Hato 1.0, kept for applications that require it. It holds no
/// classes: the client lives in the module `org.mqtt4j.hato`, which this module requires
/// transitively.
module org.mqtt4j.hato.client {
    requires transitive org.mqtt4j.hato;
}
