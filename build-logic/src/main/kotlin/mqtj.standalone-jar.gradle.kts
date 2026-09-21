// A standalone jar has no runtime dependencies at all: neither external libraries nor other MQTJ
// jars. Applied to mqtj-client and mqtj-broker.

plugins {
    id("mqtj.java-library")
}

val runtimeArtifacts = configurations.runtimeClasspath.flatMap {
    it.incoming.artifacts.resolvedArtifacts.map { artifacts ->
        artifacts.map { artifact -> artifact.id.displayName }.sorted()
    }
}
val verifyStandaloneRuntime = tasks.register("verifyStandaloneRuntime") {
    group = "verification"
    description = "Verifies that this single-jar library has no runtime dependencies."
    val found = runtimeArtifacts
    inputs.property("runtimeArtifacts", found)
    doLast {
        check(found.get().isEmpty()) {
            "A standalone library cannot require extra runtime artifacts: ${found.get()}"
        }
    }
}
tasks.check {
    dependsOn(verifyStandaloneRuntime)
}
