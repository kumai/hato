// A standalone jar has no runtime dependencies at all: neither external libraries nor other Hato
// jars. Applied to hato-client and hato-broker.

plugins {
    id("hato.java-library")
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
