// Conventions for the published library modules: mqtj-client, mqtj-broker, and mqtj-testkit.

import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.language.base.plugins.LifecycleBasePlugin

plugins {
    id("mqtj.java-conventions")
    `java-library`
    id("mqtj.publishing")
}

java {
    withSourcesJar()
    withJavadocJar()
}

for (taskName in listOf("jar", "sourcesJar")) {
    tasks.named<Jar>(taskName) {
        from(layout.settingsDirectory.file("LICENSE")) {
            into("META-INF")
        }
    }
}

tasks.named<Javadoc>("javadoc") {
    exclude("**/internal/**")
    // Public signatures may refer to internal supertypes. Resolve those sources without
    // including their pages in the published API documentation.
    inputs.files(sourceSets.main.map { it.allJava })
        .withPropertyName("resolutionSources")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    (options as StandardJavadocDocletOptions).addStringOption(
        "-source-path",
        sourceSets.main.get().java.sourceDirectories.asPath
    )
}

tasks.named<Jar>("jar") {
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
        )
    }
}

// The published modules depend on the JDK and on each other only. This check fails the build when
// anything else appears on the runtime classpath, so a dependency cannot slip in unnoticed.
val externalRuntimeDependencies = configurations.runtimeClasspath.flatMap { classpath ->
    classpath.incoming.artifacts.resolvedArtifacts.map { artifacts ->
        artifacts.map { it.id.componentIdentifier }
            .filterIsInstance<ModuleComponentIdentifier>()
            .map { it.displayName }
            .sorted()
    }
}

val verifyNoExternalRuntimeDependencies = tasks.register("verifyNoExternalRuntimeDependencies") {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Fails if the runtime classpath contains anything other than MQTJ's own modules."
    val moduleName = project.name
    val found = externalRuntimeDependencies
    inputs.property("externalRuntimeDependencies", found)
    doLast {
        if (found.get().isNotEmpty()) {
            throw GradleException(
                "$moduleName must not have runtime dependencies, but its runtime classpath contains: ${found.get()}"
            )
        }
    }
}

tasks.named("check") {
    dependsOn(verifyNoExternalRuntimeDependencies)
}
