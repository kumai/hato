// Conventions for the published library modules: hato-client, hato-broker, and hato-testkit.

import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.language.base.plugins.LifecycleBasePlugin

plugins {
    id("hato.java-conventions")
    `java-library`
    id("hato.publishing")
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

// The API documentation of the javadoc jar and of the GitHub Pages site.
fun Javadoc.documentPublicApi() {
    exclude("**/internal/**")
    // Javadoc writes its labels in the default locale of the JVM that runs it, so a machine with
    // another language would build different pages from a release tag. -locale cannot fix this:
    // English is the base bundle, and the lookup tries the default locale before it.
    options.jFlags("-Duser.language=en", "-Duser.country=US")
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

tasks.named<Javadoc>("javadoc") {
    documentPublicApi()
}

// GitHub Pages puts the API documentation of the modules side by side under api/, so the site's
// copy may link from one module's pages to another's, while the pages in the javadoc jar stand
// alone. The site project takes this copy through pagesJavadocElements.
val pagesJavadoc = tasks.register<Javadoc>("pagesJavadoc") {
    description = "Generates the API documentation for the GitHub Pages site."
    group = JavaBasePlugin.DOCUMENTATION_GROUP
    val javadoc = tasks.named<Javadoc>("javadoc").get()
    source = javadoc.source
    classpath = javadoc.classpath
    title = javadoc.title
    destinationDir = layout.buildDirectory.dir("docs/pagesJavadoc").get().asFile
    documentPublicApi()
}

configurations.consumable("pagesJavadocElements") {
    outgoing.artifact(pagesJavadoc.map { requireNotNull(it.destinationDir) }) {
        builtBy(pagesJavadoc)
    }
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
    description = "Fails if the runtime classpath contains anything other than Hato's own modules."
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
