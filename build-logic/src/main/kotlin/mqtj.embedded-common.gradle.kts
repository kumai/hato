// Embeds mqtj-common under this project's internal package by default. A consumer may map
// reusable value sources into its public API package.
// A source dependency is not a Java dependency: it must never appear in the POM, in the Gradle
// runtime variants, or in the module-info.class of a published library.

plugins {
    id("mqtj.java-library")
}

val embeddedCommon = configurations.dependencyScope("embeddedCommon")
val embeddedCommonSources = configurations.resolvable("embeddedCommonSources") {
    extendsFrom(embeddedCommon.get())
}
dependencies {
    add(embeddedCommon.name, project(path = ":mqtj-common", configuration = "commonSourcesElements"))
}

val embedCommonSources = tasks.register<EmbedCommonSources>("embedCommonSources") {
    sourceArchives.from(embeddedCommonSources)
    sourcePackage = "org.mqtt4j.mqtj.common"
    destinationPackage = "org.mqtt4j.mqtj.${project.name.removePrefix("mqtj-")}.internal.common"
    outputDirectory = layout.buildDirectory.dir("generated/sources/embeddedCommon/java/main")
}

sourceSets.main {
    java.srcDir(embedCommonSources.flatMap { it.outputDirectory })
}

// The embedded copies are checked at their source in mqtj-common. Checking them again here would
// report findings in generated files, and the longer package names can push a line over the limit.
tasks.named<Checkstyle>("checkstyleMain") {
    setSource(fileTree("src/main/java") { include("**/*.java") })
}

// The project's own internal code lives outside the embedded packages. Public value packages
// may be explicitly allowed to share a package with the consumer's public API. src/main/java is named
// explicitly because the main source set also contains the generated directory.
val verifyEmbeddedPackagesAreDisjoint = tasks.register<VerifyDisjointPackages>("verifyEmbeddedPackagesAreDisjoint") {
    embeddedSources = embedCommonSources.flatMap { it.outputDirectory }
    ownSources.from(fileTree("src/main/java") { include("**/*.java") })
}
tasks.check {
    dependsOn(verifyEmbeddedPackagesAreDisjoint)
}
