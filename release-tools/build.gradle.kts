plugins {
    id("hato.java-conventions")
    application
}

description = "Release checks and the Maven Central upload for the release workflow. Not published."

application {
    mainClass = "org.mqtt4j.hato.release.ReleaseTool"
}

// The first job of the release workflow. The tool reads the GitHub ref from the environment and
// writes the version to publish to GITHUB_OUTPUT.
tasks.register<JavaExec>("checkReleaseTag") {
    group = "publishing"
    description = "Checks that the run's tag names a version that can be published."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass = application.mainClass
    args("version", layout.settingsDirectory.file("gradle.properties").asFile.path)
    notCompatibleWithConfigurationCache("Reads the GitHub Actions environment of each run")
}

// The last job of the release workflow. The tool reads the released version from the
// environment, moves gradle.properties on if that release calls for it, and writes the new
// version to GITHUB_OUTPUT.
tasks.register<JavaExec>("moveToNextVersion") {
    group = "publishing"
    description = "Moves gradle.properties to the next minor SNAPSHOT after a release of its version."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass = application.mainClass
    args("next", layout.settingsDirectory.file("gradle.properties").asFile.path)
    notCompatibleWithConfigurationCache("Reads the GitHub Actions environment of each run")
}
