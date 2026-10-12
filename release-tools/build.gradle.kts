plugins {
    id("mqtj.java-conventions")
    application
}

description = "Release checks and the Maven Central upload for the release workflow. Not published."

application {
    mainClass = "org.mqtt4j.mqtj.release.ReleaseTool"
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
