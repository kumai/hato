plugins {
    id("hato.java-conventions")
}

description = "Tests the final library jars and their publication metadata. Not published."

val clientJarDependencies = configurations.dependencyScope("clientJarDependencies")
val clientJar = configurations.resolvable("clientJar") {
    extendsFrom(clientJarDependencies.get())
}
val brokerJarDependencies = configurations.dependencyScope("brokerJarDependencies")
val brokerJar = configurations.resolvable("brokerJar") {
    extendsFrom(brokerJarDependencies.get())
}
val testkitJarsDependencies = configurations.dependencyScope("testkitJarsDependencies")
val testkitJars = configurations.resolvable("testkitJars") {
    extendsFrom(testkitJarsDependencies.get())
}
val sourceJarsDependencies = configurations.dependencyScope("sourceJarsDependencies")
val sourceJars = configurations.resolvable("sourceJars") {
    extendsFrom(sourceJarsDependencies.get())
}
val javadocJarsDependencies = configurations.dependencyScope("javadocJarsDependencies")
val javadocJars = configurations.resolvable("javadocJars") {
    extendsFrom(javadocJarsDependencies.get())
}
val stagedPublicationsDependencies = configurations.dependencyScope("stagedPublicationsDependencies")
val stagedPublications = configurations.resolvable("stagedPublications") {
    extendsFrom(stagedPublicationsDependencies.get())
}
val publicationMetadataDependencies = configurations.dependencyScope("publicationMetadataDependencies")
val publicationMetadata = configurations.resolvable("publicationMetadata") {
    extendsFrom(publicationMetadataDependencies.get())
}
val releaseToolsDependencies = configurations.dependencyScope("releaseToolsDependencies")
val releaseTools = configurations.resolvable("releaseTools") {
    extendsFrom(releaseToolsDependencies.get())
}
val releasedJarsDependencies = configurations.dependencyScope("releasedJarsDependencies")
val releasedJars = configurations.resolvable("releasedJars") {
    extendsFrom(releasedJarsDependencies.get())
}
val japicmpDependencies = configurations.dependencyScope("japicmpDependencies")
val japicmp = configurations.resolvable("japicmp") {
    extendsFrom(japicmpDependencies.get())
    // The jar with dependencies runs on its own.
    isTransitive = false
}

dependencies {
    add(clientJarDependencies.name, project(path = ":hato-client", configuration = "runtimeElements"))
    add(brokerJarDependencies.name, project(path = ":hato-broker", configuration = "runtimeElements"))
    add(testkitJarsDependencies.name, project(path = ":hato-testkit", configuration = "runtimeElements"))
    for (module in listOf("hato-client", "hato-broker", "hato-testkit")) {
        add(sourceJarsDependencies.name, project(path = ":$module", configuration = "sourcesElements"))
        add(javadocJarsDependencies.name, project(path = ":$module", configuration = "javadocElements"))
        add(
            stagedPublicationsDependencies.name,
            project(path = ":$module", configuration = "stagedPublicationElements")
        )
        add(publicationMetadataDependencies.name,
            project(path = ":$module", configuration = "publicationMetadataElements"))
    }
    add(
        releaseToolsDependencies.name,
        project(path = ":release-tools", configuration = "runtimeElements")
    )
    // The test kit of the last release brings the client and broker of the same release.
    add(releasedJarsDependencies.name, libs.hato.release.testkit)
    add(japicmpDependencies.name, variantOf(libs.japicmp) { classifier("jar-with-dependencies") })
}

val distributionArguments = objects.newInstance<DistributionTestArguments>().apply {
    clientJar.from(configurations["clientJar"])
    brokerJar.from(configurations["brokerJar"])
    testkitJars.from(configurations["testkitJars"])
    sourceJars.from(configurations["sourceJars"])
    javadocJars.from(configurations["javadocJars"])
    repositoryLicense.from(layout.settingsDirectory.file("LICENSE"))
    publicationMetadata.from(configurations["publicationMetadata"])
}

tasks.test {
    jvmArgumentProviders.add(distributionArguments)
}

val releaseBundle = tasks.register<Zip>("releaseBundle") {
    group = "publishing"
    description = "Bundles the three Maven publications for a non-SNAPSHOT release."
    val releaseVersion = project.version.toString()
    archiveFileName = "hato-$releaseVersion.zip"
    destinationDirectory = layout.buildDirectory.dir("distributions")
    from(stagedPublications) {
        // Central accepts version contents, not repository-level maven-metadata files.
        include("org/mqtt4j/*/$releaseVersion/**")
    }
}

// The release workflow runs the same check on the signed bundle before uploading it.
tasks.register<JavaExec>("verifyReleaseBundle") {
    group = "publishing"
    description = "Checks that the release bundle holds every signed file that Central requires."
    val releaseVersion = project.version.toString()
    val bundle = releaseBundle.flatMap { it.archiveFile }
    inputs.file(bundle)
    classpath = files(releaseTools)
    mainClass = "org.mqtt4j.hato.release.ReleaseTool"
    argumentProviders.add(CommandLineArgumentProvider {
        listOf("bundle", releaseVersion, bundle.get().asFile.absolutePath)
    })
}

// A report, not a gate: nothing depends on it, so it runs only when asked for. It compares only
// the API packages, which leaves the broker out.
tasks.register<JavaExec>("reportApiChanges") {
    group = "reporting"
    description = "Reports how the API of the client and the test kit differs from the last release."
    val released = files(releasedJars)
    val built = files(testkitJars)
    val htmlReport = layout.buildDirectory.file("reports/api-changes.html")
    // Outside the compatibility promise, as the README says.
    val experimental = listOf(
        "FaultyTransport",
        "BrokerCapabilities",
        "BrokerControls",
        "ResponseKind",
        "HeldResponse",
    )
    inputs.files(released, built)
    classpath = files(japicmp)
    mainClass = "japicmp.JApiCmp"
    // japicmp writes the file but does not create its directory.
    doFirst {
        htmlReport.get().asFile.parentFile.mkdirs()
    }
    argumentProviders.add(CommandLineArgumentProvider {
        listOf(
            "--old", released.joinToString(";") { it.absolutePath },
            "--new", built.joinToString(";") { it.absolutePath },
            "--include", "org.mqtt4j.hato;org.mqtt4j.hato.testkit",
            "--include-exclusively",
            "--exclude", experimental.joinToString(";") { "org.mqtt4j.hato.testkit.$it" },
            "--only-modified",
            "--markdown",
            "--html-file", htmlReport.get().asFile.absolutePath,
            "--report-only-filename",
            "--reproducible-report",
        )
    })
}
