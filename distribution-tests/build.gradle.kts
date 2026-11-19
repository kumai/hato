plugins {
    id("hato.java-conventions")
}

description = "Tests the final library jars and their publication metadata. Not published."

val hatoJarDependencies = configurations.dependencyScope("hatoJarDependencies")
val hatoJar = configurations.resolvable("hatoJar") {
    extendsFrom(hatoJarDependencies.get())
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

dependencies {
    add(hatoJarDependencies.name, project(path = ":hato", configuration = "runtimeElements"))
    add(testkitJarsDependencies.name, project(path = ":hato-testkit", configuration = "runtimeElements"))
    for (module in listOf("hato", "hato-testkit")) {
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
}

val distributionArguments = objects.newInstance<DistributionTestArguments>().apply {
    hatoJar.from(configurations["hatoJar"])
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
    description = "Bundles the Maven publications for a non-SNAPSHOT release."
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
