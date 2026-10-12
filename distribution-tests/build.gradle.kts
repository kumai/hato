plugins {
    id("mqtj.java-conventions")
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
val publicationMetadataDependencies = configurations.dependencyScope("publicationMetadataDependencies")
val publicationMetadata = configurations.resolvable("publicationMetadata") {
    extendsFrom(publicationMetadataDependencies.get())
}

dependencies {
    add(clientJarDependencies.name, project(path = ":mqtj-client", configuration = "runtimeElements"))
    add(brokerJarDependencies.name, project(path = ":mqtj-broker", configuration = "runtimeElements"))
    add(testkitJarsDependencies.name, project(path = ":mqtj-testkit", configuration = "runtimeElements"))
    for (module in listOf("mqtj-client", "mqtj-broker", "mqtj-testkit")) {
        add(sourceJarsDependencies.name, project(path = ":$module", configuration = "sourcesElements"))
        add(javadocJarsDependencies.name, project(path = ":$module", configuration = "javadocElements"))
        add(publicationMetadataDependencies.name,
            project(path = ":$module", configuration = "publicationMetadataElements"))
    }
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
