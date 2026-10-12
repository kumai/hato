// Publishing conventions: the Maven publication, local staging, and optional signing.

plugins {
    id("hato.java-conventions")
    `maven-publish`
    signing
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name = project.name
                description = provider { project.description }
                url = "https://github.com/kumai/hato"
                licenses {
                    license {
                        name = "MIT License"
                        url = "https://opensource.org/license/mit"
                    }
                }
                developers {
                    developer {
                        id = "kumai"
                        name = "Ryuichi Kumai"
                        url = "https://github.com/kumai"
                    }
                }
                scm {
                    url = "https://github.com/kumai/hato"
                    connection = "scm:git:https://github.com/kumai/hato.git"
                    developerConnection = "scm:git:ssh://git@github.com/kumai/hato.git"
                }
            }
        }
    }
    repositories {
        maven {
            name = "staging"
            url = uri(layout.buildDirectory.dir("staging-repository"))
        }
    }
}

signing {
    // Signing is needed only for releases. The key comes from the environment, so nothing is stored here.
    val signingKey = providers.environmentVariable("HATO_SIGNING_KEY")
    val signingPassword = providers.environmentVariable("HATO_SIGNING_PASSWORD")
    isRequired = signingKey.isPresent
    if (signingKey.isPresent) {
        useInMemoryPgpKeys(signingKey.get(), signingPassword.orNull)
        sign(publishing.publications)
    }
}

val stagingRepository = layout.buildDirectory.dir("staging-repository")
val verifyReleaseVersion = tasks.register("verifyReleaseVersion") {
    val releaseVersion = project.version.toString()
    doLast {
        // The same rule as the release workflow's version check.
        if (releaseVersion.contains("SNAPSHOT", ignoreCase = true)) {
            throw GradleException("Release staging requires -Pversion=<non-SNAPSHOT version>")
        }
    }
}
val prepareStagingRepository = tasks.register<Delete>("prepareStagingRepository") {
    dependsOn(verifyReleaseVersion)
    // A previous signed build must not leave signatures behind in an unsigned local bundle.
    delete(stagingRepository)
}
val stagePublication = tasks.named("publishMavenPublicationToStagingRepository") {
    dependsOn(prepareStagingRepository)
}
val stagedPublicationElements = configurations.consumable("stagedPublicationElements")
artifacts {
    add(stagedPublicationElements.name, stagingRepository) {
        builtBy(stagePublication)
    }
}

// Expose generated publication files to distribution tests without publishing anywhere.
val publicationMetadataElements = configurations.consumable("publicationMetadataElements")
val generatedPom = tasks.named<GenerateMavenPom>("generatePomFileForMavenPublication")
val generatedModuleMetadata = tasks.named<GenerateModuleMetadata>("generateMetadataFileForMavenPublication")
artifacts {
    add(publicationMetadataElements.name, generatedPom.map { it.destination }) {
        builtBy(generatedPom)
    }
    add(publicationMetadataElements.name, generatedModuleMetadata.flatMap { it.outputFile }) {
        builtBy(generatedModuleMetadata)
    }
}
