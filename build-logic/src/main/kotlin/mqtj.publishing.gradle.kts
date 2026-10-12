// Publishing conventions: the Maven publication, its POM, and optional signing.
// The upload to Maven Central (Central Publisher Portal) is added when the first release is near.

plugins {
    id("mqtj.java-conventions")
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
                url = "https://github.com/kumai/mqtj"
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
                    url = "https://github.com/kumai/mqtj"
                    connection = "scm:git:https://github.com/kumai/mqtj.git"
                    developerConnection = "scm:git:ssh://git@github.com/kumai/mqtj.git"
                }
            }
        }
    }
}

signing {
    // Signing is needed only for releases. The key comes from the environment, so nothing is stored here.
    val signingKey = providers.environmentVariable("MQTJ_SIGNING_KEY")
    val signingPassword = providers.environmentVariable("MQTJ_SIGNING_PASSWORD")
    isRequired = signingKey.isPresent
    if (signingKey.isPresent) {
        useInMemoryPgpKeys(signingKey.get(), signingPassword.orNull)
        sign(publishing.publications)
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
