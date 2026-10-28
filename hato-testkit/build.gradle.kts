plugins {
    id("hato.embedded-common")
}

// Published in every version's POM, which cannot change after release: describe no planned feature.
description = "Test support for applications that use Hato: stand-ins for the publishing and subscribing roles, fault injection, a manual timer scheduler, and controls for the embedded broker."

dependencies {
    api(project(":hato-client"))
    api(project(":hato-broker"))
}

// On GitHub Pages, the client's and the broker's API documentation sit next to the test kit's,
// under api/client/ and api/broker/, so the site's copy links to their pages for the types that
// the test kit refers to.
tasks.named<Javadoc>("pagesJavadoc") {
    for (module in listOf("client", "broker")) {
        val documentation = layout.settingsDirectory.dir("hato-$module/build/docs/pagesJavadoc")
        dependsOn(":hato-$module:pagesJavadoc")
        inputs.file(documentation.file("element-list"))
            .withPropertyName("${module}ElementList")
            .withPathSensitivity(PathSensitivity.NONE)
        (options as StandardJavadocDocletOptions).linksOffline(
            "../$module/",
            documentation.asFile.path
        )
    }
}

testing {
    suites {
        named<JvmTestSuite>("integrationTest") {
            dependencies {
                implementation(libs.awaitility)
            }
        }
    }
}
