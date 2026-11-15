plugins {
    id("hato.java-library")
}

// Published in every version's POM, which cannot change after release: describe no planned feature.
description = "Test support for applications that use Hato: stand-ins for the publishing and subscribing roles, fault injection, a manual timer scheduler, and controls for the embedded broker."

dependencies {
    api(project(":hato"))
}

// On GitHub Pages, the API documentation of Hato sits next to the test kit's, under api/hato/, so
// the site's copy links to its pages for the types that the test kit refers to.
tasks.named<Javadoc>("pagesJavadoc") {
    val documentation = layout.settingsDirectory.dir("hato/build/docs/pagesJavadoc")
    dependsOn(":hato:pagesJavadoc")
    inputs.file(documentation.file("element-list"))
        .withPropertyName("hatoElementList")
        .withPathSensitivity(PathSensitivity.NONE)
    (options as StandardJavadocDocletOptions).linksOffline("../hato/", documentation.asFile.path)
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
