plugins {
    id("hato.embedded-common")
}

// Published in every version's POM, which cannot change after release: describe no planned feature.
description = "Test support for applications that use Hato: fault injection, a manual timer scheduler, and controls for the embedded broker."

dependencies {
    api(project(":hato-client"))
    api(project(":hato-broker"))
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
