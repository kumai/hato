plugins {
    id("hato.embedded-common")
}

// Published in every version's POM, which cannot change after release: describe no planned feature.
description = "Test support for applications that use Hato: fault injection and a manual timer scheduler."

dependencies {
    api(project(":hato-client"))
    api(project(":hato-broker"))
}
