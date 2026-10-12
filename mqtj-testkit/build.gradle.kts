plugins {
    id("mqtj.embedded-common")
}

// Published in every version's POM, which cannot change after release: describe no planned feature.
description = "Test support for applications that use MQTJ: fault injection and a manual timer scheduler."

dependencies {
    api(project(":mqtj-client"))
    api(project(":mqtj-broker"))
}
