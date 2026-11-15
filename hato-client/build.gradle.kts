plugins {
    id("hato.java-library")
}

// Published in every version's POM, which cannot change after release: describe no planned feature.
description = "The client coordinates of Hato before the client and the broker became one jar. " +
    "Holds no classes and depends on org.mqtt4j:hato."

dependencies {
    api(project(":hato"))
}
