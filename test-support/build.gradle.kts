plugins {
    id("mqtj.java-conventions")
    `java-library`
}

description = "Private broker fixtures for the repository's integration tests."

dependencies {
    api(libs.testcontainers)
}
