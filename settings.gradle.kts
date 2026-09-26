// Root of the MQTJ build. The root project holds settings and conventions only;
// all source code lives in the subprojects listed at the bottom.

pluginManagement {
    // Convention plugins live in an included build, not in buildSrc.
    includeBuild("build-logic")
}

plugins {
    // Downloads the Java 25 toolchain, and the daemon JVM, when they are not installed.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

dependencyResolutionManagement {
    // Repositories are declared once, here. Subprojects must not declare their own.
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
    }
}

rootProject.name = "mqtj"

include("mqtj-common")
include("mqtj-client")
include("mqtj-broker")
include("mqtj-testkit")
include("examples")
include("native-smoke")
include("distribution-tests")
include("test-support")
