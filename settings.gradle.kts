// Root of the Hato build. The root project holds settings and conventions only;
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

rootProject.name = "hato"

include("hato-common")
include("hato-client")
include("hato-broker")
include("hato-testkit")
include("examples")
include("native-smoke")
include("distribution-tests")
include("test-support")
include("release-tools")
include("site")
