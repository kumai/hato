// Conventions shared by every Java project in the build: toolchain, compiler settings, and tests.
// Projects that are not published (examples, native-smoke) apply this directly; the published
// modules get it through mqtj.java-library.

import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone
import org.gradle.api.artifacts.VersionCatalogsExtension
import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    java
    checkstyle
    id("net.ltgt.errorprone")
}

val libs = the<VersionCatalogsExtension>().named("libs")

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

dependencies {
    // JSpecify documents nullness for readers and static analysis. It is compile-time only: the
    // published jars declare no dependency on it, and consumers compile without it (CONTRIBUTING.md).
    compileOnly(libs.findLibrary("jspecify").get())
    // NullAway checks the nullness contract of every @NullMarked package inside javac.
    add("errorprone", libs.findLibrary("errorprone-core").get())
    add("errorprone", libs.findLibrary("nullaway").get())
    // Checkstyle enforces the layout and import rules of docs/conventions/java.md.
    add("checkstyle", libs.findLibrary("checkstyle").get())
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release = 25
    // Every lint category is on and warnings fail the build. Turn a category off here only with a reason.
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
    options.errorprone {
        // Error Prone is the vehicle for NullAway only; its own bug patterns are not enabled.
        disableAllChecks = true
        check("NullAway", CheckSeverity.ERROR)
        // Only @NullMarked code is checked, with JSpecify semantics including generics.
        option("NullAway:OnlyNullMarked", "true")
        option("NullAway:JSpecifyMode", "true")
        option("NullAway:CheckOptionalEmptiness", "true")
        option("NullAway:HandleTestAssertionLibraries", "true")
    }
}

checkstyle {
    // Rules: config/checkstyle/checkstyle.xml. A finding fails the build, as a javac warning does.
    maxWarnings = 0
}

tasks.withType<Javadoc>().configureEach {
    options.encoding = "UTF-8"
    // Javadoc is written in Markdown (JEP 467). Missing comments are not an error; wrong ones are.
    (options as StandardJavadocDocletOptions).addBooleanOption("Xdoclint:all,-missing", true)
}

testing {
    suites {
        withType<JvmTestSuite>().configureEach {
            useJUnitJupiter(libs.findVersion("junit").get().requiredVersion)
            dependencies {
                implementation(libs.findLibrary("assertj-core").get())
                compileOnly(libs.findLibrary("jspecify").get())
            }
            targets.configureEach {
                testTask.configure {
                    testLogging {
                        events("failed")
                        exceptionFormat = TestExceptionFormat.FULL
                    }
                }
            }
        }

        // Tests that need a real broker, Docker, or the network. Everything else is a unit test.
        register<JvmTestSuite>("integrationTest") {
            dependencies {
                implementation(project())
            }
            targets.configureEach {
                testTask.configure {
                    shouldRunAfter(tasks.named("test"))
                }
            }
        }
    }
}

tasks.named("check") {
    dependsOn(testing.suites.named("integrationTest"))
}

// Byte-for-byte reproducible jars.
tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}
