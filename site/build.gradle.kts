// The GitHub Pages site: the landing page in src, and the API documentation of the published
// modules under api/. The release workflow publishes it from the release tag; see
// docs/releasing.md.

plugins {
    base
}

val apiDocumentation = listOf("client", "testkit", "broker").associateWith { module ->
    val declared = configurations.dependencyScope("${module}ApiDocumentation")
    dependencies {
        add(declared.name, project(path = ":hato-$module", configuration = "pagesJavadocElements"))
    }
    configurations.resolvable("${module}ApiDocumentationFiles") {
        extendsFrom(declared.get())
    }
}

val assembleSite = tasks.register<Sync>("assembleSite") {
    description = "Assembles the GitHub Pages site: the landing page and the API documentation."
    group = "documentation"
    from("src")
    for ((module, documentation) in apiDocumentation) {
        from(documentation) {
            into("api/$module")
        }
    }
    into(layout.buildDirectory.dir("site"))
}

// Building the site in every build keeps a broken page or link from waiting for release day.
tasks.assemble {
    dependsOn(assembleSite)
}
