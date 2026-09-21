import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.SetProperty
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import java.io.File

/**
 * Fails when a package of the embedded shared sources is also declared by the consumer's own
 * sources, except for explicitly allowed public API packages. Internal implementation packages
 * must not silently mix generated and hand-written code.
 */
abstract class VerifyDisjointPackages : DefaultTask() {
    /** Public API packages may combine generated shared values with distribution-specific types. */
    @get:Input
    abstract val allowedSharedPackages: SetProperty<String>

    init {
        allowedSharedPackages.convention(emptySet())
    }

    @get:InputDirectory
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val embeddedSources: DirectoryProperty

    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val ownSources: ConfigurableFileCollection

    @TaskAction
    fun verify() {
        val embedded = packagesOf(embeddedSources.get().asFile.walkTopDown().filter { it.isFile }.toList())
        val shared = (embedded intersect packagesOf(ownSources.files)) - allowedSharedPackages.get()
        if (shared.isNotEmpty()) {
            throw GradleException(
                "Embedded packages must not also be declared by this project's own sources: ${shared.sorted()}"
            )
        }
    }

    private fun packagesOf(files: Iterable<File>): Set<String> =
        files.filter { it.name.endsWith(".java") }
            .mapNotNull { PACKAGE.find(it.readText())?.groupValues?.get(1) }
            .toSet()

    private companion object {
        val PACKAGE = Regex("""^\s*package\s+([\w.]+)\s*;""", RegexOption.MULTILINE)
    }
}
