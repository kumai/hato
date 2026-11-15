import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction

/**
 * Writes indexes of the compiled classes as resources that the library reads at run time. Each
 * index holds the binary name of every class outside its excluded packages, one per line in sorted
 * order. Module and package descriptors are left out.
 */
@CacheableTask
abstract class WriteClassIndex : DefaultTask() {
    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val classDirectories: ConfigurableFileCollection

    /** The resource directory of the indexes, for example `org/mqtt4j/hato/internal/concurrent`. */
    @get:Input
    abstract val resourceDirectory: Property<String>

    /**
     * The file name of each index, and the package prefixes whose classes it leaves out, for
     * example `org.mqtt4j.hato.broker.`.
     */
    @get:Input
    abstract val indexes: MapProperty<String, List<String>>

    @get:OutputDirectory
    abstract val outputDirectory: DirectoryProperty

    @TaskAction
    fun write() {
        val names = sortedSetOf<String>()
        classDirectories.asFileTree.visit {
            if (!isDirectory && name.endsWith(".class") && name != "module-info.class" &&
                name != "package-info.class"
            ) {
                names.add(relativePath.pathString.removeSuffix(".class").replace('/', '.'))
            }
        }
        val root = outputDirectory.get().asFile
        root.deleteRecursively()
        val directory = root.resolve(resourceDirectory.get())
        directory.mkdirs()
        for ((file, excluded) in indexes.get()) {
            val included = names.filter { name -> excluded.none { name.startsWith(it) } }
            directory.resolve(file).writeText(included.joinToString("") { "$it\n" })
        }
    }
}
