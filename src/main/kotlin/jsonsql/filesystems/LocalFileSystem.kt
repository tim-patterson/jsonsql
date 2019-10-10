package jsonsql.filesystems

import jsonsql.functions.StringInspector
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.net.URI
import java.time.Instant

object LocalFileSystem: StreamFileSystem() {
    override fun listDir(path: String): Sequence<Map<String, Any?>> {
        return file(path).walk().filter { it.isFile }.map {
            val filePath = it.absoluteFile.toURI().toString()
            mapOf(
                    "path" to filePath,
                    "name" to it.name,
                    "parent" to it.parent,
                    "extension" to it.extension,
                    "last_modified" to StringInspector.inspect(Instant.ofEpochMilli(it.lastModified())),
                    "size" to it.length()
            )
        }
    }

    override fun readSingle(file: Map<String, Any?>): InputStream {
        return file(file["path"] as String).inputStream()
    }

    override fun write(path: String): OutputStream {
        val file = file(path)
        file.parentFile.mkdirs()
        return file.outputStream()
    }

    private fun file(path: String): File {
        return if(path.startsWith("file:")) {
            File(URI.create(path))
        } else {
            File(path)
        }
    }
}