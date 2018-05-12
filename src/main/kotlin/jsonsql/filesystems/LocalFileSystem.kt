package jsonsql.filesystems

import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.net.URI
import java.time.Instant

object LocalFileSystem: StreamFileSystem() {
    override fun listDir(path: String): List<Map<String, Any?>> {
        return file(path).walk().filter { it.isFile }.map {
            val filePath = it.absoluteFile.toURI().toString()
            mapOf(
                    "path" to filePath,
                    "name" to it.name,
                    "parent" to it.parent,
                    "extension" to it.extension,
                    "last_modified" to Instant.ofEpochMilli(it.lastModified()),
                    "size" to it.length()
            )
        }.toList()
    }

    override fun read(path: String): Iterator<InputStream> {
        return listDir(path).filter { it["size"] != 0 }.asSequence().map {
            file(it["path"] as String).inputStream()
        }.iterator()
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