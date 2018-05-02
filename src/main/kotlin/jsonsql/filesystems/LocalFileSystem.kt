package jsonsql.filesystems

import java.io.File
import java.io.InputStream
import java.net.URI

object LocalFileSystem: FileSystem {
    override fun listDir(path: String): List<String> {
        return file(path).walk().filter { it.isFile }.map { it.absoluteFile.toURI().toString() }.toList()
    }

    override fun open(path: String): InputStream {
        return file(path).inputStream()
    }

    private fun file(path: String): File {
        return if(path.startsWith("file:")) {
            File(URI.create(path))
        } else {
            File(path)
        }
    }
}