package lambdadb.filesystems

import java.io.File
import java.io.InputStream


object LocalFileSystem: FileSystem {
    override fun listDir(authority: String, path: String): List<String> {
        TODO()
    }

    override fun open(authority: String, path: String): InputStream {
        return File(path).inputStream()
    }
}