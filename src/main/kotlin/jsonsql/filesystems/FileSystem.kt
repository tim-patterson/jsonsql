package jsonsql.filesystems

import java.io.InputStream
import java.net.URI

interface FileSystem {
    fun listDir(path: String): List<String>
    fun open(path: String): InputStream

    companion object {
        fun listDir(path: String): List<String> {
            return fileSystem(path).listDir(path)
        }

        fun open(path: String): InputStream {
            return fileSystem(path).open(path)
        }

        private fun fileSystem(path: String): FileSystem {
            val scheme = URI.create(path).scheme
            return when(scheme) {
                null -> LocalFileSystem
                "file" -> LocalFileSystem
                "s3" -> S3FileSystem
                "http" -> HttpFileSystem
                "https" -> HttpFileSystem
                else -> TODO("Unknown filesystem $scheme")
            }
        }
    }
}
