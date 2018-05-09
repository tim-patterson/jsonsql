package jsonsql.filesystems

import java.io.InputStream
import java.io.OutputStream
import java.net.URI

interface FileSystem {
    fun listDir(path: String): List<String>
    fun read(path: String): InputStream
    fun write(path: String): OutputStream

    companion object {
        fun listDir(path: String) = fileSystem(path).listDir(path)
        fun read(path: String) = fileSystem(path).read(path)
        fun write(path: String) = fileSystem(path).write(path)

        private fun fileSystem(path: String): FileSystem {
            val scheme = URI.create(path).scheme
            return when(scheme) {
                null -> LocalFileSystem
                "file" -> LocalFileSystem
                "s3" -> S3FileSystem
                "http" -> HttpFileSystem
                "https" -> HttpFileSystem
                "kafka" -> KafkaFileSystem
                else -> TODO("Unknown filesystem $scheme")
            }
        }
    }
}
