package jsonsql.filesystems

import java.io.InputStream
import java.net.URI

interface FileSystem {
    fun listDir(authority: String, path: String): List<String>
    fun open(authority: String, path: String): InputStream

    companion object {
        fun listDir(directory: String): List<String> {
            val uri = URI.create(directory)
            val scheme = uri.scheme ?: "file"

            return fileSystem(directory).listDir(uri.authority.orEmpty(), uri.path).map {
                URI(scheme,"//$it",null).toString()
            }
        }

        fun open(file: String): InputStream {
            val uri = URI.create(file)
            return fileSystem(file).open(uri.authority.orEmpty(), uri.path)
        }

        private fun fileSystem(path: String): FileSystem {
            val scheme = URI.create(path).scheme
            return when(scheme) {
                null -> LocalFileSystem
                "file" -> LocalFileSystem
                "s3" -> S3FileSystem
                else -> TODO("Unknown filesystem $scheme")
            }
        }
    }
}
