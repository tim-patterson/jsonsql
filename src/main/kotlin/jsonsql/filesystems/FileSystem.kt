package jsonsql.filesystems

import java.io.InputStream
import java.io.OutputStream
import java.net.URI

sealed class FileSystem {
    // String to list of file attributes
    // each must contain a "path" attribute
    abstract fun listDir(path: String): List<Map<String, Any?>>

    companion object {
        fun listDir(path: String) = from(path).listDir(path)

        fun from(path: String): FileSystem {
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

abstract class StreamFileSystem: FileSystem() {
    abstract fun read(path: String): Iterator<InputStream>
    abstract fun write(path: String): OutputStream
}

abstract class EventFileSystem: FileSystem() {
    abstract fun read(path: String, terminating: Boolean = true): EventReader
    abstract fun write(path: String): EventWriter

    interface EventReader : AutoCloseable {
        fun next(): ByteArray?
    }

    interface EventWriter : AutoCloseable {
        fun write(event: ByteArray)
    }
}

